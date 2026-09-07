"""Live civic extractor: ingest national Irish planning applications -> silver parquet.

Implements the LOCKED plan in doc/archive/PLANNING_PERMISSION_SCOPING.md §8:
  ArcGIS REST FeatureServer Layer 0 points and Layer 1 published application-site polygons
  -> decision_normalised + application_type_normalised (raw preserved, no-inference)
  -> DQ guards (future dates, FloorArea/AreaofSite sentinels, one-off reconcile,
     Ireland-bbox geo guard, row-count assertion)
  -> parquet via services.parquet_io.save_parquet (atomic, zstd, statistics).

Layer 1 is retained separately as ``planning_application_sites.parquet``. It is public
planning-register geometry, not proof of a submitted red-line, parcel identity, ownership or
legal interest. Keeping it allows a review workflow to propose measured spatial relationships
instead of guessing from point proximity.

For a bounded smoke test, pull one council first:
    python planning/civic/extractors/planning_applications_ingest.py --authority "Carlow County Council"
Full national sweep (~248 pages / ~495k rows):
    python planning/civic/extractors/planning_applications_ingest.py

Gotchas baked in (project_planning_arcgis_validation / reference_geometry_validation_sources):
  - ITMEasting/ITMNorthing attribute columns are EMPTY -> coords come from geometry only.
  - Applicant* identity columns are empty at source -> dropped anyway (privacy-first).
  - Out-of-bounds coords aren't fixed by make_valid -> detect + quarantine (geo_in_bounds flag).
"""

# Runtime import intentionally precedes Polars; native thread caps are load-order sensitive.
# ruff: noqa: I001

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import logging
import re
import tempfile
from pathlib import Path

# Keep runtime caps ahead of Polars/Shapely native imports.
import services.runtime_env as _runtime_env  # noqa: F401

import polars as pl
import shapely
from shapely.errors import ShapelyError
from shapely.geometry import shape

from planning.civic.acquisition import AcquisitionError, ArcGISStagedCollector
from services.coverage_io import save_coverage
from services.extract_runner import run_extractor
from services.geometry import polygonal_geometries
from services.http_engine import fetch_json
from services.logging_setup import setup_standalone_logging

LOG = logging.getLogger("planning_applications_ingest")

L0 = "https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/IrishPlanningApplications/FeatureServer/0"
L1 = "https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/IrishPlanningApplications/FeatureServer/1"
PAGE = 2000
OUT = Path(__file__).resolve().parents[3] / "data/silver/parquet"
OUT_META = Path(__file__).resolve().parents[3] / "data/_meta"
IRELAND_BBOX = (-11.0, 51.0, -5.0, 56.0)  # (min_lon, min_lat, max_lon, max_lat)

# Columns to DROP: ArcGIS internals, empty ITM attrs (coords come from geometry),
# and the applicant-identity PII columns (empty at source; dropped regardless).
DROP_COLS = {
    "OBJECTID",
    "ORIG_FID",
    "ITMEasting",
    "ITMNorthing",
    "ApplicantForename",
    "ApplicantSurname",
    "ApplicantAddress",
}
# Epoch-millisecond date columns (ArcGIS returns dates as ms UTC).
# Split by direction: PAST events can't legitimately be in the future (ceiling =
# next year); FORWARD-looking dates (due/expiry/appeal-submitted) legitimately can,
# so only absurd values (2260-style garbage) are culled with a generous ceiling.
PAST_DATE_COLS = [
    "ReceivedDate",
    "WithdrawnDate",
    "DecisionDate",
    "GrantDate",
    "AppealDecisionDate",
    "FIRequestDate",
    "FIRecDate",
    "ETL_DATE",
]
FWD_DATE_COLS = ["DecisionDueDate", "ExpiryDate", "AppealSubmittedDate"]
DATE_COLS = PAST_DATE_COLS + FWD_DATE_COLS
FLOOR_YEAR = 1963  # the modern planning system starts with the 1963 Planning Act

_HEADERS = {"User-Agent": "dail-tracker-planning-ingest/1.0"}

SITE_FIELDS = (
    "PlanningAuthority",
    "ApplicationNumber",
    "DevelopmentDescription",
    "DevelopmentAddress",
    "ApplicationStatus",
    "ApplicationType",
    "Decision",
    "AreaofSite",
    "ReceivedDate",
    "DecisionDate",
    "AppealRefNumber",
    "AppealDecision",
    "AppealDecisionDate",
    "LinkAppDetails",
    "ETL_DATE",
    "SiteId",
)
SITE_DATE_COLS = ("ReceivedDate", "DecisionDate", "AppealDecisionDate", "ETL_DATE")


def _query(layer_url: str = L0, **params) -> dict:
    params.setdefault("f", "json")
    response, _ = fetch_json(layer_url + "/query", params=params, headers=_HEADERS, timeout=120)
    return response


def _arcgis_request(url: str, params: dict) -> dict:
    """Injected collector seam: metadata URLs and /query URLs are both exact."""
    response, _ = fetch_json(url, params=params, headers=_HEADERS, timeout=120)
    return response


def fetch(where: str, max_pages: int | None) -> list[dict]:
    """Paginated geometry pull. Returns list of {attributes..., lon, lat}."""
    rows: list[dict] = []
    offset, page_no = 0, 0
    while True:
        resp = _query(
            where=where,
            outFields="*",
            returnGeometry="true",
            outSR="4326",
            resultOffset=offset,
            resultRecordCount=PAGE,
            orderByFields="OBJECTID",
        )
        feats = resp.get("features", [])
        if not feats:
            break
        for f in feats:
            attrs = dict(f.get("attributes") or {})
            geom = f.get("geometry") or {}
            attrs["lon"] = geom.get("x")
            attrs["lat"] = geom.get("y")
            rows.append(attrs)
        page_no += 1
        LOG.info("page %d: +%d rows (total %d)", page_no, len(feats), len(rows))
        if not resp.get("exceededTransferLimit") and len(feats) < PAGE:
            break
        offset += PAGE
        if max_pages and page_no >= max_pages:
            LOG.info("stopping at max_pages=%d", max_pages)
            break
    return rows


# What `shapely.geometry.shape()` ACTUALLY raises on a malformed GeoJSON geometry. Until
# 2026-08-29 this was `(TypeError, ValueError)`, which catches only ONE of the four shapes below —
# so a single malformed feature aborted the whole national ingest instead of being counted
# `unreadable` and skipped, which is what the reason code exists for. Verified against the
# installed shapely: GeometryTypeError subclasses ShapelyError, NOT ValueError.
#   {"type": "NotAThing", ...}        -> GeometryTypeError   (was uncaught)
#   {"type": "Polygon"}               -> KeyError            (was uncaught, no "coordinates")
#   {"coordinates": [...]}            -> AttributeError      (was uncaught, no "type")
#   {"type": "Polygon", "coords": "x"} -> ValueError          (the only one caught before)
# Deliberately an explicit tuple rather than a bare `except Exception`: a MemoryError or a
# KeyboardInterrupt mid-fetch must still stop the run, not be recorded as a bad polygon.
_UNREADABLE_GEOMETRY = (TypeError, ValueError, KeyError, AttributeError, ShapelyError)


def _polygonal_geometry(value: dict | None):
    """Return valid polygonal WGS84 geometry or ``None``; never promote other geometry types."""
    if not value:
        return None, "empty"
    try:
        geometry = shape(value)
    except _UNREADABLE_GEOMETRY:
        return None, "unreadable"
    if geometry.is_empty:
        return None, "empty"
    repaired = False
    if not geometry.is_valid:
        geometry = shapely.make_valid(geometry)
        repaired = True
    if geometry.geom_type == "GeometryCollection":
        polygonal = [
            member
            for member in geometry.geoms
            if member.geom_type in {"Polygon", "MultiPolygon"} and not member.is_empty
        ]
        geometry = shapely.union_all(polygonal) if polygonal else None
    if geometry is None or geometry.is_empty or geometry.geom_type not in {"Polygon", "MultiPolygon"}:
        return None, "not_polygonal"
    minx, miny, maxx, maxy = geometry.bounds
    ireland = IRELAND_BBOX
    if not (ireland[0] <= minx <= maxx <= ireland[2] and ireland[1] <= miny <= maxy <= ireland[3]):
        return None, "bounds_escape"
    return geometry, "repaired" if repaired else "ok"


def fetch_sites(where: str, max_pages: int | None) -> tuple[list[dict], dict[str, int]]:
    """Pull Layer 1 GeoJSON and retain provenance-bearing WKB plus query bboxes."""
    rows: list[dict] = []
    reasons: dict[str, int] = {}
    offset, page_no = 0, 0
    while True:
        response = _query(
            L1,
            where=where,
            outFields=",".join(SITE_FIELDS),
            returnGeometry="true",
            outSR="4326",
            resultOffset=offset,
            resultRecordCount=PAGE,
            orderByFields="OBJECTID",
            f="geojson",
        )
        features = response.get("features", [])
        if not features:
            break
        # One DuckDB pass for the whole page rather than one shapely call per feature — see
        # services/geometry.py. The reason histogram below is byte-identical to the per-feature
        # version's (pinned by tools/geometry_differential.py); it is provenance, written to the
        # coverage JSON, so a change in it is a regression, not a detail.
        parsed = polygonal_geometries([feature.get("geometry") for feature in features], ireland_bbox=IRELAND_BBOX)
        for feature, result in zip(features, parsed, strict=True):
            reasons[result.reason] = reasons.get(result.reason, 0) + 1
            if result.wkb is None:
                continue
            properties = feature.get("properties") or {}
            minx, miny, maxx, maxy = result.bounds
            row = {field: properties.get(field) for field in SITE_FIELDS}
            row.update(
                {
                    "wkb": result.wkb,
                    "bbox_minx": minx,
                    "bbox_miny": miny,
                    "bbox_maxx": maxx,
                    "bbox_maxy": maxy,
                    "geometry_repaired": result.reason == "repaired",
                    "source_layer_url": L1,
                    "source_licence": "CC BY 4.0",
                    "source_checked_date": dt.date.today(),
                }
            )
            rows.append(row)
        page_no += 1
        LOG.info("sites page %d: +%d features (%d retained)", page_no, len(features), len(rows))
        if len(features) < PAGE:
            break
        offset += len(features)
        if max_pages and page_no >= max_pages:
            LOG.info("stopping sites at max_pages=%d", max_pages)
            break
    return rows, reasons


def transform_sites(rows: list[dict]) -> pl.DataFrame:
    """Normalise dates while leaving published decisions and descriptions as reported."""
    df = pl.DataFrame(rows, infer_schema_length=None)
    for column in SITE_DATE_COLS:
        if column in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col(column).cast(pl.Int64, strict=False), time_unit="ms").dt.date().alias(column)
            )
    if "AreaofSite" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("AreaofSite") == 0).then(None).otherwise(pl.col("AreaofSite")).alias("AreaofSite")
        )
    return df


def _counts_by_authority(df: pl.DataFrame) -> dict[str, int]:
    if df.is_empty() or "PlanningAuthority" not in df.columns:
        return {}
    return {
        str(row["PlanningAuthority"]): int(row["len"])
        for row in df.group_by("PlanningAuthority").len().iter_rows(named=True)
    }


def _has_any(col: pl.Expr, terms: list[str]) -> pl.Expr:
    """OR of literal (non-regex) substring matches — safe for '(' , '.' , 'F.I.' etc."""
    out: pl.Expr | None = None
    for t in terms:
        c = col.str.contains(t, literal=True)
        out = c if out is None else (out | c)
    assert out is not None
    return out


# Back-compat map: rich decision_category -> the original 7-value decision_normalised
# contract that downstream sandbox scripts depend on (planning_decision_profiles.py uses
# the whitelist {Granted, Granted-Conditional, Refused} for `decided`). The decided
# whitelist values are preserved EXACTLY; everything else folds to the legacy buckets.
_CATEGORY_TO_NORMALISED = {
    "granted": "Granted",
    "granted_conditional": "Granted-Conditional",
    "refused": "Refused",
    "invalid": "Invalid",
    "withdrawn": "Withdrawn",
    "in_progress": "Undecided/None",
    "incomplete": "Undecided/None",
    "no_decision": "Undecided/None",
    "section_5_exemption": "Other",
    "local_authority_development": "Other",
    "split_decision": "Other",
    "court_action": "Other",
    "referral": "Other",
    "compliance": "Other",
    "other": "Other",
}


def _decision_category(decision_col: str, status_col: str) -> pl.Expr:
    """Iris-style ordered classifier: each branch wins first, never overridden.

    PRIMARY pass = the real-world outcome statement in `Decision`. RESIDUAL reducer =
    where `Decision` is blank/N/A, consult the secondary real-world fact `ApplicationStatus`
    (a status, not an inference). NO outcome is ever inferred: a finalised app with no
    decision text stays `no_decision`, never guessed as granted/refused.
    Ordering traps: invalid/withdraw/refuse BEFORE grant ("REFUSE PERMISSION" contains
    PERMISSION); UNCONDITIONAL BEFORE CONDITIONAL (it contains "CONDITIONAL"); Section-5
    and Part-8 BEFORE the generic grant/approve catch ("PART 8 APPROVED" contains APPROVED;
    "DECLARED NOT EXEMPT" contains EXEMPT)."""
    d = pl.col(decision_col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    s = pl.col(status_col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    d_blank = d.is_null() | (d == "") | (d == "N/A")
    return (
        # ---- primary: explicit outcome / process in the Decision text ----
        pl.when(_has_any(d, ["WITHDRAW"]))
        .then(pl.lit("withdrawn"))
        .when(_has_any(d, ["INVALID", "INVA"]))
        .then(pl.lit("invalid"))
        # Section 5 declaration of exemption (a distinct decision dataset, §11.6)
        .when(_has_any(d, ["S5 ", "SECTION 5", "EXEMPT"]))
        .then(pl.lit("section_5_exemption"))
        # Part 8 / s.179A = local-authority own development approval
        .when(_has_any(d, ["PART 8", "S179A", "PROPOSAL TO PROCEED"]))
        .then(pl.lit("local_authority_development"))
        .when(_has_any(d, ["SPLIT DECISION"]))
        .then(pl.lit("split_decision"))
        .when(_has_any(d, ["QUASH", "HIGH COURT"]))
        .then(pl.lit("court_action"))
        .when(_has_any(d, ["REFER", "DETERMINATION", "OTHER BODY", "SECTION 37(5)", "FILE CLOSED"]))
        .then(pl.lit("referral"))
        .when(_has_any(d, ["COMPLIANCE"]))
        .then(pl.lit("compliance"))
        # in-process states masquerading as a "decision" — NOT a final outcome
        .when(
            _has_any(
                d,
                [
                    "ADDITIONAL INFORMATION",
                    "CLARIFICATION",
                    "REQUEST",
                    "EXT OF TIME",
                    "TIME EXTENSION",
                    "EXTENSION OF TIME",
                    "REVISED PUBLIC NOTICE",
                    "REVISED NEWSPAPER NOTICE",
                ],
            )
        )
        .then(pl.lit("in_progress"))
        .when(_has_any(d, ["CANNOT DETERMINE", "CANNOT BE CONSIDERED"]))
        .then(pl.lit("no_decision"))
        .when(_has_any(d, ["REFUS"]))
        .then(pl.lit("refused"))
        .when(_has_any(d, ["UNCONDITION"]))
        .then(pl.lit("granted"))
        .when(_has_any(d, ["CONDITION"]))
        .then(pl.lit("granted_conditional"))
        .when(_has_any(d, ["GRANT", "PERMISSION", "APPROVE"]))
        .then(pl.lit("granted"))
        # ---- residual reducer: Decision blank/N/A -> use ApplicationStatus ----
        .when(d_blank & _has_any(s, ["WITHDRAW"]))
        .then(pl.lit("withdrawn"))
        .when(d_blank & _has_any(s, ["INVALID"]))
        .then(pl.lit("invalid"))
        .when(
            d_blank
            & _has_any(s, ["INCOMPLET", "PRE_VALIDATION", "VALIDATION", "UNREGISTERED", "REGISTRATION", "REGISTERED"])
        )
        .then(pl.lit("incomplete"))
        .when(
            d_blank
            & _has_any(
                s,
                [
                    "FURTHER INFORMATION",
                    "F.I.",
                    "AWAITING",
                    "ASSESSMENT",
                    "NEW APPLICATION",
                    "PLANNERS REPORT",
                    "RECOMMENDATION",
                    "PUBLICATION",
                    "OFFICER ALLOCATION",
                    "35 DAY",
                ],
            )
        )
        .then(pl.lit("in_progress"))
        .when(d_blank & _has_any(s, ["REFERRAL"]))
        .then(pl.lit("referral"))
        # status says decided/closed but the outcome text is absent -> DO NOT infer it
        .when(d_blank & _has_any(s, ["FINALISED", "DECISION", "FINAL GRANT", "APPEAL", "CLOSED"]))
        .then(pl.lit("no_decision"))
        .when(d_blank)
        .then(pl.lit("no_decision"))
        .otherwise(pl.lit("other"))
    )


def _decision_subtype(decision_col: str, status_col: str) -> pl.Expr:
    """Finer detail under decision_category (references the just-computed category)."""
    d = pl.col(decision_col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    s = pl.col(status_col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    cat = pl.col("decision_category")
    return (
        pl.when(cat == "withdrawn")
        .then(
            pl.when(_has_any(d, ["DEEMED"]) | _has_any(s, ["DEEMED"]))
            .then(pl.lit("deemed_withdrawn"))
            .otherwise(pl.lit("withdrawn"))
        )
        .when(cat == "section_5_exemption")
        .then(
            pl.when(_has_any(d, ["NOT EXEMPT"]))
            .then(pl.lit("not_exempt"))
            .when(_has_any(d, ["SPLIT"]))
            .then(pl.lit("split"))
            .when(_has_any(d, ["REQ", "AI"]))
            .then(pl.lit("further_information"))
            .otherwise(pl.lit("exempt"))
        )
        .when(cat == "local_authority_development")
        .then(pl.when(_has_any(d, ["REJECT"])).then(pl.lit("rejected")).otherwise(pl.lit("approved")))
        .when(cat == "compliance")
        .then(pl.when(_has_any(d, ["DISAPPROVE"])).then(pl.lit("disapproved")).otherwise(pl.lit("approved")))
        .when(cat == "in_progress")
        .then(
            pl.when(_has_any(d, ["CLARIFICATION"]))
            .then(pl.lit("clarification"))
            .when(
                _has_any(d, ["ADDITIONAL INFORMATION", "FURTHER INFORMATION"])
                | _has_any(s, ["FURTHER INFORMATION", "F.I."])
            )
            .then(pl.lit("further_information"))
            .when(_has_any(d, ["TIME"]))
            .then(pl.lit("time_extension"))
            .when(_has_any(d, ["NOTICE"]))
            .then(pl.lit("revised_notice"))
            .when(_has_any(s, ["NEW APPLICATION"]))
            .then(pl.lit("new_application"))
            .otherwise(pl.lit("in_assessment"))
        )
        .when(cat == "granted")
        .then(
            pl.when(_has_any(d, ["UNCONDITION"]))
            .then(pl.lit("unconditional"))
            .when(_has_any(d, ["RETENTION"]))
            .then(pl.lit("retention"))
            .otherwise(pl.lit("grant"))
        )
        .when(cat == "granted_conditional")
        .then(pl.lit("conditional"))
        .when(cat == "no_decision")
        .then(
            pl.when(_has_any(d, ["CANNOT"]))
            .then(pl.lit("cannot_determine"))
            .when(d.is_null() | (d == "") | (d == "N/A"))
            .then(pl.lit("outcome_unstated"))
            .otherwise(pl.lit("unknown"))
        )
        .otherwise(cat)  # refused/invalid/split_decision/court_action/referral/incomplete/other
    )


def _norm_apptype(col: str) -> pl.Expr:
    a = pl.col(col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    return (
        pl.when(a.is_null() | (a == ""))
        .then(pl.lit("Unknown"))
        .when(a.str.contains("RETENTION"))
        .then(pl.lit("Retention"))
        .when(a.str.contains("OUTLINE"))
        .then(pl.lit("Outline"))
        .when(a.str.contains("EXTENSION"))
        .then(pl.lit("Extension of Duration"))
        .when(a.str.contains("PERMISSION"))
        .then(pl.lit("Permission"))
        .otherwise(pl.lit("Other"))
    )


def transform(rows: list[dict]) -> pl.DataFrame:
    df = pl.DataFrame(rows, infer_schema_length=None)
    df = df.drop([c for c in DROP_COLS if c in df.columns])

    # Epoch-ms -> Date
    for c in DATE_COLS:
        if c in df.columns:
            df = df.with_columns(
                pl.from_epoch(pl.col(c).cast(pl.Int64, strict=False), time_unit="ms").dt.date().alias(c)
            )

    this_year = dt.date.today().year
    minlon, minlat, maxlon, maxlat = IRELAND_BBOX

    df = df.with_columns(
        _decision_category("Decision", "ApplicationStatus").alias("decision_category"),
        pl.col("Decision").alias("decision_raw"),
        _norm_apptype("ApplicationType").alias("application_type_normalised"),
        # one-off reconcile (two inconsistent source flags)
        (
            (pl.col("OneOffHouse").cast(pl.Utf8).str.to_uppercase() == "Y")
            | (pl.col("OneOffKPI").cast(pl.Utf8).str.to_uppercase().is_in(["YES", "Y"]))
        ).alias("is_one_off_house"),
        # geo guard
        (pl.col("lon").is_between(minlon, maxlon) & pl.col("lat").is_between(minlat, maxlat))
        .fill_null(False)
        .alias("geo_in_bounds"),
    )

    # second pass: subtype + back-compat decision_normalised (both reference decision_category)
    df = df.with_columns(
        _decision_subtype("Decision", "ApplicationStatus").alias("decision_subtype"),
        pl.col("decision_category")
        .replace_strict(_CATEGORY_TO_NORMALISED, default="Other")
        .alias("decision_normalised"),
    )

    # --- date hygiene: null out-of-range dates AND record the flag, both computed
    # from the ORIGINAL values in a single with_columns (so the audit trail is
    # accurate — the earlier bug recomputed flags after nulling, blanking them).
    clean_exprs, flag_exprs = [], []
    for c in DATE_COLS:
        if c not in df.columns:
            continue
        ceiling = (this_year + 1) if c in PAST_DATE_COLS else (this_year + 50)
        bad = ((pl.col(c).dt.year() < FLOOR_YEAR) | (pl.col(c).dt.year() > ceiling)).fill_null(False)
        clean_exprs.append(pl.when(bad).then(None).otherwise(pl.col(c)).alias(c))
        flag_exprs.append(pl.when(bad).then(pl.lit(f"bad_date:{c}")).otherwise(None))

    sentinel_exprs = [
        pl.when(pl.col("FloorArea").is_in([0, 1])).then(None).otherwise(pl.col("FloorArea")).alias("FloorArea"),
        pl.when(pl.col("AreaofSite") == 0).then(None).otherwise(pl.col("AreaofSite")).alias("AreaofSite"),
    ]
    geo_flag = pl.when(~pl.col("geo_in_bounds")).then(pl.lit("geo_out_of_bounds")).otherwise(None)

    df = df.with_columns(
        *clean_exprs,
        *sentinel_exprs,
        pl.concat_list([*flag_exprs, geo_flag]).list.drop_nulls().alias("dq_flags"),
    )
    return df


_POINT_REQUIRED_FIELDS = (
    "Decision",
    "ApplicationStatus",
    "ApplicationType",
    "OneOffHouse",
    "OneOffKPI",
    "FloorArea",
    "AreaofSite",
)


def _adapt_point_page(features: list[dict], object_id_field: str) -> list[dict]:
    """Convert one ArcGIS JSON page while discarding transport/identity fields."""
    rows = []
    for feature in features:
        attrs = dict(feature.get("attributes") or feature.get("properties") or {})
        attrs.pop(object_id_field, None)
        for column in DROP_COLS:
            attrs.pop(column, None)
        geometry = feature.get("geometry") or {}
        attrs["lon"] = geometry.get("x")
        attrs["lat"] = geometry.get("y")
        for column in _POINT_REQUIRED_FIELDS:
            attrs.setdefault(column, None)
        rows.append(attrs)
    return rows


def _adapt_site_page(features: list[dict], object_id_field: str) -> tuple[list[dict], dict[str, int]]:
    """Retain published polygon geometry and provenance from one GeoJSON page."""
    reasons: dict[str, int] = {}
    parsed = polygonal_geometries([feature.get("geometry") for feature in features], ireland_bbox=IRELAND_BBOX)
    rows: list[dict] = []
    for feature, result in zip(features, parsed, strict=True):
        reasons[result.reason] = reasons.get(result.reason, 0) + 1
        if result.wkb is None:
            continue
        properties = dict(feature.get("properties") or feature.get("attributes") or {})
        properties.pop(object_id_field, None)
        minx, miny, maxx, maxy = result.bounds
        row = {field: properties.get(field) for field in SITE_FIELDS}
        row.update(
            {
                "wkb": result.wkb,
                "bbox_minx": minx,
                "bbox_miny": miny,
                "bbox_maxx": maxx,
                "bbox_maxy": maxy,
                "geometry_repaired": result.reason == "repaired",
                "source_layer_url": L1,
                "source_licence": "CC BY 4.0",
                "source_checked_date": dt.date.today(),
            }
        )
        rows.append(row)
    return rows, reasons


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return parsed


def _safe_slug(authority: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", authority.lower()).strip("_")
    return slug or "authority"


def _source_where(authority: str | None) -> str:
    if authority is None:
        return "1=1"
    return "PlanningAuthority='" + authority.replace("'", "''") + "'"


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _existing_authority_counts(path: Path, authority: str | None = None) -> dict[str, int]:
    if not path.exists():
        return {}
    frame = pl.read_parquet(path, columns=["PlanningAuthority"])
    if authority is not None:
        frame = frame.filter(pl.col("PlanningAuthority") == authority)
    return _counts_by_authority(frame)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--authority", help="single PlanningAuthority (smoke test), else national")
    ap.add_argument(
        "--max-pages", type=_positive_int, default=None, help="collection budget; incomplete runs do not publish"
    )
    ap.add_argument("--checkpoint-dir", type=Path, help="SQLite/parquet checkpoint directory")
    ap.add_argument("--resume", action="store_true", help="resume an existing checkpoint")
    ap.add_argument("--page-size", type=_positive_int, default=PAGE, help="bounded OBJECTID page size")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--points-only", action="store_true", help="refresh Layer 0 without Layer 1")
    mode.add_argument("--sites-only", action="store_true", help="refresh Layer 1 without Layer 0")
    args = ap.parse_args()

    setup_standalone_logging("planning_applications_ingest")
    started_utc = dt.datetime.now(dt.UTC).isoformat()
    OUT.mkdir(parents=True, exist_ok=True)
    OUT_META.mkdir(parents=True, exist_ok=True)
    if args.resume and args.checkpoint_dir is None:
        ap.error("--resume requires --checkpoint-dir")
    checkpoint = args.checkpoint_dir or Path(tempfile.mkdtemp(prefix="dail-planning-", dir=str(OUT)))
    where = _source_where(args.authority)
    slug = _safe_slug(args.authority) if args.authority else None
    out_name = f"planning_applications_{slug}.parquet" if slug else "planning_applications_silver.parquet"
    site_out_name = f"planning_application_sites_{slug}.parquet" if slug else "planning_application_sites.parquet"
    coverage_name = (
        f"planning_application_sites_{slug}_coverage.json" if slug else "planning_application_sites_coverage.json"
    )
    request = _arcgis_request
    results = []
    with ArcGISStagedCollector(
        request,
        checkpoint,
        resume=args.resume,
        page_size=args.page_size,
        max_pages=args.max_pages,
    ) as collector:
        if not args.sites_only:
            results.append(
                collector.collect_layer(
                    L0,
                    where=where,
                    layer_name="points",
                    adapt_page=_adapt_point_page,
                    drop_cols=DROP_COLS,
                    transform=transform,
                    output_params={"f": "json", "outFields": "*"},
                )
            )
        if not args.points_only:
            results.append(
                collector.collect_layer(
                    L1,
                    where=where,
                    layer_name="sites",
                    adapt_page=_adapt_site_page,
                    drop_cols=DROP_COLS,
                    transform=transform_sites,
                    output_params={"f": "geojson", "outFields": ",".join(SITE_FIELDS)},
                )
            )
        if any(not result.complete for result in results):
            print(f"INCOMPLETE checkpoint: {checkpoint} | rerun with --resume and without --max-pages")
            return
        collector.revalidate_layers(results)
        point_result = next((result for result in results if result.layer_name == "points"), None)
        site_result = next((result for result in results if result.layer_name == "sites"), None)
        point_counts = (
            collector.staged_stats(point_result)[1]
            if point_result is not None
            else _existing_authority_counts(
                OUT / ("planning_applications_silver.parquet" if not slug else out_name), args.authority
            )
        )
        site_counts = collector.staged_stats(site_result)[1] if site_result is not None else {}
        if not args.authority:
            if point_result is not None:
                old = _existing_authority_counts(OUT / "planning_applications_silver.parquet")
                new_total, _ = collector.staged_stats(point_result)
                old_total = sum(old.values())
                if old_total and new_total < max(1, int(old_total * 0.9)):
                    raise AcquisitionError(f"point row loss guard: {new_total} below 90% of {old_total}")
                if set(old) - set(point_counts):
                    raise AcquisitionError(f"point authority disappearance: {sorted(set(old) - set(point_counts))}")
            if site_result is not None:
                site_total, _ = collector.staged_stats(site_result)
                if site_total < 450_000:
                    raise AcquisitionError(f"site row floor: {site_total} below 450000")
                old_sites = _existing_authority_counts(OUT / "planning_application_sites.parquet")
                if set(old_sites) - set(site_counts):
                    raise AcquisitionError(f"site authority disappearance: {sorted(set(old_sites) - set(site_counts))}")
        candidate_dir = checkpoint / "candidates"
        candidate_dir.mkdir(parents=True, exist_ok=True)
        point_candidate = candidate_dir / out_name
        site_candidate = candidate_dir / site_out_name
        coverage_candidate = candidate_dir / coverage_name
        if point_result is not None:
            collector.assemble(point_result, point_candidate)
        if site_result is not None:
            collector.assemble(
                site_result,
                site_candidate,
                min_rows=450_000 if not args.authority else None,
                compression_level=9,
                geoparquet=True,
                source_crs="EPSG:4326_XY",
            )
            authorities = [
                {
                    "planning_authority": authority,
                    "point_rows": point_counts.get(authority, 0),
                    "polygon_rows": site_counts.get(authority, 0),
                    "polygon_point_ratio": round(site_counts.get(authority, 0) / point_counts[authority], 4)
                    if point_counts.get(authority)
                    else None,
                }
                for authority in sorted(set(point_counts) | set(site_counts))
            ]
            coverage = {
                "schema": "dail-planning-application-sites-coverage/1",
                "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
                "source_layer": L1,
                "source_licence": "CC BY 4.0",
                "source_geometry_note": "Published planning-register site geometry; not verified submitted red-line, parcel identity, ownership or legal interest.",
                "live_polygon_rows": site_result.expected_count,
                "pulled_polygon_rows": site_result.fetched_count,
                "retained_polygon_rows": site_result.retained_count,
                "geometry_results": site_result.geometry_reasons,
                "authorities": authorities,
            }
            save_coverage(coverage, coverage_candidate)
        # Both final artifacts are fully written and validated before either
        # canonical path is replaced. Cross-file replacement remains a per-file
        # atomic operation; a process crash between replacements is reported by
        # the manifest/next run rather than treated as a multi-file transaction.
        from planning.civic.acquisition import publication_lock

        with publication_lock(OUT / ".planning_applications.publish.lock"):
            for candidate, destination in (
                (point_candidate, OUT / out_name),
                (site_candidate, OUT / site_out_name),
                (coverage_candidate, OUT_META / coverage_name),
            ):
                if candidate.exists():
                    candidate.replace(destination)
            artifacts = {}
            artifact_paths = [OUT / out_name] if point_result is not None else []
            if site_result is not None:
                artifact_paths.extend((OUT / site_out_name, OUT_META / coverage_name))
            for path in artifact_paths:
                if path.exists():
                    artifacts[path.name] = {"path": str(path), "sha256": _file_sha256(path)}
            manifest = {
                "schema": "dail-planning-acquisition-manifest/1",
                "started_utc": started_utc,
                "finished_utc": dt.datetime.now(dt.UTC).isoformat(),
                "scope": "authority" if args.authority else "national",
                "authority": args.authority,
                "layers": [result.layer_name for result in results],
                "checkpoint_dir": str(checkpoint),
                "checkpoint_reused_pages": sum(result.reused_pages for result in results),
                "checkpoint_fetched_pages": sum(result.fetched_pages for result in results),
                "source_identity": {result.layer_name: result.identity_hash for result in results},
                "counts": {
                    "points": point_result.retained_count if point_result else None,
                    "sites": site_result.retained_count if site_result else None,
                },
                "authority_counts": {"points": point_counts, "sites": site_counts},
                "consistency_limitations": sorted(
                    {item for result in results for item in result.consistency_limitations}
                ),
                "artifacts": artifacts,
            }
            manifest_name = (
                f"planning_applications_{slug}_acquisition_manifest.json"
                if slug
                else "planning_applications_acquisition_manifest.json"
            )
            save_coverage(manifest, OUT_META / manifest_name)
            print(f"OK acquisition: {checkpoint} | layers {','.join(result.layer_name for result in results)}")


if __name__ == "__main__":
    run_extractor(main)
