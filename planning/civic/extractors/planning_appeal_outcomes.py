"""Authoritative council-overturn metric — applications x ACP appeal decisions.

Fixes the data-quality trap in the national profile (planning_decision_profiles.py): the applications
feed's self-reported `AppealDecision` is unreliable (empty-string default; per-council vendor quirks —
Wexford stamps AppealRefNumber on every row + uses "n/a", Westmeath logs appeals as "MODIFIED"). The
TRUSTWORTHY source is An Coimisiún Pleanála's OWN decision (registry PC02, CC-BY), joined to the
council decision via the §Angle-4 recipe (6-digit core of AppealRefNumber -> ABPCASEID).

Output: per matched appeal — council decision vs ABP decision -> overturned/upheld; + a per-council
ranking of how often ABP overturns the council. OCR-free. Powers v_la_planning_overturn ("Who runs
your county") and the per-council overturn signal.

Promoted out of pipeline_sandbox/ 2026-06-20 — runs in pipeline.py as the planning_appeal_outcomes
chain. FETCHES the ACP ArcGIS FeatureServer; reads the COMMITTED planning_applications_silver (the
national planning ingest is NOT yet a pipeline chain, so that silver is a static input here). The
save_parquet min_rows floor refuses to overwrite the silver with a degraded/partial ArcGIS pull.

Two link methods (each row tagged ``match_method``):
  • appeal_ref — the EXACT council→ACP link via the 6-digit core of AppealRefNumber → ABPCASEID.
    Primary and authoritative wherever the council populates that field.
  • spatial_temporal — fallback for appeals the ref join missed (e.g. Cork County publishes NO
    AppealRefNumber on any of its 126k applications). Matches each ACP case to the application at the
    SAME authority that is nearest (~≤55 m, via ACP case-polygon centroid ↔ application lon/lat) AND
    whose decision PRE-DATES the appeal lodgement (most recent such). Validated against Kerry (which
    DOES populate the ref): this rule reproduces the known council decision 98.4% of the time — the
    date constraint is what disambiguates rural sites with a refusal followed by a later grant
    (spatial-only is only 86%). Recovers all 31 councils; Cork County lands at 26.4% (≈ the national rate).

SECOND OUTPUT — the case SPINE (planning_acp_cases.parquet), added 2026-07-20. The ACP fetch already
pulls every case polygon and computes a centroid for the spatial fallback, then throws the coordinates
away; the outcomes silver is a JOIN PRODUCT (only cases matched to a council application) and cannot
answer "which appeals are near this point". The spine persists the register as-is — every case, matched
or not, with its representative point — so the siting brief can show nearby appeal context. Kept a
SEPARATE table on purpose: merging it into the outcomes fact would make an unmatched case look like a
case with no appeal outcome (the `0 = not-matched != absent` trap in reference_join_map).

Inputs:  ACP Cases_2016_Onwards FeatureServer layer 3 (PC02); planning_applications_silver.parquet (PC01)
Output:  data/silver/parquet/planning_appeal_outcomes.parquet
         data/silver/parquet/planning_acp_cases.parquet   (the case spine)
         data/silver/parquet/planning_acp_case_sites.parquet (published case polygons)
         data/_meta/planning_appeal_outcomes_coverage.json
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import re
import unicodedata
from pathlib import Path

import polars as pl
import shapely

from planning.civic.extractors.planning_appeal_vector import (
    OUT_COLS as _OUT_COLS,
)
from planning.civic.extractors.planning_appeal_vector import (
    appeal_case_expr,
    abp_decision_expr,
    authority_key_expr,
    case_status_expr,
    council_decision_expr,
    epoch_ms_date_expr,
    spatial_temporal_matches,
)
from planning.civic.extractors.planning_applications_ingest import _polygonal_geometry
from services.coverage_io import save_coverage
from services.http_engine import fetch_json
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_appeal_outcomes")
ROOT = Path(__file__).resolve().parents[3]
SILVER = ROOT / "data/silver/parquet/planning_applications_silver.parquet"
OUT = ROOT / "data/silver/parquet/planning_appeal_outcomes.parquet"
OUT_SPINE = ROOT / "data/silver/parquet/planning_acp_cases.parquet"
OUT_SITES = ROOT / "data/silver/parquet/planning_acp_case_sites.parquet"
OUT_COV = ROOT / "data/_meta/planning_appeal_outcomes_coverage.json"
OUT_SITES_COV = ROOT / "data/_meta/planning_acp_case_sites_coverage.json"
ACP = "https://services-eu1.arcgis.com/o56BSnENmD5mYs3j/arcgis/rest/services/Cases_2016_Onwards/FeatureServer/3/query"
ACP_LAYER = ACP.removesuffix("/query")
_APPLICATION_COLUMNS = (
    "ApplicationNumber",
    "PlanningAuthority",
    "decision_normalised",
    "AppealRefNumber",
    "DecisionDate",
    "lon",
    "lat",
)

# Cases still before the Board carry a DECISION of "Case is due to be decided by <date>" — 1,383 of
# the register's 1,580 distinct DECISION strings on 2026-07-20. They are NOT outcomes; abp_decision_expr maps
# them to OTHER, which would pool them with withdrawn/invalid. The spine marks them `live` instead.
_LIVE = re.compile(r"^\s*case is due to be decided", re.I)
# metres per degree at ~53°N — a spread/size measure only (never a distance the user is shown).
_M_PER_DEG_LAT, _M_PER_DEG_LON = 111_320.0, 67_000.0
# Matching constants are imported from the mutation-testable vector seam.
# candidate. A large-scale scheme's ACP case-polygon centroid can sit further from the matching
# application's own point coordinate than a one-off house's: ABP-322540 (Castlepark, Mallow, a
# 469-unit LRD) was missed at the tight radius because its true application, 24/6036, sits ~89 m
# from the ACP centroid — found 2026-08 when the fallback instead matched an unrelated 2007
# permission on the same site (see _MAX_LOOKBACK_YEARS below).
# the application under appeal — PDA 2000's default permission lifespan is 5 years. Without this
# bound the fallback confidently matched ABP-322540 (lodged 2025) to a 2007 permission on the same
# site 18 years earlier, because it was the only pre-dated candidate within the search radius —
# found 2026-08 by an LRD cross-check. Unmatched beats mismatched: no plausible candidate -> no row.


def _auth_key(name: str | None) -> str:
    """Normalise a planning-authority name to a join key: drop any ' - … Section' suffix
    (only 'Cork County Council - West Cork Section' exists), strip accents/punctuation.
    Maps ACP's PLANINGATY onto the application feed's PlanningAuthority (e.g. West Cork
    folds into Cork County) so the spatial fallback only matches within the same council."""
    s = re.sub(r"\s*-\s*.*$", "", name or "")
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"[^a-z]", "", s.lower())


def _ms_to_date(ms) -> dt.date | None:
    return dt.datetime.fromtimestamp(ms / 1000, dt.UTC).date() if ms is not None else None


def _centroid(geom: dict | None) -> tuple[float | None, float | None]:
    if not geom:
        return None, None
    pts = [p for ring in geom.get("rings", []) for p in ring]
    if not pts:
        return None, None
    return sum(p[0] for p in pts) / len(pts), sum(p[1] for p in pts) / len(pts)


def _ring_area(geom: dict | None) -> float:
    """Absolute shoelace area of a feature's rings, in degrees² — a RELATIVE size measure used only
    to pick the representative polygon of a multi-polygon case. Never shown, never a real area."""
    if not geom:
        return 0.0
    total = 0.0
    for ring in geom.get("rings") or []:
        s = 0.0
        for i in range(len(ring) - 1):
            s += ring[i][0] * ring[i + 1][1] - ring[i + 1][0] * ring[i][1]
        total += abs(s) / 2
    return total


def _case_status(decision: str | None, decided_ms) -> str:
    """`decided` = the register records a concluded outcome; `live` = it does not.

    `live` covers two populations that a reader does not need to tell apart, because neither has an
    outcome to report: cases genuinely still before the Board (no DECIDED_ON at all), and 38 stale
    rows whose DECISION still reads "Case is due to be decided by <date>" while DECIDED_ON holds a
    target date long past (lodged 2019-20, targets 2020-04-27 to 2025-07-23, none in the future —
    verified 2026-07-20). For those the date is an OBJECTIVE, not a decision, which is why
    _build_spine nulls decided_date whenever the status is `live`.
    """
    return "live" if decided_ms is None or _LIVE.match(decision or "") else "decided"


def _council_decision(d: str | None) -> str:
    if d in ("Granted", "Granted-Conditional"):
        return "GRANT"
    return "REFUSE" if d == "Refused" else "OTHER"


def _fetch_acp() -> pl.DataFrame:
    rows, off = [], 0
    while True:
        response, _ = fetch_json(
            ACP,
            params={
                "where": "1=1",
                # DEVDESC/DEVADDRESS/LINKABPWEB/UPDATED_ON are for the case spine only (site context +
                # the provenance link). The register is case-grained and carries NO applicant name.
                "outFields": "ABPCASEID,DECISION,PLANINGATY,CATEGORY,DECIDED_ON,LODGEDON,"
                "DEVDESC,DEVADDRESS,LINKABPWEB,UPDATED_ON",
                "returnGeometry": "true",  # case-site polygon — centroid feeds the spatial fallback
                "outSR": 4326,
                "resultOffset": off,
                "resultRecordCount": 2000,
                "f": "json",
            },
            timeout=120,
        )
        f = response.get("features", [])
        if not f:
            break
        for x in f:
            lon, lat = _centroid(x.get("geometry"))
            rows.append({**x["attributes"], "lon": lon, "lat": lat, "area": _ring_area(x.get("geometry"))})
        off += len(f)
        if len(f) < 2000:
            break
    df = pl.DataFrame(rows)
    return df.with_columns(
        # WHITESPACE strip only — NEVER digit-strip. 546 of the 26,254 register IDs carry a letter
        # prefix (RL/LV/FS/SU/RP/QD/VV/…), and digit-stripping collapses distinct cases onto one key
        # (DV0005, QD0005, VV0005 and ZE0005 all become "0005"). Verified 2026-07-20.
        pl.col("ABPCASEID").cast(pl.Utf8).str.strip_chars().alias("abp_case"),
        abp_decision_expr("DECISION").alias("abp_decision"),
        epoch_ms_date_expr("LODGEDON").alias("lodged_date"),
        authority_key_expr("PLANINGATY").alias("auth_key"),
    )


def _fetch_acp_sites() -> tuple[pl.DataFrame, dict[str, int], int]:
    """Retain every published ACP case polygon part without changing the centroid join."""
    rows: list[dict] = []
    reasons: dict[str, int] = {}
    offset = 0
    while True:
        response, _ = fetch_json(
            ACP,
            params={
                "where": "1=1",
                "outFields": (
                    "ABPCASEID,DECISION,PLANINGATY,CATEGORY,DECIDED_ON,LODGEDON,DEVDESC,LINKABPWEB,UPDATED_ON"
                ),
                "returnGeometry": "true",
                "outSR": 4326,
                "resultOffset": offset,
                "resultRecordCount": 2000,
                "orderByFields": "OBJECTID",
                "f": "geojson",
            },
            timeout=120,
        )
        features = response.get("features", [])
        if not features:
            break
        for feature in features:
            geometry, reason = _polygonal_geometry(feature.get("geometry"))
            reasons[reason] = reasons.get(reason, 0) + 1
            if geometry is None:
                continue
            properties = feature.get("properties") or {}
            minx, miny, maxx, maxy = geometry.bounds
            rows.append(
                {
                    "abp_case": str(properties.get("ABPCASEID") or "").strip(),
                    "planning_authority": properties.get("PLANINGATY"),
                    "category": properties.get("CATEGORY"),
                    "decision_as_reported": properties.get("DECISION"),
                    "lodged_date": properties.get("LODGEDON"),
                    "decided_date": properties.get("DECIDED_ON"),
                    "updated_date": properties.get("UPDATED_ON"),
                    "description_as_reported": properties.get("DEVDESC"),
                    "case_url": properties.get("LINKABPWEB"),
                    "wkb": shapely.to_wkb(geometry),
                    "bbox_minx": minx,
                    "bbox_miny": miny,
                    "bbox_maxx": maxx,
                    "bbox_maxy": maxy,
                    "geometry_repaired": reason == "repaired",
                    "source_layer_url": ACP_LAYER,
                    "source_licence": "No reuse licence stated on the ArcGIS service; owner clearance required",
                    "source_checked_date": dt.date.today(),
                }
            )
        offset += len(features)
        if len(features) < 2000:
            break
    frame = pl.DataFrame(rows, infer_schema_length=None)
    if frame.height:
        frame = frame.with_columns(
            epoch_ms_date_expr("lodged_date").alias("lodged_date"),
            epoch_ms_date_expr("decided_date").alias("decided_date"),
            epoch_ms_date_expr("updated_date").alias("updated_date"),
        )
    return frame, reasons, sum(reasons.values())


def _load_applications(path: Path = SILVER) -> pl.DataFrame:
    """Read only the seven application columns used by both match paths."""
    return pl.read_parquet(path, columns=list(_APPLICATION_COLUMNS))


def _build_spine(acp: pl.DataFrame) -> pl.DataFrame:
    """The ACP case register as a spine: ONE row per case, with a representative point.

    Dedupe (33 cases / 37 surplus features on 2026-07-20): a case may be published as several
    polygons. We take the LARGEST polygon's centroid as the representative point rather than
    averaging them — averaging a multi-site case invents a location where nothing was proposed
    (case 245660's two polygons lie 102.7 km apart; 6 cases exceed 1 km). `n_polygons` and
    `site_spread_m` carry that fact forward so a consumer can caveat instead of being misled.

    Deliberately NOT applied to the outcomes join above: deduping there would change the promoted
    overturn metric, which is a separate decision from persisting the register.
    """
    ranked = acp.sort("area", descending=True)  # group_by().first() then takes the largest polygon
    spine = ranked.group_by("abp_case").agg(
        pl.len().alias("n_polygons"),
        pl.col("lon").first(),
        pl.col("lat").first(),
        # bbox diagonal of this case's centroids — 0 for single-polygon cases
        ((pl.col("lat").max() - pl.col("lat").min()) * _M_PER_DEG_LAT).alias("_dy"),
        ((pl.col("lon").max() - pl.col("lon").min()) * _M_PER_DEG_LON).alias("_dx"),
        pl.col("CATEGORY").first().alias("category"),
        pl.col("DECISION").first().alias("decision_raw"),
        pl.col("abp_decision").first(),
        pl.col("PLANINGATY").first().alias("planning_authority"),
        pl.col("DECIDED_ON").first(),
        pl.col("lodged_date").first(),
        pl.col("DEVDESC").first().alias("dev_desc"),
        pl.col("DEVADDRESS").first().alias("dev_address"),
        pl.col("LINKABPWEB").first().alias("case_url"),
        pl.col("UPDATED_ON").first(),
    )
    return (
        spine.with_columns(
            ((pl.col("_dx") ** 2 + pl.col("_dy") ** 2).sqrt()).round(0).alias("site_spread_m"),
            case_status_expr("decision_raw", "DECIDED_ON").alias("status"),
            epoch_ms_date_expr("DECIDED_ON").alias("decided_date"),
            epoch_ms_date_expr("DECIDED_ON").alias("target_date"),
            epoch_ms_date_expr("UPDATED_ON").alias("updated_date"),
        )
        .with_columns(
            # decided_date must mean ONE thing: the date the Board concluded the case. For a `live`
            # row the register's DECIDED_ON is a target, so publishing it here would let a target
            # date be read (and sorted, and cited) as an outcome date. Kept as target_date instead.
            pl.when(pl.col("status") == "live").then(None).otherwise(pl.col("decided_date")).alias("decided_date"),
            pl.when(pl.col("status") == "live").then(pl.col("target_date")).otherwise(None).alias("target_date"),
        )
        .select(
            "abp_case",
            "lon",
            "lat",
            "status",
            "category",
            "abp_decision",
            "decision_raw",
            "planning_authority",
            "decided_date",
            "target_date",
            "lodged_date",
            "dev_desc",
            "dev_address",
            "case_url",
            "n_polygons",
            "site_spread_m",
            "updated_date",
        )
        .sort("abp_case")
    )


def _spatial_temporal_matches(residual: pl.DataFrame, apps: pl.DataFrame) -> pl.DataFrame:
    """Compatibility entry point for the native, order-preserving matcher."""
    return spatial_temporal_matches(residual, apps)


def _refresh_acp_sites() -> tuple[pl.DataFrame, dict[str, int], int]:
    case_sites, geometry_results, rows_pulled = _fetch_acp_sites()
    save_parquet(
        case_sites,
        OUT_SITES,
        min_rows=20_000,
        compression_level=9,
        geoparquet=True,
        source_crs="EPSG:4326_XY",
    )
    coverage = {
        "schema": "dail-planning-acp-case-sites-coverage/1",
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
        "source_layer": ACP_LAYER,
        "source_licence": ("No reuse licence stated on the ArcGIS service; owner clearance required"),
        "source_coverage_note": (
            "The official service says not all cases are included, invalid and withdrawn cases "
            "are typically not mapped, and updates may lag. A zero-result search is scoped to "
            "this snapshot and is not proof that no case exists."
        ),
        "source_geometry_note": (
            "Published case geometry; not proof of a submitted red-line, parcel identity, ownership or legal interest."
        ),
        "pulled_polygon_rows": rows_pulled,
        "retained_polygon_rows": case_sites.height,
        "geometry_results": geometry_results,
    }
    save_coverage(coverage, OUT_SITES_COV)
    LOG.info(
        "ACP case polygons: %d retained of %d pulled -> %s | %s",
        case_sites.height,
        rows_pulled,
        OUT_SITES,
        geometry_results,
    )
    return case_sites, geometry_results, rows_pulled


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--sites-only",
        action="store_true",
        help="refresh the published ACP case-polygon snapshot without rewriting outcome facts",
    )
    args = parser.parse_args()
    setup_standalone_logging("planning_appeal_outcomes")
    if args.sites_only:
        _refresh_acp_sites()
        return
    if not SILVER.exists():
        raise SystemExit(f"silver missing: {SILVER}")
    acp = _fetch_acp()
    LOG.info(
        "ACP cases: %d (GRANT=%d REFUSE=%d OTHER=%d)",
        acp.height,
        *(acp.filter(pl.col("abp_decision") == v).height for v in ("GRANT", "REFUSE", "OTHER")),
    )

    # Preserve source polygons in a separate part-grained fact. The existing case spine and
    # appeal matcher deliberately keep their validated representative-centroid semantics.
    case_sites, site_geometry_results, site_rows_pulled = _refresh_acp_sites()

    # SPINE — every case with a representative point, independent of whether it joins to a council
    # application. Written before the join so a council-side failure can't cost us the register.
    spine = _build_spine(acp)
    n_live = spine.filter(pl.col("status") == "live").height
    n_multi = spine.filter(pl.col("n_polygons") > 1).height
    # Floor: the register is 26,254 features / 26,217 cases (2026-07-20) and only grows. A truncated
    # pagination run must not overwrite the spine with a partial register.
    save_parquet(spine, OUT_SPINE, min_rows=20_000)
    LOG.info(
        "ACP case spine: %d cases (%d live / %d decided; %d multi-polygon) -> %s",
        spine.height,
        n_live,
        spine.height - n_live,
        n_multi,
        OUT_SPINE,
    )

    apps_all = _load_applications()
    # PRIMARY — exact appeal_ref → ABPCASEID link (authoritative wherever the council fills it).
    apps_ref = (
        apps_all.filter(pl.col("AppealRefNumber").is_not_null() & (pl.col("AppealRefNumber").str.strip_chars() != ""))
        .with_columns(
            appeal_case_expr("AppealRefNumber").alias("abp_case"),
            council_decision_expr("decision_normalised").alias("council_decision"),
        )
        .filter(pl.col("abp_case").is_not_null())
    )

    primary = (
        apps_ref.join(
            acp.select("abp_case", "abp_decision", "PLANINGATY", "CATEGORY", "DECIDED_ON"), on="abp_case", how="inner"
        )
        .with_columns(pl.lit("appeal_ref").alias("match_method"))
        .select(_OUT_COLS)
    )
    LOG.info("appeal_ref matches: %d (of %d apps with an appeal ref)", primary.height, apps_ref.height)

    # FALLBACK — spatial+temporal recovery for ACP cases the ref join didn't reach.
    matched = set(primary["abp_case"].to_list())
    residual = acp.filter(
        ~pl.col("abp_case").is_in(matched) & pl.col("lat").is_not_null() & pl.col("lon").is_not_null()
    )
    fallback = _spatial_temporal_matches(residual, apps_all)
    LOG.info("spatial_temporal matches: %d (of %d unmatched ACP cases with coords)", fallback.height, residual.height)

    allm = pl.concat([primary, fallback], how="vertical")
    clear = allm.filter((pl.col("council_decision") != "OTHER") & (pl.col("abp_decision") != "OTHER"))
    clear = clear.with_columns((pl.col("council_decision") != pl.col("abp_decision")).alias("overturned"))
    # Row floor: the clear-vs-clear set is ~14k+ (ref + spatial) and only grows as ABP adds cases.
    # A partial ArcGIS pull (outage mid-pagination, schema drift) would thin it; refuse to overwrite
    # the silver below this floor rather than ship a truncated overturn metric to the LA page.
    save_parquet(clear, OUT, min_rows=10_000)

    n = clear.height
    rev = clear.filter(pl.col("overturned")).height
    g2r = clear.filter((pl.col("council_decision") == "GRANT") & (pl.col("abp_decision") == "REFUSE")).height
    r2g = clear.filter((pl.col("council_decision") == "REFUSE") & (pl.col("abp_decision") == "GRANT")).height
    by_method = dict(clear.group_by("match_method").len().iter_rows())
    LOG.info(
        "clear-vs-clear appeals: %d (appeal_ref=%d spatial_temporal=%d) | ABP OVERTURNED council: %d (%.1f%%) "
        "[grant->refuse %d, refuse->grant %d] | upheld %d",
        n,
        by_method.get("appeal_ref", 0),
        by_method.get("spatial_temporal", 0),
        rev,
        100 * rev / n,
        g2r,
        r2g,
        n - rev,
    )

    # per-council overturn ranking (authoritative; min 25 clear appeals)
    rank = (
        clear.group_by("PlanningAuthority")
        .agg(pl.len().alias("appeals"), pl.col("overturned").sum().alias("overturned"))
        .with_columns((100 * pl.col("overturned") / pl.col("appeals")).round(1).alias("overturn_pct"))
        .filter(pl.col("appeals") >= 25)
        .sort("overturn_pct", descending=True)
    )
    LOG.info("per-council overturn (top, min 25 appeals):\n%s", rank.head(12))

    cov = {
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
        "layer": "silver",
        "source": "PC02 ACP Cases_2016_Onwards x PC01 applications. Primary: AppealRefNumber 6-digit -> "
        "ABPCASEID. Fallback (match_method=spatial_temporal): nearest same-authority application "
        "(ACP centroid <=55m) whose decision pre-dates the appeal; validated 98.4% vs Kerry ground truth.",
        "acp_cases": acp.height,
        "spine_cases": spine.height,
        "spine_live": n_live,
        "spine_decided": spine.height - n_live,
        "spine_multi_polygon": n_multi,
        "case_site_polygon_rows": case_sites.height,
        "case_site_polygon_rows_pulled": site_rows_pulled,
        "case_site_geometry_results": site_geometry_results,
        "case_site_note": (
            "planning_acp_case_sites.parquet retains every published case polygon part for "
            "reviewable spatial comparison. Published register geometry is not proof of a "
            "submitted red-line, parcel identity, ownership or legal interest."
        ),
        "spine_note": "planning_acp_cases.parquet — one row per ACP case (register spine, matched or "
        "not). Representative point = largest polygon's centroid; multi-site cases carry n_polygons "
        "+ site_spread_m. status='live' cases are still before the Board and have NO outcome.",
        "appeals_joined": int(primary.height + fallback.height),
        "matches_appeal_ref": int(by_method.get("appeal_ref", 0)),
        "matches_spatial_temporal": int(by_method.get("spatial_temporal", 0)),
        "clear_vs_clear": n,
        "abp_overturned_council": rev,
        "overturn_pct": round(100 * rev / n, 1),
        "council_grant_to_abp_refuse": g2r,
        "council_refuse_to_abp_grant": r2g,
        "caveat": "ABP appeals are de novo; overturn = outcome flipped. ACP feed 2016+. Correlation/record, not a quality judgement.",
    }
    save_coverage(cov, OUT_COV)
    LOG.info("wrote %d outcomes -> %s ; coverage -> %s", n, OUT, OUT_COV)


if __name__ == "__main__":
    main()
