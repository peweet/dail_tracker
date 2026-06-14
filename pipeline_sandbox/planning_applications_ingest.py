"""Phase 0 (sandbox): ingest national Irish planning applications -> silver parquet.

Implements the LOCKED plan in doc/PLANNING_PERMISSION_SCOPING.md §8:
  ArcGIS REST FeatureServer Layer 0 (paginated geometry pull, EPSG:4326)
  -> decision_normalised + application_type_normalised (raw preserved, no-inference)
  -> DQ guards (future dates, FloorArea/AreaofSite sentinels, one-off reconcile,
     Ireland-bbox geo guard, row-count assertion)
  -> parquet via services.parquet_io.save_parquet (atomic, zstd L3, statistics).

Sandbox only until validated. SMOKE-TEST one council first:
    python pipeline_sandbox/planning_applications_ingest.py --authority "Carlow County Council"
Full national sweep (~248 pages / ~495k rows):
    python pipeline_sandbox/planning_applications_ingest.py

Gotchas baked in (project_planning_arcgis_validation / reference_geometry_validation_sources):
  - ITMEasting/ITMNorthing attribute columns are EMPTY -> coords come from geometry only.
  - Applicant* identity columns are empty at source -> dropped anyway (privacy-first).
  - Out-of-bounds coords aren't fixed by make_valid -> detect + quarantine (geo_in_bounds flag).
"""

from __future__ import annotations

import argparse
import datetime as dt
from pathlib import Path

import polars as pl
import requests

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

import logging

LOG = logging.getLogger("planning_applications_ingest")

L0 = ("https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/"
      "IrishPlanningApplications/FeatureServer/0")
PAGE = 2000
OUT = Path("pipeline_sandbox/_planning_output")
IRELAND_BBOX = (-11.0, 51.0, -5.0, 56.0)  # (min_lon, min_lat, max_lon, max_lat)

# Columns to DROP: ArcGIS internals, empty ITM attrs (coords come from geometry),
# and the applicant-identity PII columns (empty at source; dropped regardless).
DROP_COLS = {
    "OBJECTID", "ORIG_FID", "ITMEasting", "ITMNorthing",
    "ApplicantForename", "ApplicantSurname", "ApplicantAddress",
}
# Epoch-millisecond date columns (ArcGIS returns dates as ms UTC).
# Split by direction: PAST events can't legitimately be in the future (ceiling =
# next year); FORWARD-looking dates (due/expiry/appeal-submitted) legitimately can,
# so only absurd values (2260-style garbage) are culled with a generous ceiling.
PAST_DATE_COLS = [
    "ReceivedDate", "WithdrawnDate", "DecisionDate", "GrantDate",
    "AppealDecisionDate", "FIRequestDate", "FIRecDate", "ETL_DATE",
]
FWD_DATE_COLS = ["DecisionDueDate", "ExpiryDate", "AppealSubmittedDate"]
DATE_COLS = PAST_DATE_COLS + FWD_DATE_COLS
FLOOR_YEAR = 1963  # the modern planning system starts with the 1963 Planning Act

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "dail-tracker-planning-ingest/1.0"


def _query(**params) -> dict:
    params.setdefault("f", "json")
    r = _SESSION.get(L0 + "/query", params=params, timeout=120)
    r.raise_for_status()
    return r.json()


def fetch(where: str, max_pages: int | None) -> list[dict]:
    """Paginated geometry pull. Returns list of {attributes..., lon, lat}."""
    rows: list[dict] = []
    offset, page_no = 0, 0
    while True:
        resp = _query(
            where=where, outFields="*", returnGeometry="true", outSR="4326",
            resultOffset=offset, resultRecordCount=PAGE, orderByFields="OBJECTID",
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


def _norm_decision(col: str) -> pl.Expr:
    d = pl.col(col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    # Order matters: invalid/withdraw/refuse BEFORE grant (REFUSE PERMISSION contains
    # PERMISSION); conditional BEFORE plain grant.
    return (
        pl.when(d.is_null() | (d == "") | (d == "N/A")).then(pl.lit("Undecided/None"))
        .when(d.str.contains("WITHDRAW")).then(pl.lit("Withdrawn"))
        .when(d.str.contains("INVALID") | d.str.contains("INVA")).then(pl.lit("Invalid"))
        .when(d.str.contains("REFUS")).then(pl.lit("Refused"))
        # UNCONDITIONAL (granted with NO conditions) must be checked BEFORE the generic
        # CONDITION match, since "UNCONDITIONAL" literally contains "CONDITIONAL".
        .when(d.str.contains("UNCONDITION")).then(pl.lit("Granted"))
        .when(d.str.contains("CONDITION")).then(pl.lit("Granted-Conditional"))
        .when(d.str.contains("GRANT") | d.str.contains("PERMISSION")).then(pl.lit("Granted"))
        .otherwise(pl.lit("Other"))
    )


def _norm_apptype(col: str) -> pl.Expr:
    a = pl.col(col).cast(pl.Utf8).str.to_uppercase().str.strip_chars()
    return (
        pl.when(a.is_null() | (a == "")).then(pl.lit("Unknown"))
        .when(a.str.contains("RETENTION")).then(pl.lit("Retention"))
        .when(a.str.contains("OUTLINE")).then(pl.lit("Outline"))
        .when(a.str.contains("EXTENSION")).then(pl.lit("Extension of Duration"))
        .when(a.str.contains("PERMISSION")).then(pl.lit("Permission"))
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
        _norm_decision("Decision").alias("decision_normalised"),
        pl.col("Decision").alias("decision_raw"),
        _norm_apptype("ApplicationType").alias("application_type_normalised"),
        # one-off reconcile (two inconsistent source flags)
        (
            (pl.col("OneOffHouse").cast(pl.Utf8).str.to_uppercase() == "Y")
            | (pl.col("OneOffKPI").cast(pl.Utf8).str.to_uppercase().is_in(["YES", "Y"]))
        ).alias("is_one_off_house"),
        # geo guard
        (
            pl.col("lon").is_between(minlon, maxlon) & pl.col("lat").is_between(minlat, maxlat)
        ).fill_null(False).alias("geo_in_bounds"),
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--authority", help="single PlanningAuthority (smoke test), else national")
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages (smoke test)")
    args = ap.parse_args()

    setup_standalone_logging("planning_applications_ingest")
    OUT.mkdir(parents=True, exist_ok=True)

    if args.authority:
        where = f"PlanningAuthority='{args.authority}'"
        live = _query(where=where, returnCountOnly="true").get("count")
        LOG.info("SMOKE TEST authority=%s | live count=%s", args.authority, live)
        out_name = "planning_applications_" + args.authority.lower().replace(" ", "_") + ".parquet"
    else:
        where = "1=1"
        live = _query(where=where, returnCountOnly="true").get("count")
        LOG.info("NATIONAL sweep | live count=%s", live)
        out_name = "planning_applications_silver.parquet"

    rows = fetch(where, args.max_pages)
    df = transform(rows)

    # --- validations / report ---
    n = df.height
    LOG.info("rows pulled=%d (live reported=%s)", n, live)
    if live and not args.max_pages:
        assert abs(n - int(live)) <= max(5, int(live) * 0.001), f"row-count drift: {n} vs {live}"
    geo_ok = df["geo_in_bounds"].sum()
    LOG.info("decision_normalised: %s", df["decision_normalised"].value_counts(sort=True).to_dicts())
    LOG.info("application_type_normalised: %s", df["application_type_normalised"].value_counts(sort=True).to_dicts())
    LOG.info("one-off houses: %d/%d (%.1f%%)", df["is_one_off_house"].sum(), n, 100 * df["is_one_off_house"].sum() / n)
    flagged = df.filter(pl.col("dq_flags").list.len() > 0).height
    flag_breakdown = (
        df.select(pl.col("dq_flags").explode()).drop_nulls()
        .to_series().value_counts(sort=True).head(12).to_dicts()
    )
    LOG.info("geo_in_bounds: %d/%d | rows with dq_flags: %d | breakdown: %s",
             geo_ok, n, flagged, flag_breakdown)

    dest = save_parquet(df, OUT / out_name)
    LOG.info("wrote %s (%d rows, %d cols)", dest, df.height, df.width)
    print(f"OK: {dest} | {df.height} rows | geo_in_bounds {geo_ok}/{n}")


if __name__ == "__main__":
    main()
