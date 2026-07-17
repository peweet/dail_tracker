"""Social-housing construction pipeline per local authority — committed gold.

Self-contained extractor (fetch + transform), following the pattern of the other
promoted housing extractors: it pulls the DHLGH Social Housing Construction Status
Report (a single, stable open-data CSV on the housing.gov.ie CKAN portal) and lands
one row per local authority — the number of schemes and dwelling units in the
pipeline. This is the ONE per-LA housing metric not already surfaced anywhere in the
app (council-home vacancy/reletting/maintenance come from NOAC; the SSHA waiting list
and private rent are on the Housing page; national supply/HAP are national).

Grain: local authority (31). ``pipeline_units`` is a live-count of dwellings in the
State's build programme at the report date — a SNAPSHOT, refreshed each quarter, so
it carries ``value_safe_to_sum=False`` at the row level (do not sum across LAs and
across quarters — the same scheme recurs quarter to quarter until delivered).

Source : DHLGH Social Housing Construction Status Report (opendata.housing.gov.ie).
Licence: housing.gov.ie open data (PSI re-use).
Writes : data/gold/parquet/housing_construction_pipeline.parquet
Run    : python -m extractors.housing_construction_pipeline_extract [--write]
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path

import polars as pl
import requests

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from extractors.noac_collection_rates_extract import canonical_la  # house LA normaliser
from services.parquet_io import save_parquet

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_OUT = _ROOT / "data" / "gold" / "parquet" / "housing_construction_pipeline.parquet"

# CSR Q4 2025 — housing.gov.ie CKAN resource. Bump this when a newer quarter lands.
SOURCE_URL = (
    "https://opendata.housing.gov.ie/dataset/debe4451-2f14-442b-bb9a-a69f8749ad55/"
    "resource/06a687fb-3618-403d-b2cc-abb820034510/download/csr-q4-2025.csv"
)
SOURCE_NAME = "DHLGH Social Housing Construction Status Report Q4 2025"
SOURCE_PERIOD = "2025-Q4"

# The 31 local authorities — a completeness guard, not a filter.
EXPECTED_LA_COUNT = 31


def fetch_csv() -> pl.DataFrame:
    r = requests.get(SOURCE_URL, headers={"User-Agent": "Mozilla/5.0"}, timeout=60)
    r.raise_for_status()
    try:
        return pl.read_csv(BytesIO(r.content), skip_lines=2, infer_schema_length=5000)
    except Exception:
        decoded = r.content.decode("latin-1").encode("utf-8")
        return pl.read_csv(BytesIO(decoded), skip_lines=2, infer_schema_length=5000)


def _col(df: pl.DataFrame, *candidates: str, contains: str | None = None) -> str | None:
    lower = {c.lower().strip(): c for c in df.columns}
    for cand in candidates:
        if cand.lower() in lower:
            return lower[cand.lower()]
    if contains:
        for lc, orig in lower.items():
            if contains in lc:
                return orig
    return None


def build() -> pl.DataFrame:
    df = fetch_csv()
    la_col = _col(df, "la", "local authority", contains="local authorit")
    units_col = _col(df, "no. of units", "number of units", contains="unit")
    # The CSR is wide: one row per scheme, milestone columns holding the quarter the
    # scheme reached that stage. A scheme's CURRENT status is its furthest-right
    # non-empty milestone. We only need "On Site" and "Completed" to split the pipeline.
    onsite_col = _col(df, "on site", contains="on site")
    completed_col = _col(df, "completed", contains="complete")
    if not la_col or not units_col:
        raise RuntimeError(f"CSR schema changed — LA/units column not found in {df.columns[:15]}")

    def _reached(col: str | None) -> pl.Expr:
        """True when a milestone column holds a real quarter (not null/blank)."""
        if not col:
            return pl.lit(False)
        return pl.col(col).cast(pl.Utf8).fill_null("").str.strip_chars() != ""

    is_completed = _reached(completed_col)
    is_on_site = _reached(onsite_col) & ~is_completed  # currently on site, not yet handed over

    work = df.select(
        pl.col(la_col).map_elements(canonical_la, return_dtype=pl.Utf8).alias("local_authority"),
        pl.col(units_col).cast(pl.Int64, strict=False).alias("_units"),
        is_completed.alias("_completed"),
        is_on_site.alias("_on_site"),
    ).filter(pl.col("local_authority").is_not_null() & (pl.col("local_authority") != ""))

    active = ~pl.col("_completed")  # the pipeline = everything not yet completed
    agg = work.group_by("local_authority").agg(
        pl.col("_units").filter(active).sum().alias("pipeline_units"),
        active.sum().alias("pipeline_schemes"),
        pl.col("_units").filter(pl.col("_on_site")).sum().alias("units_on_site"),
        pl.col("_on_site").sum().alias("schemes_on_site"),
        pl.col("_units").filter(pl.col("_completed")).sum().alias("units_completed"),
    ).sort("pipeline_units", descending=True)

    stamped = agg.with_columns(
        pl.lit(SOURCE_PERIOD).alias("report_period"),
        pl.lit(SOURCE_NAME).alias("source_name"),
        pl.lit(SOURCE_URL).alias("source_url"),
        pl.lit(datetime.now(timezone.utc).isoformat()).alias("fetched_at"),
        pl.lit("ckan_csv").alias("extraction_method"),
        pl.lit("public").alias("privacy_tier"),
        pl.lit(False).alias("value_safe_to_sum"),  # snapshot: recurs quarter to quarter
    )
    return stamped


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true", help="write committed gold (else dry-run preview)")
    args = ap.parse_args()

    df = build()
    n_la = df["local_authority"].n_unique()
    print(f"Built construction pipeline: {df.height} rows, {n_la} LAs, "
          f"{df['pipeline_units'].sum():,} pipeline units "
          f"({df['units_on_site'].sum():,} on site), {df['pipeline_schemes'].sum():,} schemes")
    print(df.select(["local_authority", "pipeline_schemes", "pipeline_units",
                     "units_on_site", "units_completed"]).head(8))
    if n_la != EXPECTED_LA_COUNT:
        print(f"  [warn] expected {EXPECTED_LA_COUNT} LAs, got {n_la} — check canonicalisation")

    if args.write:
        save_parquet(df, _OUT, min_rows=25)
        print(f"\nWrote {_OUT.relative_to(_ROOT)} ({_OUT.stat().st_size:,} bytes)")
    else:
        print("\n(dry-run — pass --write to commit gold)")


if __name__ == "__main__":
    main()
