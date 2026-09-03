"""IPAV Farming Report — curated national/provincial land sale + rent averages to gold parquet.

The IPAV (Institute of Professional Auctioneers & Valuers) Farming Report is an annual PDF
survey of member auctioneers' achieved prices — agent-reported averages, not a transaction
register, published each spring for the prior year. No machine-readable release exists, so
the chart-labelled figures live as a curated CSV (data/_meta/ipav_farming_report.csv, the
scsi_agri_land_values.csv pattern) with transcription provenance in
doc/LAND_VALUE_SOURCES_QUALITY.md. This module validates that CSV and lands it as parquet —
it never fetches anything.

A FOURTH land-value grain beside the SCSI survey, CSO ARA02 and the PPR: never average,
difference or sum across sources, and never present any figure as the value of a specific
parcel. (c) IPAV, all rights reserved — internal analysis only until reuse is cleared.

    python -m extractors.ipav_farming_extract [--dry-run]
"""

from __future__ import annotations

# isort: off
# Caps the BLAS thread count before polars loads. Ordering is the contract;
# see services/runtime_env.py.
import services.runtime_env  # noqa: F401
# isort: on

import argparse
import logging
from pathlib import Path

import polars as pl

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("ipav_farming")

_ROOT = Path(__file__).resolve().parents[1]
CSV = _ROOT / "data" / "_meta" / "ipav_farming_report.csv"
OUT = _ROOT / "data" / "gold" / "parquet" / "ipav_farming_report.parquet"

GEO_LEVELS = ("national", "province")
PROVINCES = ("Leinster", "Munster", "Connacht", "Ulster")
MEASURES = (
    "sale_grazing",
    "sale_forestry",
    "rent_conacre_grazing",
    "rent_longterm_lease_grazing",
    "rent_tillage",
)
NATIONAL_SALE_YEARS = tuple(range(2016, 2026))  # the back-series the 2025 edition publishes


def load() -> pl.DataFrame:
    return pl.read_csv(
        CSV,
        comment_prefix="#",
        schema_overrides={"year": pl.Int32, "eur_per_acre": pl.Float64},
    )


def validate(df: pl.DataFrame) -> list[str]:
    problems: list[str] = []
    if df["eur_per_acre"].null_count():
        problems.append("null eur_per_acre (the curated CSV has no n/a cells)")
    if set(df["geo_level"].unique().to_list()) != set(GEO_LEVELS):
        problems.append("unexpected geo_level labels")
    if set(df["measure"].unique().to_list()) != set(MEASURES):
        problems.append("unexpected measure labels")
    bad_geo = df.filter(
        ((pl.col("geo_level") == "national") & (pl.col("geo") != "Ireland"))
        | ((pl.col("geo_level") == "province") & ~pl.col("geo").is_in(list(PROVINCES)))
    )
    if bad_geo.height:
        problems.append(f"geo/geo_level mismatch on {bad_geo.height} row(s)")
    # The national sale back-series must be continuous — a dropped chart bar is the
    # likeliest transcription slip on the one series every headline quotes.
    sale_years = (
        df.filter((pl.col("geo_level") == "national") & (pl.col("measure") == "sale_grazing"))["year"].sort().to_list()
    )
    if sale_years != list(NATIONAL_SALE_YEARS):
        problems.append(f"national sale_grazing years {sale_years} != {list(NATIONAL_SALE_YEARS)}")
    dup = df.group_by(["year", "geo_level", "geo", "measure"]).len().filter(pl.col("len") > 1)
    if dup.height:
        problems.append(f"duplicate (year, geo, measure) rows: {dup.height}")
    return problems


def main() -> int:
    setup_standalone_logging("ipav_farming")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="validate only; write nothing")
    args = ap.parse_args()

    df = load()
    problems = validate(df)
    if problems:
        for p in problems:
            LOG.error("%s", p)
        return 1
    LOG.info("ipav_farming_report: %d rows, valid", df.height)
    if not args.dry_run:
        save_parquet(df, OUT, min_rows=54)  # this edition's exact count; later editions append
        LOG.info("wrote %s", OUT.relative_to(_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
