"""Irish Farmers Journal Land Price Report — free-tier republished figures to gold parquet.

The Journal's report is the closest thing to a transaction register for Irish farmland
(auctioneer-input private-treaty + auction sales, county grain) — and its county tables are
paywalled. This table holds ONLY figures verifiable from free articles or independent press
republication, each row with its source URL (data/_meta/fj_land_price_report.csv). Coverage
is therefore partial by construction: national averages plus published county extremes, and
a year is absent when no citable free figure exists — never derived from a percentage.

A FIFTH land-value grain; the never-mix rule applies as everywhere else. (c) the Journal
for the underlying report — figures here are as republished, Reported band.

    python -m extractors.fj_land_price_extract [--dry-run]
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

LOG = logging.getLogger("fj_land_price")

_ROOT = Path(__file__).resolve().parents[1]
CSV = _ROOT / "data" / "_meta" / "fj_land_price_report.csv"
OUT = _ROOT / "data" / "gold" / "parquet" / "fj_land_price_report.parquet"

SCOPES = ("national", "county_extreme")


def load() -> pl.DataFrame:
    return pl.read_csv(
        CSV,
        comment_prefix="#",
        schema_overrides={"year": pl.Int32, "eur_per_acre": pl.Float64},
    )


def validate(df: pl.DataFrame) -> list[str]:
    problems: list[str] = []
    if df["eur_per_acre"].null_count():
        problems.append("null eur_per_acre")
    if not set(df["scope"].unique().to_list()) <= set(SCOPES):
        problems.append("unexpected scope labels")
    # Every row must carry its citation — the whole point of this table.
    uncited = df.filter(~pl.col("source_url").str.starts_with("http"))
    if uncited.height:
        problems.append(f"{uncited.height} row(s) without a source URL")
    bad_nat = df.filter((pl.col("scope") == "national") & (pl.col("geo") != "Ireland"))
    if bad_nat.height:
        problems.append("national rows must have geo=Ireland")
    return problems


def main() -> int:
    setup_standalone_logging("fj_land_price")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="validate only; write nothing")
    args = ap.parse_args()

    df = load()
    problems = validate(df)
    if problems:
        for p in problems:
            LOG.error("%s", p)
        return 1
    LOG.info("fj_land_price_report: %d rows, valid", df.height)
    if not args.dry_run:
        save_parquet(df, OUT, min_rows=4)
        LOG.info("wrote %s", OUT.relative_to(_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
