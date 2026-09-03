"""SCSI/Teagasc Annual Agricultural Land Market Review — curated tables to gold parquet.

The review is an annual survey of SCSI auctioneers/valuers (169 respondents for the 2025
year), published as a PDF each April. There is no machine-readable release, so the numbers
live as curated CSVs under data/_meta/ (the scsi_tender_price_index.csv pattern), transcribed
by hand with the verification recorded in doc/SCSI_AGRI_LAND_INGEST_QUALITY.md. This module
validates those CSVs and lands them as parquet — it never fetches anything.

Two grains, kept in two tables:
  scsi_agri_land_values  — county x plot-size band x quality, average EUR/acre (Dublin
                           excluded by the source; 25 counties x 3 bands x 2 qualities).
  scsi_agri_land_rental  — province x land use x year, EUR/acre rental (n/a rows kept as
                           nulls; the source publishes no county rental grain).

These are SURVEY OPINIONS of typical values, not transactions — a different grain from both
the CSO ARA02 series (stamp-duty derived medians by NUTS3) and the PPR (recorded residential
sales). Never average or difference across the three, and never present any of them as the
value of a specific parcel.

(c) SCSI/Teagasc, no open licence stated: internal analysis only until reuse is cleared.

    python -m extractors.scsi_agri_land_extract [--dry-run]
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

LOG = logging.getLogger("scsi_agri_land")

_ROOT = Path(__file__).resolve().parents[1]
_META = _ROOT / "data" / "_meta"
_OUT = _ROOT / "data" / "gold" / "parquet"

VALUES_CSV = _META / "scsi_agri_land_values.csv"
RENTAL_CSV = _META / "scsi_agri_land_rental.csv"

COUNTIES = 25  # 26 counties minus Dublin, which the source excludes
SIZE_BANDS = ("lt50", "50_100", "gt100")
QUALITIES = ("poor", "good")
PROVINCES = ("Leinster", "Munster", "Connacht/Ulster")
LAND_USES = (
    "grazing_meadowing_silage",
    "grazing_only",
    "cereal_crops",
    "potatoes",
    "other_crops_beet_maize_beans",
)


def load_values() -> pl.DataFrame:
    return pl.read_csv(
        VALUES_CSV,
        comment_prefix="#",
        schema_overrides={"survey_year": pl.Int32, "eur_per_acre": pl.Float64},
    )


def load_rental() -> pl.DataFrame:
    return pl.read_csv(
        RENTAL_CSV,
        comment_prefix="#",
        schema_overrides={
            "year": pl.Int32,
            "eur_per_acre": pl.Float64,
            "pct_change_published": pl.Float64,
        },
    )


def validate_values(df: pl.DataFrame) -> list[str]:
    """Transcription tripwires. Each catches a real hand-copy failure shape."""
    problems: list[str] = []
    for year in df["survey_year"].unique().to_list():
        sub = df.filter(pl.col("survey_year") == year)
        expected = COUNTIES * len(SIZE_BANDS) * len(QUALITIES)
        if sub.height != expected:
            problems.append(f"{year}: {sub.height} rows, expected {expected}")
        if sub["county"].n_unique() != COUNTIES:
            problems.append(f"{year}: {sub['county'].n_unique()} counties, expected {COUNTIES}")
    if df["eur_per_acre"].null_count():
        problems.append("null eur_per_acre in the values table (the source has no n/a cells)")
    if set(df["size_band"].unique().to_list()) != set(SIZE_BANDS):
        problems.append("unexpected size_band labels")
    if set(df["quality"].unique().to_list()) != set(QUALITIES):
        problems.append("unexpected quality labels")
    # A transposed poor/good pair is the likeliest single-cell copy error, and in the source
    # good land is dearer than poor in every county/band pair — so equality or inversion is
    # evidence of a transcription slip, not a market fact.
    wide = df.pivot(on="quality", index=["survey_year", "county", "size_band"], values="eur_per_acre")
    inverted = wide.filter(pl.col("poor") >= pl.col("good"))
    if inverted.height:
        rows = inverted.select("county", "size_band").rows()
        problems.append(f"poor >= good (transposition?) at {rows}")
    return problems


def validate_rental(df: pl.DataFrame) -> list[str]:
    problems: list[str] = []
    expected = len(PROVINCES) * len(LAND_USES) * 2  # two published years per edition
    if df.height != expected:
        problems.append(f"{df.height} rental rows, expected {expected}")
    if set(df["province"].unique().to_list()) != set(PROVINCES):
        problems.append("unexpected province labels")
    if set(df["land_use"].unique().to_list()) != set(LAND_USES):
        problems.append("unexpected land_use labels")
    # n/a cells are legitimate (Connacht/Ulster tillage) but only there.
    stray_nulls = df.filter(pl.col("eur_per_acre").is_null() & (pl.col("province") != "Connacht/Ulster"))
    if stray_nulls.height:
        problems.append(f"null rental outside Connacht/Ulster: {stray_nulls.height} rows")
    return problems


def main() -> int:
    setup_standalone_logging("scsi_agri_land")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="validate only; write nothing")
    args = ap.parse_args()

    status = 0
    for name, loader, checker in (
        ("scsi_agri_land_values", load_values, validate_values),
        ("scsi_agri_land_rental", load_rental, validate_rental),
    ):
        df = loader()
        problems = checker(df)
        if problems:
            status = 1
            for p in problems:
                LOG.error("%s: %s", name, p)
            continue
        LOG.info("%s: %d rows, valid", name, df.height)
        if not args.dry_run:
            path = _OUT / f"{name}.parquet"
            # Row floors = this edition's exact counts; future editions only append years.
            floor = 150 if name == "scsi_agri_land_values" else 30
            save_parquet(df, path, min_rows=floor)
            LOG.info("wrote %s", path.relative_to(_ROOT))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
