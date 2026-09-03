"""Representative land value index — the held land-price sources in ONE long table, side by side.

Reads five gold tables and emits `land_value_index.parquet`: one row per
(source, year, geo, land_class, measure, unit), SALE values only (rents stay in the
per-source tables). This is a UNION FOR DISPLAY — the point is that a reader can see what
each source says about a place and year in one query. It is NOT a blended index:

  * every row keeps its `source` and `method`, and figures are as published (no unit
    conversion — a source's per-acre and per-hectare rows travel separately);
  * rows from different sources must never be averaged, differenced or summed — they are
    different measurements (agent surveys vs stamp-duty returns) of different markets
    (agricultural vs residentially zoned) at different grains;
  * the one within-source derivation is the SCSI county average across its three plot-size
    bands, which reproduces the report's own headline method (its quoted county figures are
    exactly this mean — see doc/SCSI_AGRI_LAND_INGEST_QUALITY.md).

Sources → land classes:
  scsi_survey     county × agri_good/agri_poor       (SCSI/Teagasc agent survey)
  ipav_survey     national/province × agri_grazing, forestry (IPAV member survey)
  cso_stamp_duty  NUTS3/national × agri_all/agri_arable/agri_grassland (ARA02)
  cso_zoned       NUTS3/national (2018-2024) + county (2024) × residential_zoned (RZLPA01/02)

    python -m extractors.land_value_index_build [--dry-run]
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

LOG = logging.getLogger("land_value_index")

_ROOT = Path(__file__).resolve().parents[1]
_GOLD = _ROOT / "data" / "gold" / "parquet"
OUT = _GOLD / "land_value_index.parquet"

SCHEMA = ("source", "method", "year", "geo_level", "geo", "land_class", "measure", "unit", "value_eur")

_PRICE_STATS = {
    "Mean Price per Acre": ("mean", "eur_per_acre"),
    "Median Price per Acre": ("median", "eur_per_acre"),
    "Mean Price per Hectare": ("mean", "eur_per_hectare"),
    "Median Price per Hectare": ("median", "eur_per_hectare"),
}


def _rows(df: pl.DataFrame, **constants: str) -> pl.DataFrame:
    return df.with_columns([pl.lit(v).alias(k) for k, v in constants.items()]).select(SCHEMA)


def _from_scsi() -> pl.DataFrame:
    df = pl.read_parquet(_GOLD / "scsi_agri_land_values.parquet")
    county = (
        df.group_by(["survey_year", "county", "quality"])
        .agg(pl.col("eur_per_acre").mean().alias("value_eur"))
        .with_columns(
            pl.col("survey_year").cast(pl.Int32).alias("year"),
            pl.col("county").alias("geo"),
            ("agri_" + pl.col("quality")).alias("land_class"),
        )
    )
    return _rows(
        county,
        source="scsi_survey",
        method="SCSI/Teagasc agent survey — mean of the three published plot-size bands",
        geo_level="county",
        measure="survey_average",
        unit="eur_per_acre",
    )


def _from_ipav() -> pl.DataFrame:
    df = pl.read_parquet(_GOLD / "ipav_farming_report.parquet")
    sales = df.filter(pl.col("measure").str.starts_with("sale_")).with_columns(
        pl.col("year").cast(pl.Int32),
        pl.col("eur_per_acre").alias("value_eur"),
        pl.col("measure")
        .str.replace("sale_grazing", "agri_grazing")
        .str.replace("sale_forestry", "forestry")
        .alias("land_class"),
    )
    return _rows(
        sales,
        source="ipav_survey",
        method="IPAV member-auctioneer survey of achieved prices",
        measure="survey_average",
        unit="eur_per_acre",
    )


def _cso_prices(df: pl.DataFrame, geo_col: str) -> pl.DataFrame:
    out = (
        df.filter(pl.col("Statistic Label").is_in(list(_PRICE_STATS)))
        .with_columns(
            pl.col("Year").cast(pl.Int32).alias("year"),
            pl.col("VALUE").cast(pl.Float64, strict=False).alias("value_eur"),
            pl.col(geo_col).alias("geo"),
            pl.col("Statistic Label").replace_strict({k: v[0] for k, v in _PRICE_STATS.items()}).alias("measure"),
            pl.col("Statistic Label").replace_strict({k: v[1] for k, v in _PRICE_STATS.items()}).alias("unit"),
        )
        .drop_nulls("value_eur")
    )
    return out.with_columns(
        pl.when(pl.col("geo") == "Ireland").then(pl.lit("national")).otherwise(pl.lit("nuts3")).alias("geo_level")
    )


def _from_ara02() -> pl.DataFrame:
    df = pl.read_parquet(_GOLD / "cso_ara02.parquet")
    priced = _cso_prices(df, "Region").with_columns(
        pl.col("Type of Land Use")
        .replace_strict(
            {"All Land Types": "agri_all", "Arable Land": "agri_arable", "Permanent Grassland": "agri_grassland"}
        )
        .alias("land_class")
    )
    return priced.with_columns(
        pl.lit("cso_stamp_duty").alias("source"),
        pl.lit("CSO ARA02 — Revenue stamp-duty returns; excludes development land and parcels under 0.2 ha").alias(
            "method"
        ),
    ).select(SCHEMA)


def _from_rzlpa01() -> pl.DataFrame:
    df = pl.read_parquet(_GOLD / "cso_rzlpa01.parquet")
    priced = _cso_prices(df, "NUTS 3 Region").with_columns(pl.lit("residential_zoned").alias("land_class"))
    return priced.with_columns(
        pl.lit("cso_zoned").alias("source"),
        pl.lit("CSO RZLPA01 — Revenue data on residentially zoned land sales").alias("method"),
    ).select(SCHEMA)


def _from_rzlpa02() -> pl.DataFrame:
    df = pl.read_parquet(_GOLD / "cso_rzlpa02.parquet")
    priced = (
        _cso_prices(df, "County or NUTS 3 Region")
        .filter(pl.col("geo").str.starts_with("Co. "))
        .with_columns(
            pl.col("geo").str.strip_prefix("Co. "),
            pl.lit("residential_zoned").alias("land_class"),
            pl.lit("county").alias("geo_level"),
        )
    )
    return priced.with_columns(
        pl.lit("cso_zoned").alias("source"),
        pl.lit("CSO RZLPA02 — Revenue data on residentially zoned land sales, county grain").alias("method"),
    ).select(SCHEMA)


def _from_fj() -> pl.DataFrame:
    # National rows only: the discrete table's county rows are published EXTREMES (a highest
    # or lowest county), not county averages — placing them beside averages would misread.
    df = pl.read_parquet(_GOLD / "fj_land_price_report.parquet")
    national = df.filter(pl.col("scope") == "national").with_columns(
        pl.col("year").cast(pl.Int32),
        pl.col("eur_per_acre").alias("value_eur"),
        pl.lit("agri_all").alias("land_class"),
    )
    return _rows(
        national,
        source="fj_compilation",
        method="Irish Farmers Journal transaction compilation — free-tier republished national "
        "figures only; county tables paywalled",
        geo_level="national",
        measure="compilation_average",
        unit="eur_per_acre",
    )


def build() -> pl.DataFrame:
    parts = [_from_scsi(), _from_ipav(), _from_ara02(), _from_rzlpa01(), _from_rzlpa02(), _from_fj()]
    return (
        pl.concat(parts)
        # One precision across sources: euro figures rounded to 2 decimal places, unique keys,
        # deterministic sort. Rounding is presentation precision, not a value change — every
        # source publishes whole euro.
        .with_columns(pl.col("value_eur").round(2))
        .unique(subset=["source", "year", "geo_level", "geo", "land_class", "measure", "unit"], keep="first")
        .sort(["source", "year", "geo_level", "geo", "land_class", "measure", "unit"])
    )


def validate(df: pl.DataFrame) -> list[str]:
    problems: list[str] = []
    if df["value_eur"].null_count():
        problems.append("null value_eur rows")
    expected_sources = {"scsi_survey", "ipav_survey", "cso_stamp_duty", "cso_zoned", "fj_compilation"}
    if set(df["source"].unique().to_list()) != expected_sources:
        problems.append(f"sources {df['source'].unique().to_list()} != {sorted(expected_sources)}")
    dup = (
        df.group_by(["source", "year", "geo_level", "geo", "land_class", "measure", "unit"])
        .len()
        .filter(pl.col("len") > 1)
    )
    if dup.height:
        problems.append(f"{dup.height} duplicate key row(s)")
    if df.filter(pl.col("value_eur") <= 0).height:
        problems.append("non-positive value_eur")
    return problems


def main() -> int:
    setup_standalone_logging("land_value_index")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--dry-run", action="store_true", help="validate only; write nothing")
    args = ap.parse_args()

    df = build()
    problems = validate(df)
    if problems:
        for p in problems:
            LOG.error("%s", p)
        return 1
    per_source = {r["source"]: r["len"] for r in df.group_by("source").len().iter_rows(named=True)}
    LOG.info("land_value_index: %d rows %s", df.height, per_source)
    if not args.dry_run:
        save_parquet(df, OUT, min_rows=500)
        LOG.info("wrote %s", OUT.relative_to(_ROOT))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
