"""IPAV ingest + land value index: committed data passes, tripwires can fail, no blending.

Same contract as test_scsi_agri_land_extract.py: hand-curated CSVs make the validators the
extraction gate, so each check is exercised on a corrupted frame. The index tests pin the
no-blending rule — every row keeps its source, and no cross-source aggregate exists.
"""

from __future__ import annotations

import polars as pl
import pytest

from extractors.ipav_farming_extract import load as load_ipav
from extractors.ipav_farming_extract import validate as validate_ipav
from extractors.land_value_index_build import SCHEMA, build
from extractors.land_value_index_build import validate as validate_index


def test_committed_ipav_csv_is_valid():
    df = load_ipav()
    assert validate_ipav(df) == []
    assert df.height == 54
    # The two headline figures every secondary report quotes (IPAV 2025 edition, p2-3).
    national = df.filter((pl.col("geo_level") == "national") & (pl.col("measure") == "sale_grazing"))
    assert national.filter(pl.col("year") == 2025)["eur_per_acre"][0] == 14442.0
    assert national.filter(pl.col("year") == 2024)["eur_per_acre"][0] == 13949.0


def test_ipav_validator_catches_a_dropped_series_year():
    df = load_ipav().filter(~((pl.col("year") == 2019) & (pl.col("measure") == "sale_grazing")))
    assert any("sale_grazing years" in p for p in validate_ipav(df))


def test_ipav_validator_catches_a_misplaced_geo():
    df = load_ipav().with_columns(
        pl.when(pl.col("geo") == "Ulster").then(pl.lit("national")).otherwise(pl.col("geo_level")).alias("geo_level")
    )
    assert any("mismatch" in p for p in validate_ipav(df))


@pytest.fixture(scope="module")
def index() -> pl.DataFrame:
    return build()


def test_index_builds_valid_with_all_four_sources(index):
    assert validate_index(index) == []
    assert list(index.columns) == list(SCHEMA)
    assert set(index["source"].unique().to_list()) == {
        "scsi_survey",
        "ipav_survey",
        "cso_stamp_duty",
        "cso_zoned",
        "fj_compilation",
    }
    # Precision contract: every value carries at most 2 decimal places (tolerance for
    # binary float representation of the rounded value).
    off_grid = index.filter(((pl.col("value_eur") * 100) - (pl.col("value_eur") * 100).round(0)).abs() > 1e-6)
    assert off_grid.height == 0
    # 25 counties x 2 qualities from the SCSI survey, one row each.
    assert index.filter(pl.col("source") == "scsi_survey").height == 50


def test_index_carries_no_blended_rows(index):
    """Every row is one source's figure; a cross-source aggregate must not exist."""
    assert index["method"].null_count() == 0
    assert not any("blend" in m or "combined" in m for m in index["method"].unique().to_list())
    # The SCSI within-source mean reproduces the report's own headline (Wexford good).
    wexford = index.filter(
        (pl.col("source") == "scsi_survey") & (pl.col("geo") == "Wexford") & (pl.col("land_class") == "agri_good")
    )
    assert round(wexford["value_eur"][0]) == 19226


def test_index_units_stay_as_published(index):
    """Per-acre and per-hectare rows travel separately — no silent conversion."""
    units = set(index["unit"].unique().to_list())
    assert units == {"eur_per_acre", "eur_per_hectare"}
    # Surveys publish per-acre only; a per-hectare survey row would be an invented figure.
    survey_units = set(index.filter(pl.col("source").is_in(["scsi_survey", "ipav_survey"]))["unit"].unique().to_list())
    assert survey_units == {"eur_per_acre"}


def test_index_validator_catches_a_duplicate_key(index):
    doubled = pl.concat([index, index.head(1)])
    assert any("duplicate" in p for p in validate_index(doubled))
