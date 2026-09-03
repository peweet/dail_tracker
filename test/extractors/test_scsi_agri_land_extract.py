"""SCSI land-review ingest: committed CSVs pass, and each tripwire can actually fail.

The transcription is by hand from a PDF, so the validators ARE the extraction gate — per
feedback_prove_a_gate_can_fail, each check is exercised on a deliberately corrupted frame
(edited copies of the loaded frame, never the committed CSVs).
"""

from __future__ import annotations

import polars as pl

from extractors.scsi_agri_land_extract import (
    COUNTIES,
    load_rental,
    load_values,
    validate_rental,
    validate_values,
)


def test_committed_values_csv_is_valid_and_complete():
    df = load_values()
    assert validate_values(df) == []
    assert df.height == COUNTIES * 3 * 2
    # The report's own headline county averages, reproduced from the transcription — the
    # cross-check that catches a mistyped cell (SCSI 2026 review, Key highlights p5).
    good = df.filter(pl.col("quality") == "good").group_by("county").agg(pl.col("eur_per_acre").mean())
    assert round(good.filter(pl.col("county") == "Wexford")["eur_per_acre"][0]) == 19226
    assert round(good.filter(pl.col("county") == "Kildare")["eur_per_acre"][0]) == 19200
    poor = df.filter(pl.col("quality") == "poor").group_by("county").agg(pl.col("eur_per_acre").mean())
    assert round(poor.filter(pl.col("county") == "Leitrim")["eur_per_acre"][0]) == 3772


def test_committed_rental_csv_is_valid():
    df = load_rental()
    assert validate_rental(df) == []
    assert df.height == 30
    # n/a cells exist only where the source publishes n/a (Connacht/Ulster tillage uses).
    assert df.filter(pl.col("eur_per_acre").is_null())["province"].unique().to_list() == ["Connacht/Ulster"]


def test_values_validator_catches_a_transposed_pair():
    df = load_values()
    swapped = df.with_columns(
        pl.when((pl.col("county") == "Louth") & (pl.col("size_band") == "lt50"))
        .then(pl.when(pl.col("quality") == "poor").then(pl.lit(17250.0)).otherwise(pl.lit(10250.0)))
        .otherwise(pl.col("eur_per_acre"))
        .alias("eur_per_acre")
    )
    assert any("transposition" in p for p in validate_values(swapped))


def test_values_validator_catches_a_dropped_county():
    df = load_values().filter(pl.col("county") != "Leitrim")
    assert validate_values(df) != []


def test_rental_validator_catches_a_stray_null():
    df = load_rental().with_columns(
        pl.when((pl.col("province") == "Leinster") & (pl.col("land_use") == "potatoes"))
        .then(None)
        .otherwise(pl.col("eur_per_acre"))
        .alias("eur_per_acre")
    )
    assert any("outside Connacht/Ulster" in p for p in validate_rental(df))
