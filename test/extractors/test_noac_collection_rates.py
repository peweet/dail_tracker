"""Tests for the NOAC M2 collection-rates extractor (extractors/noac_collection_rates_extract.py).

Two layers (mirrors test_la_afs.py):
  1. Pure-function unit tests — the LA-name canonicaliser + cell parser, run in CI.
  2. Source-parse invariants — run extract() + fidelity_check against the committed
     NOAC PDF and assert GREEN (>=30 LAs, 5 years 2020-2024, rates in 0-130, full 2024
     coverage). Skips if the PDF isn't present (it is large / may be gitignored).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "extractors"))
from noac_collection_rates_extract import (  # noqa: E402
    _SRC,
    _is_la_row,
    _to_float,
    canonical_la,
    extract,
    fidelity_check,
)


# ---- 1. pure-function unit tests (run in CI) --------------------------------
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Strips a trailing "…Council"; leaves the bare County/City type word (the NOAC
        # table prints "Carlow County" / "Cork City", which stay as-is and are what the
        # collection view's la_map keys on).
        ("Carlow County Council", "Carlow"),
        ("Cork City", "Cork City"),
        ("Limerick City and County Council", "Limerick"),
        ("Dún Laoghaire-Rathdown", "Dun Laoghaire-Rathdown"),  # fada -> plain
        ("DLR", "Dun Laoghaire-Rathdown"),  # NOAC's abbreviation
    ],
)
def test_canonical_la(raw: str, expected: str):
    assert canonical_la(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [("90.4%", 90.4), ("101", 101.0), ("-", None), ("—", None), ("", None), ("n/a", None)],
)
def test_to_float(raw, expected):
    assert _to_float(raw) == expected


def test_is_la_row():
    assert _is_la_row("Donegal County Council")
    assert _is_la_row("Dún Laoghaire-Rathdown")  # fada-insensitive
    assert not _is_la_row("Average national collection level")


# ---- 2. source-parse invariants (skip if PDF absent) ------------------------
@pytest.fixture(scope="module")
def parsed():
    if not _SRC.exists():
        pytest.skip(f"source PDF not present: {_SRC}")
    return extract()


def test_parse_is_green(parsed):
    rpt = fidelity_check(parsed)
    assert rpt["green"], rpt["checks"]


def test_31_las_5_years(parsed):
    assert parsed["la"].n_unique() == 31
    assert sorted(parsed["year"].unique().to_list()) == [2020, 2021, 2022, 2023, 2024]


def test_rates_in_range(parsed):
    import polars as pl

    for col in ("commercial_rates_collection_pct", "rent_annuities_collection_pct", "housing_loans_collection_pct"):
        bad = parsed.filter((pl.col(col) < 0) | (pl.col(col) > 130)).height
        assert bad == 0, f"{col} has {bad} out-of-range values"
