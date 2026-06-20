"""Tests for the Derelict Sites Levy extractor (extractors/derelict_sites_levy_extract.py).

Two layers (mirrors test_la_afs.py):
  1. Pure-function unit tests — the name-cleaning + number-parsing primitives, run in CI
     (no data files).
  2. Source-parse invariants — run extract() + fidelity_check against the committed
     gov.ie XLSX and assert GREEN (31 LAs, per-LA sums reconcile to the file's own
     Total row, non-negative money). Skips if the source isn't present.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "extractors"))
from derelict_sites_levy_extract import _SRC, _clean_la, _num, extract, fidelity_check  # noqa: E402


# ---- 1. pure-function unit tests (run in CI) --------------------------------
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("Dún Laoghaire Rathdown", "Dun Laoghaire-Rathdown"),
        ("Cork City & County", "Cork City and County"),
        ("  Donegal  ", "Donegal"),
        ("Limerick City and County", "Limerick City and County"),
    ],
)
def test_clean_la(raw: str, expected: str):
    assert _clean_la(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("€1,234.50", 1234.5),
        ("2,181,761", 2181761.0),
        ("-", None),
        ("", None),
        (None, None),
        ("0", 0.0),
    ],
)
def test_num(raw, expected):
    assert _num(raw) == expected


# ---- 2. source-parse invariants (skip if XLSX absent) -----------------------
@pytest.fixture(scope="module")
def parsed():
    if not _SRC.exists():
        pytest.skip(f"source XLSX not present: {_SRC}")
    return extract()  # (df, totals)


def test_parse_is_green(parsed):
    df, totals = parsed
    rpt = fidelity_check(df, totals)
    assert rpt["green"], rpt["checks"]


def test_31_councils(parsed):
    df, _ = parsed
    assert df["la"].n_unique() == 31


def test_reconciles_to_file_total(parsed):
    df, totals = parsed
    for col in ("amount_levied_eur", "total_received_eur", "cumulative_outstanding_eur"):
        assert abs(df[col].sum() - totals[col]) < 1, f"{col} does not reconcile to the file Total row"


def test_money_non_negative(parsed):
    df, _ = parsed
    for col in ("amount_levied_eur", "cumulative_outstanding_eur"):
        assert df.filter(pl.col(col) < 0).height == 0
