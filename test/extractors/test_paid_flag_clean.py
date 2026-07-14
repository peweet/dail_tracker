"""Regression guard for the paid_flag column-misalignment cleaner
(extractors/_paid_flag_clean.py — see doc/archive/DATA_QUALITY_AUDIT.md).

Unit tests pin the repair semantics on synthetic rows; the integration test asserts
the real facts ship a flag-or-null paid_flag (DAIL_INTEGRATION_TESTS=1, like the
DQ sentinel sweep).
"""

from __future__ import annotations

import os
from pathlib import Path

import polars as pl
import pytest

from extractors._paid_flag_clean import FLAG_TOKENS, clean_paid_flag, paid_flag_is_clean

ROOT = Path(__file__).resolve().parents[2]


def _df(rows):
    return pl.DataFrame(rows, schema={"paid_flag": pl.Utf8, "description": pl.Utf8, "amount_eur": pl.Float64})


def test_genuine_flags_kept():
    df = _df(
        [
            {"paid_flag": "Y", "description": "IT Services", "amount_eur": 1.0},
            {"paid_flag": "Not Paid", "description": "Roofworks", "amount_eur": 2.0},
            {"paid_flag": "P", "description": "Survey", "amount_eur": 3.0},
        ]
    )
    out, stats = clean_paid_flag(df)
    assert out["paid_flag"].to_list() == ["Y", "Not Paid", "P"]
    assert stats["n_genuine"] == 3 and stats["n_leak"] == 0
    assert paid_flag_is_clean(out) == 0


def test_recoverable_text_moves_to_empty_description():
    df = _df([{"paid_flag": "Building Mtce", "description": None, "amount_eur": 5.0}])
    out, stats = clean_paid_flag(df)
    assert out["paid_flag"].to_list() == [None]
    assert out["description"].to_list() == ["Building Mtce"]
    assert stats["n_recovered"] == 1


def test_text_dropped_when_description_present():
    df = _df([{"paid_flag": "Fitouts", "description": "Real Desc", "amount_eur": 5.0}])
    out, _ = clean_paid_flag(df)
    assert out["paid_flag"].to_list() == [None]
    assert out["description"].to_list() == ["Real Desc"]  # untouched


def test_date_and_amount_leaks_nulled_not_recovered():
    df = _df(
        [
            {"paid_flag": "Dec-21", "description": None, "amount_eur": 5.0},
            {"paid_flag": "4574249.01", "description": None, "amount_eur": 5.0},
            {"paid_flag": "€80,000.00", "description": None, "amount_eur": 5.0},
        ]
    )
    out, stats = clean_paid_flag(df)
    assert out["paid_flag"].to_list() == [None, None, None]
    # a date/amount must NOT be promoted into description (no-inference)
    assert out["description"].to_list() == [None, None, None]
    assert stats["n_month"] == 1 and stats["n_amount"] == 2 and stats["n_recovered"] == 0


def test_idempotent_and_invariants():
    df = _df(
        [
            {"paid_flag": "Y", "description": "A", "amount_eur": 1.0},
            {"paid_flag": "Survey Works", "description": None, "amount_eur": 2.0},
            {"paid_flag": "Nov-23", "description": "B", "amount_eur": 3.0},
        ]
    )
    out1, _ = clean_paid_flag(df)
    out2, stats2 = clean_paid_flag(out1)  # second pass is a no-op
    assert out1.to_dicts() == out2.to_dicts()
    assert stats2["n_leak"] == 0
    assert out1.height == df.height
    assert abs((out1["amount_eur"].sum()) - (df["amount_eur"].sum())) < 1e-9


def test_flag_tokens_are_lowercase():
    assert all(t == t.lower() for t in FLAG_TOKENS)


@pytest.mark.skipif(os.environ.get("DAIL_INTEGRATION_TESTS") != "1", reason="needs real pipeline output")
@pytest.mark.parametrize(
    "rel",
    [
        "data/silver/parquet/public_payments_fact.parquet",
        "data/gold/parquet/procurement_payments_fact.parquet",
    ],
)
def test_real_facts_paid_flag_is_clean(rel):
    p = ROOT / rel
    if not p.exists():
        pytest.skip(f"{rel} not built")
    bad = paid_flag_is_clean(pl.read_parquet(p, columns=["paid_flag"]))
    assert bad == 0, f"{rel}: {bad} non-flag values in paid_flag — run tools/patch_paid_flag_misalignment.py"
