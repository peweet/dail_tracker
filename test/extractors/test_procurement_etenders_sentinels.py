"""Null-sentinel coercion tests for extractors/procurement_etenders_extract.py.

The OGP source CSV writes literal strings for missing values — and it mixes
casings and spellings ("NULL", "Null", "n/a"). The original 2026-06-11 fix
matched the exact string "NULL" only, so "Null" and "n/a" suppliers survived
into gold and formed bogus supplier groups. These tests lock the sentinel set
and its case-insensitive, trim-tolerant matching, and guard against the
opposite failure: real names that merely START with a sentinel ("Null Island
Ltd") must never be wiped.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

pytest.importorskip("polars")
import polars as pl  # noqa: E402
import procurement_etenders_extract as pe  # noqa: E402


def _flags(values: list[str | None]) -> list[bool | None]:
    return (
        pl.DataFrame({"v": values}, schema={"v": pl.Utf8})
        .select(pe.null_sentinel_expr(pl.col("v")))
        .to_series()
        .to_list()
    )


def test_sentinel_expr_catches_all_casings_and_padding():
    assert _flags(["NULL", "Null", "null", " NULL ", "n/a", "N/A", "N/a", "\tn/a "]) == [True] * 8


def test_sentinel_expr_keeps_real_names():
    # Names containing or starting with a sentinel are NOT sentinels themselves.
    keep = ["Null Island Ltd", "Nallytics", "Na Fianna Construction", "A/B Testing Ltd", "Acme Ltd"]
    assert _flags(keep) == [False] * len(keep)


def test_sentinel_expr_passes_through_null_and_empty():
    # Real nulls stay null (is_in -> null, falsy in a filter); '' is handled by
    # the separate empty-string gates, not the sentinel set.
    assert _flags([None, ""]) == [None, False]


def test_coerce_null_sentinels_scrubs_every_string_column():
    df = pl.DataFrame(
        {
            "supplier": ["Acme Ltd", "Null", "n/a"],
            "Contracting Authority": ["NULL", "Dublin City Council", " Null "],
            "value_eur": [1.0, 2.0, 3.0],  # non-string column must be untouched
        }
    )
    out = pe.coerce_null_sentinels(df)
    assert out["supplier"].to_list() == ["Acme Ltd", None, None]
    assert out["Contracting Authority"].to_list() == [None, "Dublin City Council", None]
    assert out["value_eur"].to_list() == [1.0, 2.0, 3.0]


def test_sentinel_set_is_locked():
    """The three production sites (row filter, exploded-supplier filter, final
    coercion) all derive from NULL_SENTINELS — shrinking it silently reopens the
    stringified-null bug, so changes must be deliberate."""
    assert set(pe.NULL_SENTINELS) >= {"null", "n/a"}
    assert all(s == s.lower() == s.strip() for s in pe.NULL_SENTINELS)
