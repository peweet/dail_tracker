"""Unit tests for the pure helpers in extractors/noac_indicators_long_extract.py.

The long-form NOAC extractor promotes every per-LA series to gold, keyed on a
council label and a derived numeric value, so its string primitives are exactly
the silent-break surface:

  * ``_squish``  — accent-fold + lowercase + drop non-alphanumerics (the match key).
  * ``_la``      — squish-match a raw label to the PAGE key (e.g. "Carlow"), with
                   the ``DLR`` alias. NB: returns the _PAIRS value, NOT the canonical.
  * ``_clean``   — newline->space, collapse runs of whitespace, strip ends.
  * ``_label``   — turn a raw column header into a readable series label.
  * ``_numeric`` — parse a cell to float, incl. MM:SS time -> decimal minutes.

The PDF/table walk (``main``) is not tested — it reads the NOAC PDF off disk.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "extractors"))
try:
    from noac_indicators_long_extract import (  # noqa: E402
        _clean,
        _la,
        _label,
        _numeric,
        _squish,
    )
except ModuleNotFoundError:  # pragma: no cover - defensive, repo convention
    sys.path.insert(0, str(ROOT))
    from noac_indicators_long_extract import (  # noqa: E402
        _clean,
        _la,
        _label,
        _numeric,
        _squish,
    )


# ── _squish ──────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Accent-fold + lowercase + drop spaces/hyphens/case -> bare alnum key.
        ("Dún Laoghaire-Rathdown", "dunlaoghairerathdown"),
        # Internal whitespace AND a line-wrap newline are both dropped (non-alnum),
        # which is the whole point: council labels match despite cell wraps.
        ("  Carlow  County  ", "carlowcounty"),
        ("Cork\nCity", "corkcity"),
        # None / empty -> "" via the (s or "") guard.
        (None, ""),
        ("", ""),
        # Footnote marks are non-alnum and disappear.
        ("Mayo County*", "mayocounty"),
    ],
)
def test_squish(raw, expected):
    assert _squish(raw) == expected


# ── _la ──────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # _la returns the _PAIRS VALUE (the page key), which for most counties is the
        # bare name, not the canonical "… County". Pins that distinction explicitly.
        ("Carlow County", "Carlow"),
        ("Cork City", "Cork City"),
        ("Cork County", "Cork County"),
        ("Limerick City and County", "Limerick"),
        ("South Dublin County", "South Dublin"),
        # Accent-fold path: the fada-bearing label still squish-matches.
        ("Dún Laoghaire-Rathdown", "Dun Laoghaire-Rathdown"),
        # The lone special-cased alias.
        ("DLR", "Dun Laoghaire-Rathdown"),
    ],
)
def test_la_maps_known_labels(raw, expected):
    assert _la(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        None,
        "",
        # A summary / average row must not resolve to a council.
        "Average national",
        # Squish does not strip the word "council", so "carlowcountycouncil" is not a
        # key in the squish-keyed _PAIRS map -> None.
        "Carlow County Council",
    ],
)
def test_la_returns_none_for_non_matches(raw):
    assert _la(raw) is None


# ── _clean ───────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Newline -> space, runs of whitespace collapse to one, ends stripped.
        ("  a\n  b   c ", "a b c"),
        ("Total\nPaid", "Total Paid"),
        # None -> "" (not the string "None").
        (None, ""),
        ("", ""),
        # Non-string input is coerced via str().
        (42, "42"),
    ],
)
def test_clean(raw, expected):
    assert _clean(raw) == expected


# ── _label ───────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("header", "expected"),
    [
        # Enumerator strip: "A. " / "B (b): " / "H1 " / "R2. " prefixes are removed.
        ("A. Number of houses", "Number of houses"),
        ("B (b): Percentage poor", "Percentage poor"),
        ("H1 Households", "Households"),
        ("R2. Roads maintained", "Roads maintained"),
        # Trailing "as at <date>" clause is dropped.
        ("Total dwellings as at 31/12/2024", "Total dwellings"),
        # Trailing "(based on … census)" clause is dropped.
        ("Cost per capita (based on 2022 census)", "Cost per capita"),
        # Ordinary prose headers must survive intact — the enumerator strip only fires
        # when a real delimiter is present, so it no longer eats leading characters.
        ("Simple header", "Simple header"),
        ("Buildings inspected as a percentage", "Buildings inspected as a percentage"),
        ("Net expenditure", "Net expenditure"),
        ("% Area Grossly Polluted", "% Area Grossly Polluted"),
        # Empty header falls back to the original input ("" stays "").
        ("", ""),
    ],
)
def test_label(header, expected):
    assert _label(header) == expected


def test_label_truncates_to_90_chars():
    # Labels are capped at 90 chars so a runaway merged header can't bloat the column.
    long_header = "z" * 200
    out = _label(long_header)
    assert len(out) == 90


# ── _numeric ─────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # MM:SS response time -> decimal minutes, rounded to 3 dp: 6 + 13/60 = 6.217.
        ("06:13", 6.217),
        ("10:00", 10.0),
        # Currency / percent / thousands stripped to the numeric core.
        ("€38.19", 38.19),
        ("12.3%", 12.3),
        ("1,234.5", 1234.5),
        # Accounting negatives via "(" or "-".
        ("(4.5)", -4.5),
        ("-2", -2.0),
        ("0", 0.0),
    ],
)
def test_numeric_parses_values(raw, expected):
    assert _numeric(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        # Non-numeric published strings (e.g. a Yes/No indicator cell) -> None.
        "Yes",
        "",
        # Lone "." -> no digits -> None.
        ".",
        None,
    ],
)
def test_numeric_returns_none(raw):
    assert _numeric(raw) is None
