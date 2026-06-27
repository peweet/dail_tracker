"""Unit tests for the pure helpers in extractors/noac_scorecard_history_extract.py.

This multi-year extractor locates each metric by a PREDICATE over a table's
column-header text (so it survives pagination/phrasing drift across annual NOAC
reports). The pure surface worth pinning:

  * ``_squish`` — accent-fold + lowercase + drop non-alphanumerics (council match key).
  * ``_canon``  — squish-match to the canonical "… County/City" name + ``DLR`` alias.
  * ``_num``    — cell -> float, with blank/placeholder -> None.
  * ``_has(*tokens)`` / ``_not(*tokens)`` / ``_all(*preds)`` — the header-matching
    predicate combinators that select the right column for each metric. These are
    the part most likely to silently mis-target a column if their semantics shift.

``_locate`` is NOT tested: it opens a PDF off disk (fitz.open(path)), i.e. it does
real file IO, which these pure tests must avoid.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "extractors"))
try:
    from noac_scorecard_history_extract import (  # noqa: E402
        _all,
        _canon,
        _has,
        _not,
        _num,
        _squish,
    )
except ModuleNotFoundError:  # pragma: no cover - defensive, repo convention
    sys.path.insert(0, str(ROOT))
    from noac_scorecard_history_extract import (  # noqa: E402
        _all,
        _canon,
        _has,
        _not,
        _num,
        _squish,
    )


# ── _squish ──────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # The headline use-case named in the docstring: a cell line-wrap inside the
        # council name must squish to the same key as the unwrapped name.
        ("Dún Laoghaire-\nRathdown", "dunlaoghairerathdown"),
        ("Dun Laoghaire-Rathdown", "dunlaoghairerathdown"),
        ("  Carlow  County  ", "carlowcounty"),
        (None, ""),
        ("", ""),
    ],
)
def test_squish(raw, expected):
    assert _squish(raw) == expected


# ── _canon ───────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # _canon (unlike noac_indicators_long._la) returns the CANONICAL "… County/City"
        # name — the same key family as the noac_*_wide gold tables.
        ("Carlow County", "Carlow County"),
        ("Cork City", "Cork City"),
        ("Limerick City and County", "Limerick City and County"),
        # Accent-fold + line-wrap both handled, then mapped to the canonical name.
        ("Dún Laoghaire-\nRathdown", "Dun Laoghaire-Rathdown"),
        # The hard-coded alias.
        ("DLR", "Dun Laoghaire-Rathdown"),
    ],
)
def test_canon_maps_known_labels(raw, expected):
    assert _canon(raw) == expected


@pytest.mark.parametrize("raw", [None, "", "Average", "Not A Council"])
def test_canon_returns_none_for_non_matches(raw):
    assert _canon(raw) is None


# ── _num ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("€1,234", 1234.0),
        ("90%", 90.0),
        ("0", 0.0),
        # Accounting negatives via "(" or "-".
        ("(5)", -5.0),
        ("-2.5", -2.5),
        # Non-string coerced via str().
        (1234, 1234.0),
    ],
)
def test_num_parses_values(raw, expected):
    assert _num(raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", "-", "n/a", "N/A", "*", None, "abc", "."])
def test_num_returns_none(raw):
    assert _num(raw) is None


# ── predicate combinators: _has / _not / _all ────────────────────────────────
def test_has_matches_when_all_tokens_present():
    # _has(*tokens) -> a predicate true iff EVERY token is a substring of the header.
    # The header text is lowercased by the caller (_locate) before these run, so the
    # predicates themselves are plain CASE-SENSITIVE substring checks against lowercase.
    pred = _has("fire", "within 10 minutes")
    assert pred("fires reached within 10 minutes of the fire being received") is True
    # Missing one of the required tokens -> False.
    assert pred("fires reached within 5 minutes") is False


def test_has_single_token():
    pred = _has("settled claims")
    assert pred("cost per capita of settled claims") is True
    assert pred("cost per capita of open claims") is False


def test_has_is_case_sensitive():
    # Pins that _has does NO case-folding itself (it relies on the caller lowercasing).
    # An upper-case header would NOT match a lower-case token — a real foot-gun if a
    # future caller forgets the .lower(), so worth a regression pin.
    pred = _has("fire")
    assert pred("fire brigade") is True
    assert pred("FIRE BRIGADE") is False


def test_not_is_the_negation():
    # _not(*tokens) -> true iff NONE of the tokens appear.
    pred = _not("self")
    assert pred("medically certified sickness absence") is True
    assert pred("self certified sickness absence") is False


def test_not_multiple_tokens():
    # True only when every listed exclusion token is absent.
    pred = _not("regional", "secondary", "tertiary")
    assert pred("local primary roads psci 1-4") is True
    assert pred("regional and local primary roads") is False
    assert pred("secondary roads") is False


def test_all_ands_predicates():
    # _all(*preds) -> true iff every sub-predicate is true (logical AND).
    pred = _all(_has("primary road", "1-4"), _not("regional", "secondary", "tertiary"))
    assert pred("local primary road condition psci 1-4") is True
    # has-side fails (no "1-4").
    assert pred("local primary road condition psci grades") is False
    # not-side fails (contains "regional").
    assert pred("regional primary road condition 1-4") is False


def test_all_empty_is_vacuously_true():
    # all() over no predicates is True — documents the edge so it isn't a surprise.
    assert _all()("anything at all") is True
