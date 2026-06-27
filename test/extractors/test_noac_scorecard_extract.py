"""Unit tests for the pure helpers in extractors/noac_scorecard_extract.py.

Covers the two silent-break-prone primitives that gate the single-year NOAC
accountability scorecard gold table:

  * ``_canon_la(raw)`` — maps a raw NOAC row label to the canonical ``noac_la``
    join key (accent-fold + trailing-mark/whitespace strip + the lone ``DLR``
    alias). If this drifts, councils silently drop out of the join and the
    ``missing councils`` guard in ``main`` either over- or under-fires.
  * ``_num(s)`` — parses a published cell ("€1,234.50", "90%", "(5)", "-") to a
    float (or None for blanks/placeholders). Wrong here = wrong money/percentages
    flowing straight into gold.

Source IO (``_table``, ``main``) is intentionally NOT exercised — it opens the
NOAC PDF off disk, which these pure tests must never touch.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# These extractors insert the repo root onto sys.path themselves (for
# ``from services.parquet_io import save_parquet``); importing them by bare name
# off the extractors/ dir mirrors test_noac_collection_rates.py and works because
# the module's own ROOT-insert satisfies the sibling ``services`` import.
ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "extractors"))
try:
    from noac_scorecard_extract import _canon_la, _num  # noqa: E402
except ModuleNotFoundError:  # pragma: no cover - defensive, repo convention
    sys.path.insert(0, str(ROOT))
    from noac_scorecard_extract import _canon_la, _num  # noqa: E402


# ── _canon_la ────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Exact canonical labels pass straight through — these ARE the gold/la_map keys
        # ("Carlow County", "Cork City", "Limerick City and County"), not "…Council".
        ("Carlow County", "Carlow County"),
        ("Cork City", "Cork City"),
        ("Limerick City and County", "Limerick City and County"),
        ("Waterford City and County", "Waterford City and County"),
        ("Dun Laoghaire-Rathdown", "Dun Laoghaire-Rathdown"),
        # Accent-fold: the NOAC PDF prints the fada; the canonical name is plain ASCII.
        ("Dún Laoghaire-Rathdown", "Dun Laoghaire-Rathdown"),
        # The one hard-coded alias NOAC uses for that council.
        ("DLR", "Dun Laoghaire-Rathdown"),
        # Leading/trailing whitespace is stripped before matching.
        ("  Carlow County  ", "Carlow County"),
        # Trailing footnote marks (*, †) + whitespace are stripped by the [*†‡\s]+$ rule.
        ("Carlow County*", "Carlow County"),
        ("Carlow County †", "Carlow County"),
    ],
)
def test_canon_la_maps_known_labels(raw, expected):
    assert _canon_la(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        # None / empty -> None (the (raw or "") guard, no canonical match).
        None,
        "",
        # A non-council header row must NOT canonicalise (it would corrupt the join).
        "Average national collection level",
        # Casing matters: NFKD only folds accents, it does NOT lowercase, so a
        # differently-cased label has no canonical match. Pins that the fold map is
        # case-sensitive (a deliberate, brittle property worth catching if it changes).
        "carlow county",
        # "…Council" is NOT stripped by this canon (unlike noac_collection_rates'
        # canonical_la). The raw NOAC table already prints the bare type word, so a
        # trailing "Council" yields no match. Regression-pins the divergence.
        "Carlow County Council",
        # Internal whitespace from a cell line-wrap is NOT collapsed (only trailing
        # whitespace is stripped), so the embedded newline->space breaks the match for
        # the no-space canonical "Dun Laoghaire-Rathdown".
        "Dun Laoghaire-\nRathdown",
    ],
)
def test_canon_la_returns_none_for_non_matches(raw):
    assert _canon_la(raw) is None


# ── _num ─────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        # Thousands separators + currency symbol stripped to the numeric core.
        ("€1,234.50", 1234.50),
        ("1,234", 1234.0),
        # Percent sign stripped — stored as the bare number.
        ("90%", 90.0),
        ("12.3%", 12.3),
        ("0", 0.0),
        # Accounting-style negatives: a leading "(" OR any "-" flips the sign.
        ("(5)", -5.0),
        ("(1,234.5)", -1234.5),
        ("-2.5", -2.5),
        # An int passed in (not a string) is coerced via str() then parsed.
        (1234, 1234.0),
    ],
)
def test_num_parses_values(raw, expected):
    assert _num(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        # Blank / explicit placeholder tokens all map to None (no spurious 0.0).
        "",
        "   ",
        "-",
        "n/a",
        "N/A",
        "*",
        None,
        # Non-numeric text -> no digits -> None.
        "abc",
        # A lone "." has no digits after the [^\d.] strip -> None (guards the d == "." case).
        ".",
    ],
)
def test_num_returns_none_for_blanks_and_placeholders(raw):
    assert _num(raw) is None
