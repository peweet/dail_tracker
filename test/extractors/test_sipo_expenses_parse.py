"""Unit tests for the PURE parse layer of extractors/sipo_expenses_paddle_etl.py.

The extractor is two-stage by design: PaddleOCR renders each page to a list of
cell dicts ({text, score, x0, y0, x1, y1}), then a pure geometry parser turns
cells → rows. The OCR stage is irreducible I/O; this whole parse layer is pure
and is where the documented OCR-mangling bugs live (doc/SIPO_OCR_INVESTIGATION.md
— e.g. €17,844.78 garbling). These tests drive it with synthetic cells: no PDF,
no OCR.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

from sipo_expenses_paddle_etl import (  # noqa: E402
    column_split,
    find_total_spend,
    is_money,
    match_constituency,
    parse_money,
    parse_page,
    xc,
    yc,
)

# ── parse_money (the OCR-mangling bug zone) ──────────────────────────────────


@pytest.mark.parametrize(
    "text,expected",
    [
        ("€17,844.78", 17844.78),  # the documented garble case
        ("€1,000.00", 1000.00),
        ("€0.00", 0.0),
        ("€48,600", 48600.0),  # whole euros, no decimals → kept as integer value
        ("(€1,234.56)", 1234.56),  # parentheses stripped
        ("1.234,56", 1234.56),  # trailing ",56" 2dp group → 1234.56 (euro-format tolerant)
    ],
)
def test_parse_money(text, expected):
    assert parse_money(text) == expected


def test_parse_money_non_numeric_is_none():
    assert parse_money("nil") is None
    assert parse_money("Dublin Central") is None
    assert parse_money("€") is None


# ── is_money (classification) ────────────────────────────────────────────────


def test_is_money_classifies_cells():
    assert is_money({"text": "€1,000.00"}) == 1000.0
    assert is_money({"text": "nil"}) == 0.0  # explicit zero expenditure
    assert is_money({"text": "NIL"}) == 0.0
    assert is_money({"text": "500"}) == 500.0  # 3+ digits, no €
    assert is_money({"text": "Dublin Central"}) is None
    assert is_money({"text": "12"}) is None  # too few digits, no €


# ── geometry helpers ─────────────────────────────────────────────────────────


def test_xc_yc_centres():
    c = {"x0": 10, "x1": 30, "y0": 100, "y1": 120}
    assert xc(c) == 20
    assert yc(c) == 110


def test_column_split_finds_gap_or_none():
    money = [
        {"x0": 90, "x1": 110},  # xc 100
        {"x0": 290, "x1": 310},  # xc 300
        {"x0": 95, "x1": 105},  # xc 100
        {"x0": 295, "x1": 305},  # xc 300
    ]
    # largest gap between 100 and 300 → split at the midpoint
    assert column_split(money) == 200
    # a single column → no split
    assert column_split([{"x0": 90, "x1": 110}]) is None


# ── find_total_spend (the reconciliation checksum) ───────────────────────────


def test_find_total_spend_picks_rightmost_money_on_total_row():
    cells = [
        {"text": "TOTAL", "score": 1.0, "x0": 10, "y0": 100, "x1": 60, "y1": 112},
        {"text": "€1,000.00", "score": 1.0, "x0": 200, "y0": 100, "x1": 280, "y1": 112},
        {"text": "€36,729.60", "score": 1.0, "x0": 400, "y0": 100, "x1": 500, "y1": 112},
        {"text": "noise", "score": 1.0, "x0": 10, "y0": 300, "x1": 60, "y1": 312},
    ]
    # the printed TOTAL row's rightmost money = the expenditure checksum
    assert find_total_spend(cells, split_x=300) == 36729.60


def test_find_total_spend_none_without_total_row():
    cells = [{"text": "€1,000.00", "score": 1.0, "x0": 200, "y0": 100, "x1": 280, "y1": 112}]
    assert find_total_spend(cells, split_x=None) is None


# ── match_constituency (fuzzy 43-closed-set anchor) ──────────────────────────

_NORM_KEYS = ["dublincentral", "corknorthcentral", "galwaywest"]
_NORM_TO_NAME = {
    "dublincentral": "Dublin Central",
    "corknorthcentral": "Cork North-Central",
    "galwaywest": "Galway West",
}


def test_match_constituency_exact_and_reject():
    name, conf = match_constituency("Dublin Central", _NORM_KEYS, _NORM_TO_NAME)
    assert name == "Dublin Central"
    assert conf == 1.0
    # a bare direction word is never a constituency
    assert match_constituency("North", _NORM_KEYS, _NORM_TO_NAME) == (None, 0.0)
    # too-short candidate
    assert match_constituency("xy", _NORM_KEYS, _NORM_TO_NAME) == (None, 0.0)


# ── parse_page (the nearest-anchor pairing, end to end on synthetic cells) ───


def _cell(text, x0, y, *, score=1.0, w=80, h=12):
    return {"text": text, "score": score, "x0": x0, "y0": y, "x1": x0 + w, "y1": y + h}


def test_parse_page_pairs_constituency_name_assigned_and_spend():
    # 3 rows: [name] [constituency] [assigned €, left band] [spend €, right band]
    cells = []
    layout = [
        ("John Murphy", "Dublin Central", "€10,000.00", "€5,000.00", 100),
        ("Mary Ryan", "Cork North-Central", "€20,000.00", "€8,000.00", 160),
        ("Pat Walsh", "Galway West", "€30,000.00", "€9,000.00", 220),
    ]
    for name, con, assigned, spend, y in layout:
        cells.append(_cell(name, 10, y))
        cells.append(_cell(con, 120, y))
        cells.append(_cell(assigned, 300, y))  # xc 340 → left band
        cells.append(_cell(spend, 500, y))  # xc 540 → right band

    name_to_seats = {"Dublin Central": 4, "Cork North-Central": 4, "Galway West": 5}
    rows = parse_page(cells, 1, _NORM_KEYS, _NORM_TO_NAME, name_to_seats)

    assert len(rows) == 3
    by_con = {r["constituency"]: r for r in rows}
    assert set(by_con) == {"Dublin Central", "Cork North-Central", "Galway West"}

    dc = by_con["Dublin Central"]
    assert dc["assigned"] == 10000.0
    assert dc["spend"] == 5000.0
    assert dc["limit"] == 48600  # 4-seat statutory limit
    assert "John Murphy" in dc["name_raw"]
    assert dc["page"] == 1

    # 5-seat constituency picks up the higher statutory limit
    assert by_con["Galway West"]["limit"] == 58350
