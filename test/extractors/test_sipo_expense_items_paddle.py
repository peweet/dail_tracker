"""Unit tests for the PURE parse layer of
extractors/sipo_expense_items_paddle_etl.py (Part-4 itemised expenses).

Like the sibling Part-3 ETL, this extractor is two-stage: PaddleOCR renders each
page to cell dicts ({text, score, x0, y0, x1, y1}), then a pure geometry parser
clusters cells -> rows and reads Part-4 line items + the Expenses-Review category
totals. The OCR stage (ocr_page/process_party/main) is irreducible I/O and is NOT
tested here. These tests drive the pure functions with synthetic cells: no PDF,
no OCR, no parquet.

Covered (pure): parse_money, cluster_rows, rightmost_money, parse_item_row,
parse_summary_row. Skipped (I/O / side-effects): ocr_page, process_party, main, hr.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "extractors"))

from sipo_expense_items_paddle_etl import (  # noqa: E402
    cluster_rows,
    parse_item_row,
    parse_money,
    parse_summary_row,
    rightmost_money,
)


def _cell(text, x0, y, *, score=1.0, w=80, h=12):
    """Mimic the PaddleOCR cell dict the parsers consume."""
    return {"text": text, "score": score, "x0": x0, "y0": y, "x1": x0 + w, "y1": y + h}


# ── parse_money ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "text,expected",
    [
        ("€8,860.76", 8860.76),  # no-space euro with 2dp tail
        ("€ 4.305.00", 4305.00),  # OCR thousands-dot garble -> 4305.00
        ("€24,853.05", 24853.05),
        ("€0.00", 0.0),
        ("€48,600", 48600.0),  # whole euros, no decimals
        ("(€1,234.56)", 1234.56),  # parentheses stripped
        ("1.234,56", 1234.56),  # euro-format ",56" 2dp tail
        ("  €1,000.00  ", 1000.0),  # surrounding whitespace
        ("500", 500.0),  # bare integer
    ],
)
def test_parse_money_happy(text, expected):
    assert parse_money(text) == expected


@pytest.mark.parametrize("text", ["", "nil", "Advertising", "€", "—", "$$$", "_,.\\"])
def test_parse_money_non_numeric_is_none(text):
    assert parse_money(text) is None


# ── cluster_rows (y-tolerance grouping) ──────────────────────────────────────


def test_cluster_rows_groups_within_tolerance():
    # two cells share a y-band (centres 106 vs 108, within tol 9); third sits far below
    cells = [
        _cell("A1", 10, 100),  # yc 106
        _cell("€10.00", 500, 102),  # yc 108 -> same row
        _cell("B2", 10, 300),  # yc 306 -> new row
    ]
    rows = cluster_rows(cells, y_tol=9)
    assert len(rows) == 2
    assert [c["text"] for c in rows[0]] == ["A1", "€10.00"]
    assert [c["text"] for c in rows[1]] == ["B2"]


def test_cluster_rows_splits_beyond_tolerance():
    # centres 106 and 124 differ by 18 > tol 9 -> separate rows
    cells = [_cell("A1", 10, 100), _cell("€10.00", 500, 118)]
    rows = cluster_rows(cells, y_tol=9)
    assert len(rows) == 2


def test_cluster_rows_sorts_each_row_by_x():
    # supply cells out of left-to-right order; row must come back x-sorted
    cells = [_cell("right", 500, 100), _cell("left", 10, 102), _cell("mid", 200, 101)]
    rows = cluster_rows(cells, y_tol=9)
    assert len(rows) == 1
    assert [c["text"] for c in rows[0]] == ["left", "mid", "right"]


# ── rightmost_money (>= width*0.55 band, rightmost wins) ─────────────────────


def test_rightmost_money_picks_rightmost_in_band():
    width = 1000
    row = [
        _cell("A1", 10, 100),  # left, ignored (text not money anyway)
        _cell("€10.00", 600, 100),  # x0 600 >= 550, in band
        _cell("€99.00", 800, 100),  # x0 800, rightmost in band -> wins
    ]
    best = rightmost_money(row, width)
    assert best is not None
    cell, value = best
    assert value == 99.00
    assert cell["x0"] == 800


def test_rightmost_money_ignores_left_of_threshold():
    width = 1000
    # money sits at x0 400 < width*0.55 (=550) -> excluded -> no result
    row = [_cell("€10.00", 400, 100)]
    assert rightmost_money(row, width) is None


def test_rightmost_money_none_when_no_money():
    width = 1000
    row = [_cell("Advertising", 600, 100), _cell("nil", 800, 100)]
    assert rightmost_money(row, width) is None


# ── parse_item_row (Part-4 line item) ────────────────────────────────────────


def test_parse_item_row_happy():
    width = 1000
    # Ref on the far left (< width*0.30 = 300), description in the middle, cost on the right
    row = [
        _cell("A16", 10, 100),
        _cell("Meta ads", 350, 100),
        _cell("€24,853.05", 800, 100),
    ]
    out = parse_item_row(row, width)
    assert out is not None
    assert out["ref"] == "A16"
    assert out["section"] == "4A"
    assert out["category"] == "Advertising"
    assert out["item_description"] == "Meta ads"
    assert out["cost_eur"] == 24853.05
    assert out["cost_confidence"] == 1.0
    assert out["row_min_confidence"] == 1.0


def test_parse_item_row_letter_maps_section():
    width = 1000
    # F-prefix ref -> 4F Transport and Travel
    row = [_cell("F3", 10, 100), _cell("Taxi", 350, 100), _cell("€42.00", 800, 100)]
    out = parse_item_row(row, width)
    assert out["section"] == "4F"
    assert out["category"] == "Transport and Travel"
    assert out["ref"] == "F3"


def test_parse_item_row_rejects_when_no_ref():
    width = 1000
    # no Ref cell at all -> None
    row = [_cell("Meta ads", 350, 100), _cell("€24,853.05", 800, 100)]
    assert parse_item_row(row, width) is None


def test_parse_item_row_rejects_ref_too_far_right():
    width = 1000
    # an A16-looking cell but at x0 400 (>= width*0.30 = 300) is not accepted as a Ref
    row = [_cell("A16", 400, 100), _cell("€24,853.05", 800, 100)]
    assert parse_item_row(row, width) is None


def test_parse_item_row_no_cost_yields_null_cost():
    width = 1000
    # ref + description but no money cell in the right band -> cost None, description kept
    row = [_cell("E2", 10, 100), _cell("Printer paper", 350, 100)]
    out = parse_item_row(row, width)
    assert out is not None
    assert out["section"] == "4E"
    assert out["cost_eur"] is None
    assert out["item_description"] == "Printer paper"


def test_parse_item_row_rounds_min_confidence():
    width = 1000
    row = [
        _cell("A1", 10, 100, score=0.91),
        _cell("Flyers", 350, 100, score=0.72),
        _cell("€100.00", 800, 100, score=0.999),
    ]
    out = parse_item_row(row, width)
    assert out["row_min_confidence"] == 0.72
    assert out["cost_confidence"] == 0.999


# ── parse_summary_row (Expenses-Review category totals) ──────────────────────


def test_parse_summary_row_section_and_total():
    width = 1000
    row = [_cell("4A - Advertising", 50, 100), _cell("€12,345.00", 700, 100)]
    out = parse_summary_row(row, width)
    assert out is not None
    assert out["section"] == "4A"
    assert out["category"] == "Advertising"
    assert out["category_total_eur"] == 12345.00
    assert out["is_overall"] is False
    assert out["total_confidence"] == 1.0


def test_parse_summary_row_alternate_5_scheme_normalises():
    width = 1000
    # the 5J..5R alternate numbering normalises to canonical 4A..4H (5J -> 4A)
    row = [_cell("5J - Advertising", 50, 100), _cell("€500.00", 700, 100)]
    out = parse_summary_row(row, width)
    assert out["section"] == "4A"
    assert out["category"] == "Advertising"


def test_parse_summary_row_overall_total():
    width = 1000
    row = [_cell("Overall Expense total:", 50, 100), _cell("€100,000.00", 700, 100)]
    out = parse_summary_row(row, width)
    assert out is not None
    assert out["section"] == "TOTAL"
    assert out["category"] == "Overall Expense total"
    assert out["category_total_eur"] == 100000.00
    assert out["is_overall"] is True


def test_parse_summary_row_excludes_section_token_from_money():
    width = 1000
    # the '4C' token must not be misread as €4; the real €0.00 heading total wins
    row = [_cell("4C", 50, 100), _cell("Election Posters", 250, 100), _cell("€0.00", 700, 100)]
    out = parse_summary_row(row, width)
    assert out["section"] == "4C"
    assert out["category"] == "Election Posters"
    assert out["category_total_eur"] == 0.0


def test_parse_summary_row_position_agnostic_money_left():
    width = 1000
    # parties flip the column order: money may sit to the LEFT of the section token
    row = [_cell("€7,500.00", 50, 100), _cell("4B - Publicity", 700, 100)]
    out = parse_summary_row(row, width)
    assert out["section"] == "4B"
    assert out["category_total_eur"] == 7500.00


def test_parse_summary_row_rejects_non_summary_row():
    width = 1000
    # no section token and not an overall-total row -> None
    row = [_cell("John Murphy", 50, 100), _cell("€10.00", 700, 100)]
    assert parse_summary_row(row, width) is None


def test_parse_summary_row_section_without_money_is_none():
    width = 1000
    # section token present but no parseable amount anywhere -> None
    row = [_cell("4A - Advertising", 50, 100), _cell("not a number", 700, 100)]
    assert parse_summary_row(row, width) is None
