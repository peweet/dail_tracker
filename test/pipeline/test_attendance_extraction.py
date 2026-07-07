"""Unit tests for attendance/attendance.py — the TAA verification-PDF parser.

The bug this module was rewritten to fix: a member's *sitting* dates overflow onto a
headerless continuation page, and the old find_tables() path silently dropped them, so
high-attendance members were truncated to ~one page. The fix attributes each date to a
column by its x-coordinate (_COLUMN_X_SPLIT) and carries the member header across pages.
These tests pin that behaviour with a hand-built in-memory PDF (no fixture files needed)
plus the published-Sub-total reconciliation that is the regression tripwire, and exercise
the per-(member, year) counting in _build_fact_table.

PyMuPDF (fitz) is a pipeline extra; the file skips cleanly when it is absent.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

fitz = pytest.importorskip("fitz")

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from attendance import attendance as att  # noqa: E402

# x-coords either side of the 200.0 split: sitting (left) vs other (right).
_LEFT_X = 78.0
_RIGHT_X = 313.0


def _member_page(doc, name, sitting_dates, other_dates, sub_sitting=None, sub_other=None):
    """Append one member's page: name header, a 'Deputy,' period line, two date columns,
    and (optionally) the published Sub-total/Totals block the reconciler reads."""
    page = doc.new_page(width=600, height=800)
    page.insert_text((_LEFT_X, 40), name)
    page.insert_text((_LEFT_X, 56), "Deputy, 33rd Dail, Attendance Limit: 120")
    y = 90
    for d in sitting_dates:
        page.insert_text((_LEFT_X, y), d)
        y += 16
    y = 90
    for d in other_dates:
        page.insert_text((_RIGHT_X, y), d)
        y += 16
    if sub_sitting is not None:
        page.insert_text((_LEFT_X, y + 30), f"Sub-total: {sub_sitting}")
        page.insert_text((_LEFT_X, y + 46), f"Sub-total: {sub_other}")
        page.insert_text((_LEFT_X, y + 62), "Totals")
    return page


# ── _parse_member_name ───────────────────────────────────────────────────────


def test_parse_member_name_split_on_first_space():
    """identifier = raw header spaces→underscores; name split on the FIRST space only,
    so a multi-word surname keeps the join key (identifier) stable."""
    ident, first, last = att._parse_member_name("Ni Mhurchu Caoimhe")
    assert ident == "Ni_Mhurchu_Caoimhe"
    assert first == "Mhurchu Caoimhe"  # remainder after the first space
    assert last == "Ni"


def test_parse_member_name_single_token():
    ident, first, last = att._parse_member_name("Cher")
    assert ident == "Cher" and first == "Cher" and last == ""


# ── x-coordinate column assignment (the core fix) ────────────────────────────


def test_dates_split_into_sitting_vs_other_by_x():
    doc = fitz.open()
    _member_page(doc, "Murphy Ann", ["01/01/2024", "02/01/2024", "03/01/2024"], ["08/01/2024"])
    rows = att._extract_pdf_member_dates(doc)
    sitting = [text for (_id, _f, _l, text, kind, _iso) in rows if kind == "sitting"]
    other = [text for (_id, _f, _l, text, kind, _iso) in rows if kind == "other"]
    assert sorted(sitting) == ["01/01/2024", "02/01/2024", "03/01/2024"]
    assert other == ["08/01/2024"]
    # every date attributed to the one member whose 'Deputy,' header preceded them
    assert {r[0] for r in rows} == {"Murphy_Ann"}


def test_period_header_lines_are_not_counted_as_dates():
    """The 'Deputy, … Limit: 120' and name lines carry no date cells; only the two date
    columns produce rows — guards against header text leaking into the counts."""
    doc = fitz.open()
    _member_page(doc, "Murphy Ann", ["01/01/2024"], ["08/01/2024"])
    rows = att._extract_pdf_member_dates(doc)
    assert len(rows) == 2  # exactly the two real dates, nothing from the header lines


def test_continuation_page_dates_attributed_to_member():
    """A headerless second page (sitting overflow) must still be attributed to the member
    whose header appeared on the previous page — the exact truncation bug."""
    doc = fitz.open()
    _member_page(doc, "Murphy Ann", ["01/01/2024"], [])
    cont = doc.new_page(width=600, height=800)  # NO header row
    cont.insert_text((_LEFT_X, 90), "02/01/2024")
    cont.insert_text((_LEFT_X, 106), "03/01/2024")
    rows = att._extract_pdf_member_dates(doc)
    sitting = [t for (_i, _f, _l, t, kind, _iso) in rows if kind == "sitting"]
    assert sorted(sitting) == ["01/01/2024", "02/01/2024", "03/01/2024"]


# ── published-Sub-total reconciliation ───────────────────────────────────────


def test_published_totals_parsed_from_subtotal_block():
    doc = fitz.open()
    _member_page(doc, "Murphy Ann", ["01/01/2024", "02/01/2024"], ["08/01/2024"], sub_sitting=2, sub_other=1)
    totals = att._published_totals_for_doc(doc)
    assert totals["Murphy_Ann"] == (2, 1)


def test_extracted_counts_match_published_subtotals(tmp_path):
    """The reconciler must report zero mismatches when the x-split assignment reproduces
    the PDF's own published figures — this is the bug's regression tripwire."""
    doc = fitz.open()
    _member_page(
        doc, "Murphy Ann", ["01/01/2024", "02/01/2024", "03/01/2024"], ["08/01/2024"], sub_sitting=3, sub_other=1
    )
    pdf = tmp_path / "verification-of-attendance-2024.pdf"
    doc.save(str(pdf))
    assert att._reconcile_against_published(tmp_path) == 0


def test_reconcile_flags_a_dropped_sitting_day(tmp_path):
    """If the published sub-total claims more sitting days than the geometry yields, the
    reconciler must count a mismatch (i.e. it would catch a regression of the bug)."""
    doc = fitz.open()
    _member_page(
        doc, "Murphy Ann", ["01/01/2024"], ["08/01/2024"], sub_sitting=5, sub_other=1
    )  # published 5, only 1 extractable
    pdf = tmp_path / "verification-of-attendance-2024.pdf"
    doc.save(str(pdf))
    assert att._reconcile_against_published(tmp_path) == 1


# ── _build_fact_table counting ───────────────────────────────────────────────


def test_build_fact_table_counts_unique_days_per_member_year(tmp_path):
    """sitting/other counts are distinct days per (identifier, year); the total is their
    sum; and `house` tags every row. Drives the real writer off a synthetic silver CSV."""
    silver = pd.DataFrame(
        {
            "identifier": ["Murphy_Ann", "Murphy_Ann", "Murphy_Ann", "Nolan_Bee"],
            "first_name": ["Ann", "Ann", "Ann", "Bee"],
            "last_name": ["Murphy", "Murphy", "Murphy", "Nolan"],
            "sitting_days_attendance": ["01/01/2024", "02/01/2024", None, "01/01/2024"],
            "other_days_attendance": [None, None, "08/01/2024", None],
            "year": [2024, 2024, 2024, 2024],
            "iso_sitting_days_attendance": ["2024-01-01", "2024-01-02", None, "2024-01-01"],
            "iso_other_days_attendance": [None, None, "2024-01-08", None],
        }
    )
    silver_csv = tmp_path / "silver.csv"
    silver.to_csv(silver_csv, index=False)
    fact_csv = tmp_path / "fact.csv"
    fact_parquet = tmp_path / "fact.parquet"

    att._build_fact_table(silver_csv, fact_csv, fact_parquet, house="Dail")

    fact = pd.read_csv(fact_csv)
    ann = fact[fact["identifier"] == "Murphy_Ann"].iloc[0]
    assert ann["sitting_days_count"] == 2
    assert ann["other_days_count"] == 1
    assert ann["sitting_total_days"] == 3  # sitting + other
    bee = fact[fact["identifier"] == "Nolan_Bee"].iloc[0]
    assert bee["sitting_days_count"] == 1 and bee["other_days_count"] == 0
    assert (fact["house"] == "Dail").all()
    assert fact_parquet.exists()
