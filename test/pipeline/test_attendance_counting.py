"""Counting-invariant tests for attendance/attendance.py.

The attendance fact table has a sharp, easy-to-break counting rule that has
regressed more than once:

  * The TAA PDFs publish TWO independent date columns per row (a *sitting* date
    and an *other* date), paired only by row index. The same sitting date can
    appear in N rows paired with N different other dates. Counting ROWS or
    not-null FLAGS therefore inflates the day count; the published meaning is the
    count of DISTINCT dates in each column — ``nunique`` per (member, year).
  * The source PDFs are cumulative: every refresh restates all prior days, so the
    raw concat is 3-9x duplicated and must be deduped on
    (member, sitting_date, other_date) before any counting.

These tests pin both rules so a future refactor of ``_build_fact_table`` can't
silently re-inflate counts. The first group uses synthetic silver rows; the
second group runs the real x-coordinate extractor against the bronze PDFs and
reconciles it with the PDFs' own published per-member Sub-totals — the direct
guard for the continuation-page truncation bug (sitting days capped at ~72/page).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
from attendance import attendance as att
from config import ATTENDANCE_PDF_DIR


def _write_silver(tmp_path: Path, rows: list[dict]) -> Path:
    """Write a minimal silver CSV with the columns _build_fact_table reads."""
    df = pd.DataFrame(rows)
    csv = tmp_path / "silver.csv"
    df.to_csv(csv, index=False)
    return csv


def _build(tmp_path: Path, rows: list[dict], house: str | None = None) -> pd.DataFrame:
    silver = _write_silver(tmp_path, rows)
    fact_csv = tmp_path / "fact.csv"
    fact_parquet = tmp_path / "fact.parquet"
    att._build_fact_table(silver, fact_csv, fact_parquet, house=house)
    return pd.read_csv(fact_csv)


def _counts(fact: pd.DataFrame, identifier: str, year: int) -> tuple[int, int, int]:
    """(sitting_days_count, other_days_count, sitting_total_days) for a member-year.

    The counts are broadcast onto every row of the group, so take the first.
    """
    g = fact[(fact["identifier"] == identifier) & (fact["year"] == year)].iloc[0]
    return int(g["sitting_days_count"]), int(g["other_days_count"]), int(g["sitting_total_days"])


# ── nunique, not row count / flag count ───────────────────────────────────────


def test_repeated_sitting_date_paired_with_different_other_dates_is_not_inflated(tmp_path):
    """The classic pairing-inflation trap.

    Two distinct sitting dates and two distinct other dates appear across four
    paired rows. Row-counting or flag-counting would report 4; the correct answer
    is 2 sitting + 2 other = 4 *total* (but each category count is 2, not 4).
    """
    rows = [
        {"identifier": "A_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-01", "iso_other_days_attendance": "2025-02-01"},
        {"identifier": "A_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-01", "iso_other_days_attendance": "2025-02-02"},
        {"identifier": "A_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-02", "iso_other_days_attendance": "2025-02-01"},
        {"identifier": "A_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-02", "iso_other_days_attendance": "2025-02-02"},
    ]
    fact = _build(tmp_path, rows)
    sitting, other, total = _counts(fact, "A_TD", 2025)
    assert sitting == 2  # NOT 4 (would be 4 if counting rows/flags)
    assert other == 2
    assert total == 4


def test_distinct_sitting_dates_are_counted_once_each(tmp_path):
    rows = [
        {"identifier": "B_TD", "year": 2025, "iso_sitting_days_attendance": d, "iso_other_days_attendance": ""}
        for d in ["2025-01-01", "2025-01-02", "2025-01-03"]
    ]
    fact = _build(tmp_path, rows)
    sitting, other, total = _counts(fact, "B_TD", 2025)
    assert (sitting, other, total) == (3, 0, 3)


# ── cumulative-PDF deduplication ──────────────────────────────────────────────


def test_cumulative_restatement_duplicates_collapse(tmp_path):
    """Three identical (member, sitting, other) rows (a 3x cumulative restatement)
    must collapse to a single counted day, not three."""
    dup = {"identifier": "C_TD", "year": 2025, "iso_sitting_days_attendance": "2025-03-04", "iso_other_days_attendance": "2025-03-05"}
    fact = _build(tmp_path, [dict(dup), dict(dup), dict(dup)])
    sitting, other, total = _counts(fact, "C_TD", 2025)
    assert (sitting, other, total) == (1, 1, 2)


# ── missing dates / NaN handling ──────────────────────────────────────────────


def test_null_other_date_does_not_count_toward_other(tmp_path):
    rows = [
        {"identifier": "D_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-01", "iso_other_days_attendance": ""},
        {"identifier": "D_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-02", "iso_other_days_attendance": ""},
    ]
    fact = _build(tmp_path, rows)
    sitting, other, total = _counts(fact, "D_TD", 2025)
    assert (sitting, other, total) == (2, 0, 2)


# ── per-(member, year) isolation ──────────────────────────────────────────────


def test_counts_are_scoped_per_member_and_year(tmp_path):
    rows = [
        {"identifier": "E_TD", "year": 2024, "iso_sitting_days_attendance": "2024-01-01", "iso_other_days_attendance": ""},
        {"identifier": "E_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-01", "iso_other_days_attendance": ""},
        {"identifier": "E_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-02", "iso_other_days_attendance": ""},
        {"identifier": "F_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-01", "iso_other_days_attendance": ""},
    ]
    fact = _build(tmp_path, rows)
    assert _counts(fact, "E_TD", 2024) == (1, 0, 1)
    assert _counts(fact, "E_TD", 2025) == (2, 0, 2)
    assert _counts(fact, "F_TD", 2025) == (1, 0, 1)


def test_house_tag_is_applied_when_given(tmp_path):
    rows = [{"identifier": "G_TD", "year": 2025, "iso_sitting_days_attendance": "2025-01-01", "iso_other_days_attendance": ""}]
    fact = _build(tmp_path, rows, house="Seanad")
    assert set(fact["house"]) == {"Seanad"}


# ── x-coordinate extraction vs the PDFs' own published totals ─────────────────
# These run against the real bronze TAA PDFs and are the direct regression guard
# for the continuation-page truncation bug: the x-coordinate extractor must
# reproduce each PDF's printed per-member "Sub-total:" figures exactly. They skip
# cleanly when the bronze PDFs aren't in the checkout.

def _verification_pdfs():
    if not ATTENDANCE_PDF_DIR.is_dir():
        return []
    return [p for p in sorted(ATTENDANCE_PDF_DIR.glob("*.pdf")) if att._ATTENDANCE_PDF_MARKER in p.name.lower()]


@pytest.mark.integration
def test_extraction_reconciles_with_published_subtotals_every_pdf():
    """For every verification PDF, extracted distinct (sitting, other) date counts
    equal that PDF's own published Sub-totals — the truncation bug cannot pass."""
    pdfs = _verification_pdfs()
    if not pdfs:
        pytest.skip(f"no verification PDFs in {ATTENDANCE_PDF_DIR}")
    mismatches = att._reconcile_against_published(ATTENDANCE_PDF_DIR)
    assert mismatches == 0, f"{mismatches} member(s) disagree with published Sub-totals"


@pytest.mark.integration
def test_extraction_recovers_continuation_page_sitting_days():
    """A high-attendance member's sitting days must exceed one page (~72) — the exact
    symptom of the old bug, where every busy TD capped at the first page's row count."""
    import fitz

    pdf = next(
        (p for p in _verification_pdfs() if "01-january-2023-to-31-december-2023" in p.name),
        None,
    )
    if pdf is None:
        pytest.skip("2023 full-year verification PDF not present")
    per_member: dict[str, set] = {}
    for ident, _fn, _ln, _text, kind, iso in att._extract_pdf_member_dates(fitz.open(str(pdf))):
        if kind == "sitting":
            per_member.setdefault(ident, set()).add(iso)
    top = max(len(v) for v in per_member.values())
    assert top > 80, (
        f"top 2023 sitting-day count is only {top}; the continuation-page rows are "
        "being dropped again (the ~72-per-page truncation bug)."
    )
