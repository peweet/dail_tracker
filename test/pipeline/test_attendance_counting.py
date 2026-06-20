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

These tests pin both rules on synthetic data so a future refactor of
``_build_fact_table`` / ``_build_silver_csv`` can't silently re-inflate counts.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).parents[2]))
from attendance import attendance as att


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


# ── silver dedup (cumulative restatement at the silver layer) ─────────────────


def test_build_silver_csv_dedups_cumulative_rows(tmp_path):
    """_build_silver_csv must collapse the 3-9x cumulative duplication the raw PDF
    concat produces, keyed on (member, sitting_date, other_date)."""
    # PDF-shaped frame: identifier, first/last name, then the two raw date columns
    # named exactly as the PDFs publish them (so the rename path is exercised).
    raw = pd.DataFrame(
        {
            "identifier": ["H_TD"] * 4,
            "first_name": ["Hugh"] * 4,
            "last_name": ["TD"] * 4,
            "Sitting days attendance": ["01/01/2025", "01/01/2025", "02/01/2025", "02/01/2025"],
            "Other days attendance": ["03/01/2025", "03/01/2025", "04/01/2025", "04/01/2025"],
        }
    )
    out = tmp_path / "silver.csv"
    att._build_silver_csv([raw], out)
    silver = pd.read_csv(out)
    # 4 raw rows → 2 distinct (sitting, other) pairs after dedup.
    assert len(silver) == 2
    assert set(silver["year"].astype(str)) == {"2025"}
    assert silver["iso_sitting_days_attendance"].nunique() == 2
