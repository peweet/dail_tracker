"""Unit tests for the pure (non-GPU) helpers in extractors/diary_ocr.py.

The OCR itself needs PaddleOCR/GPU and isn't unit-tested, but the deterministic post-processing
is — and was uncovered: (1) cells_to_text reconstructs reading order from OCR cell geometry for
the linear (daily-list) scans; (2) _clamp_year_to_file snaps an OCR year-misread that's off by
exactly one back to the file's known single year (which also fixes the minister attribution that
flows from the date). Imported lazily so a missing paddleocr never blocks these.
"""

from __future__ import annotations

from datetime import date

from extractors.diary_ocr import _clamp_year_to_file, cells_to_text


def _c(t, x0, y0):
    return {"t": t, "x0": x0, "y0": y0, "x1": x0 + 100, "y1": y0 + 30}


def test_cells_to_text_orders_top_to_bottom_then_left_to_right():
    # one page; a y-band groups same-row cells, rows ordered by y
    page = [_c("Subject", 400, 200), _c("10:00", 100, 200), _c("Next line", 100, 260)]
    out = cells_to_text([page])
    # "10:00" (x=100) precedes "Subject" (x=400) on the same y-band row
    assert out == "10:00 Subject\nNext line"


def test_cells_to_text_handles_multiple_pages():
    out = cells_to_text([[_c("Page one", 100, 100)], [_c("Page two", 100, 100)]])
    assert out.splitlines() == ["Page one", "Page two"]


def test_clamp_year_snaps_off_by_one():
    e = {"entry_date": date(2025, 12, 18), "subject": "x"}
    # a Dec-2024 scan misread as 2025 → snap back to the file's known year (keeps month/day)
    assert _clamp_year_to_file(e, 2024)["entry_date"] == date(2024, 12, 18)


def test_clamp_year_leaves_correct_year_untouched():
    e = {"entry_date": date(2024, 6, 1), "subject": "x"}
    assert _clamp_year_to_file(e, 2024)["entry_date"] == date(2024, 6, 1)


def test_clamp_year_ignores_wildly_wrong_years():
    # only an off-by-EXACTLY-one is a confident OCR slip; a 2-year gap is left for inspection
    e = {"entry_date": date(2022, 6, 1), "subject": "x"}
    assert _clamp_year_to_file(e, 2025)["entry_date"] == date(2022, 6, 1)
