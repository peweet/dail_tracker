"""Unit tests for the scanned-diary WEEK-GRID parser (extractors/diary_grid_parse.py).

This is the geometry-aware parser for the OCR'd Outlook week-view scans (DPER/Taoiseach/older
DCCS). It had ZERO coverage despite being brand-new and complex — these pin the load-bearing
behaviour against synthetic OCR cells modelled on the real Ossian Smyth layout:
  * day assigned by x-COLUMN (weekday header) + week-header date, NOT reading order
  * week-start date drives every column via +i days (so month/year rollover is automatic)
  * page chrome ("DFIN Diary", "...Calendar (Ctrl+E)") and venue tags are dropped from subjects
  * a page with no weekday headers is NOT a grid -> returns [] (caller falls back to linear)
"""

from __future__ import annotations

from datetime import date

from extractors.diary_grid_parse import parse_grid, parse_grid_page


def _cell(t, x0, y0, x1=None, y1=None):
    return {"t": t, "x0": x0, "y0": y0, "x1": x1 if x1 is not None else x0 + 200, "y1": y1 if y1 is not None else y0 + 36}


# weekday header row (x-centres from the real Smyth scan); shared by the grid fixtures
_WEEKHDR = [
    _cell("MONDAY", 356, 377, 482),
    _cell("TUESDAY", 946, 377, 1055),
    _cell("WEDNESDAY", 1529, 377, 1677),
    _cell("THURSDAY", 2116, 377, 2247),
    _cell("FRIDAy", 2700, 377, 2795),  # OCR lowercases the last char sometimes
]


def _grid_page(week_header, events):
    return [_cell(week_header, 282, 304, 707), *_WEEKHDR, *events]


def test_event_assigned_to_its_weekday_column():
    page = _grid_page(
        "4 - 8 January 2021",
        [
            _cell("Meeting with IBEC", 360, 660, 520),  # Monday column
            _cell("Meeting with Microsoft", 2705, 662, 3100),  # Friday column
        ],
    )
    ents = parse_grid_page(page, 2021)
    by_subj = {e["subject"]: e["entry_date"] for e in ents}
    assert by_subj["Meeting with IBEC"] == date(2021, 1, 4)  # Monday
    assert by_subj["Meeting with Microsoft"] == date(2021, 1, 8)  # Friday (Mon + 4)


def test_week_header_month_rollover():
    # week spanning two months: Friday column = Monday-start + 4 days rolls into July
    page = _grid_page("28 June - 2 July 2021", [_cell("Cabinet sub-committee", 2705, 660, 3000)])
    ents = parse_grid_page(page, 2021)
    assert ents[0]["entry_date"] == date(2021, 7, 2)


def test_chrome_and_venue_dropped_from_subject():
    page = _grid_page(
        "4 - 8 January 2021",
        [
            _cell("Meeting with Stripe", 360, 660, 520),
            _cell("Microsoft Teams Meeting", 360, 690, 520),  # venue tag, same block
            _cell("DFIN Diary", 360, 720, 520),  # page chrome, same block
        ],
    )
    ents = parse_grid_page(page, 2021)
    subj = " ".join(e["subject"] for e in ents)
    assert "Stripe" in subj
    assert "Teams" not in subj and "DFIN Diary" not in subj


def test_far_apart_cells_are_separate_events():
    page = _grid_page(
        "4 - 8 January 2021",
        [
            _cell("Morning briefing", 360, 600, 520),
            _cell("Afternoon launch", 360, 1200, 520),  # >70px gap → distinct engagement
        ],
    )
    subjects = {e["subject"] for e in parse_grid_page(page, 2021)}
    assert "Morning briefing" in subjects
    assert "Afternoon launch" in subjects


def test_non_grid_page_returns_empty():
    # no weekday headers → not a recognisable grid → [] (caller uses the linear parser instead)
    page = [_cell("Monday 4 January", 100, 100), _cell("10:00 Meeting with X", 100, 140)]
    assert parse_grid_page(page, 2021) == []


def test_unparseable_week_header_returns_empty():
    page = [*_WEEKHDR, _cell("Some random title", 282, 304, 707), _cell("Meeting", 360, 660)]
    assert parse_grid_page(page, 2021) == []  # no week-start date → cannot place columns


def test_parse_grid_aggregates_pages():
    p1 = _grid_page("4 - 8 January 2021", [_cell("Meeting A", 360, 660, 520)])
    p2 = _grid_page("11 - 15 January 2021", [_cell("Meeting B", 2705, 660, 3000)])
    ents = parse_grid([p1, p2], 2021)
    dates = {e["subject"]: e["entry_date"] for e in ents}
    assert dates["Meeting A"] == date(2021, 1, 4)
    assert dates["Meeting B"] == date(2021, 1, 15)  # Friday of the second week
