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

from extractors.diary_grid_parse import parse_day_grid, parse_day_grid_page, parse_grid, parse_grid_page


def _cell(t, x0, y0, x1=None, y1=None):
    return {
        "t": t,
        "x0": x0,
        "y0": y0,
        "x1": x1 if x1 is not None else x0 + 200,
        "y1": y1 if y1 is not None else y0 + 36,
    }


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


# ── the 2-column DAY-PAIR weekly layout (Education scans) ─────────────────────────────────
# Days own explicit header cells in a LEFT and RIGHT column; events carry inline times and are
# dated by the nearest day-header above them in the same column (no weekday-column geometry).
def _daypair_page():
    return [
        _cell("8 January 2018 -", 172, 229, 971),  # week header (sets year)
        _cell("January 2018", 2201, 233, 2503),  # mini-cal month label (NOT a day header — no leading day)
        # row band 1: Mon left / Tue right
        _cell("8 January", 164, 744, 420),
        _cell("9 January", 1765, 748, 2022),
        _cell("14:00 - 15:00 Min_Cal: Meeting with the President of UL", 204, 865, 1719),
        _cell("(DES)", 211, 916, 309),  # continuation of the 14:00 event
        _cell("10:00 - 11:30 Min Cal: Attending the PLE evaluation event", 1817, 877, 3270),
    ]


def test_day_pair_event_dated_by_nearest_header_in_column():
    ents = parse_day_grid_page(_daypair_page(), 2018)
    by_subj = {e["subject"]: e for e in ents}
    # left-column event → 8 Jan (Min_Cal marker stripped, continuation appended)
    assert by_subj["Meeting with the President of UL (DES)"]["entry_date"] == date(2018, 1, 8)
    assert by_subj["Meeting with the President of UL (DES)"]["time_slot"] == "14:00-15:00"
    # right-column event → 9 Jan, NOT 8 Jan (column geometry, not reading order)
    assert by_subj["Attending the PLE evaluation event"]["entry_date"] == date(2018, 1, 9)


def test_day_pair_weekday_prefixed_header():
    # the rotated 2021-2022 scans print "Monday 31 May" / "Tuesday 1 June"
    page = [
        _cell("6 June 2021", 200, 200, 700),  # week header → year
        _cell("Monday 31 May", 160, 740, 600),
        _cell("Tuesday 1 June", 1760, 744, 2200),
        _cell("11:00 - 12:15 Quarterly Management Board", 200, 860, 1600),
        _cell("07:20 - 07:50 Media engagements", 1800, 870, 3000),
    ]
    by_subj = {e["subject"]: e["entry_date"] for e in parse_day_grid_page(page, 2021)}
    assert by_subj["Quarterly Management Board"] == date(2021, 5, 31)
    assert by_subj["Media engagements"] == date(2021, 6, 1)


def test_single_column_is_not_day_grid():
    # a daily-LIST scan (one column of date headers) must return [] so the caller uses linear
    page = [
        _cell("8 January 2018", 160, 200, 500),
        _cell("9 January 2018", 160, 600, 500),
        _cell("10:00 - 11:00 Meeting", 160, 260, 900),
    ]
    assert parse_day_grid_page(page, 2018) == []


def test_day_grid_aggregates_pages():
    ents = parse_day_grid([_daypair_page(), _daypair_page()], 2018)
    assert len(ents) == 4  # two identical pages → both parsed (dedup happens at merge, not here)
