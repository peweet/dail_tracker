"""Regression coverage for diary_date_from_lines' masthead parsing.

2026-09-04: the Courts Service dropped "THE ... DAY OF" from the masthead
("MONDAY THE 8TH DAY OF JUNE 2026" -> "FRIDAY 4TH SEPTEMBER 2026"), which
silently broke the old _DATE_RE (no match -> diary_date_from_lines returned
None -> the poller refused every diary as "source drift"). These cases pin
both the old and new upstream phrasings so a future reformat is caught by a
failing test rather than a live pipeline chain.
"""

from extractors.legal_diary_extract import diary_date_from_lines


def test_current_masthead_format_no_the_no_day_of():
    lines = ["SOME HEADER", "FRIDAY 4TH SEPTEMBER 2026", "case line"]
    assert diary_date_from_lines(lines) == "2026-09-04"


def test_legacy_masthead_format_with_the_and_day_of():
    lines = ["SOME HEADER", "MONDAY THE 8TH DAY OF JUNE 2026", "case line"]
    assert diary_date_from_lines(lines) == "2026-06-08"


def test_prefers_canonical_allcaps_line_over_mixed_case_mention():
    lines = [
        "Friday the 4th September 2026",  # carried-over mixed-case mention
        "FRIDAY 4TH SEPTEMBER 2026",  # canonical masthead, repeated
        "FRIDAY 4TH SEPTEMBER 2026",
    ]
    assert diary_date_from_lines(lines) == "2026-09-04"


def test_no_date_line_returns_none():
    assert diary_date_from_lines(["no date here", "nothing to see"]) is None


def test_masthead_with_trailing_annotation_beats_unrelated_vacation_list():
    """2026-08-25.docx regression: a vacation notice listed dozens of future
    sitting dates in mixed case, and the true masthead carried a trailing
    "(VACATION)" annotation that broke an exact-match canonical check — the
    parser fell back to `mentioned` and picked an unrelated repeated future
    date (2026-10-20) instead of the real diary date (2026-08-25)."""
    lines = [
        "Wednesday the 26th August 2026",  # vacation-notice list entry
        "Tuesday the 20th October 2026",
        "Tuesday the 20th October 2026",  # repeated -> would win a naive tally
        "TUESDAY THE 25TH DAY OF AUGUST 2026 (VACATION)",  # true masthead
        "HIGH COURT BAIL LIST TUESDAY 25TH AUGUST 2026",  # date not at line start
    ]
    assert diary_date_from_lines(lines) == "2026-08-25"
