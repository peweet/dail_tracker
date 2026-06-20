"""Unit tests for the ministerial-diaries entry parser (extractors/ministerial_diaries_extract.py).

Pure-function coverage of the date→time→subject state machine that turns a born-digital
diary PDF's text layer into engagement rows. Both published layout generations are pinned:
the 2022-24 long-date form ("04 January 2023") and the 2025-26 short-date form ("9 Feb" with
the year supplied by the filename). The no-year-no-default guard (a short date that can't be
resolved to a real calendar date is dropped, never guessed) is the load-bearing case.
"""

from __future__ import annotations

from datetime import date

from extractors.ministerial_diaries_extract import parse_entries


def test_long_date_layout_2022_24():
    text = "\n".join(
        [
            "Thursday, 04 January 2023",
            "09:30 – 10:30",
            "Meeting with IBEC",
            "11:00 – 12:00",
            "Pre-Cabinet",
        ]
    )
    out = parse_entries(text, default_year=None, default_month=None)
    assert len(out) == 2
    assert out[0]["entry_date"] == date(2023, 1, 4)
    assert out[0]["time_slot"] == "09:30-10:30"
    assert out[0]["subject"] == "Meeting with IBEC"
    assert out[1]["subject"] == "Pre-Cabinet"


def test_short_date_layout_uses_default_year():
    text = "\n".join(
        [
            "9 Feb",
            "10:00 – 12:30",
            "Roundtable with Wind Energy Ireland",
        ]
    )
    out = parse_entries(text, default_year=2025, default_month=2)
    assert len(out) == 1
    assert out[0]["entry_date"] == date(2025, 2, 9)
    assert out[0]["time_slot"] == "10:00-12:30"


def test_inline_subject_on_time_line():
    text = "\n".join(["12 March 2024", "14:00 – 15:00 Cabinet Committee on Housing"])
    out = parse_entries(text, default_year=None, default_month=None)
    assert len(out) == 1
    assert out[0]["subject"] == "Cabinet Committee on Housing"


def test_all_day_slot():
    text = "\n".join(["3 June 2025", "All Day National Ploughing Championships"])
    out = parse_entries(text, default_year=2025, default_month=6)
    assert len(out) == 1
    assert out[0]["time_slot"] == "all-day"
    assert "Ploughing" in out[0]["subject"]


def test_short_date_without_year_is_dropped_not_guessed():
    # A short date with no year and no default cannot be resolved — the no-inference
    # rule says drop it, never fabricate a year. The following entry must not inherit it.
    text = "\n".join(["9 Feb", "10:00 – 11:00", "Mystery meeting"])
    out = parse_entries(text, default_year=None, default_month=None)
    assert out == []


def test_label_and_blank_noise_lines_ignored():
    text = "\n".join(["04 January 2023", "Time", "09:30 – 10:30", "", "Subject", "Meeting with ISME"])
    out = parse_entries(text, default_year=None, default_month=None)
    assert len(out) == 1
    assert out[0]["subject"] == "Meeting with ISME"
