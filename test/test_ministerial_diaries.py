"""Unit tests for the ministerial-diaries entry parser (extractors/ministerial_diaries_extract.py).

Pure-function coverage of the date→time→subject state machine that turns a born-digital
diary PDF's text layer into engagement rows. Both published layout generations are pinned:
the 2022-24 long-date form ("04 January 2023") and the 2025-26 short-date form ("9 Feb" with
the year supplied by the filename). The no-year-no-default guard (a short date that can't be
resolved to a real calendar date is dropped, never guessed) is the load-bearing case.
"""

from __future__ import annotations

from datetime import date

from extractors._diary_minister import resolve_minister
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


def test_date_with_trailing_weekday_and_inline_time_range():
    # O'Brien 2025 (Transport/DECC): "DD Month YYYY - Weekday" header + "HH:MM- HH:MM - subject"
    # on one line. Without the trailing-weekday allowance the date never matches and entries
    # inherit the last good date (the 2024-07-08 collapse). The leading "- " is stripped.
    text = "\n".join(
        [
            "27 January 2025 - Monday",
            "11:00- 15:00 - Meeting with Department of Transport officials",
            "28 January 2025 - Tuesday",
            "10:00 - 12:00 - Meeting with DECC Management Board",
        ]
    )
    out = parse_entries(text, default_year=None, default_month=None)
    assert [e["entry_date"] for e in out] == [date(2025, 1, 27), date(2025, 1, 28)]
    assert out[0]["subject"] == "Meeting with Department of Transport officials"


def test_date_with_ordinal_and_trailing_weekday():
    # Ryan H2-2024: "1st July 2024 Monday" (ordinal + trailing weekday, no separator). Each date
    # must advance — the bug stamped every entry with the one date that happened to match.
    text = "\n".join(
        [
            "1st July 2024 Monday",
            "10:10 - 11:40",
            "Meeting with DECC Corporate Governance",
            "2nd July 2024 Tuesday",
            "09:00 - 12:00",
            "GOVERNMENT MEETING",
        ]
    )
    out = parse_entries(text, default_year=None, default_month=None)
    assert [e["entry_date"] for e in out] == [date(2024, 7, 1), date(2024, 7, 2)]


def test_numeric_date_header_subject_on_next_line():
    # Early DETE junior diaries (Halligan/Breen/Mitchell-O'Connor 2016-18 "Meeting Time / Subject"
    # two-column print): a bare "DD/MM/YYYY HH:MM" header on its own line with the subject on the
    # NEXT line. The inline one-liner regex misses these (no same-line subject) → they used to parse
    # to zero (text_layout_unrecognised). A date with NO time is a status row, not a meeting → dropped.
    text = "\n".join(
        [
            "Meeting Time",
            "Subject",
            "02/10/2017 10:30",
            "DBEI Departmental Brexit workshop",
            "02/10/2017 15:00",
            "IDA Ireland Photo Op",
            "03/10/2017",  # date only, no time → "Dáil not sitting" is not an engagement
            "Dáil not sitting",
        ]
    )
    out = parse_entries(text, default_year=None, default_month=None)
    assert len(out) == 2
    assert out[0]["entry_date"] == date(2017, 10, 2)
    assert out[0]["time_slot"] == "10:30"
    assert out[0]["subject"] == "DBEI Departmental Brexit workshop"
    assert out[1]["subject"] == "IDA Ireland Photo Op"


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


def test_inline_ddmmyyyy_layout():
    # Early DETE (Breen/Halligan 2016-18) ship one engagement per line: "DD/MM/YYYY HH:MM subject".
    text = "\n".join(
        [
            "Meeting Time",
            "Subject",
            "09/01/2017 11:00 Enterprise Ireland End of year Results",
            "13/01/2017 14:15 Meeting with the Taoiseach",
        ]
    )
    out = parse_entries(text, default_year=None, default_month=None)
    assert len(out) == 2
    assert out[0]["entry_date"] == date(2017, 1, 9)  # day-first
    assert out[0]["time_slot"] == "11:00"
    assert out[0]["subject"] == "Enterprise Ireland End of year Results"
    assert out[1]["entry_date"] == date(2017, 1, 13)


def test_bare_timestamp_without_subject_is_not_an_entry():
    # A Finance print-timestamp line with nothing after the time must NOT become an engagement.
    text = "\n".join(["14/04/2025 11:35"])
    assert parse_entries(text, default_year=None, default_month=None) == []


def test_multiyear_weekday_list_uses_section_year():
    # HEALTH "April 23 to Jan 25" list: yearless "Sat 1 Apr" headers dated by the running
    # "Month YYYY" section header, so a multi-year document is not stamped with one inferred year.
    text = "\n".join(
        [
            "April 2023",
            "Sat 1 Apr",
            "17:00 – 19:00",
            "Good Friday Agreement event",
            "January 2025",
            "Wed 1 Jan",
            "All Day",
            "Bank Holiday",
        ]
    )
    out = parse_entries(text, default_year=None, default_month=None)
    assert len(out) == 2
    assert out[0]["entry_date"] == date(2023, 4, 1)
    assert out[1]["entry_date"] == date(2025, 1, 1)  # year rolled forward by the section header


def test_taoiseach_attributed_by_date_across_rotation():
    # The Taoiseach's own (surname-less) diary attributes to the holder on the entry date.
    f = "taoiseachs-diary-2023_q2.pdf"
    assert resolve_minister(f, "TAOISEACH", date(2022, 6, 1)) == "Martin"
    assert resolve_minister(f, "TAOISEACH", date(2023, 5, 1)) == "Varadkar"
    assert resolve_minister(f, "TAOISEACH", date(2024, 9, 1)) == "Harris"


def test_dfheris_acronym_not_a_surname():
    # "Minister_DFHERIS_Calendar" must NOT coin the surname "Dfheri"; attribute by date instead.
    f = "Minister_DFHERIS_Calendar_2025.pdf"
    assert resolve_minister(f, "DFHERIS", date(2025, 6, 1)) == "Lawless"
    assert resolve_minister(f, "DFHERIS", date(2023, 6, 1)) == "Harris"
    # a file that DOES carry the surname still wins over the date rule
    assert resolve_minister("Minister_ODonovan_Calendar_2024.pdf", "DFHERIS", date(2024, 9, 1)) == "O'Donovan"


def test_education_attributed_by_date_across_lineage():
    # The Education collection publishes back to 2016 with generic, surname-less
    # "ministers-diary-<month>-<year>.pdf" files → attribute the senior minister by entry date
    # across the full lineage (who_was_minister-verified). This pins the rules the 14k-entry
    # Education attribution rests on (regression guard: only McEntee was covered before).
    f = "ministers-diary-march-2018.pdf"
    assert resolve_minister(f, "EDUCATION", date(2016, 3, 1)) == "O'Sullivan"
    assert resolve_minister(f, "EDUCATION", date(2017, 6, 1)) == "Bruton"
    assert resolve_minister(f, "EDUCATION", date(2019, 6, 1)) == "McHugh"
    assert resolve_minister(f, "EDUCATION", date(2022, 6, 1)) == "Foley"
    assert resolve_minister(f, "EDUCATION", date(2025, 6, 1)) == "McEntee"


def test_education_mos_surname_in_filename_wins_over_date_rule():
    # Michael Moynihan (MoS for Special Education, 2025-) publishes "Minister_Moynihans_Diary_*"
    # in the Education collection — the surname in the filename must win over the senior date rule
    # (which would otherwise stamp McEntee on the 2025 date).
    assert resolve_minister("Minister_Moynihans_Diary_August_2025.pdf", "EDUCATION", date(2025, 8, 1)) == "Moynihan"
    # the surname-less senior file on the same date still resolves to the senior minister
    assert resolve_minister("ministers-diary-august-2025.pdf", "EDUCATION", date(2025, 8, 1)) == "McEntee"


def test_batch_dept_lineage_rules():
    # The 2026-06 batch added the senior-minister date rules for the OCR'd/recovered departments
    # (who_was_minister-verified). Generic surname-less files attribute by entry date.
    # FINANCE: Donohoe predates McGrath (the Dec-2022 handover).
    assert resolve_minister("ministers-diary-december-2022.pdf", "FINANCE", date(2022, 12, 1)) == "Donohoe"
    assert resolve_minister("ministers-diary-december-2022.pdf", "FINANCE", date(2022, 12, 20)) == "McGrath"
    # HEALTH lineage: Harris -> Donnelly -> Carroll MacNeill.
    assert resolve_minister("ministers-diary-2018.pdf", "HEALTH", date(2018, 4, 1)) == "Harris"
    assert resolve_minister("ministers-diary-2022.pdf", "HEALTH", date(2022, 4, 1)) == "Donnelly"
    assert resolve_minister("Ministers_Diary_Jan_25_to_Aug_25.pdf", "HEALTH", date(2025, 4, 1)) == "Carroll MacNeill"
    # JUSTICE: Flanagan predates McEntee.
    assert resolve_minister("ministers-diary-2020.pdf", "JUSTICE", date(2020, 3, 1)) == "Flanagan"
    # DCCS: surname-less senior file -> O'Donovan (the current minister, 2025-).
    assert resolve_minister("Ministers_Diary_Q4.pdf", "DCCS", date(2025, 11, 1)) == "O'Donovan"


def test_mos_guard_handles_underscore_delimited_filename():
    # REGRESSION (2026-06): `\bmos\b` fails on "Q4_2025_MoS_Diary" because the underscores are word
    # chars (no \b between "_" and "M"), so the Minister-of-State guard silently let a DCCS MoS file
    # inherit the senior O'Donovan date rule. The letter-lookaround _MOS_RE keeps MoS files None.
    assert resolve_minister("Q4_2025_MoS_Diary_2.pdf", "DCCS", date(2025, 11, 1)) is None
    assert resolve_minister("minister-of-state-diary-2025.pdf", "DCCS", date(2025, 11, 1)) is None
    # ...but "mos" inside a real word must NOT trip the guard (no false MoS match)
    assert resolve_minister("ministers-diary-2025.pdf", "DCCS", date(2025, 11, 1)) == "O'Donovan"


def test_minical_titles_do_not_drift_year():
    # Outlook export (has the "Mo Tu We Th" mini-cal grid): a January-2026 mini-cal title must NOT
    # roll the running year forward — the entry stays in the file's year.
    export = "\n".join(["Mo Tu We Th Fr Sa Su", "January 2026", "15 December 2025", "10:00 – 10:30", "Briefing"])
    out = parse_entries(export, default_year=2025, default_month=None)
    assert len(out) == 1
    assert out[0]["entry_date"] == date(2025, 12, 15)
    # but a weekday-LIST (no grid) DOES use the section header to date its yearless lines
    weeklist = "\n".join(["January 2026", "Thu 15 Jan", "10:00 – 10:30", "Briefing"])
    out2 = parse_entries(weeklist, default_year=2025, default_month=None)
    assert out2[0]["entry_date"] == date(2026, 1, 15)
