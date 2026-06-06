"""Unit tests for iris/public_appointments_enrichment.py.

The module turns Iris ``public_appointment`` notices (English + curated-Irish,
judicial + state-board + special-adviser) into the gold appointments entity.
Every extraction decision lives in a pure text/df function — no I/O — so these
drive them with hand-built notice text and assert the classification,
name/role/body extraction, the curated Irish→English maps, the exclusion
filters (military / bankrupt / contaminant), and the composed English summary.
"""

from __future__ import annotations

import polars as pl

from iris.public_appointments_enrichment import (
    appointing_authority,
    appointment_type,
    enrich,
    english_summary,
    extract_appointees,
    extract_body,
    extract_role,
    looks_like_name,
    _who,
)


# ── looks_like_name ──────────────────────────────────────────────────────────


def test_looks_like_name_accepts_real_names():
    assert looks_like_name("Carol Gibbons")
    assert looks_like_name("Mary Lou McDonald")


def test_looks_like_name_rejects_non_names():
    assert not looks_like_name("")
    assert not looks_like_name("23 January 2024")  # digits
    assert not looks_like_name("Department of Health")  # stop-word
    assert not looks_like_name("(Judge of the Circuit Court)")  # parenthetical role
    assert not looks_like_name("AS")  # bare post-nominal, no word >= 3 chars
    assert not looks_like_name("x" * 61)  # too long


# ── appointing_authority ─────────────────────────────────────────────────────


def test_appointing_authority():
    assert appointing_authority("The President of Ireland has appointed …") == "President"
    assert appointing_authority("The Government today appointed …") == "Government"
    assert appointing_authority("The Minister for Health has appointed …") == "Minister"
    # department-header title with no explicit appointer → ministerial act
    assert appointing_authority("appointed to the board", title="Department of Health") == "Minister"
    assert appointing_authority("Notice of something unrelated") == "Unknown"


# ── appointment_type (precedence: military > special_adviser > judicial > board)


def test_appointment_type_precedence():
    assert appointment_type("special_adviser_appointment", "", "appointed as LIEUTENANT in the ARMY") == "military"
    assert appointment_type("special_adviser_appointment", "", "ordinary text") == "special_adviser"
    assert appointment_type("", "BREITHEAMH DEN CHÚIRT DÚICHE", "a cheapadh") == "judicial"
    assert appointment_type("", "Appointment to the Board of Bord Bia", "the Minister appointed") == "state_board"


# ── extract_role ─────────────────────────────────────────────────────────────


def test_extract_role():
    assert extract_role("anything", "special_adviser") == "Special adviser"
    assert extract_role("appointed as President of the High Court", "judicial") == "Court President"
    assert extract_role("appointed as a Judge", "judicial") == "Judge"
    assert extract_role("appointed Jane as a member of the board", "state_board") == "Member"
    assert extract_role("appointed as chairperson of the board", "state_board") == "Chairperson"
    # Irish role via the curated MAR <role> map
    assert extract_role("a cheapadh mar CHATHAOIRLEACH ar an mbord", "state_board") == "Chairperson"


# ── extract_body (curated Irish→English + title parsing) ─────────────────────


def test_extract_body_judicial_uses_curated_court_map():
    assert extract_body("", "BREITHEAMH A CHEAPADH DEN CHÚIRT DÚICHE", "judicial") == "District Court"
    # judicial with no recognised court → generic "Courts"
    assert extract_body("", "appointed as a judge", "judicial") == "Courts"


def test_extract_body_state_board_from_title():
    # curated body name normalises via the BODIES map
    assert extract_body("Appointment to the Board of Bord Bia", "txt", "state_board") == "Bord Bia"
    # un-curated body: the cleaned title segment is returned verbatim
    assert extract_body("Appointment to the Board of Acme Authority", "txt", "state_board") == "Acme Authority"


# ── extract_appointees ───────────────────────────────────────────────────────


def test_extract_appointees_english_single_on_verb_line():
    t = "The Minister has appointed Ms Carol Gibbons as a member of the board."
    assert extract_appointees(t) == ["Ms Carol Gibbons"]


def test_extract_appointees_english_multiple_with_and():
    t = "The Minister appointed John Smith and Mary Jones as board members."
    assert extract_appointees(t) == ["John Smith", "Mary Jones"]


def test_extract_appointees_none_when_no_person_named():
    # a special-adviser Order names no individual
    t = "Notice of the Appointment of Special Advisers Order 2024."
    assert extract_appointees(t) == []


# ── _who / english_summary ───────────────────────────────────────────────────


def test_who_collapses_multiple():
    assert _who([]) == "—"
    assert _who(["Jane Roe"]) == "Jane Roe"
    assert _who(["A", "B", "C"]) == "A + 2 others"


def test_english_summary_board_and_judicial():
    assert (
        english_summary("Minister", "state_board", ["Jane Roe"], "Member", "Bord Bia", None, "t")
        == "Minister appointed Jane Roe as Member, Bord Bia"
    )
    assert (
        english_summary("President", "judicial", ["John Doe"], "Judge", "High Court", None, "t")
        == "President appointed John Doe as Judge, High Court"
    )


# ── enrich (the row-wise integration + exclusion filters) ────────────────────


def _src(rows):
    return pl.DataFrame(rows)


def test_enrich_board_row_and_exclusions():
    df = _src(
        [
            {
                "raw_text": "The Minister for Health has appointed Ms Carol Gibbons "
                "as a member of the board of Bord Bia.",
                "title": "Appointment to the Board of Bord Bia",
                "notice_subtype": "",
                "notice_ref": "R1",
                "issue_date": "2024-01-01",
                "source_file": "a.pdf",
            },
            {  # bankrupt notice → excluded
                "raw_text": "NOTICE that John Doe was ADJUDICATED BANKRUPT on 1 Jan 2024.",
                "title": "Bankruptcy",
                "notice_subtype": "",
                "notice_ref": "R2",
                "issue_date": "2024-01-02",
                "source_file": "b.pdf",
            },
            {  # military commission → excluded
                "raw_text": "appointed as a LIEUTENANT in ÓGLAIGH NA hÉIREANN.",
                "title": "Defence Forces",
                "notice_subtype": "",
                "notice_ref": "R3",
                "issue_date": "2024-01-03",
                "source_file": "c.pdf",
            },
        ]
    )
    out = enrich(df)
    # only the board appointment survives the exclusion filters
    assert out.height == 1
    row = out.row(0, named=True)
    assert row["appointee"] == "Ms Carol Gibbons"
    assert row["appointing_authority"] == "Minister"
    assert row["appointment_type"] == "state_board"
    assert row["body"] == "Bord Bia"
    assert row["appointee_count"] == 1
    assert row["english_summary"] == "Minister appointed Ms Carol Gibbons as Member, Bord Bia"
