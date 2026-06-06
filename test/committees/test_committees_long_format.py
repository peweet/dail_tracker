"""Unit tests for the pure reshape helpers in
committees/committees_long_format_etl.py.

The ETL unpivots wide ``committee_N_*`` / ``office_N_*`` member slots into
long-format rows. Every transform decision (URL slugging, chamber-suffix
strip, status normalisation, chair detection, empty-slot skipping, slot
discovery) lives in a pure function — these lock that logic without touching
parquet I/O.
"""

from __future__ import annotations

import pytest

from committees.committees_long_format_etl import (
    _committee_slot_records,
    _committee_slug,
    _committee_url,
    _detect_slots,
    _office_slot_records,
)

# ── _committee_slug ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,expected",
    [
        ("Dáil Committee on Health", "health-dail"),
        ("Seanad Committee on Public Petitions", "public-petitions-seanad"),
        ("Joint Committee on Finance", "finance"),
        ("Select Committee on Justice", "justice"),
        ("Committee on Budgetary Oversight", "budgetary-oversight"),
        (None, None),
    ],
)
def test_committee_slug(name, expected):
    assert _committee_slug(name) == expected


def test_committee_slug_strips_accents_and_punctuation():
    slug = _committee_slug("Joint Committee on Gnóthaí, Public & Spending!")
    # NFKD-folded to ascii, lowercased, punctuation dropped, whitespace runs
    # collapse to a single hyphen (\s+ → "-").
    assert slug == "gnothai-public-spending"
    assert slug == slug.lower()


# ── _committee_url ───────────────────────────────────────────────────────────


def test_committee_url_happy_path():
    assert _committee_url("Joint Committee on Health", 34) == "https://www.oireachtas.ie/en/committees/34/health/"


def test_committee_url_accepts_numeric_string_dail():
    assert _committee_url("Joint Committee on Health", "34").endswith("/committees/34/health/")


@pytest.mark.parametrize(
    "name,dail", [(None, 34), ("Joint Committee on Health", None), ("Joint Committee on Health", "not-a-number")]
)
def test_committee_url_none_on_bad_input(name, dail):
    assert _committee_url(name, dail) is None


# ── _committee_slot_records ──────────────────────────────────────────────────


def _row(**over):
    base = {
        "full_name": "Mary Lou McDonald",
        "party": "Sinn Féin",
        "constituency_name": "Dublin Central",
        "dail_number": 34,
        "committee_1_name_en": "Joint Committee on Health (Dáil Éireann)",
        "committee_1_role_title": "Cathaoirleach",
        "committee_1_type": "Joint",
        "committee_1_main_status": "Live",
        "committee_1_role_start_date": "2024-01-01 00:00:00+00:00",
    }
    base.update(over)
    return base


def test_committee_record_full_shape():
    rec = _committee_slot_records(_row(), 1, "Dáil")
    assert rec is not None
    # chamber-suffix stripped from the committee name.
    assert rec["committee"] == "Joint Committee on Health"
    assert rec["chamber"] == "Dáil"
    assert rec["name"] == "Mary Lou McDonald"
    # "Live" → "Active" via _STATUS_MAP.
    assert rec["status"] == "Active"
    # Cathaoirleach role → is_chair True.
    assert rec["is_chair"] is True
    assert rec["committee_url"] == "https://www.oireachtas.ie/en/committees/34/health/"


def test_committee_record_empty_slot_returns_none():
    assert _committee_slot_records(_row(committee_1_name_en=None), 1, "Dáil") is None
    assert _committee_slot_records(_row(committee_1_name_en=""), 1, "Dáil") is None


def test_committee_record_defaults_and_status_fallback():
    rec = _committee_slot_records(
        _row(committee_1_role_title=None, committee_1_type=None, committee_1_main_status="Mystery", party=None),
        1,
        "Seanad",
    )
    assert rec["role"] == "Member"  # role default
    assert rec["is_chair"] is False
    assert rec["type"] == "Unknown"
    assert rec["status"] == "Unknown"  # unmapped status → Unknown
    assert rec["party"] == "Unknown"  # null party → Unknown


def test_committee_record_start_falls_back_to_member_date():
    # No role-start, but a member-start date present → start uses the fallback.
    rec = _committee_slot_records(
        _row(committee_1_role_start_date=None, committee_1_member_start_date="2023-02-02 00:00:00+00:00"),
        1,
        "Dáil",
    )
    assert rec["start"] == "2023-02-02 00:00:00+00:00"


# ── _office_slot_records ─────────────────────────────────────────────────────


def test_office_record_shape_and_strip():
    row = {
        "full_name": "Seán Ó Fearghaíl",
        "party": "Fianna Fáil",
        "office_1_name": "  Ceann Comhairle  ",
        "office_1_start_date": "2020-01-01",
        "office_1_end_date": None,
    }
    rec = _office_slot_records(row, 1, "Dáil")
    assert rec == {
        "chamber": "Dáil",
        "name": "Seán Ó Fearghaíl",
        "party": "Fianna Fáil",
        "office": "Ceann Comhairle",  # whitespace stripped
        "start": "2020-01-01",
        "end": None,
    }


def test_office_record_empty_returns_none():
    assert _office_slot_records({"office_2_name": None}, 2, "Dáil") is None
    assert _office_slot_records({"office_2_name": ""}, 2, "Dáil") is None


# ── _detect_slots ────────────────────────────────────────────────────────────


def test_detect_slots_finds_and_sorts_indices():
    cols = [
        "full_name",
        "committee_1_name_en",
        "committee_3_role_title",
        "committee_2_type",
        "office_1_name",
        "party",
    ]
    assert _detect_slots(cols, "committee") == [1, 2, 3]
    assert _detect_slots(cols, "office") == [1]
    assert _detect_slots(cols, "absent") == []
