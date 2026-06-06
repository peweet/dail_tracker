"""Tests for dail_tracker_core.queries.member_overview.

Two layers (mirrors the other core query tests):
  1. Unit (always runs, no data): a query against a connection with no views
     returns an *unavailable* QueryResult (the old page helper `_q` swallowed
     errors AND `conn is None` into a silent empty DataFrame; both now map to
     unavailable — `conn is None` is asserted here too).
  2. Integration (skips if the member-overview conn can't be built): real views,
     column contracts for the main retrieval fns + the dynamic filter builders.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

from dail_tracker_core.queries import member_overview as q
from dail_tracker_core.results import QueryResult

_EXPECTED_COLUMNS = {
    "member_list": {"unique_member_code", "member_name", "party_name", "constituency", "house"},
    "identity_attendance": {"member_name", "party_name", "constituency", "is_minister", "year"},
    "att_all_years": {"year", "attended_count", "is_minister"},
    "question_feed": {"question_date", "question_type", "ministry", "topic", "question_text",
                      "question_ref", "oireachtas_url"},
    "debate_sections": {"debate_date", "debate_section_id", "chamber", "topic",
                        "question_count", "oireachtas_url"},
}


# ---------------------------------------------------------------------------
# 1. Unit
# ---------------------------------------------------------------------------


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()
    try:
        r = q.member_list(conn)
        assert isinstance(r, QueryResult)
        assert r.ok is False
        assert r.unavailable_reason is not None
        assert r.is_empty
    finally:
        conn.close()


def test_none_connection_is_unavailable():
    r = q.member_list(None)
    assert r.ok is False
    assert r.is_empty


# ---------------------------------------------------------------------------
# 2. Integration — uses the production 4-phase connection builder
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    util = Path(__file__).resolve().parents[1] / "utility"
    if str(util) not in sys.path:
        sys.path.insert(0, str(util))
    try:
        from data_access.member_overview_data import get_member_overview_conn
    except Exception as exc:  # noqa: BLE001 — streamlit/config import side-effects
        pytest.skip(f"member_overview_data not importable: {exc}")
    return get_member_overview_conn()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"member_overview views not available: {result.unavailable_reason}")
    return result


def test_member_list_columns(conn):
    r = _result_or_skip(q.member_list(conn))
    assert _EXPECTED_COLUMNS["member_list"].issubset(set(r.data.columns))


def test_identity_attendance_columns(conn):
    members = _result_or_skip(q.member_list(conn))
    if members.is_empty:
        pytest.skip("no members on file")
    code = str(members.data["unique_member_code"].iloc[0])
    r = _result_or_skip(q.identity_attendance(conn, code))
    assert _EXPECTED_COLUMNS["identity_attendance"].issubset(set(r.data.columns))


def test_att_all_years_columns_and_limit(conn):
    members = _result_or_skip(q.member_list(conn))
    code = str(members.data["unique_member_code"].iloc[0])
    r = _result_or_skip(q.att_all_years(conn, code))
    assert _EXPECTED_COLUMNS["att_all_years"].issubset(set(r.data.columns))
    assert len(r.data) <= 20


def test_join_key_by_name_resolves(conn):
    members = _result_or_skip(q.member_list(conn))
    row = members.data.iloc[0]
    r = _result_or_skip(q.join_key_by_name(conn, str(row["member_name"]), str(row["house"])))
    assert len(r.data) <= 1
    if not r.data.empty:
        assert r.data["unique_member_code"].iloc[0] == row["unique_member_code"]


def test_question_feed_filters_and_columns(conn):
    members = _result_or_skip(q.member_list(conn))
    # find a member with questions
    code = None
    for c in members.data["unique_member_code"].astype(str).tolist():
        yr = q.question_years(conn, c)
        if yr.ok and not yr.is_empty:
            code = c
            break
    if code is None:
        pytest.skip("no member with questions on file")
    r = _result_or_skip(q.question_feed(conn, code))
    assert _EXPECTED_COLUMNS["question_feed"].issubset(set(r.data.columns))
    assert len(r.data) <= 10000
    # ILIKE search filter narrows the set (can only ever drop rows)
    searched = _result_or_skip(q.question_feed(conn, code, search_text="zzznotarealword"))
    assert len(searched.data) <= len(r.data)


def test_debate_sections_columns(conn):
    members = _result_or_skip(q.member_list(conn))
    code = None
    for c in members.data["unique_member_code"].astype(str).tolist():
        dy = q.debate_years(conn, c)
        if dy.ok and not dy.is_empty:
            code = c
            break
    if code is None:
        pytest.skip("no member with debates on file")
    r = _result_or_skip(q.debate_sections(conn, code))
    assert _EXPECTED_COLUMNS["debate_sections"].issubset(set(r.data.columns))
    assert len(r.data) <= 1000
