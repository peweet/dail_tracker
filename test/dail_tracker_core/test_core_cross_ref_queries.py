"""Tests for dail_tracker_core.queries.cross_ref (votes × interests join).

Two layers (mirrors test_core_interests_queries):
  1. Unit (always runs, no data): bad enum / missing selector / missing views all
     return an *unavailable* QueryResult, and the director/shareholder predicates
     carry the nil-declaration guard (regression for the over-count bug where every
     on-register member counted as a director).
  2. Integration (skips if the gold/silver data is absent): real views via
     api_conn, column contracts + count invariants.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.queries import cross_ref as q
from dail_tracker_core.results import QueryResult

# ---------------------------------------------------------------------------
# 1. Unit
# ---------------------------------------------------------------------------


def test_unknown_interest_is_unavailable():
    conn = duckdb.connect()
    try:
        r = q.voting_vs_interests(conn, vote_id="x", interest="spaceship")
        assert isinstance(r, QueryResult)
        assert r.ok is False and r.unavailable_reason is not None
    finally:
        conn.close()


def test_missing_selector_is_unavailable():
    conn = duckdb.connect()
    try:
        r = q.voting_vs_interests(conn, interest="landlord")  # no vote_id, no keyword
        assert r.ok is False
    finally:
        conn.close()


def test_missing_views_is_unavailable_not_silent_empty():
    conn = duckdb.connect()
    try:
        r = q.voting_vs_interests(conn, keyword="housing", interest="landlord")
        assert r.ok is False and r.is_empty
        b = q.division_interest_breakdown(conn, "2021-07-08_76")
        assert b.ok is False
    finally:
        conn.close()


def test_director_shareholder_predicates_carry_nil_guard():
    # Regression: most Directorships/Shares rows are nil "No interests declared";
    # the predicate MUST exclude them or every member counts as a director.
    for key in ("director", "shareholder"):
        assert "no interests declared" in q._INTEREST_SQL[key].lower()
    # landlord/property use real pipeline booleans, no text guard needed.
    assert q._INTEREST_SQL["landlord"] == "landlord_flag"


# ---------------------------------------------------------------------------
# 2. Integration
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    try:
        from dail_tracker_core.connections import api_conn
    except Exception as exc:  # pragma: no cover
        pytest.skip(f"api_conn unavailable: {exc}")
    c = api_conn()
    yield c
    c.close()


def _or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"cross_ref views not available: {result.unavailable_reason}")
    return result


def test_voting_vs_interests_columns_and_landlord_flag(conn):
    r = _or_skip(q.voting_vs_interests(conn, keyword="housing", vote_type="Voted No", interest="landlord"))
    expected = {
        "vote_id", "vote_date", "debate_title", "member_id", "member_name",
        "party_name", "constituency", "vote_type", "held_in_vote_year",
    }
    assert expected.issubset(set(r.data.columns))
    if not r.data.empty:
        assert set(r.data["vote_type"].unique()) <= {"Voted No"}
        assert r.data["held_in_vote_year"].dtype == bool


def test_division_breakdown_counts_within_bounds(conn):
    r = _or_skip(q.division_interest_breakdown(conn, "2021-07-08_76"))
    if r.data.empty:
        pytest.skip("division not present in this data snapshot")
    for _, row in r.data.iterrows():
        assert row["on_register"] <= row["members"]
        for col in ("landlords", "property_owners", "directors", "shareholders"):
            assert 0 <= row[col] <= row["on_register"], f"{col} out of bounds"
