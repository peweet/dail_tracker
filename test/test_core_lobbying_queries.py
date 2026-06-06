"""Tests for dail_tracker_core.queries.lobbying.

Two layers (mirrors test_core_procurement_queries):
  1. Unit (always runs, no data): a query against a connection with no views
     returns an *unavailable* QueryResult — the deliberate replacement for the
     old _safe() swallow-to-empty behaviour. Plus the empty-keyword topic
     short-circuit returns an ok-but-empty result without touching the DB.
  2. Integration (skips if the lobbying gold is absent): against the real
     registered views, each query returns the columns the page depends on.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views, register_views
from dail_tracker_core.queries import lobbying as q
from dail_tracker_core.results import QueryResult

_EXPECTED_COLUMNS = {
    "politician_index": {
        "rank", "member_name", "unique_member_code", "chamber", "position",
        "return_count", "distinct_orgs", "distinct_policy_areas", "first_period", "last_period",
    },
    "org_index": {"lobbyist_name", "sector", "return_count", "rcn", "state_adjacent_flag"},
    "revolving_door": {
        "individual_name", "former_position", "former_chamber", "chamber_display",
        "return_count", "distinct_firms", "distinct_policy_areas", "distinct_politicians_targeted",
    },
    "policy_area_summary": {"public_policy_area", "return_count", "distinct_orgs", "distinct_politicians"},
}


# ---------------------------------------------------------------------------
# 1. Unit — no data needed
# ---------------------------------------------------------------------------


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()  # no views registered → the view does not exist
    try:
        result = q.summary(conn)
        assert isinstance(result, QueryResult)
        assert result.ok is False
        assert result.unavailable_reason is not None
        assert result.is_empty
    finally:
        conn.close()


def test_empty_keywords_short_circuits_to_ok_empty():
    # No DB access needed — the short-circuit must return a valid ok-empty result
    # (the page renders an empty state, not an error), and must NOT raise on a
    # connection with no views.
    conn = duckdb.connect()
    try:
        for r in (q.topic_returns(conn, ()), q.topic_summary(conn, ())):
            assert r.ok is True
            assert r.is_empty
        # whitespace-only keywords are also "no keywords"
        assert q.topic_returns(conn, ("  ", "")).ok is True
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Integration — real views; skip if the lobbying gold is not built
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["lobbying_*.sql"], swallow_errors=False)
    register_views(c, ["charity_financials_by_year.sql"], swallow_errors=True)
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"lobbying views not available: {result.unavailable_reason}")
    return result


def test_summary_single_row(conn):
    r = _result_or_skip(q.summary(conn))
    assert len(r.data) <= 1


def test_politician_index_columns(conn):
    r = _result_or_skip(q.politician_index(conn))
    assert _EXPECTED_COLUMNS["politician_index"].issubset(set(r.data.columns))


def test_org_index_columns_and_exclude_filter(conn):
    full = _result_or_skip(q.org_index(conn, exclude_state_adjacent=False))
    assert _EXPECTED_COLUMNS["org_index"].issubset(set(full.data.columns))
    excl = _result_or_skip(q.org_index(conn, exclude_state_adjacent=True))
    # excluding state-adjacent orgs can only ever drop rows, never add them
    assert len(excl.data) <= len(full.data)


def test_revolving_door_columns_and_limit(conn):
    r = _result_or_skip(q.revolving_door(conn, limit=5))
    assert _EXPECTED_COLUMNS["revolving_door"].issubset(set(r.data.columns))
    assert len(r.data) <= 5
    # limit=None returns the full (>= limited) set
    full = _result_or_skip(q.revolving_door(conn, limit=None))
    assert len(full.data) >= len(r.data)


def test_policy_area_summary_columns(conn):
    r = _result_or_skip(q.policy_area_summary(conn))
    assert _EXPECTED_COLUMNS["policy_area_summary"].issubset(set(r.data.columns))


def test_topic_returns_real_keyword_runs(conn):
    r = _result_or_skip(q.topic_returns(conn, ("housing",)))
    # ran against the real view; column contract present even if zero rows
    assert {"return_id", "lobbyist_name", "public_policy_area", "period_start_date"}.issubset(set(r.data.columns))
