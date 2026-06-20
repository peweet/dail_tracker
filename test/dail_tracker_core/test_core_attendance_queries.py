"""Tests for dail_tracker_core.queries.attendance.

Two layers (mirrors test_core_procurement_queries):
  1. Unit (always runs, no data): a query against a connection with no views
     returns an *unavailable* QueryResult — proving a missing view surfaces as
     a 3-state result, not a silent empty DataFrame.
  2. Integration (skips if the attendance parquet is absent): against the real
     registered views, each query returns the columns the page depends on.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import attendance as q
from dail_tracker_core.results import QueryResult

_EXPECTED_COLUMNS = {
    "distinct_members": {"member_name"},
    "distinct_years": {"year"},
    "missing_members": {
        "member_name",
        "party_name",
        "constituency",
        "ministerial_office",
        "departments_held",
        "missing_reason",
    },
    "year_ranking": {
        "member_name",
        "party_name",
        "constituency",
        "attended_count",
        "is_minister",
        "rank_high",
        "rank_low",
    },
    "chamber_sitting_days": {"year", "sitting_days"},
}


# ---------------------------------------------------------------------------
# 1. Unit — DuckDB failure surfaces as unavailable (no data needed)
# ---------------------------------------------------------------------------


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()  # no views registered → the view does not exist
    try:
        result = q.year_ranking(conn, 2024, "Dáil")
        assert isinstance(result, QueryResult)
        assert result.ok is False
        assert result.unavailable_reason is not None
        assert result.is_empty
    finally:
        conn.close()


def test_summary_probe_unavailable_reads_as_not_ready():
    conn = duckdb.connect()
    try:
        r = q.summary_probe(conn)
        # The wrapper computes (ok and not is_empty); on a bare conn that is False.
        assert (r.ok and not r.is_empty) is False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Integration — real views; skip if the attendance parquet is not built
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["attendance_*.sql"], swallow_errors=True)
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"attendance views not available: {result.unavailable_reason}")
    return result


def test_distinct_members_columns(conn):
    r = _result_or_skip(q.distinct_members(conn, "Dáil"))
    assert _EXPECTED_COLUMNS["distinct_members"].issubset(set(r.data.columns))


def test_distinct_years_columns(conn):
    r = _result_or_skip(q.distinct_years(conn, "Dáil"))
    assert _EXPECTED_COLUMNS["distinct_years"].issubset(set(r.data.columns))


def test_missing_members_columns(conn):
    r = _result_or_skip(q.missing_members(conn))
    assert _EXPECTED_COLUMNS["missing_members"].issubset(set(r.data.columns))


def test_year_ranking_columns_and_limit(conn):
    years = _result_or_skip(q.distinct_years(conn, "Dáil"))
    if years.is_empty:
        pytest.skip("no attendance years on file")
    year = int(years.data["year"].iloc[0])
    r = _result_or_skip(q.year_ranking(conn, year, "Dáil"))
    assert _EXPECTED_COLUMNS["year_ranking"].issubset(set(r.data.columns))
    assert len(r.data) <= 500  # LIMIT respected


def test_chamber_sitting_days_columns(conn):
    r = _result_or_skip(q.chamber_sitting_days(conn, "Seanad"))
    assert _EXPECTED_COLUMNS["chamber_sitting_days"].issubset(set(r.data.columns))
