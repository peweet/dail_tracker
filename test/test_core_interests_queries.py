"""Tests for dail_tracker_core.queries.interests.

Two layers (mirrors test_core_procurement_queries):
  1. Unit (always runs, no data): a query against a connection with no views
     returns an *unavailable* QueryResult (replaces the old _safe silent-empty).
  2. Integration (skips if the interests gold is absent): real views, column
     contracts + the AND-together detail filters.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import interests as q
from dail_tracker_core.results import QueryResult

_EXPECTED_COLUMNS = {
    "detail": {
        "member_name", "party_name", "constituency", "declaration_year",
        "interest_category", "interest_text", "landlord_flag", "property_flag",
    },
    "member_index": {
        "rank", "member_name", "party_name", "constituency",
        "total_declarations", "directorship_count", "property_count", "share_count",
        "is_landlord", "is_property_owner",
    },
}


# ---------------------------------------------------------------------------
# 1. Unit
# ---------------------------------------------------------------------------


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()
    try:
        r = q.member_index(conn, "Dáil", 2024)
        assert isinstance(r, QueryResult)
        assert r.ok is False
        assert r.unavailable_reason is not None
        assert r.is_empty
    finally:
        conn.close()


def test_availability_unavailable_reads_as_false():
    conn = duckdb.connect()
    try:
        r = q.availability(conn, "Dáil")
        assert (r.ok and not r.is_empty) is False
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Integration
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(
        ["member_interests_*.sql", "member_zz_interests_*.sql"], swallow_errors=False
    )
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"interests views not available: {result.unavailable_reason}")
    return result


def test_detail_columns_and_limit(conn):
    r = _result_or_skip(q.detail(conn, "Dáil"))
    assert _EXPECTED_COLUMNS["detail"].issubset(set(r.data.columns))
    assert len(r.data) <= 1000


def test_detail_landlord_filter_is_subset(conn):
    full = _result_or_skip(q.detail(conn, "Dáil"))
    landlord = _result_or_skip(q.detail(conn, "Dáil", landlord_only=True))
    # filtering to landlord declarations can only drop rows
    assert len(landlord.data) <= len(full.data)
    if not landlord.data.empty:
        assert bool(landlord.data["landlord_flag"].all())


def test_member_index_columns_sorted_by_rank(conn):
    years = _result_or_skip(q.distinct_years(conn, "Dáil"))
    if years.is_empty:
        pytest.skip("no interests years on file")
    year = int(years.data["declaration_year"].iloc[0])
    r = _result_or_skip(q.member_index(conn, "Dáil", year))
    assert _EXPECTED_COLUMNS["member_index"].issubset(set(r.data.columns))
    ranks = r.data["rank"].tolist()
    assert ranks == sorted(ranks)  # ORDER BY rank


def test_distinct_members_present(conn):
    r = _result_or_skip(q.distinct_members(conn, "Dáil"))
    assert "member_name" in r.data.columns
