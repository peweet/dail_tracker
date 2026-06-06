"""Tests for dail_tracker_core.queries.sipo (donations + election expenses).

Two layers (mirrors test_core_procurement_queries):
  1. Unit (always runs, no data): a query against a connection with no views
     returns an *unavailable* QueryResult.
  2. Integration (skips if the sipo gold is absent): real views, column
     contracts for both the donations and expenses lenses.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import sipo as q
from dail_tracker_core.results import QueryResult

_EXPECTED_COLUMNS = {
    "donations_totals": {"total_value", "donation_count", "parties"},
    "donations_by_party": {"party", "donation_count", "total_value", "min_value", "max_value", "verify_count"},
    "party_donors": {
        "donor_name", "value_eur", "date_received_raw", "nature",
        "description_of_donor", "needs_verify", "source_page",
    },
    "expenses_totals": {"total_expenditure", "candidate_count", "parties", "excluded_count"},
    "expenses_by_party": {
        "party", "candidate_count", "total_expenditure", "max_expenditure", "verify_count", "excluded_count",
    },
    "party_candidates": {"candidate_name", "constituency", "expenditure_eur", "flag", "is_verified", "source_page"},
}


# ---------------------------------------------------------------------------
# 1. Unit
# ---------------------------------------------------------------------------


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()
    try:
        for r in (q.donations_by_party(conn), q.expenses_by_party(conn)):
            assert isinstance(r, QueryResult)
            assert r.ok is False
            assert r.unavailable_reason is not None
            assert r.is_empty
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Integration
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["sipo_*.sql"], swallow_errors=False)
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"sipo views not available: {result.unavailable_reason}")
    return result


def test_donations_totals_single_row_columns(conn):
    r = _result_or_skip(q.donations_totals(conn))
    assert len(r.data) == 1  # aggregate is always one row
    assert _EXPECTED_COLUMNS["donations_totals"].issubset(set(r.data.columns))


def test_donations_by_party_columns_sorted(conn):
    r = _result_or_skip(q.donations_by_party(conn))
    assert _EXPECTED_COLUMNS["donations_by_party"].issubset(set(r.data.columns))
    vals = r.data["total_value"].tolist()
    assert vals == sorted(vals, reverse=True)  # ORDER BY total_value DESC


def test_party_donors_columns(conn):
    by_party = _result_or_skip(q.donations_by_party(conn))
    if by_party.is_empty:
        pytest.skip("no sipo donation parties on file")
    party = by_party.data["party"].iloc[0]
    r = _result_or_skip(q.party_donors(conn, party))
    assert _EXPECTED_COLUMNS["party_donors"].issubset(set(r.data.columns))


def test_expenses_totals_single_row_columns(conn):
    r = _result_or_skip(q.expenses_totals(conn))
    assert len(r.data) == 1
    assert _EXPECTED_COLUMNS["expenses_totals"].issubset(set(r.data.columns))


def test_expenses_by_party_columns_sorted(conn):
    r = _result_or_skip(q.expenses_by_party(conn))
    assert _EXPECTED_COLUMNS["expenses_by_party"].issubset(set(r.data.columns))
    vals = r.data["total_expenditure"].tolist()
    assert vals == sorted(vals, reverse=True)


def test_party_candidates_columns(conn):
    by_party = _result_or_skip(q.expenses_by_party(conn))
    if by_party.is_empty:
        pytest.skip("no sipo expense parties on file")
    party = by_party.data["party"].iloc[0]
    r = _result_or_skip(q.party_candidates(conn, party))
    assert _EXPECTED_COLUMNS["party_candidates"].issubset(set(r.data.columns))
