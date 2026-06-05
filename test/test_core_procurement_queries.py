"""Tests for dail_tracker_core.queries.procurement.

Two layers:
  1. Unit (always runs, no data): a query against a connection with no views
     returns an *unavailable* QueryResult — proving DuckDB failures are surfaced,
     not swallowed into a silent empty DataFrame (the old _safe behaviour).
  2. Integration (skips if gold parquet absent): against the real registered
     views, each query returns the columns the published contract expects.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import procurement as q
from dail_tracker_core.results import QueryResult

# The exact column contract each fetcher is responsible for (matches the SQL).
_EXPECTED_COLUMNS = {
    "supplier_summary": {
        "supplier", "supplier_norm", "n_awards", "n_authorities", "awarded_value_safe_eur",
        "company_num", "company_status", "cro_match_method",
        "on_lobbying_register", "lobbying_returns", "is_lobbying_registrant", "is_lobbying_client",
    },
    "authority_summary": {"contracting_authority", "n_awards", "n_suppliers", "awarded_value_safe_eur"},
    "cpv_summary": {"cpv_code", "cpv_description", "n_awards", "n_suppliers", "awarded_value_safe_eur"},
    "lobbying_overlap": {
        "lobby_name", "lobby_side", "supplier", "supplier_norm", "n_lobby_returns",
        "n_award_rows", "n_authorities", "awarded_value_safe_eur",
    },
}


# ---------------------------------------------------------------------------
# 1. Unit — DuckDB failure surfaces as unavailable (no data needed)
# ---------------------------------------------------------------------------


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()  # no views registered → the view does not exist
    try:
        result = q.supplier_summary(conn)
        assert isinstance(result, QueryResult)
        assert result.ok is False
        assert result.unavailable_reason is not None
        assert result.is_empty
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 2. Integration — real views; skip if the gold parquet has not been built
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(["procurement_*.sql"], swallow_errors=True)
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"procurement gold not available: {result.unavailable_reason}")
    return result


def test_supplier_summary_columns(conn):
    r = _result_or_skip(q.supplier_summary(conn, limit=5))
    assert _EXPECTED_COLUMNS["supplier_summary"].issubset(set(r.data.columns))
    assert len(r.data) <= 5  # LIMIT respected


def test_authority_summary_columns(conn):
    r = _result_or_skip(q.authority_summary(conn, limit=5))
    assert _EXPECTED_COLUMNS["authority_summary"].issubset(set(r.data.columns))


def test_cpv_summary_columns(conn):
    r = _result_or_skip(q.cpv_summary(conn, limit=5))
    assert _EXPECTED_COLUMNS["cpv_summary"].issubset(set(r.data.columns))


def test_lobbying_overlap_columns(conn):
    r = _result_or_skip(q.lobbying_overlap(conn))
    assert _EXPECTED_COLUMNS["lobbying_overlap"].issubset(set(r.data.columns))


def test_awards_for_supplier_returns_queryresult(conn):
    # Use a supplier_norm pulled from the summary if data exists; otherwise the
    # query still returns a valid (possibly empty) ok result or unavailable.
    summary = q.supplier_summary(conn, limit=1)
    if not summary.ok or summary.is_empty:
        pytest.skip("no supplier rows to drill into")
    norm = summary.data.iloc[0]["supplier_norm"]
    r = q.awards_for_supplier(conn, norm)
    assert r.ok is True  # the supplier came from the same dataset, so it must resolve
