"""Tests for dail_tracker_core.queries.charities.

Two layers (same shape as test_core_ministerial_queries):
  1. Unit (always runs): a query with no views registered returns an *unavailable*
     QueryResult — failures are surfaced, not swallowed.
  2. Integration (skips if data/silver/charities/annual_reports.parquet absent):
     the per-year view is one-row-per-(rcn, year), the per-charity series is
     ordered, and the sector rollup is one-row-per-year.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import charities as q
from dail_tracker_core.results import QueryResult

_CONTRACT_COLS = {
    "rcn",
    "registered_charity_name",
    "period_year",
    "period_end_date",
    "gross_income",
    "gross_expenditure",
    "surplus_deficit",
    "gov_share",
    "total_assets",
    "net_assets",
    "employees_full_time",
}


def test_missing_view_is_unavailable_not_silent_empty():
    conn = duckdb.connect()
    try:
        result = q.financials_by_year(conn, 20000028)
        assert isinstance(result, QueryResult)
        assert result.ok is False
        assert result.unavailable_reason is not None
        assert result.is_empty
    finally:
        conn.close()


@pytest.fixture(scope="module")
def conn():
    c = connect_with_views(
        ["charity_financials_by_year.sql", "charity_sector_totals_by_year.sql"],
        swallow_errors=True,
    )
    yield c
    c.close()


def _result_or_skip(result: QueryResult) -> QueryResult:
    if not result.ok:
        pytest.skip(f"charity annual_reports not available: {result.unavailable_reason}")
    return result


def _any_rcn(conn) -> int:
    r = _result_or_skip(q.sector_totals_by_year(conn))
    if r.is_empty:
        pytest.skip("no charity data")
    # Grab a charity with multiple years via a direct probe on the view.
    df = conn.execute("SELECT rcn FROM v_charity_financials_by_year GROUP BY rcn HAVING COUNT(*) >= 2 LIMIT 1").df()
    if df.empty:
        pytest.skip("no multi-year charity")
    return int(df["rcn"].iloc[0])


def test_financials_by_year_columns_and_order(conn):
    rcn = _any_rcn(conn)
    r = _result_or_skip(q.financials_by_year(conn, rcn))
    assert _CONTRACT_COLS.issubset(set(r.data.columns))
    assert len(r.data) >= 2
    years = r.data["period_year"].tolist()
    assert years == sorted(years), "series not ordered oldest→newest"
    assert (r.data["rcn"] == rcn).all()


def test_one_row_per_charity_year(conn):
    # The source can hold up to 3 filings per (rcn, year); the view must collapse them.
    dup = conn.execute(
        "SELECT COUNT(*) FROM ("
        " SELECT rcn, period_year, COUNT(*) c FROM v_charity_financials_by_year"
        " GROUP BY rcn, period_year HAVING c > 1)"
    ).fetchone()[0]
    assert dup == 0, "view is not one-row-per-(rcn, period_year)"


def test_sector_totals_one_row_per_year(conn):
    r = _result_or_skip(q.sector_totals_by_year(conn))
    assert {"period_year", "n_charities", "total_gross_income"}.issubset(set(r.data.columns))
    assert len(r.data) > 0
    assert r.data["period_year"].is_unique
