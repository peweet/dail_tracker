"""Contracts for the public-payments query facade's registered rollup views."""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import public_payments as q


def test_public_payments_coverage_missing_view_is_unavailable() -> None:
    conn = duckdb.connect()
    try:
        assert q.coverage_stats(conn).ok is False
    finally:
        conn.close()


@pytest.fixture(scope="module")
def conn():
    connection = connect_with_views(["procurement_public_payments.sql"], swallow_errors=True)
    yield connection
    connection.close()


def test_public_payments_coverage_view_contract(conn) -> None:
    result = q.coverage_stats(conn)
    if not result.ok:
        pytest.skip(f"public-payments fact unavailable: {result.unavailable_reason}")

    assert len(result.data) == 1
    assert {
        "n_lines",
        "n_safe_lines",
        "n_publishers",
        "n_suppliers",
        "total_safe_eur",
        "first_year",
        "last_year",
    }.issubset(result.data.columns)


def test_supplier_quarter_totals_read_registered_rollup(conn) -> None:
    summary = q.supplier_summary(conn, limit=1)
    if not summary.ok or summary.is_empty:
        pytest.skip("public-payments fact has no suppliers")

    supplier_normalised = summary.data.iloc[0]["supplier_normalised"]
    result = q.supplier_quarter_totals(conn, supplier_normalised)
    assert result.ok is True
    assert {"year", "quarter", "period", "n_lines", "total_safe_eur"}.issubset(result.data.columns)
