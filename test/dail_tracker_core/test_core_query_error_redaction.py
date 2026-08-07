"""The reusable query runner must not expose DuckDB internals to API/MCP callers."""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import run_query


def test_query_failure_reason_is_public_safe() -> None:
    conn = duckdb.connect()
    try:
        result = run_query(conn, "SELECT * FROM missing_internal_table", label="test", log=logging.getLogger(__name__))
    finally:
        conn.close()

    assert result.ok is False
    assert result.unavailable_reason == "test data source is temporarily unavailable"
    assert "missing_internal_table" not in result.unavailable_reason
    assert "Catalog Error" not in result.unavailable_reason
