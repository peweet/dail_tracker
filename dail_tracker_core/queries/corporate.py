"""Corporate notices retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/corporate_data.py``. Retrieval-only
SELECTs against the registered ``corporate_*`` views; the page does its own
faceting/search/aggregation in pandas off these frames (unchanged). Build a
connection with ``connect_with_views(["corporate_*.sql"], swallow_errors=True)``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.exception("corporate query failed")
        return QueryResult.unavailable(f"corporate query failed: {exc}")


def corporate_notices(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Every in-scope corporate notice (personal insolvency excluded upstream)."""
    return _run(conn, "SELECT * FROM v_corporate_notices")


def cbi_notice_matches(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-notice CBI authorisation lookup (EXPERIMENTAL — sandbox source)."""
    return _run(conn, "SELECT * FROM v_corporate_cbi_notice_match")


def cbi_repeat_distress(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-firm repeat-distress aggregate (EXPERIMENTAL — sandbox source)."""
    return _run(conn, "SELECT * FROM v_corporate_cbi_repeat_distress")


def brand_aliases(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Brand -> parent_fund -> fund_type curated alias map."""
    return _run(conn, "SELECT * FROM v_corporate_brand_aliases")
