"""Public Appointments retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/appointments_data.py``. One registered
analytical surface; the page does its filtering/faceting/grouping in pandas off
this frame. Build with ``connect_with_views(["appointments_*.sql"], swallow_errors=True)``.
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
        _log.exception("appointments query failed")
        return QueryResult.unavailable(f"appointments query failed: {exc}")


def public_appointments(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Every public-appointment notice as a row — the full v_public_appointments view."""
    return _run(conn, "SELECT * FROM v_public_appointments")
