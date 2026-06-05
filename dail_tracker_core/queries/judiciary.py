"""Judiciary legal-diary retrieval — Streamlit-free.

Retrieval-only SELECTs against the registered ``judiciary_*`` views
(sql_views/judiciary_legal_diary_*.sql), which read the gold parquets produced
by extractors/legal_diary_extract.py. The page does its own faceting / grouping
in pandas off these frames.

Build a connection with
``connect_with_views(["judiciary_*.sql"], swallow_errors=True)``.

PRIVACY: the cases view is the ANONYMISED layer only — statutory in-camera
matters are dropped at the extractor and every natural person is reduced to
initials. There is no un-anonymised text to retrieve here by design.
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
        _log.exception("judiciary query failed")
        return QueryResult.unavailable(f"judiciary query failed: {exc}")


def legal_diary_schedule(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Tier A — judge sitting-sessions (officials only, no party data)."""
    return _run(conn, "SELECT * FROM v_judiciary_legal_diary_schedule")


def legal_diary_counts(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Tier B — per-session case-item counts (aggregate density)."""
    return _run(conn, "SELECT * FROM v_judiciary_legal_diary_counts")


def legal_diary_cases(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Tier C — ANONYMISED case listings + provenance link."""
    return _run(conn, "SELECT * FROM v_judiciary_legal_diary_cases")
