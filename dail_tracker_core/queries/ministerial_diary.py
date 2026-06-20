"""Ministerial-diary retrieval — Streamlit-free.

Retrieval-only SELECTs against the registered ``ministerial_diary_*`` views
(sql_views/diary/ministerial_diary_*.sql), which read the gold parquet produced
by extractors/diary_promote_gold.py (the vetted sandbox->gold promotion). The
page does its own faceting / grouping in pandas off these frames.

Build a connection with
``connect_with_views(["ministerial_diary_*.sql"], swallow_errors=True)``.

FRAMING (no inference — surfaced in the page provenance): a diary meeting is
co-occurrence, NOT a lobbying return; counts are coverage-driven; data is
quarterly-in-arrears. The only register cross-ref exposed is the POSITIVE
``corroborated`` flag (met AND lobbied the same minister).
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
        _log.exception("ministerial-diary query failed")
        return QueryResult.unavailable(f"ministerial-diary query failed: {exc}")


def org_overlap(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Organisations ranked by ministerial meetings (+ corroboration / state-body split)."""
    return _run(conn, "SELECT * FROM v_ministerial_diary_org_overlap")


def engagements(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-(engagement x org) drill-down rows (minister, dept, date, subject, source)."""
    return _run(conn, "SELECT * FROM v_ministerial_diary_engagements")


def meetings(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The BROAD landscape — every external meeting (one row each, NO org-match required)."""
    return _run(conn, "SELECT * FROM v_ministerial_diary_meetings")
