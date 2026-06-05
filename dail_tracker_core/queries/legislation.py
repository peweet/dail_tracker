"""Legislation retrieval — Streamlit-free.

Currently surfaces ``v_bill_amendment_intensity`` (data/silver/parquet/
bill_amendments.parquet): per-bill amendment-list activity — a proxy for how
contested/reworked a bill was. ``amendment_lists`` counts published amendment-list
DOCUMENTS at each stage (numbered + cream lists), not individual amendments, so
the figure is a faithful contestation signal and is never framed as a clause count.

Pure retrieval (SELECT / WHERE / ORDER BY / LIMIT). The aggregation lives in the
view; these functions are plain projections.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_VIEW = "v_bill_amendment_intensity"
_COLS = (
    "bill_id, bill_title, bill_type, bill_status, amendment_lists, distinct_stages,"
    " committee_lists, report_lists, cream_lists, dail_lists, seanad_lists,"
    " first_amendment_date, last_amendment_date"
)

MOST_CONTESTED_LIMIT = 50


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.warning("legislation query failed: %s | params=%s | error=%s", sql[:120], params, exc)
        return QueryResult.unavailable(f"legislation query failed: {exc}")


def most_contested_bills(conn: duckdb.DuckDBPyConnection, limit: int = MOST_CONTESTED_LIMIT) -> QueryResult:
    """Bills ranked by amendment-list activity (most contested first)."""
    return _run(
        conn,
        f"SELECT {_COLS} FROM {_VIEW} ORDER BY amendment_lists DESC, bill_id LIMIT ?",
        [limit],
    )


def amendment_intensity_for_bill(conn: duckdb.DuckDBPyConnection, bill_id: str) -> QueryResult:
    """Amendment activity for one bill (joins to v_legislation_index on bill_id).

    Empty (success, no rows) for a bill that drew no amendments.
    """
    return _run(conn, f"SELECT {_COLS} FROM {_VIEW} WHERE bill_id = ? LIMIT 1", [bill_id])
