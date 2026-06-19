"""National Housing screen retrieval — Streamlit-free.

Retrieval-only SQL against the registered ``v_ssha_waiting_list_*`` views (built by
``dail_tracker_core.connections.housing_conn``). All aggregation / unpivot / rollup /
per-capita lives in ``sql_views/housing/*`` — this layer only SELECTs and filters by
grain/area, returning a ``QueryResult`` so the page can tell "source unavailable" from
"no rows".
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
        _log.exception("housing query failed")
        return QueryResult.unavailable(f"housing query failed: {exc}")


def waiting_list_totals(conn: duckdb.DuckDBPyConnection, grain: str) -> QueryResult:
    """League-table headline per area at one grain ('county' | 'la' | 'national'):
    waiting total, YoY, %4yr+/%7yr+, population, waiters-per-1,000. Ordered by size."""
    return _run(
        conn,
        "SELECT * FROM v_ssha_waiting_list_totals WHERE grain = ? ORDER BY waiting_total DESC",
        [grain],
    )


def waiting_list_composition(conn: duckdb.DuckDBPyConnection, grain: str, area: str) -> QueryResult:
    """The five demographic breakdowns for one area (the distribution stripes).
    ord-then-count ordering is applied in the view."""
    return _run(
        conn,
        "SELECT dimension, category, ord, count, pct FROM v_ssha_waiting_list_composition "
        "WHERE grain = ? AND area = ? AND year = 2025",
        [grain, area],
    )
