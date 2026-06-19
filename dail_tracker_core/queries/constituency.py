"""Per-constituency dossier retrieval — Streamlit-free.

Retrieval-only SQL against the registered ``v_constituency_*`` views (built by
``dail_tracker_core.connections.constituency_conn``). All aggregation / joins /
grain-guards live in ``sql_views/constituency/*`` — this layer only SELECTs and
filters by constituency name, returning a ``QueryResult`` so the page can tell
"source unavailable" from "no rows".
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
        _log.exception("constituency query failed")
        return QueryResult.unavailable(f"constituency query failed: {exc}")


def constituency_list(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """All 43 constituencies with demographics + current TD count — the index grid."""
    return _run(conn, "SELECT * FROM v_constituency_registry ORDER BY constituency_name")


def constituency_header(conn: duckdb.DuckDBPyConnection, constituency: str) -> QueryResult:
    """Single-row header (population, per-TD ratio, seats, TD count) for one dossier."""
    return _run(
        conn,
        "SELECT * FROM v_constituency_registry WHERE constituency_name = ?",
        [constituency],
    )


def constituency_members(conn: duckdb.DuckDBPyConnection, constituency: str) -> QueryResult:
    """The constituency's current Dáil TDs (roster cards; drill to member-overview)."""
    return _run(
        conn,
        "SELECT * FROM v_constituency_members WHERE constituency_name = ? ORDER BY member_name",
        [constituency],
    )


def constituency_party_breakdown(conn: duckdb.DuckDBPyConnection, constituency: str) -> QueryResult:
    """Seats per party in the constituency (the party-composition bar)."""
    return _run(
        conn,
        "SELECT * FROM v_constituency_party_breakdown WHERE constituency_name = ? "
        "ORDER BY n_seats DESC, party_name",
        [constituency],
    )


def constituency_council_context(conn: duckdb.DuckDBPyConnection, constituency: str) -> QueryResult:
    """The local authority(ies) serving this area, each with its OWN money side by
    side (revenue / capital / PO / payment — never summed, never apportioned)."""
    return _run(
        conn,
        "SELECT * FROM v_constituency_council_context WHERE constituency_name = ?",
        [constituency],
    )
