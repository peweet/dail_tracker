"""Your-Councillors retrieval — Streamlit-free, retrieval-only.

SELECTs against the registered gold views (built by constituency_conn):
  v_la_councillors · v_la_council_meeting_coverage · v_la_councillor_votes ·
  v_la_meeting_agendas · v_la_standing_orders · v_la_chief_executives
All filtering by council/LEA only — no joins/rollups here (those live in sql_views/*).
Returns QueryResult so the page can distinguish "source unavailable" from "no rows".
"""
from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001
        _log.exception("your_councillors query failed")
        return QueryResult.unavailable(f"your_councillors query failed: {exc}")


def councils(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(conn, "SELECT DISTINCT local_authority FROM v_la_councillors ORDER BY local_authority")


def leas(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT DISTINCT lea FROM v_la_councillors WHERE local_authority = ? "
                      "AND lea <> '' ORDER BY lea", [la])


def roster(conn: duckdb.DuckDBPyConnection, la: str, lea: str) -> QueryResult:
    return _run(conn, "SELECT name, party, lea, status FROM v_la_councillors "
                      "WHERE local_authority = ? AND lea = ? ORDER BY name", [la, lea])


def councillor(conn: duckdb.DuckDBPyConnection, la: str, name: str) -> QueryResult:
    return _run(conn, "SELECT name, party, lea, status FROM v_la_councillors "
                      "WHERE local_authority = ? AND name = ?", [la, name])


def coverage(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_la_council_meeting_coverage WHERE local_authority = ?", [la])


def votes(conn: duckdb.DuckDBPyConnection, la: str, member: str) -> QueryResult:
    return _run(conn, "SELECT meeting_date, motion, vote FROM v_la_councillor_votes "
                      "WHERE local_authority = ? AND member = ?", [la, member])


def agendas(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT meeting_date, agenda, source_url FROM v_la_meeting_agendas "
                      "WHERE local_authority = ?", [la])


def standing_orders(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_la_standing_orders WHERE local_authority = ?", [la])


def chief_executive(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """The unelected executive head — reused from the CE roster view for the 'who really
    holds power' card."""
    return _run(conn, "SELECT chief_executive, head_title, appointed_year, source_url "
                      "FROM v_la_chief_executives WHERE local_authority = ?", [la])
