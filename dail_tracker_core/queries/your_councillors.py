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

from dail_tracker_core.queries import run_query
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    return run_query(conn, sql, params, label="your_councillors", log=_log)


def councils(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(conn, "SELECT DISTINCT local_authority FROM v_la_councillors ORDER BY local_authority")


def leas(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(
        conn, "SELECT DISTINCT lea FROM v_la_councillors WHERE local_authority = ? AND lea <> '' ORDER BY lea", [la]
    )


def roster(conn: duckdb.DuckDBPyConnection, la: str, lea: str) -> QueryResult:
    return _run(
        conn,
        "SELECT name, party, lea, status FROM v_la_councillors WHERE local_authority = ? AND lea = ? ORDER BY name",
        [la, lea],
    )


def roster_council(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Every elected member for a whole council (all LEAs) — the API's council-wide roster
    (the page rosters one LEA at a time; the API serves the full council in one call)."""
    return _run(
        conn, "SELECT name, party, lea, status FROM v_la_councillors WHERE local_authority = ? ORDER BY lea, name", [la]
    )


def councillor(conn: duckdb.DuckDBPyConnection, la: str, name: str) -> QueryResult:
    return _run(
        conn, "SELECT name, party, lea, status FROM v_la_councillors WHERE local_authority = ? AND name = ?", [la, name]
    )


def coverage(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_la_council_meeting_coverage WHERE local_authority = ?", [la])


def roll_call_councils(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Councils whose minutes record named roll-call votes (drives the honest coverage copy —
    the page must never hardcode 'currently Carlow' now that the tier set grows)."""
    return _run(
        conn,
        "SELECT local_authority FROM v_la_council_meeting_coverage "
        "WHERE tier = 'roll_call' ORDER BY local_authority",
    )


def votes(conn: duckdb.DuckDBPyConnection, la: str, member: str) -> QueryResult:
    return _run(
        conn,
        "SELECT meeting_date, motion, vote FROM v_la_councillor_votes WHERE local_authority = ? AND member = ?",
        [la, member],
    )


def councillor_payments(conn: duckdb.DuckDBPyConnection, la: str, member: str) -> QueryResult:
    """ACTUAL s.142 register payments for one councillor (year × category, pre-aggregated in
    the view). Only the open-data councils (South Dublin, Dublin City) return rows — the page
    renders the statutory rate schedule for everyone and actuals only where published."""
    return _run(
        conn,
        "SELECT year, category, amount_eur FROM v_la_councillor_payments "
        "WHERE local_authority = ? AND councillor = ? ORDER BY year DESC, amount_eur DESC",
        [la, member],
    )


def plan_directions(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """Every time the Planning Regulator took this council's ADOPTED plan to the Minister, and
    what the Minister did — newest first. A RESERVED-function override: the members zoned, and
    an unelected regulator + a Minister reviewed the vote. plan_outcome distinguishes an override
    (direction_issued) from the Minister DECLINING to override them (minister_declined) — never
    present this as a pure 'times overruled' count.

    ⚠️ Not to be confused with v_la_planning_overturn (the appeals board overturning the CHIEF
    EXECUTIVE's planners) — different actors, different decision, never combine."""
    return _run(
        conn,
        "SELECT plan_name, plan_type, plan_outcome, first_doc_date, last_doc_date, "
        "n_documents, outcome_doc_url "
        "FROM v_la_plan_directions WHERE local_authority = ? "
        "ORDER BY last_doc_date DESC",
        [la],
    )


def agendas(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(
        conn, "SELECT meeting_date, agenda, source_url FROM v_la_meeting_agendas WHERE local_authority = ?", [la]
    )


def standing_orders(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    return _run(conn, "SELECT * FROM v_la_standing_orders WHERE local_authority = ?", [la])


def chief_executive(conn: duckdb.DuckDBPyConnection, la: str) -> QueryResult:
    """The unelected executive head — reused from the CE roster view for the 'who really
    holds power' card."""
    return _run(
        conn,
        "SELECT chief_executive, head_title, appointed_year, source_url "
        "FROM v_la_chief_executives WHERE local_authority = ?",
        [la],
    )
