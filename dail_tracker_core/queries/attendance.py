"""Attendance retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/attendance_data.py``. Retrieval-only
SELECTs against the ``attendance_*`` views (which UNION both chambers with a
``house`` column). All ranking/partitioning/aggregation lives in those views;
these functions only SELECT / WHERE / ORDER BY / LIMIT.

Each function takes an explicit ``conn`` and returns a ``QueryResult`` so a
missing view or DuckDB error surfaces as *unavailable* rather than a silent
empty frame. The thin Streamlit wrapper (``data_access/attendance_data.py``)
reshapes the success frames into the exact dict/bool/DataFrame contracts the
page already depends on.

Build with ``connect_with_views(["attendance_*.sql"], swallow_errors=True)`` —
attendance registers soft (a missing optional view degrades a section to its
empty state rather than taking the whole page down), matching prior behaviour.
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
        _log.warning("attendance query failed: %s | %s", sql[:120], exc)
        return QueryResult.unavailable(f"attendance query failed: {exc}")


def summary_probe(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row liveness probe against v_attendance_summary.

    The wrapper's ``views_ready()`` maps (ok AND has-row) -> True; an
    unavailable result (view not registered) cleanly reads as "not ready"
    instead of raising, which is the readiness check's intent.
    """
    return _run(conn, "SELECT 1 AS one FROM v_attendance_summary LIMIT 1")


def distinct_members(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    """Distinct member names for a house — the page's member dropdown options."""
    return _run(
        conn,
        "SELECT DISTINCT member_name FROM v_attendance_member_summary"
        " WHERE house = ? ORDER BY member_name LIMIT 2000",
        [house],
    )


def distinct_years(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    """Distinct reporting years for a house, most-recent first — the year filter."""
    return _run(
        conn,
        "SELECT DISTINCT year FROM v_attendance_member_year_summary"
        " WHERE house = ? ORDER BY year DESC LIMIT 100",
        [house],
    )


def missing_members(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Roster members with no row in the attendance parquet.

    Two groups via ``missing_reason``: ``office_holder`` (ministers — documented
    TAA gap) and ``no_record_on_file`` (Taoiseach + genuine roster gaps).
    """
    return _run(
        conn,
        "SELECT member_name, party_name, constituency,"
        " ministerial_office, departments_held, missing_reason"
        " FROM v_attendance_missing_members"
        " ORDER BY missing_reason, member_name LIMIT 500",
    )


def year_ranking(conn: duckdb.DuckDBPyConnection, year: int, house: str = "Dáil") -> QueryResult:
    """Attenders for a given year+house, ordered best-first (rank_high ASC).

    Ranks are partitioned by (year, house) in v_attendance_year_rank, so a house
    yields a clean single-chamber ranking (Senators ranked among Senators).
    """
    return _run(
        conn,
        "SELECT member_name, party_name, constituency,"
        " attended_count, is_minister, rank_high, rank_low"
        " FROM v_attendance_year_rank WHERE year = ? AND house = ?"
        " ORDER BY rank_high ASC LIMIT 500",
        [year, house],
    )


def chamber_sitting_days(conn: duckdb.DuckDBPyConnection, house: str) -> QueryResult:
    """(year, sitting_days) for a house — the data-derived attendance-bar
    denominator used for the Seanad (the Dáil keeps SITTING_DAYS_BY_YEAR)."""
    return _run(
        conn,
        "SELECT year, sitting_days FROM v_attendance_chamber_sitting_days WHERE house = ?",
        [house],
    )
