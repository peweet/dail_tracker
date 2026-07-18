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

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


_run = make_runner("attendance", _log)


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
        "SELECT DISTINCT member_name FROM v_attendance_member_summary WHERE house = ? ORDER BY member_name LIMIT 2000",
        [house],
    )


def distinct_years(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    """Distinct reporting years for a house, most-recent first — the year filter."""
    return _run(
        conn,
        "SELECT DISTINCT year FROM v_attendance_member_year_summary WHERE house = ? ORDER BY year DESC LIMIT 100",
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
        " attended_count, sitting_days, other_days, is_minister, rank_high, rank_low"
        " FROM v_attendance_year_rank WHERE year = ? AND house = ?"
        " ORDER BY rank_high ASC LIMIT 500",
        [year, house],
    )


# ── Participation & absence model (replaces the censored TAA ranking) ─────────


def participation_years(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    """Current-term reporting years for a house, most-recent first."""
    return _run(
        conn,
        "SELECT DISTINCT year FROM v_attendance_participation_turnout WHERE house = ? ORDER BY year DESC LIMIT 20",
        [house],
    )


def participation_turnout(conn: duckdb.DuckDBPyConnection, year: int, house: str = "Dáil") -> QueryResult:
    """Division turnout for a (year, house), worst-first. Office-holders kept with
    their role flags so the UI can context-flag rather than hide them."""
    return _run(
        conn,
        "SELECT unique_member_code, member_name, party_name, constituency,"
        " voted_in, missed, total_divisions, turnout_pct,"
        " is_minister, is_chair, is_leader, role, role_note"
        " FROM v_attendance_participation_turnout WHERE year = ? AND house = ?"
        " ORDER BY turnout_pct ASC, member_name ASC LIMIT 500",
        [year, house],
    )


def participation_absences(conn: duckdb.DuckDBPyConnection, year: int, house: str = "Dáil") -> QueryResult:
    """Longest PHYSICAL-absence runs (consecutive plenary sitting days the member was
    not recorded present) for a (year, house), worst-first, with the sourced
    explanation (if any). Excludes the chair (Ceann/Leas-Cheann Comhairle /
    Cathaoirleach): not voting is their role, so a vote/sitting gap isn't a notable
    absence for them."""
    return _run(
        conn,
        "SELECT unique_member_code, member_name, party_name, longest_run_sitting_days,"
        " run_calendar_days, run_start, run_end, turnout_pct,"
        " is_minister, is_chair, is_leader, role, role_note,"
        " reason_label, source_title, source_url, is_curated"
        " FROM v_attendance_participation_absences"
        " WHERE year = ? AND house = ? AND longest_run_sitting_days > 0"
        "   AND COALESCE(is_chair, FALSE) = FALSE"
        " ORDER BY longest_run_sitting_days DESC, member_name ASC LIMIT 200",
        [year, house],
    )


def participation_divergence(conn: duckdb.DuckDBPyConnection, year: int, house: str = "Dáil") -> QueryResult:
    """ "Badged in, didn't vote" — present in the TAA record but low turnout
    (backbenchers only). Most divergent first (lowest turnout)."""
    return _run(
        conn,
        "SELECT unique_member_code, member_name, party_name, taa_days_present,"
        " votes_cast, total_divisions, turnout_pct"
        " FROM v_attendance_participation_divergence WHERE year = ? AND house = ?"
        " ORDER BY turnout_pct ASC NULLS FIRST LIMIT 100",
        [year, house],
    )


def taa_compliance(conn: duckdb.DuckDBPyConnection, year: int, house: str = "Dáil") -> QueryResult:
    """Members below the 120-day TAA threshold + the allowance deduction. Excludes
    office-holders (not paid TAA on the attendance basis), most-docked first."""
    return _run(
        conn,
        "SELECT t.unique_member_code, t.member_name, t.party_name, t.total_days,"
        " t.days_below_minimum, t.deduction_pct"
        " FROM v_attendance_taa_compliance t"
        " LEFT JOIN v_attendance_participation_turnout p"
        "   ON p.unique_member_code = t.unique_member_code AND p.house = t.house AND p.year = t.year"
        " WHERE t.year = ? AND t.house = ? AND t.meets_120 = FALSE"
        "   AND COALESCE(p.is_minister, FALSE) = FALSE AND COALESCE(p.is_chair, FALSE) = FALSE"
        " ORDER BY t.days_below_minimum DESC, t.member_name ASC LIMIT 200",
        [year, house],
    )


def taa_compliance_summary(conn: duckdb.DuckDBPyConnection, year: int, house: str = "Dáil") -> QueryResult:
    """One-row cleared/below counts for the TAA section header. Excludes office-
    holders (not paid TAA on the attendance basis) so the counts match the list."""
    return _run(
        conn,
        "SELECT count(*) AS n_total,"
        " sum(CASE WHEN t.meets_120 THEN 1 ELSE 0 END) AS n_cleared,"
        " sum(CASE WHEN NOT t.meets_120 THEN 1 ELSE 0 END) AS n_below"
        " FROM v_attendance_taa_compliance t"
        " LEFT JOIN v_attendance_participation_turnout p"
        "   ON p.unique_member_code = t.unique_member_code AND p.house = t.house AND p.year = t.year"
        " WHERE t.year = ? AND t.house = ?"
        "   AND COALESCE(p.is_minister, FALSE) = FALSE AND COALESCE(p.is_chair, FALSE) = FALSE",
        [year, house],
    )


def member_participation(conn: duckdb.DuckDBPyConnection, unique_member_code: str) -> QueryResult:
    """Per-member participation rows across the current term (turnout + role) for
    the member-overview embedded panel."""
    return _run(
        conn,
        "SELECT year, house, voted_in, missed, total_divisions, turnout_pct,"
        " is_minister, is_chair, is_leader, role, role_note"
        " FROM v_attendance_participation_turnout WHERE unique_member_code = ?"
        " ORDER BY year DESC LIMIT 20",
        [unique_member_code],
    )


def member_absences(conn: duckdb.DuckDBPyConnection, unique_member_code: str) -> QueryResult:
    """Per-member absence runs + sourced explanation, current term."""
    return _run(
        conn,
        "SELECT year, house, longest_run_sitting_days, run_calendar_days, run_start, run_end,"
        " reason_label, source_title, source_url"
        " FROM v_attendance_participation_absences WHERE unique_member_code = ?"
        " ORDER BY year DESC LIMIT 20",
        [unique_member_code],
    )


def member_taa(conn: duckdb.DuckDBPyConnection, unique_member_code: str) -> QueryResult:
    """Per-member TAA compliance across the current term."""
    return _run(
        conn,
        "SELECT year, house, total_days, meets_120, days_below_minimum, deduction_pct"
        " FROM v_attendance_taa_compliance WHERE unique_member_code = ?"
        " ORDER BY year DESC LIMIT 20",
        [unique_member_code],
    )


def chamber_sitting_days(conn: duckdb.DuckDBPyConnection, house: str) -> QueryResult:
    """(year, sitting_days) for a house — the data-derived attendance-bar
    denominator (distinct sitting dates actually in the record). Used for BOTH
    chambers now: a member can never exceed it, which kills the old "82 scheduled
    days vs 94 recorded" contradiction. config.SITTING_DAYS_BY_YEAR is kept only
    as an official cross-check (reconciled in test_attendance_data_consistency)."""
    return _run(
        conn,
        "SELECT year, sitting_days FROM v_attendance_chamber_sitting_days WHERE house = ?",
        [house],
    )
