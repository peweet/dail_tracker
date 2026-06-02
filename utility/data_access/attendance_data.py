"""
Attendance data-access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/attendance_*.sql
- All retrieval functions for the attendance pages (attendance.py +
  attendance_overview.py). Pages call these; they never run SQL themselves.

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP_BY_MULTI_DIM, HAVING, WINDOW in ad-hoc retrieval SQL
- Business metric definitions
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st
from data_access._sql_registry import register_views


@st.cache_resource
def get_attendance_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    register_views(conn, ["attendance_*.sql"], swallow_errors=True)
    return conn


# ── Retrieval (SELECT / WHERE / ORDER BY / LIMIT only) ────────────────────────


@st.cache_data(ttl=300)
def views_ready() -> bool:
    return not get_attendance_conn().execute("SELECT 1 FROM v_attendance_summary LIMIT 1").df().empty


@st.cache_data(ttl=300)
def fetch_filter_options() -> dict[str, list]:
    conn = get_attendance_conn()
    # Standalone /attendance is the TD page — scope to Dáil so the unioned
    # Seanad rows don't leak into its member dropdown / year filter.
    members = conn.execute(
        "SELECT DISTINCT member_name FROM v_attendance_member_summary"
        " WHERE house = 'Dáil' ORDER BY member_name LIMIT 2000"
    ).fetchall()
    years = conn.execute(
        "SELECT DISTINCT year FROM v_attendance_member_year_summary"
        " WHERE house = 'Dáil' ORDER BY year DESC LIMIT 100"
    ).fetchall()
    return {
        "members": [r[0] for r in members],
        "years": [r[0] for r in years],
    }


@st.cache_data(ttl=300)
def fetch_missing_members() -> pd.DataFrame:
    """Roster TDs with no row in the attendance parquet.

    Two groups via the `missing_reason` column:
      • office_holder      — ministers/ministers-of-state; documented TAA gap
      • no_record_on_file  — everyone else (Taoiseach + genuine roster gaps)
    """
    return (
        get_attendance_conn()
        .execute(
            "SELECT member_name, party_name, constituency,"
            " ministerial_office, departments_held, missing_reason"
            " FROM v_attendance_missing_members"
            " ORDER BY missing_reason, member_name LIMIT 500"
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_year_ranking(year: int) -> pd.DataFrame:
    """Top and bottom attenders for a given year from v_attendance_year_rank."""
    return (
        get_attendance_conn()
        .execute(
            "SELECT member_name, party_name, constituency,"
            " attended_count, is_minister, rank_high, rank_low"
            " FROM v_attendance_year_rank WHERE year = ? AND house = 'Dáil'"
            " ORDER BY rank_high ASC LIMIT 500",
            [year],
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_global_stats() -> pd.DataFrame:
    return (
        get_attendance_conn()
        .execute(
            "SELECT members_count, sitting_count, first_sitting_date, last_sitting_date"
            " FROM v_attendance_summary LIMIT 1"
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_available_years() -> list[int]:
    """Returns years ASC — oldest on the left, newest on the right."""
    rows = (
        get_attendance_conn()
        .execute(
            "SELECT DISTINCT year FROM v_attendance_member_year_summary"
            " WHERE year IS NOT NULL AND house = 'Dáil' ORDER BY year ASC"
        )
        .fetchall()
    )
    return [int(r[0]) for r in rows]


@st.cache_data(ttl=300)
def fetch_alltime_ranking() -> pd.DataFrame:
    return (
        get_attendance_conn()
        .execute(
            "SELECT member_name, party_name, constituency,"
            " attended_count, sitting_count"
            " FROM v_attendance_member_summary WHERE house = 'Dáil'"
            " ORDER BY attended_count DESC LIMIT 500"
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_year_member_counts() -> pd.DataFrame:
    """Per-year member count — used for the year summary strip.

    Sources v_attendance_year_member_counts (rollup defined in the view).
    """
    return (
        get_attendance_conn()
        .execute(
            "SELECT year, members_count FROM v_attendance_year_member_counts"
            " WHERE house = 'Dáil' ORDER BY year ASC"
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_member_years(td_name: str) -> pd.DataFrame:
    return (
        get_attendance_conn()
        .execute(
            "SELECT CAST(year AS INTEGER) AS year, attended_count"
            " FROM v_attendance_member_year_summary"
            " WHERE member_name = ? ORDER BY year DESC LIMIT 100",
            [td_name],
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_chamber_sitting_days(house: str) -> dict[int, int]:
    """{year: distinct chamber sitting days} for a house — the data-derived
    attendance-bar denominator used for Seanad (Dáil keeps SITTING_DAYS_BY_YEAR).
    """
    rows = (
        get_attendance_conn()
        .execute(
            "SELECT year, sitting_days FROM v_attendance_chamber_sitting_days WHERE house = ?",
            [house],
        )
        .fetchall()
    )
    return {int(r[0]): int(r[1]) for r in rows}


@st.cache_data(ttl=300)
def fetch_member_profile(td_name: str) -> pd.DataFrame:
    return (
        get_attendance_conn()
        .execute(
            "SELECT member_name, party_name, constituency"
            " FROM v_attendance_member_summary WHERE member_name = ? LIMIT 1",
            [td_name],
        )
        .df()
    )


@st.cache_data(ttl=300)
def fetch_member_timeline(td_name: str, year: int) -> pd.DataFrame:
    return (
        get_attendance_conn()
        .execute(
            "SELECT sitting_date, attendance_status"
            " FROM v_attendance_timeline"
            " WHERE member_name = ? AND year(sitting_date) = ?"
            " ORDER BY sitting_date ASC LIMIT 400",
            [td_name, year],
        )
        .df()
    )
