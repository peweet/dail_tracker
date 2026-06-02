"""
Attendance data-access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/attendance_*.sql
- All retrieval functions for the attendance page (attendance.py).
  The page calls these; it never runs SQL itself.

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
def fetch_filter_options(house: str = "Dáil") -> dict[str, list]:
    conn = get_attendance_conn()
    # The attendance views UNION both chambers with a `house` column; scope to
    # the picked house so the member dropdown / year filter stay single-chamber.
    members = conn.execute(
        "SELECT DISTINCT member_name FROM v_attendance_member_summary WHERE house = ? ORDER BY member_name LIMIT 2000",
        [house],
    ).fetchall()
    years = conn.execute(
        "SELECT DISTINCT year FROM v_attendance_member_year_summary WHERE house = ? ORDER BY year DESC LIMIT 100",
        [house],
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
def fetch_year_ranking(year: int, house: str = "Dáil") -> pd.DataFrame:
    """Top and bottom attenders for a given year from v_attendance_year_rank.

    Ranks are partitioned by (year, house) in the view, so passing a house
    yields a clean single-chamber ranking (Senators ranked among Senators).
    """
    return (
        get_attendance_conn()
        .execute(
            "SELECT member_name, party_name, constituency,"
            " attended_count, is_minister, rank_high, rank_low"
            " FROM v_attendance_year_rank WHERE year = ? AND house = ?"
            " ORDER BY rank_high ASC LIMIT 500",
            [year, house],
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
