"""Attendance data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult state-handling live in
``dail_tracker_core.queries.attendance``; this file owns only the Streamlit
caching and the small presentation-layer reshaping the page already expected
(dict of option lists, a readiness bool, a {year: sitting_days} map).

``get_attendance_conn`` is still exported because ui/attendance_panel.py builds
its own per-TD timeline SELECTs against the same connection — preserving it
keeps that consumer working unchanged.

Forbidden here (unchanged): read_parquet, parquet_scan, CREATE VIEW,
pandas groupby/merge/pivot business logic, multi-dim GROUP BY.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import attendance as _q


@st.cache_resource
def get_attendance_conn() -> duckdb.DuckDBPyConnection:
    # swallow_errors=True preserves the prior register_views(...) behaviour:
    # a missing optional attendance view degrades that section to its empty
    # state rather than taking the whole page down.
    return connect_with_views(["attendance_*.sql"], swallow_errors=True)


# ── Retrieval wrappers (caching + presentation reshaping only) ────────────────


@st.cache_data(ttl=300)
def views_ready() -> bool:
    r = _q.summary_probe(get_attendance_conn())
    return r.ok and not r.is_empty


@st.cache_data(ttl=300)
def fetch_filter_options(house: str = "Dáil") -> dict[str, list]:
    conn = get_attendance_conn()
    members = _q.distinct_members(conn, house)
    years = _q.distinct_years(conn, house)
    return {
        "members": members.data["member_name"].tolist() if (members.ok and not members.is_empty) else [],
        "years": years.data["year"].tolist() if (years.ok and not years.is_empty) else [],
    }


@st.cache_data(ttl=300)
def fetch_missing_members() -> pd.DataFrame:
    """Roster TDs with no row in the attendance parquet (see core docstring)."""
    return _q.missing_members(get_attendance_conn()).data


@st.cache_data(ttl=300)
def fetch_year_ranking(year: int, house: str = "Dáil") -> pd.DataFrame:
    """Top and bottom attenders for a given year (single-chamber)."""
    return _q.year_ranking(get_attendance_conn(), year, house).data


@st.cache_data(ttl=300)
def fetch_chamber_sitting_days(house: str) -> dict[int, int]:
    """{year: distinct chamber sitting days} — the Seanad attendance-bar denominator."""
    r = _q.chamber_sitting_days(get_attendance_conn(), house)
    if not r.ok or r.is_empty:
        return {}
    df = r.data
    return {int(y): int(s) for y, s in zip(df["year"], df["sitting_days"])}
