"""
Attendance data-access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/attendance_*.sql

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP_BY_MULTI_DIM, HAVING, WINDOW in ad-hoc retrieval SQL
- Business metric definitions
"""

from __future__ import annotations

import duckdb
import streamlit as st

from data_access._sql_registry import register_views


@st.cache_resource
def get_attendance_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    register_views(conn, ["attendance_*.sql"], swallow_errors=True)
    return conn
