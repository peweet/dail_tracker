"""
Attendance data-access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/attendance_*.sql

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP_BY_MULTI_DIM, HAVING, WINDOW in ad-hoc retrieval SQL
- Business metric definitions
"""

from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import streamlit as st

_log = logging.getLogger(__name__)
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SQL_VIEWS = _PROJECT_ROOT / "sql_views"


def _absolutize_data_paths(sql: str) -> str:
    # SQL views use literals like read_csv_auto('data/silver/...').
    # DuckDB resolves those against CWD, so a Streamlit launch from utility/
    # breaks queries. Rewrite to absolute project paths at registration time.
    return sql.replace("'data/", f"'{_PROJECT_ROOT.as_posix()}/data/")


@st.cache_resource
def get_attendance_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("attendance_*.sql")):
        try:
            conn.execute(_absolutize_data_paths(sql_file.read_text(encoding="utf-8")))
        except Exception as exc:
            _log.warning("attendance view registration failed: %s | %s", sql_file.name, exc)
    return conn
