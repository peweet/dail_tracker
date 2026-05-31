"""Public Appointments data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/appointments_*.sql
- Retrieval SQL for the Public Appointments page (SELECT / WHERE / ORDER BY /
  LIMIT only)

Forbidden here (same rules as the Streamlit page file):
- JOIN, multi-column GROUP BY, HAVING, WINDOW in ad-hoc retrieval SQL
- CREATE VIEW / CREATE TABLE / read_parquet (parquet access lives in the
  registered view, not here)
- pandas merge / pivot / business-metric definitions
"""
from __future__ import annotations

import logging
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_SQL_VIEWS = _PROJECT_ROOT / "sql_views"
_log = logging.getLogger(__name__)


def _absolutize_data_paths(sql: str) -> str:
    # SQL views use literals like read_parquet('data/gold/...'). DuckDB
    # resolves those against CWD, so a Streamlit launch from utility/ would
    # break the query. Rewrite to absolute project paths at registration.
    return sql.replace("'data/", f"'{_PROJECT_ROOT.as_posix()}/data/")


@st.cache_resource
def get_appointments_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("appointments_*.sql")):
        try:
            conn.execute(_absolutize_data_paths(sql_file.read_text(encoding="utf-8")))
        except Exception as e:
            _log.warning("appointments view failed to load: %s | %s", sql_file.name, e)
    return conn


def _safe(sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        return get_appointments_conn().execute(sql, params or []).df()
    except Exception:
        _log.exception("appointments query failed")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_public_appointments() -> pd.DataFrame:
    """Every public-appointment notice as a row — the full v_public_appointments
    view. One registered analytical surface; the page does its filtering,
    faceting, and grouping in pandas off this frame."""
    return _safe("SELECT * FROM v_public_appointments")
