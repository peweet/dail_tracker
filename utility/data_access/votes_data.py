"""
Votes data-access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/vote*.sql
- {PARQUET_PATH} template substitution for parquet-backed views

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP_BY_MULTI_DIM, HAVING, WINDOW in ad-hoc retrieval SQL
- Business metric definitions
"""
from __future__ import annotations
import logging
from pathlib import Path

import streamlit as st

from config import GOLD_VOTE_HISTORY_PARQUET

_log = logging.getLogger(__name__)
_SQL_VIEWS = Path(__file__).resolve().parents[2] / "sql_views"
_PARQUET = GOLD_VOTE_HISTORY_PARQUET.as_posix()

try:
    import duckdb as _duckdb
except ImportError:
    _duckdb = None


@st.cache_resource
def get_votes_conn():
    if _duckdb is None:
        return None
    conn = _duckdb.connect()
    if _SQL_VIEWS.exists():
        for f in sorted(_SQL_VIEWS.glob("vote*.sql")):
            try:
                sql = f.read_text(encoding="utf-8").replace("{PARQUET_PATH}", _PARQUET)
                conn.execute(sql)
            except Exception as exc:
                _log.warning("votes view registration failed: %s | %s", f.name, exc)
    return conn
