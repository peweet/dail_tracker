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

import duckdb
import streamlit as st

from config import GOLD_VOTE_HISTORY_PARQUET
from data_access._sql_registry import register_views

_PARQUET = GOLD_VOTE_HISTORY_PARQUET.as_posix()


@st.cache_resource
def get_votes_conn():
    conn = duckdb.connect()
    register_views(
        conn,
        ["vote*.sql"],
        substitutions={"{PARQUET_PATH}": _PARQUET},
        swallow_errors=True,
    )
    return conn
