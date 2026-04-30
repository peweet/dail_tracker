"""
Member Overview data-access layer — unified DuckDB connection.

Loads all per-domain views needed by member_overview.py in dependency order.

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP_BY_MULTI_DIM, HAVING, WINDOW in ad-hoc retrieval SQL
- Business metric definitions
"""
from __future__ import annotations
import logging
from pathlib import Path
import sys

_HERE = Path(__file__).resolve().parent
_UTIL = _HERE.parent
if str(_UTIL) not in sys.path:
    sys.path.insert(0, str(_UTIL))

import streamlit as st

_log = logging.getLogger(__name__)
_SQL_VIEWS = _HERE.parents[1] / "sql_views"

try:
    import duckdb as _duckdb
except ImportError:
    _duckdb = None

# Ordered — payments_base must precede payments_member_detail and payments_yearly_evolution
_DOMAIN_FILES = [
    "attendance_member_year_summary.sql",
    "payments_base.sql",
    "payments_member_detail.sql",
    "payments_yearly_evolution.sql",
    "lobbying_revolving_door.sql",
    "legislation_index.sql",
]

# These views read the vote parquet and need {PARQUET_PATH} substituted
_VOTE_FILES = [
    "vote_td_summary.sql",
    "vote_member_detail.sql",
]


@st.cache_resource
def get_member_overview_conn():
    if _duckdb is None:
        return None
    conn = _duckdb.connect()

    for fname in _DOMAIN_FILES:
        fpath = _SQL_VIEWS / fname
        if not fpath.exists():
            _log.warning("member_overview: SQL file not found: %s", fname)
            continue
        try:
            conn.execute(fpath.read_text(encoding="utf-8"))
        except Exception as exc:
            _log.warning("member_overview view failed: %s | %s", fname, exc)

    try:
        from config import GOLD_VOTE_HISTORY_PARQUET
        parquet = GOLD_VOTE_HISTORY_PARQUET.as_posix()
        for fname in _VOTE_FILES:
            fpath = _SQL_VIEWS / fname
            if not fpath.exists():
                _log.warning("member_overview: vote SQL not found: %s", fname)
                continue
            try:
                sql = fpath.read_text(encoding="utf-8").replace("{PARQUET_PATH}", parquet)
                conn.execute(sql)
            except Exception as exc:
                _log.warning("member_overview vote view failed: %s | %s", fname, exc)
    except Exception as exc:
        _log.warning("member_overview: could not load vote views: %s", exc)

    return conn
