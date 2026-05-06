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

# Ordered — payments_base must precede its dependents
_DOMAIN_FILES = [
    "attendance_member_year_summary.sql",
    "payments_base.sql",
    "payments_member_detail.sql",
    "payments_yearly_evolution.sql",
    "lobbying_revolving_door.sql",
    "legislation_index.sql",
    "v_debate_listings.sql",
]

# {MEMBER_PARQUET_PATH} substituted with absolute path from config
_REGISTRY_FILES = [
    "member_registry.sql",
]

# {PARQUET_PATH} substituted with absolute path from config
_VOTE_FILES = [
    "vote_td_summary.sql",
    "vote_member_detail.sql",
]


def _load_sql(conn, fpath: Path, substitutions: dict[str, str]) -> None:
    if not fpath.exists():
        _log.warning("member_overview: SQL file not found: %s", fpath.name)
        return
    try:
        sql = fpath.read_text(encoding="utf-8")
        for key, val in substitutions.items():
            sql = sql.replace(key, val)
        conn.execute(sql)
    except Exception as exc:
        _log.warning("member_overview view failed: %s | %s", fpath.name, exc)


@st.cache_resource
def get_member_overview_conn():
    if _duckdb is None:
        return None
    conn = _duckdb.connect()

    # Plain views — no path substitution needed
    for fname in _DOMAIN_FILES:
        _load_sql(conn, _SQL_VIEWS / fname, {})

    # Member registry — absolute path injected to avoid CWD ambiguity
    try:
        from config import SILVER_PARQUET_DIR
        member_parquet = (SILVER_PARQUET_DIR / "flattened_members.parquet").as_posix()
        for fname in _REGISTRY_FILES:
            _load_sql(conn, _SQL_VIEWS / fname, {"{MEMBER_PARQUET_PATH}": member_parquet})
    except Exception as exc:
        _log.warning("member_overview: could not load member registry: %s", exc)

    # Vote views — absolute path injected
    try:
        from config import GOLD_VOTE_HISTORY_PARQUET
        vote_parquet = GOLD_VOTE_HISTORY_PARQUET.as_posix()
        for fname in _VOTE_FILES:
            _load_sql(conn, _SQL_VIEWS / fname, {"{PARQUET_PATH}": vote_parquet})
    except Exception as exc:
        _log.warning("member_overview: could not load vote views: %s", exc)

    return conn
