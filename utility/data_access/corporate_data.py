"""Corporate notices data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/corporate_*.sql
- Retrieval SQL for the Corporate page (SELECT / WHERE / ORDER BY / LIMIT only)

Forbidden here (same rules as the Streamlit page file):
- JOIN, multi-column GROUP BY, HAVING, WINDOW in ad-hoc retrieval SQL
- CREATE VIEW / CREATE TABLE / read_parquet
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
    return sql.replace("'data/", f"'{_PROJECT_ROOT.as_posix()}/data/")


@st.cache_resource
def get_corporate_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    for sql_file in sorted(_SQL_VIEWS.glob("corporate_*.sql")):
        try:
            conn.execute(_absolutize_data_paths(sql_file.read_text(encoding="utf-8")))
        except Exception as e:
            _log.warning("corporate view failed to load: %s | %s", sql_file.name, e)
    return conn


def _safe(sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        return get_corporate_conn().execute(sql, params or []).df()
    except Exception:
        _log.exception("corporate query failed")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_corporate_notices() -> pd.DataFrame:
    """Every in-scope corporate notice as a row — the full v_corporate_notices
    view. Personal insolvency is excluded upstream by enrichment. The page
    does its faceting / search / aggregation in pandas off this frame."""
    return _safe("SELECT * FROM v_corporate_notices")


@st.cache_data(ttl=300)
def fetch_cbi_notice_matches() -> pd.DataFrame:
    """Per-notice CBI authorisation lookup (EXPERIMENTAL — sandbox source).

    Returns one row per (corporate notice × CBI register hit) where the
    notice's entity name is an exact normalised match for a CBI-authorised
    firm. Keyed on entity_norm; the Corporate page joins against its own
    pandas frame to render an authorisation badge on each card.
    """
    return _safe("SELECT * FROM v_corporate_cbi_notice_match")


@st.cache_data(ttl=300)
def fetch_cbi_repeat_distress() -> pd.DataFrame:
    """Per-firm repeat-distress aggregate (EXPERIMENTAL — sandbox source).

    Returns CBI-authorised firms with ≥ 2 genuine-distress notices, OR ≥ 3
    notices total including at least one distress event. Members' Voluntary
    Liquidation (solvent fund lifecycle) is suppressed by the HAVING gate to
    keep the panel honest about what is actually distress vs ETF/fund wind-up.
    """
    return _safe("SELECT * FROM v_corporate_cbi_repeat_distress")


@st.cache_data(ttl=600)
def fetch_brand_aliases() -> pd.DataFrame:
    """Brand → parent_fund → fund_type curated alias map.

    Source: data/_meta/loan_book_fund_aliases.csv. Used by the
    Corporate page methodology expander to make the panel's brand-to-parent
    classification provenance visible (so a reader sees Beltany → Goldman
    Sachs without having to inspect the CSV)."""
    csv_path = _PROJECT_ROOT / "data" / "_meta" / "loan_book_fund_aliases.csv"
    if not csv_path.exists():
        return pd.DataFrame(columns=["brand", "parent_fund", "fund_type", "notes"])
    try:
        return pd.read_csv(csv_path)
    except Exception:
        _log.exception("brand alias CSV load failed")
        return pd.DataFrame(columns=["brand", "parent_fund", "fund_type", "notes"])
