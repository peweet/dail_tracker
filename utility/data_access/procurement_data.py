"""Procurement (eTenders) data access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/procurement_*.sql
- Retrieval SQL for the (future) Procurement page (SELECT / WHERE / ORDER BY / LIMIT only)

Forbidden here (same rules as the Streamlit page file):
- JOIN, multi-column GROUP BY, HAVING, WINDOW in ad-hoc retrieval SQL
- CREATE VIEW / CREATE TABLE / read_parquet
- pandas merge / pivot / business-metric definitions

All aggregation/joins/value-gating live in the views (the firewall layer).
Gold parquets are produced by the `procurement` + `procurement_lobbying` pipeline
chains; if they have not run, fetchers return empty DataFrames gracefully.
"""

from __future__ import annotations

import logging

import duckdb
import pandas as pd
import streamlit as st
from data_access._sql_registry import register_views

_log = logging.getLogger(__name__)


@st.cache_resource
def get_procurement_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    register_views(conn, ["procurement_*.sql"], swallow_errors=True)
    return conn


def _safe(sql: str, params: list | None = None) -> pd.DataFrame:
    try:
        return get_procurement_conn().execute(sql, params or []).df()
    except Exception:
        _log.exception("procurement query failed")
        return pd.DataFrame()


@st.cache_data(ttl=300)
def fetch_supplier_summary(limit: int | None = None) -> pd.DataFrame:
    """Supplier ranking — one row per distinct supplier (company-class), ordered by
    contract count (the trustworthy metric). Carries CRO match + lobbying flags."""
    sql = (
        "SELECT supplier, supplier_norm, n_awards, n_authorities, awarded_value_safe_eur,"
        " company_num, company_status, cro_match_method,"
        " on_lobbying_register, lobbying_returns, is_lobbying_registrant, is_lobbying_client"
        " FROM v_procurement_supplier_summary ORDER BY n_awards DESC"
    )
    if limit is not None:
        sql += " LIMIT ?"
        return _safe(sql, [int(limit)])
    return _safe(sql)


@st.cache_data(ttl=300)
def fetch_awards_for_supplier(supplier_norm: str) -> pd.DataFrame:
    """Every award row for one supplier (detail view), most recent first."""
    return _safe(
        "SELECT tender_id, contracting_authority, cpv_code, cpv_description,"
        " competition_type, award_date, value_eur, value_kind, value_safe_to_sum"
        " FROM v_procurement_awards WHERE supplier_norm = ?"
        " ORDER BY award_date DESC NULLS LAST",
        [supplier_norm],
    )


@st.cache_data(ttl=300)
def fetch_authority_summary(limit: int = 50) -> pd.DataFrame:
    """Contracting authorities ranked by number of awards."""
    return _safe(
        "SELECT contracting_authority, n_awards, n_suppliers, awarded_value_safe_eur"
        " FROM v_procurement_authority_summary ORDER BY n_awards DESC LIMIT ?",
        [int(limit)],
    )


@st.cache_data(ttl=300)
def fetch_cpv_summary(limit: int = 50) -> pd.DataFrame:
    """CPV categories ranked by number of awards."""
    return _safe(
        "SELECT cpv_code, cpv_description, n_awards, n_suppliers, awarded_value_safe_eur"
        " FROM v_procurement_cpv_summary ORDER BY n_awards DESC LIMIT ?",
        [int(limit)],
    )


@st.cache_data(ttl=300)
def fetch_lobbying_overlap() -> pd.DataFrame:
    """Companies on BOTH the procurement and lobbying registers (co-occurrence
    disclosure only — never causation; see the view header)."""
    return _safe(
        "SELECT lobby_name, lobby_side, supplier, supplier_norm, n_lobby_returns,"
        " n_award_rows, n_authorities, awarded_value_safe_eur"
        " FROM v_procurement_lobbying_overlap ORDER BY n_award_rows DESC"
    )
