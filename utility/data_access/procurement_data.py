"""Procurement (eTenders) data access — thin Streamlit wrapper over dail_tracker_core.

This module is now a THIN adapter. The retrieval SQL and the QueryResult
state-handling live in ``dail_tracker_core.queries.procurement``; this file owns
only the Streamlit caching (``st.cache_resource`` for the connection,
``st.cache_data`` for per-query memoisation) and unwraps ``QueryResult`` to the
DataFrame the page expects.

Return contract is preserved EXACTLY: on a source failure the core returns an
``unavailable`` QueryResult whose ``.data`` is an empty DataFrame, so callers see
the same empty frame the old ``_safe`` produced. The richer ok/unavailable
distinction is now available in core for a future page revision that wants to
render "source unavailable" explicitly instead of an empty state.

Forbidden here (unchanged contract): JOIN / GROUP BY / HAVING / WINDOW in SQL,
CREATE VIEW, read_parquet, pandas merge/pivot, business-metric definitions — all
of which live in sql_views/ and dail_tracker_core.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import procurement as _q
from dail_tracker_core.results import QueryResult


@st.cache_resource
def get_procurement_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["procurement_*.sql"], swallow_errors=True)


# ── QueryResult-returning siblings ────────────────────────────────────────────
# The page boundary uses these to distinguish "source unavailable" (a missing
# view/parquet/DuckDB error) from "ran, no rows" — the ``.data`` wrappers below
# collapse both to an empty DataFrame for back-compat. Caching a frozen
# QueryResult is safe (see dail_tracker_core/results.py).
@st.cache_data(ttl=300)
def fetch_supplier_summary_result(limit: int | None = None) -> QueryResult:
    return _q.supplier_summary(get_procurement_conn(), limit=limit)


@st.cache_data(ttl=300)
def fetch_authority_summary_result(limit: int = 50) -> QueryResult:
    return _q.authority_summary(get_procurement_conn(), limit=limit)


@st.cache_data(ttl=300)
def fetch_cpv_summary_result(limit: int = 50) -> QueryResult:
    return _q.cpv_summary(get_procurement_conn(), limit=limit)


@st.cache_data(ttl=300)
def fetch_lobbying_overlap_result() -> QueryResult:
    return _q.lobbying_overlap(get_procurement_conn())


@st.cache_data(ttl=300)
def fetch_supplier_summary(limit: int | None = None) -> pd.DataFrame:
    """Supplier ranking — one row per distinct supplier (company-class), ordered by
    contract count (the trustworthy metric). Carries CRO match + lobbying flags."""
    return _q.supplier_summary(get_procurement_conn(), limit=limit).data


@st.cache_data(ttl=300)
def fetch_awards_for_supplier(supplier_norm: str) -> pd.DataFrame:
    """Every award row for one supplier (detail view), most recent first."""
    return _q.awards_for_supplier(get_procurement_conn(), supplier_norm).data


@st.cache_data(ttl=300)
def fetch_authority_summary(limit: int = 50) -> pd.DataFrame:
    """Contracting authorities ranked by number of awards."""
    return _q.authority_summary(get_procurement_conn(), limit=limit).data


@st.cache_data(ttl=300)
def fetch_cpv_summary(limit: int = 50) -> pd.DataFrame:
    """CPV categories ranked by number of awards."""
    return _q.cpv_summary(get_procurement_conn(), limit=limit).data


@st.cache_data(ttl=300)
def fetch_lobbying_overlap() -> pd.DataFrame:
    """Companies on BOTH the procurement and lobbying registers (co-occurrence
    disclosure only — never causation; see the view header)."""
    return _q.lobbying_overlap(get_procurement_conn()).data
