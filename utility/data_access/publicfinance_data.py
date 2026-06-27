"""Public-finance data access — thin Streamlit wrapper over dail_tracker_core.

The retrieval SQL + QueryResult handling live in
``dail_tracker_core.queries.publicfinance``; this file owns only the Streamlit
caching and unwraps ``QueryResult`` for callers.

Forbidden here (logic firewall — the checker scans this file): JOIN / GROUP BY /
HAVING / WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot,
business-metric definitions — all of which live in sql_views/ and dail_tracker_core.
"""

from __future__ import annotations

import duckdb
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import publicfinance as _q
from dail_tracker_core.results import QueryResult


@st.cache_resource
def get_publicfinance_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["publicfinance_*.sql"], swallow_errors=True)


@st.cache_data(ttl=600)
def fetch_gov_finance_annual_result() -> QueryResult:
    """National revenue/expenditure/balance per year (the 'share of total spend' denominator)."""
    return _q.gov_finance_annual(get_publicfinance_conn())
