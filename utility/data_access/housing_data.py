"""National Housing screen data access — thin Streamlit wrapper over core.

Owns only the Streamlit caching (``st.cache_resource`` for the connection,
``st.cache_data`` for per-query memoisation). All retrieval SQL + QueryResult
state live in ``dail_tracker_core.queries.housing``; all aggregation /
unpivot / rollup / per-capita live in ``sql_views/housing/*``.

Forbidden here (same contract as the other data-access modules): JOIN / GROUP BY
/ WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot, business-metric
definitions.
"""

from __future__ import annotations

import duckdb
import streamlit as st

from dail_tracker_core.connections import housing_conn
from dail_tracker_core.queries import housing as _q
from dail_tracker_core.results import QueryResult


@st.cache_resource
def get_housing_conn() -> duckdb.DuckDBPyConnection:
    """One connection per session: the SSHA waiting-list composition + totals views."""
    return housing_conn()


@st.cache_data(ttl=600)
def fetch_waiting_list_totals_result(grain: str) -> QueryResult:
    return _q.waiting_list_totals(get_housing_conn(), grain)


@st.cache_data(ttl=600)
def fetch_waiting_list_composition_result(grain: str, area: str) -> QueryResult:
    return _q.waiting_list_composition(get_housing_conn(), grain, area)


@st.cache_data(ttl=600)
def fetch_housing_supply_national_result() -> QueryResult:
    return _q.supply_national(get_housing_conn())


@st.cache_data(ttl=600)
def fetch_housing_hap_national_result() -> QueryResult:
    return _q.hap_national(get_housing_conn())


@st.cache_data(ttl=600)
def fetch_housing_completions_trend_result() -> QueryResult:
    return _q.completions_trend(get_housing_conn())


@st.cache_data(ttl=600)
def fetch_housing_rent_by_county_result(county: str) -> QueryResult:
    return _q.rent_by_county(get_housing_conn(), county)
