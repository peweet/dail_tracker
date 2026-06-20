"""Per-constituency dossier data access — thin Streamlit wrapper over core.

Owns only the Streamlit caching (``st.cache_resource`` for the connection,
``st.cache_data`` for per-query memoisation). All retrieval SQL + the
QueryResult state handling live in ``dail_tracker_core.queries.constituency``;
all aggregation/joins/grain-guards live in ``sql_views/constituency/*``.

Forbidden here (same contract as the other data-access modules): JOIN / GROUP BY
/ WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot, business-metric
definitions.
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb
import streamlit as st

from dail_tracker_core.connections import constituency_conn
from dail_tracker_core.queries import constituency as _q
from dail_tracker_core.results import QueryResult


@st.cache_resource
def get_constituency_conn() -> duckdb.DuckDBPyConnection:
    """One connection per session: member + procurement + constituency views."""
    return constituency_conn()


@st.cache_data(ttl=3600)
def fetch_constituency_outlines() -> dict:
    """Static simplified SVG outlines (43 constituencies + shared viewbox) for the
    locator thumbnail — built by reference/constituency_boundaries_extract.py.
    Display-only reference; returns {} if absent so the page just omits the map."""
    path = Path(__file__).resolve().parents[2] / "data" / "_meta" / "constituency_outlines.json"
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — missing/garbled map data must not break the page
        return {}


@st.cache_data(ttl=600)
def fetch_constituency_list_result() -> QueryResult:
    """All 43 constituencies (demographics + TD count) for the index grid."""
    return _q.constituency_list(get_constituency_conn())


@st.cache_data(ttl=600)
def fetch_constituency_map_layers_result() -> QueryResult:
    """All 43 constituencies with choropleth layer values + quintile buckets."""
    return _q.constituency_map_layers(get_constituency_conn())


@st.cache_data(ttl=300)
def fetch_constituency_header_result(constituency: str) -> QueryResult:
    return _q.constituency_header(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_constituency_members_result(constituency: str) -> QueryResult:
    return _q.constituency_members(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_constituency_party_breakdown_result(constituency: str) -> QueryResult:
    return _q.constituency_party_breakdown(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_constituency_house_work_result(constituency: str) -> QueryResult:
    return _q.constituency_house_work(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_constituency_housing_context_result(constituency: str) -> QueryResult:
    return _q.constituency_housing_context(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_constituency_housing_with_ssha_result(constituency: str) -> QueryResult:
    """Supply-side housing context already LEFT-joined with the SSHA waiting list in the core
    (the join lives in the pipeline, not the page)."""
    return _q.constituency_housing_context_with_ssha(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_constituency_ssha_waiting_list_result(constituency: str) -> QueryResult:
    return _q.constituency_ssha_waiting_list(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_constituency_waiting_composition_result(constituency: str) -> QueryResult:
    return _q.constituency_waiting_composition(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_constituency_council_housing_performance_result(constituency: str) -> QueryResult:
    return _q.constituency_council_housing_performance(get_constituency_conn(), constituency)


@st.cache_data(ttl=300)
def fetch_council_revenue_divisions_result(council: str) -> QueryResult:
    return _q.council_revenue_divisions(get_constituency_conn(), council)


@st.cache_data(ttl=300)
def fetch_council_capital_divisions_result(council: str) -> QueryResult:
    return _q.council_capital_divisions(get_constituency_conn(), council)


@st.cache_data(ttl=300)
def fetch_constituency_council_context_result(constituency: str) -> QueryResult:
    return _q.constituency_council_context(get_constituency_conn(), constituency)
