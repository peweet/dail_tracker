"""Local-government ("Who runs your county") data access — thin Streamlit wrapper.

Owns only Streamlit caching. All retrieval SQL lives in
``dail_tracker_core.queries.local_government``; all aggregation/joins/grain-guards
live in ``sql_views/constituency/*``. The council-grain views are registered on the
SAME connection the constituency dossier uses, so we reuse that one cached resource
rather than building a second heavy connection.

Forbidden here (same contract as the other data-access modules): JOIN / GROUP BY /
WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot, business-metric defs.
"""

from __future__ import annotations

import json
from pathlib import Path

import streamlit as st
from data_access.constituency_data import get_constituency_conn

from dail_tracker_core.queries import local_government as _q
from dail_tracker_core.results import QueryResult

_OUTLINES = Path(__file__).resolve().parents[2] / "data" / "_meta" / "local_authority_outlines.json"


@st.cache_data(ttl=600)
def fetch_la_outlines() -> dict:
    """Static simplified SVG outlines (31 local authorities + shared viewbox) for the
    index choropleth — built by reference/local_authority_boundaries_extract.py.
    Display-only reference; returns {} if absent so the page just omits the map."""
    try:
        return json.loads(_OUTLINES.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001 — missing/garbled map data must not break the page
        return {}


@st.cache_data(ttl=600)
def fetch_la_map_layers_result() -> QueryResult:
    """All 31 councils with choropleth layer values + quintile buckets."""
    return _q.map_layers(get_constituency_conn())


@st.cache_data(ttl=600)
def fetch_chief_executives_result() -> QueryResult:
    return _q.chief_executives(get_constituency_conn())


@st.cache_data(ttl=600)
def fetch_national_summary_result() -> QueryResult:
    return _q.national_summary(get_constituency_conn())


@st.cache_data(ttl=300)
def fetch_chief_executive_result(la: str) -> QueryResult:
    return _q.chief_executive(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_collection_rates_result(la: str) -> QueryResult:
    return _q.collection_rates(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_planning_overturn_result(la: str) -> QueryResult:
    return _q.planning_overturn(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_derelict_sites_levy_result(la: str) -> QueryResult:
    return _q.derelict_sites_levy(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_noac_scorecard_result(la: str) -> QueryResult:
    return _q.noac_scorecard(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_cash_signals_result(la: str) -> QueryResult:
    return _q.cash_signals(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_noac_scorecard_history_result(la: str) -> QueryResult:
    return _q.noac_scorecard_history(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_housing_performance_result(la: str) -> QueryResult:
    return _q.housing_performance(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_council_money_result(la: str) -> QueryResult:
    return _q.council_money(get_constituency_conn(), la)
