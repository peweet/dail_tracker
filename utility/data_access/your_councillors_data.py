"""Your-Councillors data access — thin Streamlit cache wrapper (firewall-clean).

Owns only caching. Retrieval SQL lives in dail_tracker_core.queries.your_councillors; all
joins/aggregation live in sql_views/constituency/*. Reuses the cached constituency connection
(the v_la_councillors* views register there).
"""

from __future__ import annotations

import streamlit as st
from data_access.constituency_data import get_constituency_conn

from dail_tracker_core.queries import your_councillors as _q
from dail_tracker_core.results import QueryResult


@st.cache_data(ttl=600)
def fetch_councils() -> QueryResult:
    return _q.councils(get_constituency_conn())


@st.cache_data(ttl=600)
def fetch_leas(la: str) -> QueryResult:
    return _q.leas(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_roster(la: str, lea: str) -> QueryResult:
    return _q.roster(get_constituency_conn(), la, lea)


@st.cache_data(ttl=300)
def fetch_roster_council(la: str) -> QueryResult:
    """Every elected member of a whole council (all LEAs) — for the Your Council
    dossier's inline councillors section, which shows the full council at once."""
    return _q.roster_council(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_councillor(la: str, name: str) -> QueryResult:
    return _q.councillor(get_constituency_conn(), la, name)


@st.cache_data(ttl=600)
def fetch_coverage(la: str) -> QueryResult:
    return _q.coverage(get_constituency_conn(), la)


@st.cache_data(ttl=300)
def fetch_votes(la: str, member: str) -> QueryResult:
    return _q.votes(get_constituency_conn(), la, member)


@st.cache_data(ttl=600)
def fetch_roll_call_councils() -> QueryResult:
    return _q.roll_call_councils(get_constituency_conn())


@st.cache_data(ttl=300)
def fetch_agendas(la: str) -> QueryResult:
    return _q.agendas(get_constituency_conn(), la)


@st.cache_data(ttl=600)
def fetch_standing_orders(la: str) -> QueryResult:
    return _q.standing_orders(get_constituency_conn(), la)


@st.cache_data(ttl=600)
def fetch_chief_executive(la: str) -> QueryResult:
    return _q.chief_executive(get_constituency_conn(), la)
