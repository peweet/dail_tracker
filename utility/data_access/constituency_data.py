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

import duckdb
import streamlit as st

from dail_tracker_core.connections import constituency_conn
from dail_tracker_core.queries import constituency as _q
from dail_tracker_core.results import QueryResult


@st.cache_resource
def get_constituency_conn() -> duckdb.DuckDBPyConnection:
    """One connection per session: member + procurement + constituency views."""
    return constituency_conn()


@st.cache_data(ttl=600)
def fetch_constituency_list_result() -> QueryResult:
    """All 43 constituencies (demographics + TD count) for the index grid."""
    return _q.constituency_list(get_constituency_conn())


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
def fetch_constituency_council_context_result(constituency: str) -> QueryResult:
    return _q.constituency_council_context(get_constituency_conn(), constituency)
