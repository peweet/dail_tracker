"""Ministerial-diary data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult handling live in
``dail_tracker_core.queries.ministerial_diary``; this file owns only Streamlit
caching and unwraps ``.data`` to the DataFrame the page expects (empty on a
source failure — same contract as the other data-access modules).

Forbidden here (unchanged across the layer): JOIN / multi-col GROUP BY / HAVING /
WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot, business-metric
definitions. Faceting + grouping (per-minister rollup, sector filter) happen in
the page off these frames.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import ministerial_diary as _q


@st.cache_resource
def get_diary_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["ministerial_diary_*.sql"], swallow_errors=True)


@st.cache_data(ttl=600)
def fetch_org_overlap() -> pd.DataFrame:
    """Organisations ranked by ministerial meetings (+ corroborated / is_state_body)."""
    return _q.org_overlap(get_diary_conn()).data


@st.cache_data(ttl=600)
def fetch_engagements() -> pd.DataFrame:
    """Per-(engagement x org) rows for org/minister drill-down."""
    return _q.engagements(get_diary_conn()).data
