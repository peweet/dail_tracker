"""Judiciary legal-diary data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult handling live in
``dail_tracker_core.queries.judiciary``; this file owns only Streamlit caching
and unwraps ``.data`` to the DataFrame the page expects (empty on a source
failure — same contract as the other data-access modules).

Forbidden here (unchanged across the layer): JOIN / multi-col GROUP BY / HAVING /
WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot, business-metric
definitions. Faceting + grouping happen in the page off these frames.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import judiciary as _q


@st.cache_resource
def get_judiciary_conn() -> duckdb.DuckDBPyConnection:
    return connect_with_views(["judiciary_*.sql"], swallow_errors=True)


@st.cache_data(ttl=300)
def fetch_legal_diary_schedule() -> pd.DataFrame:
    """Tier A — judge sitting-sessions (officials only, no party data)."""
    return _q.legal_diary_schedule(get_judiciary_conn()).data


@st.cache_data(ttl=300)
def fetch_legal_diary_counts() -> pd.DataFrame:
    """Tier B — per-session case-item counts."""
    return _q.legal_diary_counts(get_judiciary_conn()).data


@st.cache_data(ttl=300)
def fetch_legal_diary_cases() -> pd.DataFrame:
    """Tier C — ANONYMISED case listings + provenance link."""
    return _q.legal_diary_cases(get_judiciary_conn()).data
