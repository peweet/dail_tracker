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


# ── The Bench & Courts (green core) ─────────────────────────────────────────
@st.cache_data(ttl=600)
def fetch_roster() -> pd.DataFrame:
    """The sitting bench — one row per judge (identity grain)."""
    return _q.roster(get_judiciary_conn()).data


@st.cache_data(ttl=600)
def fetch_appointments() -> pd.DataFrame:
    """Judicial appointment events + gov.ie nomination context."""
    return _q.appointments(get_judiciary_conn()).data


@st.cache_data(ttl=600)
def fetch_profile() -> pd.DataFrame:
    """Per-judge identity summary for the career-arc drill-down."""
    return _q.profile(get_judiciary_conn()).data


@st.cache_data(ttl=600)
def fetch_nominations() -> pd.DataFrame:
    """gov.ie nomination announcements (vacancy-lifecycle context)."""
    return _q.nominations(get_judiciary_conn()).data


@st.cache_data(ttl=600)
def fetch_authority_summary() -> pd.DataFrame:
    """Aggregate: appointment-notice count by appointing authority."""
    return _q.authority_summary(get_judiciary_conn()).data


@st.cache_data(ttl=600)
def fetch_elevation_ladder() -> pd.DataFrame:
    """Aggregate: real promotions per court transition."""
    return _q.elevation_ladder(get_judiciary_conn()).data


# ── The Courts — system health (no named judges) ────────────────────────────
@st.cache_data(ttl=600)
def fetch_courts_clearance() -> pd.DataFrame:
    """Annual case clearance by court, 2017–2024."""
    return _q.courts_clearance(get_judiciary_conn()).data


@st.cache_data(ttl=600)
def fetch_courts_clearance_by_area() -> pd.DataFrame:
    """Annual case clearance by court × area of law, 2017–2024 (drill-down)."""
    return _q.courts_clearance_by_area(get_judiciary_conn()).data


@st.cache_data(ttl=600)
def fetch_courts_waiting_times() -> pd.DataFrame:
    """Published waiting-time lists (latest two years)."""
    return _q.courts_waiting_times(get_judiciary_conn()).data


@st.cache_data(ttl=600)
def fetch_courthouses() -> pd.DataFrame:
    """Active, geocoded courthouses for the venue map."""
    return _q.courthouses(get_judiciary_conn()).data
