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
    # + minister_briefs.sql: the incoming-minister BRIEF corpus (agenda layer) lives on the same
    # page as the diaries, so it shares this connection.
    return connect_with_views(["ministerial_diary_*.sql", "minister_briefs.sql"], swallow_errors=True)


@st.cache_data(ttl=600)
def fetch_org_overlap() -> pd.DataFrame:
    """Organisations ranked by ministerial meetings (+ corroborated / is_state_body)."""
    return _q.org_overlap(get_diary_conn()).data


@st.cache_data(ttl=600)
def fetch_engagements() -> pd.DataFrame:
    """Per-(engagement x org) rows for org/minister drill-down."""
    return _q.engagements(get_diary_conn()).data


@st.cache_data(ttl=600)
def fetch_meetings() -> pd.DataFrame:
    """The broad landscape — every external meeting (one row each, no org match needed)."""
    return _q.meetings(get_diary_conn()).data


@st.cache_data(ttl=600)
def fetch_minister_briefs() -> pd.DataFrame:
    """Incoming-minister BRIEF corpus — per-department stated goals / priorities / machinery-of-
    government changes (the agenda layer that pairs with the diaries). Display-only."""
    return _q.minister_briefs(get_diary_conn()).data


@st.cache_data(ttl=600)
def fetch_access_to_contracts(limit: int = 25, order_by: str = "awards_eur") -> pd.DataFrame:
    """The ACCESS × MONEY cross-reference: companies that appear in ministers' published diaries
    AND won contracts / were paid public money, ranked. Read honestly — co-occurrence is ACCESS,
    never proof a meeting caused a contract (the awards/payments carry their own never-sum grains).
    order_by ∈ {awards_eur, paid_eur, meetings, total_lobbying_returns}."""
    return _q.access_to_contracts(get_diary_conn(), limit=limit, order_by=order_by).data


# ── Period-grain rollups (the page's Year/Month filter becomes a WHERE clause) ─────────────
# year=None → whole corpus; month=None → whole year. The rollups are precomputed in the
# ministerial_diary_zz_* views at all three grains, so no pandas re-aggregation happens here
# or in the page.


@st.cache_data(ttl=600)
def fetch_minister_rollup(year: int | None = None, month: int | None = None) -> pd.DataFrame:
    """Per-minister meetings / date span / portfolio (comma-joined depts) for one period."""
    return _q.minister_rollup(get_diary_conn(), year, month).data


@st.cache_data(ttl=600)
def fetch_dept_rollup(year: int | None = None, month: int | None = None) -> pd.DataFrame:
    """Per-department meetings + distinct named ministers for one period."""
    return _q.dept_rollup(get_diary_conn(), year, month).data


@st.cache_data(ttl=600)
def fetch_dept_minister_rollup(dept: str, year: int | None = None, month: int | None = None) -> pd.DataFrame:
    """One department's ministers by meetings logged for one period (depts = full portfolio)."""
    return _q.dept_minister_rollup(get_diary_conn(), dept, year, month).data


@st.cache_data(ttl=600)
def fetch_top_orgs(entity_kind: str, year: int | None = None, month: int | None = None, top: int = 3) -> pd.DataFrame:
    """Most-named organisations per minister/department ('Most-met' card context) for one period."""
    return _q.top_orgs(get_diary_conn(), entity_kind, year, month, top=top).data
