"""Interests data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL + QueryResult state-handling live in
``dail_tracker_core.queries.interests``; this file owns only the Streamlit
caching and the small presentation reshaping the page expects (availability
bool, the {"years", "members"} options dict, plain DataFrames).

``get_interests_conn`` is still exported for symmetry with the other data-access
modules (the page imports fetchers, not the conn).

Pre-existing /interests behaviour preserved exactly: same filter options, same
column contract, same row ordering, same leaderboard rank.

Forbidden here (unchanged): read_parquet, parquet_scan, CREATE VIEW,
pandas groupby/merge/pivot business logic, multi-dim GROUP BY.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import interests as _q


@st.cache_resource
def get_interests_conn() -> duckdb.DuckDBPyConnection:
    # Detail-view glob sorts before the index glob so dependency order holds
    # (the index view reads the detail view). Loud registration — a missing
    # interests view is a real break, matching prior behaviour.
    return connect_with_views(
        ["member_interests_*.sql", "member_zz_interests_*.sql"],
        swallow_errors=False,
    )


# ── Availability guard ────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_interests_availability(house: str) -> bool:
    """True iff v_member_interests_detail has any row for this house."""
    r = _q.availability(get_interests_conn(), house)
    return r.ok and not r.is_empty


# ── Filter options ────────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_interests_filter_options(house: str) -> dict[str, list]:
    """{"years": [int], "members": [str]} for the sidebar / leaderboard filters."""
    conn = get_interests_conn()
    yrs = _q.distinct_years(conn, house)
    mem = _q.distinct_members(conn, house)
    years = yrs.data["declaration_year"].dropna().astype(int).tolist() if (yrs.ok and not yrs.is_empty) else []
    members = mem.data["member_name"].tolist() if (mem.ok and not mem.is_empty) else []
    return {"years": years, "members": members}


# ── Detail retrieval ──────────────────────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_interests(
    house: str,
    name_q: str = "",
    years: tuple[int, ...] = (),
    landlord_only: bool = False,
) -> pd.DataFrame:
    """Browse-list rows. Filters AND together; LIMIT 1000."""
    return _q.detail(get_interests_conn(), house, name_q, years, landlord_only).data


@st.cache_data(ttl=300)
def fetch_td_interests(house: str, td_name: str) -> pd.DataFrame:
    """Every declaration for one TD across all years."""
    return _q.td_interests(get_interests_conn(), house, td_name).data


@st.cache_data(ttl=300)
def fetch_td_interest_declarations(house: str, td_name: str) -> pd.DataFrame:
    """Deduped, diff-tagged declarations for one TD (change_status per category)."""
    return _q.member_declarations(get_interests_conn(), house, td_name).data


@st.cache_data(ttl=300)
def fetch_td_interest_year_summary(house: str, td_name: str) -> pd.DataFrame:
    """Per-year editorial summary for one TD (counts, diff totals, badge inputs)."""
    return _q.member_year_summary(get_interests_conn(), house, td_name).data


# ── Member index (ranked leaderboard) ─────────────────────────────────────────


@st.cache_data(ttl=300)
def fetch_member_index(house: str, year: int) -> pd.DataFrame:
    """Ranked member index for a house × year (retrieval-only over the index view)."""
    return _q.member_index(get_interests_conn(), house, year).data


@st.cache_data(ttl=300)
def fetch_member_index_alltime(house: str) -> pd.DataFrame:
    """Latest-snapshot ranked member index for a house — each member shown at their
    most recent declaration year, NOT summed across years (retrieval-only over the
    all-time index view). Drives the What They Own 'Most recent on file' view."""
    return _q.member_index_alltime(get_interests_conn(), house).data
