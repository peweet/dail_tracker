"""Votes data access — thin Streamlit wrapper over dail_tracker_core.

Retrieval SQL (incl. the parameterised WHERE/ILIKE assembly) and QueryResult
state-handling live in ``dail_tracker_core.queries.votes``; this file owns the
Streamlit caching, the connection (with the chamber parquet substitutions), and
the small UI-shaping the page consumes (picker option lists, the redirect name
string). Return contracts are unchanged.

Forbidden here (unchanged): JOIN/GROUP_BY_MULTI_DIM/HAVING/WINDOW in ad-hoc SQL,
business-metric definitions.
"""

from __future__ import annotations

import duckdb
import pandas as pd
import streamlit as st

from config import GOLD_SEANAD_VOTE_HISTORY_PARQUET, GOLD_VOTE_HISTORY_PARQUET
from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import votes as _q


@st.cache_resource
def get_votes_conn() -> duckdb.DuckDBPyConnection:
    # v_vote_base unions both chambers (Dáil + Seanad) and tags each row with a
    # `house` column; the page scopes by house via a chamber toggle.
    return connect_with_views(
        ["vote*.sql"],
        substitutions={
            "{PARQUET_PATH}": GOLD_VOTE_HISTORY_PARQUET.as_posix(),
            "{SEANAD_VOTE_PARQUET_PATH}": GOLD_SEANAD_VOTE_HISTORY_PARQUET.as_posix(),
        },
        swallow_errors=True,
    )


@st.cache_data(ttl=300)
def fetch_hero_stats(house: str = "Dáil") -> pd.DataFrame:
    return _q.result_summary(get_votes_conn(), house).data


@st.cache_data(ttl=300)
def fetch_vote_years(house: str = "Dáil") -> list[int]:
    df = _q.vote_years(get_votes_conn(), house).data
    if not df.empty and "year" in df.columns:
        return [int(y) for y in df["year"].dropna().tolist()]
    return []


@st.cache_data(ttl=300)
def fetch_member_names(party: str = "", house: str = "Dáil") -> list[str]:
    df = _q.member_names(get_votes_conn(), party, house).data
    return df["member_name"].tolist() if not df.empty and "member_name" in df.columns else []


@st.cache_data(ttl=300)
def fetch_party_names(house: str = "Dáil") -> list[str]:
    df = _q.party_names(get_votes_conn(), house).data
    return df["party_name"].tolist() if not df.empty and "party_name" in df.columns else []


@st.cache_data(ttl=300)
def fetch_td_row_by_name(member_name: str, house: str = "Dáil") -> pd.DataFrame:
    return _q.td_row_by_name(get_votes_conn(), member_name, house).data


@st.cache_data(ttl=300)
def fetch_td_name_by_id(member_id: str) -> str:
    """Look up TD display name from member_id for the legacy redirect."""
    df = _q.td_name_by_id(get_votes_conn(), member_id).data
    if df.empty or "member_name" not in df.columns:
        return ""
    val = df.iloc[0]["member_name"]
    return str(val) if val is not None else ""


@st.cache_data(ttl=300)
def fetch_vote_index(date_from, date_to, outcome, house: str = "Dáil") -> pd.DataFrame:
    return _q.vote_index(get_votes_conn(), date_from, date_to, outcome, house).data


@st.cache_data(ttl=300)
def fetch_vote_by_id(vote_id: str) -> pd.DataFrame:
    return _q.vote_by_id(get_votes_conn(), vote_id).data


@st.cache_data(ttl=300)
def fetch_party_breakdown(vote_id) -> pd.DataFrame:
    return _q.party_breakdown(get_votes_conn(), vote_id).data


@st.cache_data(ttl=300)
def fetch_division_members(vote_id) -> pd.DataFrame:
    return _q.division_members(get_votes_conn(), vote_id).data


@st.cache_data(ttl=300)
def fetch_sources(vote_id) -> pd.DataFrame:
    return _q.sources(get_votes_conn(), vote_id).data


@st.cache_data(ttl=300)
def fetch_topical_votes(topics: tuple[str, ...], house: str = "Dáil") -> pd.DataFrame:
    """Recent member votes on hot-topic debates. Used to seed the member picker cards.

    ``topics`` is a sequence of ILIKE patterns (e.g. ``"%housing%"``); the
    presentation labels stay in the page."""
    return _q.topical_votes(get_votes_conn(), topics, house).data
