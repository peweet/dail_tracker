"""
Votes data-access layer.

Owns:
- DuckDB connection bootstrapped from sql_views/vote*.sql
- {PARQUET_PATH} template substitution for parquet-backed views
- All retrieval functions for the votes page. The page calls these; it never
  runs SQL or manages the connection itself.

Forbidden here (same rules as Streamlit page files):
- JOIN, GROUP_BY_MULTI_DIM, HAVING, WINDOW in ad-hoc retrieval SQL
- Business metric definitions
"""

from __future__ import annotations

import logging

import duckdb
import pandas as pd
import streamlit as st
from data_access._sql_registry import register_views

from config import GOLD_SEANAD_VOTE_HISTORY_PARQUET, GOLD_VOTE_HISTORY_PARQUET

_log = logging.getLogger(__name__)

_PARQUET = GOLD_VOTE_HISTORY_PARQUET.as_posix()
_SEANAD_PARQUET = GOLD_SEANAD_VOTE_HISTORY_PARQUET.as_posix()

_VOTE_INDEX_LIMIT = 500
_DIVISION_MEMBERS_LIMIT = 5000


@st.cache_resource
def get_votes_conn():
    conn = duckdb.connect()
    # v_vote_base unions both chambers (Dáil + Seanad) and tags each row with a
    # `house` column; the page scopes by house via a chamber toggle.
    register_views(
        conn,
        ["vote*.sql"],
        substitutions={
            "{PARQUET_PATH}": _PARQUET,
            "{SEANAD_VOTE_PARQUET_PATH}": _SEANAD_PARQUET,
        },
        swallow_errors=True,
    )
    return conn


# ── Retrieval helpers ─────────────────────────────────────────────────────────


def _safe_query(conn, sql: str, params=()) -> pd.DataFrame:
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(sql, list(params)).df()
    except Exception as exc:
        _log.warning("votes query failed: %s | params=%s | error=%s", sql[:120], params, exc)
        return pd.DataFrame()


def _and_clauses(clauses: list[str]) -> str:
    sql = ""
    for c in clauses:
        sql = (sql + " AND " if sql else "") + c
    return sql


# ── Retrieval (SELECT / WHERE / ORDER BY / LIMIT only) ────────────────────────


@st.cache_data(ttl=300)
def fetch_hero_stats(house: str = "Dáil") -> pd.DataFrame:
    return _safe_query(
        get_votes_conn(),
        "SELECT division_count, member_count, first_vote_date, last_vote_date"
        " FROM v_vote_result_summary WHERE house = ? LIMIT 1",
        (house,),
    )


@st.cache_data(ttl=300)
def fetch_vote_years(house: str = "Dáil") -> list[int]:
    df = _safe_query(
        get_votes_conn(),
        "SELECT DISTINCT CAST(EXTRACT(YEAR FROM vote_date) AS INTEGER) AS year"
        " FROM v_vote_index WHERE vote_date IS NOT NULL AND house = ?"
        " ORDER BY year DESC LIMIT 20",
        (house,),
    )
    if not df.empty and "year" in df.columns:
        return [int(y) for y in df["year"].dropna().tolist()]
    return []


@st.cache_data(ttl=300)
def fetch_member_names(party: str = "", house: str = "Dáil") -> list[str]:
    if party:
        df = _safe_query(
            get_votes_conn(),
            "SELECT DISTINCT member_name FROM td_vote_summary"
            " WHERE member_name IS NOT NULL AND house = ? AND party_name = ?"
            " ORDER BY member_name ASC LIMIT 1000",
            (house, party),
        )
    else:
        df = _safe_query(
            get_votes_conn(),
            "SELECT DISTINCT member_name FROM td_vote_summary"
            " WHERE member_name IS NOT NULL AND house = ? ORDER BY member_name ASC LIMIT 1000",
            (house,),
        )
    return df["member_name"].tolist() if not df.empty and "member_name" in df.columns else []


@st.cache_data(ttl=300)
def fetch_party_names(house: str = "Dáil") -> list[str]:
    df = _safe_query(
        get_votes_conn(),
        "SELECT DISTINCT party_name FROM td_vote_summary"
        " WHERE party_name IS NOT NULL AND house = ? ORDER BY party_name ASC LIMIT 100",
        (house,),
    )
    return df["party_name"].tolist() if not df.empty and "party_name" in df.columns else []


@st.cache_data(ttl=300)
def fetch_td_row_by_name(member_name: str, house: str = "Dáil") -> pd.DataFrame:
    return _safe_query(
        get_votes_conn(),
        "SELECT member_id, member_name, party_name, constituency,"
        " yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_name = ? AND house = ? LIMIT 1",
        (member_name, house),
    )


@st.cache_data(ttl=300)
def fetch_td_name_by_id(member_id: str) -> str:
    """Look up TD display name from member_id for the legacy redirect.

    Used by `_render_mode_b_redirect` so the moved-callout link can show
    the member's actual name rather than the bare ID slug.
    """
    df = _safe_query(
        get_votes_conn(),
        "SELECT member_name FROM td_vote_summary WHERE member_id = ? LIMIT 1",
        (member_id,),
    )
    if df.empty or "member_name" not in df.columns:
        return ""
    val = df.iloc[0]["member_name"]
    return str(val) if val is not None else ""


@st.cache_data(ttl=300)
def fetch_vote_index(date_from, date_to, outcome, house: str = "Dáil") -> pd.DataFrame:
    clauses: list[str] = ["house = ?"]
    params: list = [house]
    if date_from:
        clauses.append("vote_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("vote_date <= ?")
        params.append(date_to)
    if outcome:
        clauses.append("vote_outcome = ?")
        params.append(outcome)
    where = ""
    body = _and_clauses(clauses)
    if body:
        where = " WHERE " + body
    sql = (
        "SELECT vote_id, vote_date, debate_title, vote_outcome,"
        " yes_count, no_count, abstained_count, margin, oireachtas_url"
        f" FROM v_vote_index{where} ORDER BY vote_date DESC LIMIT ?"
    )
    params.append(_VOTE_INDEX_LIMIT)
    return _safe_query(get_votes_conn(), sql, params)


@st.cache_data(ttl=300)
def fetch_vote_by_id(vote_id: str) -> pd.DataFrame:
    return _safe_query(
        get_votes_conn(),
        "SELECT vote_id, vote_date, debate_title, vote_outcome,"
        " yes_count, no_count, abstained_count, margin"
        " FROM v_vote_index WHERE vote_id = ? LIMIT 1",
        (vote_id,),
    )


@st.cache_data(ttl=300)
def fetch_party_breakdown(vote_id) -> pd.DataFrame:
    return _safe_query(
        get_votes_conn(),
        "SELECT party_name, vote_type, member_count, vote_pct FROM party_vote_breakdown WHERE vote_id = ? LIMIT 500",
        (vote_id,),
    )


@st.cache_data(ttl=300)
def fetch_division_members(vote_id) -> pd.DataFrame:
    return _safe_query(
        get_votes_conn(),
        "SELECT member_id, member_name, party_name, constituency, vote_type"
        " FROM v_vote_member_detail WHERE vote_id = ?"
        " AND member_name IS NOT NULL"
        " ORDER BY party_name ASC, member_name ASC LIMIT ?",
        (vote_id, _DIVISION_MEMBERS_LIMIT),
    )


@st.cache_data(ttl=300)
def fetch_sources(vote_id) -> pd.DataFrame:
    return _safe_query(
        get_votes_conn(),
        "SELECT source_url, source_document_url, official_pdf_url, legislation_url, source_label"
        " FROM v_vote_sources WHERE vote_id = ? LIMIT 50",
        (vote_id,),
    )


@st.cache_data(ttl=300)
def fetch_topical_votes(topics: tuple[str, ...], house: str = "Dáil") -> pd.DataFrame:
    """Recent member votes on hot-topic debates. Used to seed the member picker cards.

    ``topics`` is a sequence of ILIKE patterns (e.g. ``"%housing%"``); the
    presentation labels stay in the page. Retrieval-only: SELECT with
    WHERE/ORDER BY/LIMIT against the approved view.
    """
    patterns = list(topics)
    if not patterns:
        return pd.DataFrame()
    likes = " OR ".join(["debate_title ILIKE ?" for _ in patterns])
    sql = (
        "SELECT vote_date, member_id, member_name, party_name, constituency,"
        " vote_type, debate_title, vote_outcome"
        " FROM v_vote_member_detail"
        " WHERE vote_type IN ('Voted Yes', 'Voted No')"
        " AND member_name IS NOT NULL AND house = ?"
        f" AND ({likes})"
        " ORDER BY vote_date DESC LIMIT 2000"
    )
    return _safe_query(get_votes_conn(), sql, [house, *patterns])
