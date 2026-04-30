import logging
import sys
from pathlib import Path

_UTIL = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_UTIL))

import pandas as pd
import streamlit as st

_log = logging.getLogger(__name__)

from shared_css import inject_css
from ui.components import (
    empty_state,
    evidence_heading,
    sidebar_date_range,
    sidebar_member_filter,
    sidebar_page_header,
    todo_callout,
    year_selector,
)
from ui.export_controls import export_button
from ui.source_pdfs import provenance_expander
from ui.vote_explorer import render_division_panel, render_td_panel, vt_division_card_html
from data_access.votes_data import get_votes_conn

_REQUIRED_INDEX_COLS: frozenset[str] = frozenset(
    {"vote_id", "vote_date", "debate_title", "vote_outcome", "yes_count", "no_count"}
)

_VOTE_INDEX_LIMIT      = 500
_DIVISION_MEMBERS_LIMIT = 5000
_TD_HISTORY_LIMIT      = 500


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


# ── Data fetch ─────────────────────────────────────────────────────────────────

@st.cache_data(ttl=300)
def _fetch_hero_stats(_conn) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT division_count, member_count, first_vote_date, last_vote_date"
        " FROM v_vote_result_summary LIMIT 1",
    )


@st.cache_data(ttl=300)
def _fetch_vote_years(_conn) -> list[int]:
    df = _safe_query(
        _conn,
        "SELECT DISTINCT CAST(EXTRACT(YEAR FROM vote_date) AS INTEGER) AS year"
        " FROM v_vote_index WHERE vote_date IS NOT NULL ORDER BY year DESC LIMIT 20",
    )
    if not df.empty and "year" in df.columns:
        return [int(y) for y in df["year"].dropna().tolist()]
    return []


@st.cache_data(ttl=300)
def _fetch_member_names(_conn, party: str = "") -> list[str]:
    if party:
        df = _safe_query(
            _conn,
            "SELECT DISTINCT member_name FROM td_vote_summary"
            " WHERE member_name IS NOT NULL AND party_name = ?"
            " ORDER BY member_name ASC LIMIT 1000",
            (party,),
        )
    else:
        df = _safe_query(
            _conn,
            "SELECT DISTINCT member_name FROM td_vote_summary"
            " WHERE member_name IS NOT NULL ORDER BY member_name ASC LIMIT 1000",
        )
    return df["member_name"].tolist() if not df.empty and "member_name" in df.columns else []


@st.cache_data(ttl=300)
def _fetch_party_names(_conn) -> list[str]:
    df = _safe_query(
        _conn,
        "SELECT DISTINCT party_name FROM td_vote_summary"
        " WHERE party_name IS NOT NULL ORDER BY party_name ASC LIMIT 100",
    )
    return df["party_name"].tolist() if not df.empty and "party_name" in df.columns else []


@st.cache_data(ttl=300)
def _fetch_td_row_by_id(_conn, member_id: str) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT member_id, member_name, party_name, constituency,"
        " yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_id = ? LIMIT 1",
        (member_id,),
    )


@st.cache_data(ttl=300)
def _fetch_td_row_by_name(_conn, member_name: str) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT member_id, member_name, party_name, constituency,"
        " yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_name = ? LIMIT 1",
        (member_name,),
    )


@st.cache_data(ttl=300)
def _fetch_vote_index(_conn, date_from, date_to, outcome) -> pd.DataFrame:
    clauses: list[str] = []
    params: list = []
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
        " yes_count, no_count, abstained_count, margin"
        f" FROM v_vote_index{where} ORDER BY vote_date DESC LIMIT ?"
    )
    params.append(_VOTE_INDEX_LIMIT)
    return _safe_query(_conn, sql, params)


@st.cache_data(ttl=300)
def _fetch_vote_by_id(_conn, vote_id: str) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT vote_id, vote_date, debate_title, vote_outcome,"
        " yes_count, no_count, abstained_count, margin"
        " FROM v_vote_index WHERE vote_id = ? LIMIT 1",
        (vote_id,),
    )


@st.cache_data(ttl=300)
def _fetch_party_breakdown(_conn, vote_id) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT party_name, vote_type, member_count, vote_pct"
        " FROM party_vote_breakdown WHERE vote_id = ? LIMIT 500",
        (vote_id,),
    )


@st.cache_data(ttl=300)
def _fetch_division_members(_conn, vote_id) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT member_name, party_name, constituency, vote_type"
        " FROM v_vote_member_detail WHERE vote_id = ?"
        " AND member_name IS NOT NULL"
        " ORDER BY party_name ASC, member_name ASC LIMIT ?",
        (vote_id, _DIVISION_MEMBERS_LIMIT),
    )


@st.cache_data(ttl=300)
def _fetch_sources(_conn, vote_id) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT source_url, source_document_url, official_pdf_url, legislation_url, source_label"
        " FROM v_vote_sources WHERE vote_id = ? LIMIT 50",
        (vote_id,),
    )


@st.cache_data(ttl=300)
def _fetch_td_history(_conn, member_id, date_from, date_to) -> pd.DataFrame:
    clauses: list[str] = ["member_id = ?"]
    params: list = [member_id]
    if date_from:
        clauses.append("vote_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("vote_date <= ?")
        params.append(date_to)
    body = _and_clauses(clauses)
    sql = (
        "SELECT vote_id, vote_date, debate_title, vote_type, vote_outcome, oireachtas_url"
        f" FROM v_vote_member_detail WHERE {body} ORDER BY vote_date DESC LIMIT ?"
    )
    params.append(_TD_HISTORY_LIMIT)
    return _safe_query(_conn, sql, params)


@st.cache_data(ttl=300)
def _fetch_td_year_summary(_conn, member_id) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT year, yes_count, no_count, abstained_count"
        " FROM td_vote_year_summary WHERE member_id = ? ORDER BY year ASC LIMIT 50",
        (member_id,),
    )


# ── Mode A: Divisions index ────────────────────────────────────────────────────

@st.fragment
def _card_list_fragment(conn, date_from, date_to, outcome_filter) -> None:
    """Fragment: year pills + card list. Reruns only this block when year changes."""
    years = _fetch_vote_years(conn)
    eff_from = date_from
    eff_to   = date_to

    if years:
        year_strs = [str(y) for y in years]
        sel_year = year_selector(year_strs, key="v_year")
        if not eff_from:
            eff_from = f"{sel_year}-01-01"
            eff_to   = f"{sel_year}-12-31"

    vote_df = _fetch_vote_index(conn, eff_from, eff_to, outcome_filter)

    missing = sorted(c for c in _REQUIRED_INDEX_COLS if c not in vote_df.columns)
    if missing:
        todo_callout(f"v_vote_index — required columns not available: {', '.join(missing)}")
        return

    if vote_df.empty:
        empty_state(
            "No divisions found",
            "No published divisions match the current filters. "
            "Try selecting a different year or clearing the outcome filter.",
        )
        return

    total    = len(vote_df)
    show_all = st.session_state.get("v_show_all", False)
    visible  = vote_df if show_all else vote_df.head(25)
    suffix   = " · showing first 25" if not show_all and total > 25 else ""
    st.html(
        f'<p class="vt-index-caption">'
        f'{total:,} division{"s" if total != 1 else ""}{suffix}'
        f'</p>'
    )

    for i, (_, row) in enumerate(visible.iterrows()):
        card_col, btn_col = st.columns([14, 1])
        with card_col:
            st.html(vt_division_card_html(row))
        btn_col.html('<div class="dt-nav-anchor"></div>')
        if btn_col.button(
            "→",
            key=f"vt_div_{i}",
            help=str(row.get("debate_title") or "View division"),
        ):
            vote_id = str(row["vote_id"])
            st.session_state["v_sel_vote_id"] = vote_id
            st.session_state["v_from"]        = "index"
            st.session_state["v_show_all"]    = False
            st.query_params["vote"] = vote_id
            st.query_params.pop("member", None)
            st.rerun(scope="app")

    if not show_all and total > 25 and st.button(
        f"Show all {total:,} divisions", key="v_show_all_btn"
    ):
        st.session_state["v_show_all"] = True
        st.rerun()

    display_cols = [
        c for c in [
            "vote_date", "vote_id", "debate_title", "vote_outcome",
            "yes_count", "no_count", "abstained_count", "margin",
        ]
        if c in vote_df.columns
    ]
    export_button(
        vote_df[display_cols],
        label="↓ Export division list CSV",
        filename="dail_divisions.csv",
        key="exp_vote_idx",
    )


def _render_mode_a(conn, date_from, date_to, outcome_filter) -> None:
    st.html(
        '<p class="dt-kicker">Dáil Tracker · Voting Record</p>'
        '<h1 class="dt-hero">Dáil Divisions</h1>'
    )
    _card_list_fragment(conn, date_from, date_to, outcome_filter)

    hero = _fetch_hero_stats(conn)
    sections: list[str] = [
        "Divisions data sourced from the Oireachtas Open Data API. "
        "Votes are as published in the official record."
    ]
    if not hero.empty:
        r = hero.iloc[0]
        dc = r.get("division_count")
        mc = r.get("member_count")
        if dc:
            sections.insert(0, f"{int(dc):,} total divisions on record · {int(mc or 0):,} TDs recorded.")
    provenance_expander(sections=sections)
    todo_callout(
        "source_url column on v_vote_sources — "
        "confirm real oireachtas.ie URL is present, not a local file path."
    )


# ── Mode B: TD profile ─────────────────────────────────────────────────────────

def _render_mode_b(conn, member_id: str, date_from, date_to) -> None:
    if st.button("← Back to divisions", key="v_back_b"):
        st.session_state["_v_clear_member"] = True
        st.query_params.clear()
        st.rerun()

    td_df = _fetch_td_row_by_id(conn, member_id)
    if td_df.empty:
        empty_state(
            "TD not found",
            "No record found for this member in td_vote_summary.",
        )
        return

    td_row     = td_df.iloc[0]
    history_df = _fetch_td_history(conn, member_id, date_from, date_to)
    year_df    = _fetch_td_year_summary(conn, member_id)

    render_td_panel(td_row, history_df, year_df)

    evidence_heading("Sponsored bills")
    todo_callout(
        "TODO_PIPELINE_VIEW_REQUIRED: td_sponsored_bills — "
        "member's sponsored bills (bill_title, bill_year, bill_status, oireachtas_url); "
        "pipeline view required before this section can render."
    )

    provenance_expander(sections=["Voting record sourced from the Oireachtas Open Data API divisions data."])


# ── Mode C: Division evidence ──────────────────────────────────────────────────

def _render_mode_c(conn, vote_id: str, v_from: str) -> None:
    back_label = "← Back to TD record" if v_from == "td" else "← Back to divisions"
    if st.button(back_label, key="v_back_c"):
        st.session_state["_v_clear_vote"] = True
        if v_from == "td":
            mid = st.session_state.get("v_sel_member_id", "")
            if mid:
                st.query_params["member"] = mid
            st.query_params.pop("vote", None)
        else:
            st.query_params.clear()
        st.rerun()

    vote_df = _fetch_vote_by_id(conn, vote_id)
    if vote_df.empty:
        empty_state(
            "Division not found",
            f"No division on record for ID: {vote_id}.",
        )
        return

    vote_row     = vote_df.iloc[0]
    members_df   = _fetch_division_members(conn, vote_id)
    sources_df   = _fetch_sources(conn, vote_id)
    breakdown_df = _fetch_party_breakdown(conn, vote_id)

    render_division_panel(vote_row, members_df, sources_df, breakdown_df)

    provenance_expander(sections=["Division record sourced from the Oireachtas Open Data API."])


# ── Page entry point ───────────────────────────────────────────────────────────

def votes_page() -> None:
    inject_css()
    conn = get_votes_conn()

    # Resolve pending back-navigation flags before any widgets are instantiated
    if st.session_state.get("_v_clear_member"):
        st.session_state.pop("_v_clear_member", None)
        st.session_state.pop("v_sel_member_id", None)
        for _k in ("v_member_search", "v_member_select"):
            st.session_state.pop(_k, None)

    if st.session_state.get("_v_clear_vote"):
        st.session_state.pop("_v_clear_vote", None)
        st.session_state.pop("v_sel_vote_id", None)

    # Seed session state from URL query params on first load
    if "v_sel_vote_id" not in st.session_state and "vote" in st.query_params:
        st.session_state["v_sel_vote_id"] = st.query_params["vote"]
        st.session_state["v_from"] = st.query_params.get("from", "index")

    if "v_sel_member_id" not in st.session_state and "member" in st.query_params:
        st.session_state["v_sel_member_id"] = st.query_params["member"]

    sel_vote_id   = st.session_state.get("v_sel_vote_id")
    sel_member_id = st.session_state.get("v_sel_member_id")
    v_from        = st.session_state.get("v_from", "index")

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        sidebar_page_header("Dáil<br>Divisions")

        hero_df = _fetch_hero_stats(conn)
        if not hero_df.empty:
            r   = hero_df.iloc[0]
            fd  = r.get("first_vote_date")
            ld  = r.get("last_vote_date")
            if fd and ld:
                st.caption(f"Data covers {str(fd)[:7]} to {str(ld)[:7]}")

        st.divider()

        # Date range shown in Mode B only — filters TD vote history
        date_from = date_to = None
        if sel_member_id:
            date_from, date_to = sidebar_date_range("Date range", key="v_date_range")

        # Outcome and party filters shown in Mode A only
        outcome_filter = None
        sel_party = ""
        if not sel_member_id and not sel_vote_id:
            st.html('<p class="sidebar-label">Outcome</p>')
            outcome_sel = st.selectbox(
                "Outcome",
                ["All", "Carried", "Lost"],
                key="v_outcome",
                label_visibility="collapsed",
            )
            outcome_filter = None if outcome_sel == "All" else outcome_sel

            party_names = _fetch_party_names(conn)
            if party_names:
                st.html('<p class="sidebar-label">Party</p>')
                party_sel = st.selectbox(
                    "Party",
                    ["All parties"] + party_names,
                    key="v_party",
                    label_visibility="collapsed",
                )
                sel_party = "" if party_sel == "All parties" else party_sel

        st.divider()

        # Member search — filtered by party when one is selected
        member_names = _fetch_member_names(conn, sel_party)
        sel_name = sidebar_member_filter(
            "Find a TD",
            member_names,
            key_search="v_member_search",
            key_select="v_member_select",
        )

        # Handle selection / deselection
        if sel_name:
            td_lkp = _fetch_td_row_by_name(conn, sel_name)
            if not td_lkp.empty:
                new_mid = str(td_lkp.iloc[0]["member_id"])
                if new_mid != sel_member_id:
                    st.session_state["v_sel_member_id"] = new_mid
                    st.session_state.pop("v_sel_vote_id", None)
                    st.query_params["member"] = new_mid
                    st.query_params.pop("vote", None)
                    st.rerun()
        elif sel_member_id:
            st.session_state["_v_clear_member"] = True
            st.query_params.clear()
            st.rerun()

    # ── Mode routing ──────────────────────────────────────────────────────────
    if sel_vote_id:
        _render_mode_c(conn, sel_vote_id, v_from)
    elif sel_member_id:
        _render_mode_b(conn, sel_member_id, date_from, date_to)
    else:
        _render_mode_a(conn, date_from, date_to, outcome_filter)
