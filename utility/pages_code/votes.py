from pathlib import Path
import sys

_UTIL = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_UTIL))

import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.components import hero_banner, todo_callout, empty_state
from ui.table_config import vote_index_column_config, td_summary_column_config
from ui.export_controls import export_button
from ui.vote_explorer import render_division_panel, render_td_panel

try:
    import duckdb as _duckdb
except ImportError:
    _duckdb = None

_SQL_VIEWS = _UTIL.parent / "sql_views"
_PARQUET = (
    _UTIL.parent / "data" / "gold" / "parquet" / "current_dail_vote_history.parquet"
).as_posix()

_REQUIRED_INDEX_COLS: frozenset[str] = frozenset(
    {"vote_id", "vote_date", "debate_title", "vote_outcome", "yes_count", "no_count"}
)
_REQUIRED_TD_COLS: frozenset[str] = frozenset(
    {"member_id", "member_name", "party_name", "yes_count", "no_count", "division_count"}
)


@st.cache_resource
def _get_conn():
    if _duckdb is None:
        return None
    conn = _duckdb.connect()
    if _SQL_VIEWS.exists():
        for f in sorted(_SQL_VIEWS.glob("vote*.sql")):
            try:
                sql = f.read_text(encoding="utf-8").replace("{PARQUET_PATH}", _PARQUET)
                conn.execute(sql)
            except Exception:
                pass
    return conn


def _safe_query(conn, sql: str, params=()) -> pd.DataFrame:
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(sql, list(params)).df()
    except Exception:
        return pd.DataFrame()


def _and_clauses(clauses: list[str]) -> str:
    sql = ""
    for c in clauses:
        sql = (sql + " AND " if sql else "") + c
    return sql


@st.cache_data(ttl=300)
def _fetch_hero_stats(_conn) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT division_count, member_count, first_vote_date, last_vote_date"
        " FROM v_vote_result_summary LIMIT 1",
    )


@st.cache_data(ttl=300)
def _fetch_parties(_conn) -> list[str]:
    df = _safe_query(
        _conn,
        "SELECT DISTINCT party_name FROM td_vote_summary ORDER BY party_name LIMIT 100",
    )
    result = ["All parties"]
    if not df.empty and "party_name" in df.columns:
        for p in df["party_name"].dropna().tolist():
            result.append(str(p))
    return result


@st.cache_data(ttl=300)
def _fetch_vote_index(_conn, date_from, date_to, outcome, limit: int = 500) -> pd.DataFrame:
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
    params.append(limit)
    return _safe_query(_conn, sql, params)


@st.cache_data(ttl=300)
def _fetch_party_breakdown(_conn, vote_id) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT party_name, vote_type, member_count, vote_pct"
        " FROM party_vote_breakdown WHERE vote_id = ? LIMIT 500",
        (vote_id,),
    )


@st.cache_data(ttl=300)
def _fetch_division_members(_conn, vote_id, limit: int = 5000) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT member_name, party_name, constituency, vote_type"
        " FROM v_vote_member_detail WHERE vote_id = ?"
        " ORDER BY party_name ASC, member_name ASC LIMIT ?",
        (vote_id, limit),
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
def _fetch_td_summary(_conn, name_q: str, party, limit: int = 500) -> pd.DataFrame:
    clauses: list[str] = []
    params: list = []
    if name_q and name_q.strip():
        clauses.append("member_name LIKE ?")
        params.append(f"%{name_q.strip()}%")
    if party:
        clauses.append("party_name = ?")
        params.append(party)
    where = ""
    body = _and_clauses(clauses)
    if body:
        where = " WHERE " + body
    sql = (
        "SELECT member_id, member_name, party_name, constituency,"
        " yes_count, no_count, division_count"
        f" FROM td_vote_summary{where} ORDER BY member_name ASC LIMIT ?"
    )
    params.append(limit)
    return _safe_query(_conn, sql, params)


@st.cache_data(ttl=300)
def _fetch_td_history(_conn, member_id, date_from, date_to, limit: int = 500) -> pd.DataFrame:
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
    params.append(limit)
    return _safe_query(_conn, sql, params)


@st.cache_data(ttl=300)
def _fetch_td_year_summary(_conn, member_id) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT year, yes_count, no_count, abstained_count"
        " FROM td_vote_year_summary WHERE member_id = ? ORDER BY year ASC LIMIT 50",
        (member_id,),
    )


def _render_division_evidence(conn, vote_row: pd.Series) -> None:
    vote_id = vote_row.get("vote_id")
    members_df   = _fetch_division_members(conn, vote_id)
    sources_df   = _fetch_sources(conn, vote_id)
    breakdown_df = _fetch_party_breakdown(conn, vote_id)
    render_division_panel(vote_row, members_df, sources_df, breakdown_df)


def _render_divisions_view(conn, date_from, date_to, outcome) -> None:
    vote_df = _fetch_vote_index(conn, date_from, date_to, outcome)

    missing = sorted(c for c in _REQUIRED_INDEX_COLS if c not in vote_df.columns)
    if missing:
        mc_str = missing[0]
        for c in missing[1:]:
            mc_str += ", " + c
        todo_callout(f"v_vote_index — required columns not available: {mc_str}")
        return

    if vote_df.empty:
        empty_state(
            "No divisions found",
            "No published divisions match the current filters. "
            "Try widening the date range or clearing the outcome filter.",
        )
        return

    n = len(vote_df)
    st.caption(
        f"{n:,} division{'s' if n != 1 else ''} shown"
        f"{' · filtered' if (date_from or date_to or outcome) else ''}"
        " · click a row to inspect the full evidence"
    )

    display_cols = [
        c for c in [
            "vote_date", "debate_title", "vote_outcome",
            "yes_count", "no_count", "abstained_count", "margin",
        ]
        if c in vote_df.columns
    ]

    event = st.dataframe(
        vote_df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config=vote_index_column_config(),
        on_select="rerun",
        selection_mode="single-row",
        key="vote_idx_sel",
    )

    export_button(
        vote_df[display_cols],
        label="Export division list CSV",
        filename="dail_divisions.csv",
        key="exp_vote_idx",
    )

    rows = event.selection.rows
    if rows and rows[0] < len(vote_df):
        st.divider()
        _render_division_evidence(conn, vote_df.iloc[rows[0]])
    else:
        st.caption(
            "Select a row above to inspect the result, member votes, party breakdown, "
            "and official sources."
        )


def _render_td_evidence(conn, td_row: pd.Series, date_from, date_to) -> None:
    member_id = td_row.get("member_id")
    history_df   = _fetch_td_history(conn, member_id, date_from, date_to)
    year_df      = _fetch_td_year_summary(conn, member_id)
    render_td_panel(td_row, history_df, year_df)


def _render_td_record_view(conn, date_from, date_to, name_q: str, party) -> None:
    td_df = _fetch_td_summary(conn, name_q, party)

    missing = sorted(c for c in _REQUIRED_TD_COLS if c not in td_df.columns)
    if missing:
        mc_str = missing[0]
        for c in missing[1:]:
            mc_str += ", " + c
        todo_callout(f"td_vote_summary — required columns not available: {mc_str}")
        return

    if td_df.empty:
        empty_state(
            "No TDs found",
            "No TDs match the current search. Try a different name or clear the party filter.",
        )
        return

    n = len(td_df)
    st.caption(f"{n:,} TD{'s' if n != 1 else ''} · click a row to see their voting record")

    display_cols = [
        c for c in [
            "member_name", "party_name", "constituency",
            "division_count", "yes_count", "no_count",
        ]
        if c in td_df.columns
    ]

    td_event = st.dataframe(
        td_df[display_cols],
        use_container_width=True,
        hide_index=True,
        column_config=td_summary_column_config(),
        on_select="rerun",
        selection_mode="single-row",
        key="td_idx_sel",
    )

    export_button(
        td_df[display_cols],
        label="Export TD list CSV",
        filename="dail_td_voting_summary.csv",
        key="exp_td_idx",
    )

    td_rows = td_event.selection.rows
    if td_rows and td_rows[0] < len(td_df):
        st.divider()
        _render_td_evidence(conn, td_df.iloc[td_rows[0]], date_from, date_to)
    else:
        st.caption("Select a TD above to see their full voting record and year breakdown.")


def votes_page() -> None:
    inject_css()
    conn = _get_conn()

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown('<p class="page-kicker">Dáil Tracker</p>', unsafe_allow_html=True)
        st.markdown('<p class="page-title">Dáil<br>Divisions</p>', unsafe_allow_html=True)
        hero = _fetch_hero_stats(conn)
        if not hero.empty:
            r = hero.iloc[0]
            fd = r.get("first_vote_date")
            ld = r.get("last_vote_date")
            if fd and ld:
                st.caption(f"Data covers {str(fd)[:7]} to {str(ld)[:7]}")

    # ── Hero ──────────────────────────────────────────────────────────────────
    hero = _fetch_hero_stats(conn)
    div_count = "—"
    mem_count = "—"
    date_span = "—"
    if not hero.empty:
        r = hero.iloc[0]
        dc = r.get("division_count")
        mc = r.get("member_count")
        fd = r.get("first_vote_date")
        ld = r.get("last_vote_date")
        if dc:
            div_count = f"{int(dc):,}"
        if mc:
            mem_count = f"{int(mc):,}"
        if fd and ld:
            date_span = f"{str(fd)[:4]}–{str(ld)[:4]}"

    hero_banner(
        kicker="Dáil Divisions",
        title="How did TDs vote?",
        dek=(
            "Browse every published division. Select a vote to inspect the result, "
            "the full member breakdown, and the official Oireachtas evidence."
        ),
        badges=[f"{div_count} divisions", f"{mem_count} TDs recorded", date_span],
    )

    # ── Command bar ───────────────────────────────────────────────────────────
    outcome_sel   = "All"
    party_sel_raw = "All parties"

    with st.container(border=True):
        cb_left, cb_right = st.columns([2, 5])
        with cb_left:
            view = st.radio(
                "View",
                ["Divisions", "TD Record"],
                key="v_view",
                label_visibility="visible",
            )
        with cb_right:
            f1, f2, f3 = st.columns(3)
            with f1:
                date_from_input = st.date_input(
                    "From", value=None, key="v_date_from", label_visibility="visible"
                )
            with f2:
                date_to_input = st.date_input(
                    "To", value=None, key="v_date_to", label_visibility="visible"
                )
            with f3:
                if view == "Divisions":
                    outcome_sel = st.selectbox(
                        "Outcome",
                        ["All", "Carried", "Lost"],
                        key="v_outcome",
                        label_visibility="visible",
                    )
                else:
                    parties = _fetch_parties(conn)
                    party_sel_raw = st.selectbox(
                        "Party",
                        parties,
                        key="v_party",
                        label_visibility="visible",
                    )

    # Name search below command bar (TD Record only — full width for readability)
    name_q = ""
    if view == "TD Record":
        name_q = st.text_input(
            "Search by TD name",
            placeholder="e.g. McDonald, Harris, Doherty…",
            key="v_name",
            label_visibility="visible",
        )

    date_from = str(date_from_input) if date_from_input else None
    date_to   = str(date_to_input)   if date_to_input   else None

    if view == "Divisions":
        outcome_filter = None if outcome_sel == "All" else outcome_sel
        party_filter   = None
    else:
        outcome_filter = None
        party_filter   = None if (not party_sel_raw or party_sel_raw == "All parties") else party_sel_raw

    st.divider()

    if view == "Divisions":
        _render_divisions_view(conn, date_from, date_to, outcome_filter)
    else:
        _render_td_record_view(conn, date_from, date_to, name_q, party_filter)
