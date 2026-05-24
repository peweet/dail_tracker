"""
Member Overview — single-politician public accountability record.

Stage 1: Browse all TDs (identity columns; mart pending)
Stage 2: Full accountability profile — Attendance, Votes, Payments, Lobbying, Legislation

Entry points: row click, sidebar selectbox, ?member=join_key URL param

TODO_PIPELINE_VIEW_REQUIRED: v_member_overview_browse
  Pending columns: attendance_rate, payment_total_eur, declared_interests_count,
  lobbying_interactions_count, revolving_door_flag, government_status
"""

from __future__ import annotations

import logging
from html import escape as _h
from pathlib import Path
import sys

import pandas as pd
import streamlit as st

_UTIL = Path(__file__).resolve().parent.parent
if str(_UTIL) not in sys.path:
    sys.path.insert(0, str(_UTIL))

from shared_css import inject_css
from ui.avatars import avatar_credit_html, avatar_data_url, initials as _initials
from ui.components import (
    back_button,
    clean_meta,
    clickable_card_link,
    empty_state,
    find_a_td_filter,
    glossary_strip,
    member_card_html,
    page_error_boundary,
    paginate,
    pagination_controls,
    sidebar_date_range,
    sidebar_page_header,
    stat_strip,
)
from ui.entity_links import (
    PAGES,
    entity_cta_html,
    member_profile_url,
    member_votes_url,
    oireachtas_profile_url,
    social_icon_chip_html,
    source_link_html,
)
from ui.vote_explorer import member_vote_card_html
from data_access.member_overview_data import get_member_overview_conn
from pages_code.attendance import render_member_attendance
from pages_code.interests import render_member_interests
from pages_code.lobbying_2 import render_member_lobbying
from pages_code.payments import render_member_payments
from data_access.payments_data import fetch_filter_options as _pay_filter_options
from data_access.payments_data import fetch_payments_summary as _pay_summary

_log = logging.getLogger(__name__)
_STAGE_KEY = "mo_join_key"

_POLICY_AREAS: list[tuple[str, str]] = [
    ("Housing", "housing"),
    ("Health", "health"),
    ("Education", "education"),
    ("Defence", "defence"),
    ("Europe", "europe"),
    ("Crime", "crime"),
    ("Environment", "environment"),
    ("Social Welfare", "social welfare"),
    ("Finance", "finance"),
    ("Agriculture", "agriculture"),
    ("Transport", "transport"),
    ("Immigration", "immigration"),
]
_AREA_LABELS: list[str] = [lbl for lbl, _ in _POLICY_AREAS]
_AREA_LABEL_TO_KW: dict[str, str] = {lbl: kw for lbl, kw in _POLICY_AREAS}

# ── Profile section IA (Phase 2 chrome) ────────────────────────────────────────
# Section order is "most politically potent first" per project_design_principles.
# (id, expander label, ranking-page key in entity_links.PAGES). The id is the
# URL-fragment anchor (`/member-overview?member=<code>#<id>`) and the
# session-state suffix (`mo_open_<id>`). The rankings page key is used to
# render "see league table" deep links from each section's empty/lifted body.
_PROFILE_SECTIONS: list[tuple[str, str, str]] = [
    ("interests", "Interests", "interests"),
    ("lobbying", "Lobbying", "lobbying"),
    ("payments", "Payments", "payments"),
    ("attendance", "Attendance", "attendance"),
    ("votes", "Votes", "votes"),
    ("legislation", "Legislation", "legislation"),
    ("committees", "Committees", "committees"),
]


# ── Data retrieval ─────────────────────────────────────────────────────────────


def _q(conn, sql: str, params: list | None = None) -> pd.DataFrame:
    if conn is None:
        return pd.DataFrame()
    try:
        return conn.execute(sql, params or []).df()
    except Exception as exc:
        _log.warning("member_overview | %s | %s", sql[:80], exc)
        return pd.DataFrame()


@st.cache_data(ttl=300)
def _member_list(_conn) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT unique_member_code, member_name, party_name, constituency FROM v_member_registry ORDER BY member_name",
    )


@st.cache_data(ttl=300)
def _join_key_by_name(_conn, name: str) -> str | None:
    df = _q(
        _conn,
        "SELECT unique_member_code FROM v_member_registry WHERE member_name = ? LIMIT 1",
        [name],
    )
    return str(df.iloc[0]["unique_member_code"]) if not df.empty else None


@st.cache_data(ttl=300)
def _identity(_conn, join_key: str) -> dict:
    # Attendance first — has year; fall back to canonical registry if no record
    df = _q(
        _conn,
        "SELECT member_name, party_name, constituency, is_minister, year"
        " FROM v_attendance_member_year_summary"
        " WHERE unique_member_code = ? ORDER BY year DESC LIMIT 1",
        [join_key],
    )
    if not df.empty:
        return df.iloc[0].to_dict()
    df = _q(
        _conn,
        "SELECT member_name, party_name, constituency, is_minister"
        " FROM v_member_registry WHERE unique_member_code = ? LIMIT 1",
        [join_key],
    )
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _att_all_years(_conn, join_key: str) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT year, attended_count, is_minister"
        " FROM v_attendance_member_year_summary"
        " WHERE unique_member_code = ? ORDER BY year DESC LIMIT 20",
        [join_key],
    )


@st.cache_data(ttl=300)
def _att_rank_for_year(_conn, join_key: str, year: int) -> tuple[int | None, int | None]:
    """Member's attendance rank for a given year and the total ranked field size.
    Returns (rank_high, total). Both None on miss. Retrieval-only."""
    df = _q(
        _conn,
        "SELECT rank_high FROM v_attendance_year_rank WHERE unique_member_code = ? AND year = ? LIMIT 1",
        [join_key, year],
    )
    if df.empty:
        return None, None
    total_df = _q(
        _conn,
        "SELECT COUNT(*) AS n FROM v_attendance_year_rank WHERE year = ?",
        [year],
    )
    rank = int(df.iloc[0]["rank_high"]) if pd.notna(df.iloc[0]["rank_high"]) else None
    total = int(total_df.iloc[0]["n"]) if not total_df.empty else None
    return rank, total


@st.cache_data(ttl=300)
def _external_links(_conn, join_key: str) -> dict:
    """Wikidata-sourced socials + Wikipedia URL for the hero chips row.

    Returns an empty dict when the view is missing (Wikidata ETL not yet run)
    or the member has no entry — both are normal, the UI just renders fewer
    chips. Every value is the pre-derived URL; the raw handles aren't needed
    here (kept in the parquet for replay/debug only).
    """
    df = _q(
        _conn,
        "SELECT wikipedia_url, twitter_url, bluesky_url, facebook_url,"
        " instagram_url, website_url"
        " FROM v_member_external_links WHERE unique_member_code = ? LIMIT 1",
        [join_key],
    )
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    # Drop nulls so the hero block only iterates over populated platforms.
    return {k: v for k, v in row.items() if isinstance(v, str) and v.strip()}


@st.cache_data(ttl=300)
def _votes_summary(_conn, join_key: str) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_id = ? LIMIT 1",
        [join_key],
    )


@st.cache_data(ttl=300)
def _votes_by_topic(
    _conn,
    join_key: str,
    keyword: str | None = None,
    date_from: str | None = None,
    date_to: str | None = None,
) -> pd.DataFrame:
    """Per-member divisions, optionally filtered by debate-title keyword and date.

    Retrieval-only: SELECT with WHERE/ORDER BY/LIMIT against v_vote_member_detail.
    keyword=None disables the topic LIKE filter (used for the "All topics" pill).
    """
    clauses: list[str] = ["member_id = ?"]
    params: list = [join_key]
    if keyword:
        clauses.append("LOWER(debate_title) LIKE LOWER(?)")
        params.append(f"%{keyword}%")
    if date_from:
        clauses.append("vote_date >= ?")
        params.append(date_from)
    if date_to:
        clauses.append("vote_date <= ?")
        params.append(date_to)
    where = " AND ".join(clauses)
    return _q(
        _conn,
        "SELECT vote_date, debate_title, vote_type, vote_outcome, oireachtas_url"
        " FROM v_vote_member_detail"
        f" WHERE {where}"
        " ORDER BY vote_date DESC LIMIT 1000",
        params,
    )


@st.cache_data(ttl=300)
def _member_vote_years(_conn, join_key: str) -> list[int]:
    df = _q(
        _conn,
        "SELECT DISTINCT CAST(EXTRACT(YEAR FROM vote_date) AS INTEGER) AS year"
        " FROM v_vote_member_detail"
        " WHERE member_id = ? AND vote_date IS NOT NULL"
        " ORDER BY year DESC LIMIT 30",
        [join_key],
    )
    if df.empty or "year" not in df.columns:
        return []
    return [int(y) for y in df["year"].dropna().tolist()]


@st.cache_data(ttl=300)
def _pay_overview(_conn, join_key: str) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT payment_year, total_paid, taa_band_label, payment_count"
        " FROM v_payments_yearly_evolution"
        " WHERE unique_member_code = ? ORDER BY payment_year DESC LIMIT 20",
        [join_key],
    )


@st.cache_data(ttl=300)
def _pay_grand_total(_conn, join_key: str) -> float:
    # SUM permitted as presentation-layer scalar — contract §headline_metrics_row note
    df = _q(
        _conn,
        "SELECT SUM(amount_num) AS total FROM v_payments_member_detail WHERE unique_member_code = ?",
        [join_key],
    )
    if df.empty or pd.isna(df.iloc[0]["total"]):
        return 0.0
    return float(df.iloc[0]["total"])


@st.cache_data(ttl=300)
def _lobbying_rd(_conn, join_key: str) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT individual_name, former_position, return_count, distinct_firms"
        " FROM v_lobbying_revolving_door WHERE unique_member_code = ? LIMIT 5",
        [join_key],
    )


@st.cache_data(ttl=300)
def _legislation(_conn, join_key: str) -> pd.DataFrame:
    return _q(
        _conn,
        "SELECT bill_title, bill_status, bill_year, oireachtas_url"
        " FROM v_legislation_index"
        " WHERE sponsor_join_key = ?"
        " ORDER BY introduced_date DESC NULLS LAST LIMIT 50",
        [join_key],
    )


@st.cache_data(ttl=300)
def _si_signed(_conn, join_key: str) -> pd.DataFrame:
    """SIs the member signed as a departmental minister. Joined on
    si_minister_member_code (= unique_member_code)."""
    return _q(
        _conn,
        "SELECT si_id, si_year, si_title, si_signed_date, si_operation,"
        " si_department_label, si_is_eu, eisb_url"
        " FROM v_statutory_instruments"
        " WHERE si_minister_member_code = ?"
        " ORDER BY si_signed_date DESC NULLS LAST",
        [join_key],
    )


@st.cache_data(ttl=300)
def _debate_years(_conn, join_key: str) -> list[int]:
    df = _q(
        _conn,
        "SELECT DISTINCT debate_year FROM v_member_debate_sections"
        " WHERE unique_member_code = ? AND debate_year IS NOT NULL"
        " ORDER BY debate_year DESC LIMIT 30",
        [join_key],
    )
    if df.empty or "debate_year" not in df.columns:
        return []
    return [int(y) for y in df["debate_year"].dropna().tolist()]


@st.cache_data(ttl=300)
def _debate_topics(_conn, join_key: str, year: int | None = None) -> list[str]:
    clauses = ["unique_member_code = ?", "topic IS NOT NULL"]
    params: list = [join_key]
    if year is not None:
        clauses.append("debate_year = ?")
        params.append(year)
    df = _q(
        _conn,
        f"SELECT DISTINCT topic FROM v_member_debate_sections WHERE {' AND '.join(clauses)} ORDER BY topic LIMIT 100",
        params,
    )
    if df.empty or "topic" not in df.columns:
        return []
    return [str(t) for t in df["topic"].dropna().tolist()]


@st.cache_data(ttl=300)
def _debate_sections(
    _conn,
    join_key: str,
    year: int | None = None,
    topic: str | None = None,
) -> pd.DataFrame:
    """Debate sections a TD raised a question in — retrieval-only filter on
    v_member_debate_sections (SELECT / WHERE / ORDER BY / LIMIT)."""
    clauses = ["unique_member_code = ?"]
    params: list = [join_key]
    if year is not None:
        clauses.append("debate_year = ?")
        params.append(year)
    if topic:
        clauses.append("topic = ?")
        params.append(topic)
    return _q(
        _conn,
        "SELECT debate_date, debate_section_id, chamber, topic,"
        " question_count, oireachtas_url"
        " FROM v_member_debate_sections"
        f" WHERE {' AND '.join(clauses)}"
        " ORDER BY debate_date DESC LIMIT 1000",
        params,
    )


# ── Profile section renderers ──────────────────────────────────────────────────


def _section_votes(
    conn,
    join_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> None:
    st.html('<p class="section-heading">Voting record by issue</p>')

    summary = _votes_summary(conn, join_key)
    if not summary.empty:
        r = summary.iloc[0]
        yes = int(r.get("yes_count", 0) or 0)
        no = int(r.get("no_count", 0) or 0)
        ab = int(r.get("abstained_count", 0) or 0)
        div = int(r.get("division_count", 0) or 0)
        rate_pct = float(r.get("yes_rate_pct", 0) or 0)
        cast = yes + no + ab
        voted_pct = round(100.0 * cast / div, 1) if div else 0.0
        st.html(
            f"<p style=\"font-family:'Epilogue',sans-serif;font-size:0.95rem;"
            f'color:var(--text-secondary);margin:0 0 0.75rem;">'
            f"Voted in <strong>{voted_pct}%</strong> of divisions — "
            f"<strong>{rate_pct}%</strong> Aye&nbsp;·&nbsp;"
            f"<strong>{round(100 - rate_pct, 1)}%</strong> Níl when cast.</p>"
        )

    # ── Filter row 1: policy area (with "All topics") ────────────────────
    area_options = ["All topics"] + _AREA_LABELS
    selected_area = (
        st.pills(
            "Policy area",
            options=area_options,
            default="All topics",
            key="mo_vote_area",
            label_visibility="collapsed",
        )
        or "All topics"
    )

    # ── Filter row 2: year ──────────────────────────────────────────────
    available_years = _member_vote_years(conn, join_key)
    if available_years:
        year_opts = ["All years"] + [str(y) for y in available_years]
        selected_year = (
            st.radio(
                "Year",
                options=year_opts,
                index=0,
                horizontal=True,
                key="mo_vote_year",
                label_visibility="collapsed",
            )
            or "All years"
        )
    else:
        selected_year = "All years"

    # ── Resolve filters ─────────────────────────────────────────────────
    keyword = None if selected_area == "All topics" else _AREA_LABEL_TO_KW.get(selected_area)

    # Year pill takes precedence over the sidebar date range when set.
    eff_from = date_from
    eff_to = date_to
    if selected_year != "All years":
        eff_from = f"{selected_year}-01-01"
        eff_to = f"{selected_year}-12-31"

    topic_df = _votes_by_topic(conn, join_key, keyword, eff_from, eff_to)

    if topic_df.empty:
        scope = selected_area if selected_area != "All topics" else "any topic"
        year_note = (
            f" in {selected_year}"
            if selected_year != "All years"
            else " in this date range"
            if (eff_from or eff_to)
            else ""
        )
        empty_state(
            f"No votes on {scope}{year_note}",
            "Try widening the year, picking 'All topics', or clearing the date filter in the sidebar.",
        )
        return

    total = len(topic_df)
    PAGE_SIZE = 10
    # Pager key includes the active filter signature so changing any filter
    # resets to page 1 instead of leaving the user stranded past the new end.
    filter_sig = f"{keyword or 'all'}_{selected_year}_{eff_from or '_'}_{eff_to or '_'}"
    pager_key = f"mo_vote_topic_{join_key}_{filter_sig}"
    page_idx = paginate(total, key_prefix=pager_key, page_size=PAGE_SIZE)
    visible = topic_df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    start = page_idx * PAGE_SIZE + 1
    end = min((page_idx + 1) * PAGE_SIZE, total)
    scope_label = selected_area if selected_area != "All topics" else "all topics"
    year_label = selected_year if selected_year != "All years" else "all years"
    st.caption(
        f"Showing {start:,}–{end:,} of {total:,} division{'s' if total != 1 else ''} on {scope_label} · {year_label}"
    )

    for _, row in visible.iterrows():
        url = str(row.get("oireachtas_url", "") or "")
        if url in ("nan", "None"):
            url = ""
        st.html(
            member_vote_card_html(
                vote_date=row.get("vote_date"),
                debate_title=str(row.get("debate_title", "—")),
                vote_type=str(row.get("vote_type", "—")),
                vote_outcome=str(row.get("vote_outcome", "—")),
                oireachtas_url=url,
            )
        )

    pagination_controls(
        total=total,
        key_prefix=pager_key,
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="divisions",
        show_caption=False,
    )


def _section_legislation(conn, join_key: str, member_name: str) -> None:
    st.html('<p class="section-heading">Legislation sponsored</p>')

    df = _legislation(conn, join_key)
    if df.empty:
        empty_state(
            "No bills found",
            f"No bills sponsored by {member_name} in v_legislation_index.",
        )
        return

    n = len(df)
    st.caption(f"{n} bill{'s' if n != 1 else ''} sponsored")

    for _, row in df.iterrows():
        title = str(row.get("bill_title", "—"))
        status = str(row.get("bill_status", "—"))
        year = str(row.get("bill_year", "—"))
        url = str(row.get("oireachtas_url", "") or "")

        sl = status.lower()
        status_css = (
            "leg-status-enacted"
            if ("enact" in sl or "sign" in sl)
            else "leg-status-lapsed"
            if sl in ("lapsed", "withdrawn", "defeated")
            else "leg-status-active"
        )
        if url in ("nan", "None"):
            url = ""
        url_html = source_link_html(
            url,
            "Oireachtas.ie",
            aria_label="Open this bill on oireachtas.ie",
        )
        st.html(
            f'<div class="leg-bill-card" style="margin-bottom:0.3rem;">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(year)}</span>'
            f'<span class="signal {status_css}">{_h(status)}</span>'
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(title)}</div>'
            f'<div style="margin-top:0.2rem;">{url_html}</div>'
            f"</div>"
        )

    st.download_button(
        label="Export legislation (CSV)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"legislation_{member_name.replace(' ', '_')}.csv",
        mime="text/csv",
        disabled=df.empty,
        key="mo_leg_export",
        width="stretch",
    )


def _section_statutory_instruments(conn, join_key: str) -> None:
    """SIs the member signed as a minister — secondary legislation made by
    ministerial order. Conditional: rendered only when at least one SI
    resolves to this member, so non-ministers see nothing. Resolution covers
    the current government only (the limit of the ministerial-tenure data)."""
    df = _si_signed(conn, join_key)
    if df.empty:
        return

    st.divider()
    st.html('<p class="section-heading">Statutory Instruments signed</p>')

    n = len(df)
    depts = [d for d in df["si_department_label"].dropna().unique().tolist()]
    dept_str = ", ".join(depts) if depts else "—"
    eu_n = int(df["si_is_eu"].fillna(False).astype(bool).sum())
    st.caption(
        f"{n} statutory instrument{'s' if n != 1 else ''} signed as a minister "
        f"({dept_str}) — secondary legislation made by ministerial order, "
        f"{eu_n} of it EU-derived. Covers the current government only."
    )

    for _, row in df.head(50).iterrows():
        op = _h(str(row.get("si_operation", "") or "").replace("_", " ")) or "—"
        url = str(row.get("eisb_url", "") or "")
        eu_badge = (
            '<span class="signal" style="background:#fef3c7;border-color:#fcd34d;'
            'color:#92400e;margin-left:0.25rem;">EU</span>'
            if bool(row.get("si_is_eu"))
            else ""
        )
        url_html = (
            source_link_html(
                url,
                "irishstatutebook.ie",
                aria_label="Open this SI on irishstatutebook.ie",
            )
            if url.startswith("http")
            else ""
        )
        st.html(
            f'<div class="leg-bill-card" style="margin-bottom:0.3rem;">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">SI {_h(str(row.get("si_id", "—")))}</span>'
            f'<span class="signal leg-status-active">{op}</span>'
            f"{eu_badge}"
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(str(row.get("si_title", "—")))}</div>'
            f'<div style="margin-top:0.2rem;">{url_html}</div>'
            f"</div>"
        )

    st.download_button(
        label="Export statutory instruments (CSV)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"si_signed_{join_key}.csv",
        mime="text/csv",
        key="mo_si_export",
        width="stretch",
    )


def _section_debates(conn, join_key: str, member_name: str) -> None:
    st.html('<p class="section-heading">Debate participation</p>')
    st.caption(
        "Debate sections where this TD raised a parliamentary question, "
        "linked to the record on oireachtas.ie. Floor-speech attribution "
        "(who said what) is pending the debates Stage 2 AKN-XML layer."
    )

    years = _debate_years(conn, join_key)
    if not years:
        empty_state(
            "No debate references found",
            f"No parliamentary questions by {member_name} map to a debate section in v_member_debate_sections.",
        )
        return

    # ── Year filter (pills) ──────────────────────────────────────────────
    year_opts = ["All years"] + [str(y) for y in years]
    selected_year = (
        st.pills(
            "Debate year",
            options=year_opts,
            default="All years",
            key="mo_debate_year",
            label_visibility="collapsed",
        )
        or "All years"
    )
    year_val = None if selected_year == "All years" else int(selected_year)

    # ── Topic filter (selectbox — topics are free-form and numerous) ─────
    # Key is year-scoped: changing year refreshes the topic list cleanly
    # instead of stranding a now-absent selection in session state.
    topics = _debate_topics(conn, join_key, year_val)
    selected_topic = (
        st.selectbox(
            "Topic",
            options=["All topics"] + topics,
            index=0,
            key=f"mo_debate_topic_{year_val or 'all'}",
            label_visibility="collapsed",
        )
        or "All topics"
    )
    topic_val = None if selected_topic == "All topics" else selected_topic

    df = _debate_sections(conn, join_key, year_val, topic_val)
    if df.empty:
        empty_state(
            "No debate references match these filters",
            "Try a different year or topic.",
        )
        return

    total = len(df)
    PAGE_SIZE = 10
    filter_sig = f"{year_val or 'all'}_{topic_val or 'all'}"
    pager_key = f"mo_debate_{join_key}_{filter_sig}"
    page_idx = paginate(total, key_prefix=pager_key, page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    start = page_idx * PAGE_SIZE + 1
    end = min((page_idx + 1) * PAGE_SIZE, total)
    st.caption(f"Showing {start:,}–{end:,} of {total:,} debate section{'s' if total != 1 else ''}")

    for _, row in visible.iterrows():
        date_raw = str(row.get("debate_date", "") or "")
        try:
            date_disp = pd.to_datetime(date_raw).strftime("%d %b %Y")
        except Exception:
            date_disp = date_raw
        chamber = str(row.get("chamber", "") or "").title() or "—"
        topic = str(row.get("topic", "") or "").strip() or "—"
        qcount = int(row.get("question_count", 0) or 0)
        url = str(row.get("oireachtas_url", "") or "")
        if url in ("nan", "None"):
            url = ""
        url_html = source_link_html(
            url,
            "Oireachtas.ie",
            aria_label="Open this debate section on oireachtas.ie",
        )
        st.html(
            f'<div class="leg-bill-card" style="margin-bottom:0.3rem;">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(date_disp)}</span>'
            f'<span class="signal leg-status-active">{_h(chamber)}</span>'
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(topic)}</div>'
            f'<div style="margin-top:0.2rem;font-size:0.85rem;'
            f'color:var(--text-secondary);">'
            f"{qcount} question{'s' if qcount != 1 else ''} raised"
            f"&nbsp;·&nbsp;{url_html}</div>"
            f"</div>"
        )

    pagination_controls(
        total=total,
        key_prefix=pager_key,
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="debate sections",
        show_caption=False,
    )


def _section_committees() -> None:
    st.html('<p class="section-heading">Committees</p>')
    st.html(
        '<div class="leg-todo-callout">'
        '<span class="leg-todo-label">TODO PIPELINE VIEW REQUIRED</span>'
        " Per-member committee membership is pending the committees-page refactor."
        " Required view: <code>v_committee_membership</code> with columns"
        " <b>unique_member_code</b>, <b>committee_name</b>, <b>role</b> (Chair / Member),"
        " <b>start_date</b>, <b>end_date</b>."
        "</div>"
    )


_OTHER_PILL = "Other / Independent"
_OTHER_MIN = 3  # parties with fewer TDs are grouped into Other


def _named_parties(df: pd.DataFrame) -> list[str]:
    """Parties with >= _OTHER_MIN members, sorted by size desc then name."""
    if df.empty or "party_name" not in df.columns:
        return []
    counts = df["party_name"].value_counts()
    parties = df["party_name"].dropna().astype(str).unique().tolist()
    parties = [p for p in parties if p and p.lower() not in ("nan", "")]
    named = [p for p in parties if int(counts.get(p, 0)) >= _OTHER_MIN]
    return sorted(named, key=lambda p: (-int(counts.get(p, 0)), p))


def _party_pill_options(df: pd.DataFrame) -> list[str]:
    named = _named_parties(df)
    if not named:
        return []
    counts = df["party_name"].value_counts()
    in_named = sum(int(counts.get(p, 0)) for p in named)
    has_other = (len(df) - in_named) > 0
    return named + ([_OTHER_PILL] if has_other else [])


def _render_browse(conn) -> None:
    df = _member_list(conn)

    st.html(
        '<div class="dt-hero">'
        '<p class="dt-kicker">MEMBER OVERVIEW</p>'
        '<h1 style="margin:0.1rem 0 0.25rem;font-size:1.85rem;font-weight:700;'
        "font-family:'Zilla Slab',Georgia,serif;\">Browse all TDs</h1>"
        '<p class="dt-dek">Pick a TD to open their accountability profile: '
        "attendance, votes by policy area, payments, lobbying, and legislation.</p>"
        "</div>"
    )
    glossary_strip(
        [
            ("TD", "Teachta Dála, a member of the Dáil"),
            ("Accountability profile", "attendance, votes, payments, lobbying, and legislation in one place"),
        ]
    )

    if df.empty:
        empty_state("No member data", "Run the pipeline to generate attendance parquet files.")
        return

    df = df.drop_duplicates(subset=["unique_member_code"], keep="first").reset_index(drop=True)

    member_names = df["member_name"].dropna().astype(str).tolist()
    search, picked = find_a_td_filter(
        member_names,
        key_prefix="mo_browse",
        label="Find a TD",
        placeholder="Search by name, party or constituency…",
    )
    if picked:
        picked_jk = _join_key_by_name(conn, picked)
        if picked_jk:
            st.session_state[_STAGE_KEY] = picked_jk
            st.query_params["member"] = picked_jk
            st.rerun()

    party_options = _party_pill_options(df)
    selected_party = st.pills(
        "Party",
        options=["All parties"] + party_options,
        default="All parties",
        key="mo_browse_party",
        label_visibility="collapsed",
    )

    sq = (search or "").strip().lower()
    filtered = df.copy()
    if selected_party == _OTHER_PILL:
        named_set = set(_named_parties(df))
        filtered = filtered[filtered["party_name"].isna() | ~filtered["party_name"].isin(named_set)]
    elif selected_party and selected_party != "All parties":
        filtered = filtered[filtered["party_name"] == selected_party]
    if sq:
        mask = (
            filtered["member_name"].astype(str).str.lower().str.contains(sq, na=False)
            | filtered["party_name"].astype(str).str.lower().str.contains(sq, na=False)
            | filtered["constituency"].astype(str).str.lower().str.contains(sq, na=False)
        )
        filtered = filtered[mask]

    filtered = filtered.sort_values("member_name", kind="stable").reset_index(drop=True)

    showing = len(filtered)

    # Results pill — shows the current filtered count above the grid.
    st.html(f'<p class="section-heading">{showing:,} TD{"s" if showing != 1 else ""}</p>')

    if filtered.empty:
        empty_state(
            "No TDs match your filters",
            "Try clearing the search box or choosing a different party.",
        )
        return

    # Resolve the current page slice via the reusable paginate() helper.
    # The pagination_controls() call below renders the chip row + caption
    # underneath the grid using the same key_prefix / page_size.
    MO_PAGE_SIZE = 12
    pager_key = "mo_browse"
    page_idx = paginate(showing, key_prefix=pager_key, page_size=MO_PAGE_SIZE)
    visible = filtered.iloc[page_idx * MO_PAGE_SIZE : (page_idx + 1) * MO_PAGE_SIZE]

    cards = ['<div class="mo-grid">']
    for _, row in visible.iterrows():
        name = str(row.get("member_name", ""))
        party = str(row.get("party_name", "") or "")
        constit = str(row.get("constituency", "") or "")
        code = str(row["unique_member_code"])
        meta = clean_meta(party, constit)
        cards.append(
            clickable_card_link(
                href=member_profile_url(code),
                inner_html=member_card_html(
                    name=name,
                    meta=meta,
                    avatar_url=avatar_data_url(name),
                    avatar_initials=_initials(name),
                ),
                aria_label=f"View {name}",
            )
        )
    cards.append("</div>")
    st.html("\n".join(cards))

    # Pager sits BELOW the grid for less visual noise above.
    st.html('<div class="mo-browse-pager-spacer"></div>')
    pagination_controls(
        total=showing,
        key_prefix=pager_key,
        page_sizes=(MO_PAGE_SIZE,),
        default_page_size=MO_PAGE_SIZE,
        label="TDs",
    )


# ── Profile ─────────────────────────────────────────────────────────────────────


def _prev_next_member(conn, join_key: str) -> tuple[dict | None, dict | None]:
    """Return (prev, next) member dicts in alphabetical-name order, or None at ends.

    Retrieval-only: reuses _member_list which already SELECTs from v_member_registry
    ORDER BY member_name. Wraps at the ends to None so the buttons can disable.
    """
    df = _member_list(conn)
    if df.empty:
        return None, None
    df = df.drop_duplicates(subset=["unique_member_code"], keep="first").reset_index(drop=True)
    idx_match = df.index[df["unique_member_code"] == join_key]
    if len(idx_match) == 0:
        return None, None
    i = int(idx_match[0])
    prev_row = df.iloc[i - 1].to_dict() if i > 0 else None
    next_row = df.iloc[i + 1].to_dict() if i < len(df) - 1 else None
    return prev_row, next_row


def _render_profile_nav(conn, join_key: str) -> None:
    """Top-of-profile nav: [← All TDs] [← prev TD] [next TD →].

    Reuses the existing back_button styling. Prev/next set the stage join key
    and clear the query params so the URL reflects the new selection.
    """
    prev_row, next_row = _prev_next_member(conn, join_key)
    c_back, c_prev, c_next = st.columns([3, 4, 4])
    with c_back:
        if back_button("← All TDs", key="mo_all", help="Return to the full TD list"):
            st.session_state.pop(_STAGE_KEY, None)
            st.query_params.clear()
            st.rerun()
    with c_prev:
        if prev_row is not None:
            label = f"← {prev_row['member_name']}"
            if st.button(
                label, key="mo_prev_td", help=f"Previous TD alphabetically: {prev_row['member_name']}", width="stretch"
            ):
                st.session_state[_STAGE_KEY] = str(prev_row["unique_member_code"])
                st.query_params.clear()
                st.query_params["member"] = str(prev_row["unique_member_code"])
                st.rerun()
        else:
            st.button("← (start of list)", key="mo_prev_td_disabled", disabled=True, width="stretch")
    with c_next:
        if next_row is not None:
            label = f"{next_row['member_name']} →"
            if st.button(
                label, key="mo_next_td", help=f"Next TD alphabetically: {next_row['member_name']}", width="stretch"
            ):
                st.session_state[_STAGE_KEY] = str(next_row["unique_member_code"])
                st.query_params.clear()
                st.query_params["member"] = str(next_row["unique_member_code"])
                st.rerun()
        else:
            st.button("(end of list) →", key="mo_next_td_disabled", disabled=True, width="stretch")


def _render_stage2(
    conn,
    join_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> None:

    _render_profile_nav(conn, join_key)

    identity = _identity(conn, join_key)
    if not identity:
        browse_href = f"/{PAGES['member_overview']}"
        st.html(
            f'<div class="dt-callout">'
            f"<strong>This TD is not in the dataset</strong><br>"
            f'<span style="color:var(--text-meta)">No record matched <code>{_h(join_key)}</code> '
            f"in <code>v_attendance_member_year_summary</code>. The link you followed may be "
            f"out of date, or the pipeline has not yet ingested this member.</span><br>"
            f'<a class="dt-member-link" href="{_h(browse_href)}" target="_self" '
            f'style="margin-top:0.6rem;display:inline-block;">← Browse all TDs</a>'
            f"</div>"
        )
        return

    member_name = str(identity.get("member_name", ""))
    party = str(identity.get("party_name", ""))
    constituency = str(identity.get("constituency", ""))
    is_minister = str(identity.get("is_minister", "false")).lower() == "true"
    meta = clean_meta(party, constituency)

    role_html = (
        '<span class="dt-badge dt-badge-minister">Minister</span>'
        if is_minister
        else '<span class="dt-badge dt-badge-td">TD</span>'
    )

    rd_df = _lobbying_rd(conn, join_key)
    rd_html = '<span class="dt-badge dt-badge-revolving">Revolving door</span>' if not rd_df.empty else ""

    photo_url = avatar_data_url(member_name)
    photo_credit = avatar_credit_html(member_name)
    if photo_url:
        avatar_block = f'<img class="dt-profile-avatar" src="{_h(photo_url)}" alt="" loading="lazy">'
        caption_block = f'<p class="dt-profile-avatar-credit">{photo_credit}</p>' if photo_credit else ""
    else:
        avatar_block = f'<span class="dt-profile-initials" aria-hidden="true">{_h(_initials(member_name))}</span>'
        caption_block = '<p class="dt-profile-avatar-empty">No photo available</p>'

    # Hero meta strip — TD/Minister/Revolving badges share one flex row with
    # the external-link chips. Two visual "zones" inside that row:
    #   1. role/status (existing dt-badge pills)
    #   2. find-online (label chips for Profile + Wikipedia, icon chips for
    #      Twitter / Bluesky / Facebook / Instagram / Website)
    # A thin .dt-hero-sep separates the two zones without adding a heavier
    # divider; the whole row flex-wraps gracefully on narrow viewports.
    ext = _external_links(conn, join_key)
    badge_parts: list[str] = [role_html]
    if rd_html:
        badge_parts.append(rd_html)

    link_parts: list[str] = []
    profile_href = oireachtas_profile_url(join_key)
    if profile_href:
        chip = source_link_html(
            profile_href,
            "Official profile",
            aria_label=f"Open {member_name}'s official Oireachtas profile in a new tab",
        )
        if chip:
            link_parts.append(chip)
    wiki_href = ext.get("wikipedia_url")
    if wiki_href:
        chip = source_link_html(
            wiki_href,
            "Wikipedia",
            aria_label=f"Open {member_name}'s Wikipedia article in a new tab",
        )
        if chip:
            link_parts.append(chip)
    for platform, key in (
        ("twitter", "twitter_url"),
        ("bluesky", "bluesky_url"),
        ("facebook", "facebook_url"),
        ("instagram", "instagram_url"),
        ("website", "website_url"),
    ):
        chip = social_icon_chip_html(platform, ext.get(key), person_name=member_name)
        if chip:
            link_parts.append(chip)

    sep_html = '<span class="dt-hero-sep" aria-hidden="true"></span>' if link_parts else ""
    meta_row = (
        '<div class="dt-hero-meta-row">'
        + "".join(badge_parts)
        + sep_html
        + "".join(link_parts)
        + "</div>"
    )

    st.html(
        f'<div class="dt-hero">'
        f'  <p class="dt-kicker">TD ACCOUNTABILITY RECORD</p>'
        f'  <div class="dt-profile-header">'
        f'    <div class="dt-profile-avatar-col">{avatar_block}{caption_block}</div>'
        f'    <div class="dt-profile-meta-col">'
        f'      <h1 class="td-name" style="margin:0.15rem 0 0.2rem;">{_h(member_name)}</h1>'
        f'      <p class="td-meta" style="margin:0 0 0.55rem;">{_h(meta)}</p>'
        f"      {meta_row}"
        f"    </div>"
        f"  </div>"
        f"</div>"
    )

    # ── Headline stats — single source of truth, no duplication ──────────────
    att_df = _att_all_years(conn, join_key)
    pay_total = _pay_grand_total(conn, join_key)
    vote_df = _votes_summary(conn, join_key)

    if not att_df.empty:
        att_yr = int(att_df.iloc[0]["year"])
        att_days = int(att_df.iloc[0]["attended_count"])
        is_min = str(att_df.iloc[0].get("is_minister", "false")).lower() == "true"
        att_lbl = f"Days in chamber · {att_yr}"
        att_val = str(att_days)
        if is_min:
            att_sub = "Minister · plenary record only"
        else:
            rank, total = _att_rank_for_year(conn, join_key, att_yr)
            att_sub = f"Rank {rank} of {total} TDs" if rank and total else ""
    else:
        att_lbl, att_val, att_sub = "Days in chamber", "—", ""

    if not vote_df.empty:
        vr = vote_df.iloc[0]
        votes_cast = (
            int(vr.get("yes_count", 0) or 0) + int(vr.get("no_count", 0) or 0) + int(vr.get("abstained_count", 0) or 0)
        )
        cast_val = f"{votes_cast:,}"
        divs = int(vr.get("division_count", 0) or 0)
        cast_sub = f"across {divs:,} divisions" if divs else ""
    else:
        cast_val, cast_sub = "—", ""

    pay_val = f"€{pay_total:,.0f}" if pay_total else "—"
    pay_sub = "TAA · all years on record" if pay_total else ""

    stat_strip(
        [
            (att_val, att_lbl, "var(--text-primary)", att_sub),
            (cast_val, "Votes cast", "var(--signal-good)", cast_sub),
            (pay_val, "Payments received", "var(--text-primary)", pay_sub),
        ]
    )

    # ── Section nav strip (anchor links into the expanders below) ────────────
    nav_links = "".join(
        f'<a class="mo-section-chip" href="#mo-section-{sid}">{_h(label)}</a>' for sid, label, _ in _PROFILE_SECTIONS
    )
    st.html(f'<nav class="mo-section-nav" aria-label="Profile section quick links">{nav_links}</nav>')

    # ── "Open all sections" toggle (journalist mode) ─────────────────────────
    # Flips every mo_open_<sid> key. Streamlit's st.expander reads expanded=
    # on render, so a rerun after toggling propagates the new state. The
    # button label flips so the same control closes them all again.
    all_open = all(st.session_state.get(f"mo_open_{sid}", False) for sid, _, _ in _PROFILE_SECTIONS)
    btn_label = "Close all sections" if all_open else "Open all sections"
    if st.button(btn_label, key="mo_open_all_btn", help="Expand every section at once — useful for journalists"):
        new_state = not all_open
        for sid, _, _ in _PROFILE_SECTIONS:
            st.session_state[f"mo_open_{sid}"] = new_state
        st.rerun()

    # ── 7 dimension expanders ────────────────────────────────────────────────
    # Phase 2 scaffolding: chrome + lazy-load session keys are in place. Each
    # body either calls its existing inline render fn (kept verbatim) or
    # shows an empty_state placeholder pointing to the matching ranking page.
    # Phases 3–8 replace the inline calls with content lifted from the
    # /rankings/* profile branches.
    for sid, label, page_key in _PROFILE_SECTIONS:
        state_key = f"mo_open_{sid}"
        expanded = st.session_state.get(state_key, False)
        with st.expander(label, expanded=expanded):
            # Cross-page deep-link anchor: /member-overview?member=<code>#<sid>
            st.html(f'<div id="mo-section-{sid}" class="mo-section-anchor"></div>')

            if sid == "interests":
                # Phase 3 lift: full body rendered here without the per-page
                # member header (the hero above already shows it). Dáil-only —
                # member-overview never lists Senators.
                render_member_interests(
                    "Dáil",
                    member_name,
                    show_member_header=False,
                    year_pill_key=f"mo_int_year_{join_key}",
                )
            elif sid == "lobbying":
                # Revolving-door callout (member-overview-local — built from
                # v_lobbying_revolving_door_member, which lobbying_2.py does
                # not query directly). Renders above the lifted body so the
                # most politically potent flag is the first thing visible.
                rd_df = _lobbying_rd(conn, join_key)
                if not rd_df.empty:
                    rd_row = rd_df.iloc[0]
                    rc = int(rd_row.get("return_count", 0) or 0)
                    firms = int(rd_row.get("distinct_firms", 0) or 0)
                    pos = str(rd_row.get("former_position", "")).strip()
                    pos_line = f"Former position: <strong>{_h(pos)}</strong>. " if pos else ""
                    st.badge("Revolving door", icon=":material/warning:", color="orange")
                    st.html(
                        f'<div class="lob-revolving-callout">'
                        f'<div class="lob-revolving-heading">Revolving door flag</div>'
                        f'<p style="margin:0;font-size:0.88rem;color:var(--text-secondary);">'
                        f"{pos_line}"
                        f"Appears on <strong>{rc}</strong> lobbying return{'s' if rc != 1 else ''} "
                        f"across <strong>{firms}</strong> distinct firm{'s' if firms != 1 else ''}.</p>"
                        f"</div>"
                    )
                # Phase 4 lift: full lobbying body (metrics + ranked orgs +
                # policy exposure + returns + source links) rendered without
                # the per-page lobbying hero (member-overview hero is shown).
                render_member_lobbying(
                    member_name,
                    show_header=False,
                    year_pill_key=f"mo_lob_year_{join_key}",
                )
            elif sid == "payments":
                # Phase 5 lift: full payments body (year metrics + Altair
                # evolution chart + card-based all-years summary + card-based
                # payment records) without the per-page identity strip,
                # back button, or provenance footer. Two `st.dataframe`
                # views in the stand-alone page are replaced by card lists
                # here per feedback_member_overview_no_dataframes.
                _pay_year_options = _pay_filter_options().get("years", [])
                if _pay_year_options:
                    render_member_payments(
                        member_name,
                        _pay_year_options,
                        _pay_summary(),
                        show_member_header=False,
                        year_pill_key=f"mo_pay_year_{join_key}",
                    )
                else:
                    empty_state(
                        "Payments data unavailable",
                        "v_payments_summary returned no years. Run the payments pipeline.",
                    )
            elif sid == "attendance":
                # Phase 6 lift: year metrics + sitting-calendar Altair strip +
                # card-based year breakdown. No inner `st.expander` (nested
                # expanders fail in Streamlit) and no `st.dataframe` (per
                # feedback_member_overview_no_dataframes — the year breakdown
                # renders as `.att-year-row`s with a CSS-width bar).
                render_member_attendance(
                    member_name,
                    show_member_header=False,
                    year_pill_key=f"mo_att_year_{join_key}",
                    export_key_suffix="_mo",
                )
            elif sid == "votes":
                _section_votes(conn, join_key, date_from, date_to)
                st.html(entity_cta_html(member_votes_url(join_key), "Full voting history →"))
                _section_debates(conn, join_key, member_name)
            elif sid == "legislation":
                _section_legislation(conn, join_key, member_name)
                _section_statutory_instruments(conn, join_key)
            elif sid == "committees":
                _section_committees()


# ── Main entry point ───────────────────────────────────────────────────────────


@page_error_boundary
def member_overview_page() -> None:
    inject_css()
    conn = get_member_overview_conn()

    url_jk = st.query_params.get("member")
    if url_jk:
        st.session_state[_STAGE_KEY] = url_jk

    join_key = st.session_state.get(_STAGE_KEY)

    date_from: str | None = None
    date_to: str | None = None
    with st.sidebar:
        sidebar_page_header("Member<br>Overview", "OIREACHTAS EXPLORER")
        # Date filter only on the profile view — applies to the votes section.
        if join_key:
            date_from, date_to = sidebar_date_range(
                "Vote date range",
                key="mo_vote_date",
                empty_default=True,
            )

    if join_key:
        _render_stage2(conn, join_key, date_from, date_to)
    else:
        _render_browse(conn)
