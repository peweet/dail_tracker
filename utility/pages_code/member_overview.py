"""
Member Overview — single-politician public accountability record.

Stage 1: Browse all TDs (identity columns; mart pending)
Stage 2: Full accountability profile — Attendance, Votes, Payments, Lobbying, Legislation

Entry points: row click, sidebar selectbox, ?member=join_key URL param

TODO_PIPELINE_VIEW_REQUIRED: v_member_overview_browse
  Pending columns: attendance_rate, payment_total_eur, declared_interests_count,
  lobbying_interactions_count, revolving_door_flag, government_status

TODO_PIPELINE_VIEW_REQUIRED: sponsor_join_key on v_legislation_index
TODO_PIPELINE_VIEW_REQUIRED: unique_member_code on v_lobbying_revolving_door
TODO_PIPELINE_VIEW_REQUIRED: per-member lobbying view with unique_member_code filter
"""
from __future__ import annotations

import logging
import re
import unicodedata
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
    member_card_html,
    paginate,
    pagination_controls,
    sidebar_date_range,
    sidebar_page_header,
)
from ui.entity_links import (
    PAGES,
    entity_cta_html,
    member_profile_url,
    member_votes_url,
    source_link_html,
)
from ui.vote_explorer import member_vote_card_html
from data_access.member_overview_data import get_member_overview_conn

_log = logging.getLogger(__name__)
_STAGE_KEY = "mo_join_key"

_POLICY_AREAS: list[tuple[str, str]] = [
    ("Housing",        "housing"),
    ("Health",         "health"),
    ("Education",      "education"),
    ("Defence",        "defence"),
    ("Europe",         "europe"),
    ("Crime",          "crime"),
    ("Environment",    "environment"),
    ("Social Welfare", "social welfare"),
    ("Finance",        "finance"),
    ("Agriculture",    "agriculture"),
    ("Transport",      "transport"),
    ("Immigration",    "immigration"),
]
_AREA_LABELS:      list[str]      = [lbl for lbl, _ in _POLICY_AREAS]
_AREA_LABEL_TO_KW: dict[str, str] = {lbl: kw for lbl, kw in _POLICY_AREAS}


# ── Name normalisation (mirrors normalise_join_key.py pipeline logic) ──────────

def _norm_name(name: str) -> str:
    """Sorted-character key identical to the pipeline's normalise_df_td_name()."""
    name = name.lower()
    name = unicodedata.normalize("NFD", name)
    name = re.sub(r"[̀-ͯ]", "", name)
    name = re.sub(r"[\x27‘’ʼʹ`´＇]", "", name)
    name = re.sub(r"[^a-z\s]", "", name)
    name = re.sub(r"^\s*(dr|prof|rev|fr|sr|mr|mrs|ms|miss|br)\s+", "", name)
    name = re.sub(r"\s+", "", name)
    return "".join(sorted(name))


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
        "SELECT unique_member_code, member_name, party_name, constituency"
        " FROM v_member_registry ORDER BY member_name",
    )


@st.cache_data(ttl=300)
def _join_key_by_name(_conn, name: str) -> str | None:
    df = _q(
        _conn,
        "SELECT unique_member_code FROM v_member_registry"
        " WHERE member_name = ? LIMIT 1",
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
        "SELECT SUM(amount_num) AS total FROM v_payments_member_detail"
        " WHERE unique_member_code = ?",
        [join_key],
    )
    if df.empty or pd.isna(df.iloc[0]["total"]):
        return 0.0
    return float(df.iloc[0]["total"])


@st.cache_data(ttl=300)
def _lobbying_rd(_conn, member_name: str) -> pd.DataFrame:
    # Approximate name match — TODO_PIPELINE_VIEW_REQUIRED: unique_member_code on this view
    return _q(
        _conn,
        "SELECT individual_name, former_position, return_count, distinct_firms"
        " FROM v_lobbying_revolving_door WHERE individual_name = ? LIMIT 5",
        [member_name],
    )


@st.cache_data(ttl=300)
def _legislation(_conn, member_name: str) -> pd.DataFrame:
    # ILIKE on last name — TODO_PIPELINE_VIEW_REQUIRED: sponsor_join_key on v_legislation_index
    last = member_name.strip().split()[-1]
    return _q(
        _conn,
        "SELECT bill_title, bill_status, bill_year, oireachtas_url"
        " FROM v_legislation_index"
        " WHERE LOWER(sponsor) LIKE LOWER(?)"
        " ORDER BY introduced_date DESC NULLS LAST LIMIT 50",
        [f"%{last}%"],
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
        r         = summary.iloc[0]
        yes       = int(r.get("yes_count",      0) or 0)
        no        = int(r.get("no_count",       0) or 0)
        ab        = int(r.get("abstained_count",0) or 0)
        div       = int(r.get("division_count", 0) or 0)
        rate_pct  = float(r.get("yes_rate_pct", 0) or 0)
        cast      = yes + no + ab
        voted_pct = round(100.0 * cast / div, 1) if div else 0.0
        st.html(
            f'<p style="font-family:\'Epilogue\',sans-serif;font-size:0.95rem;'
            f'color:var(--text-secondary);margin:0 0 0.75rem;">'
            f'Voted in <strong>{voted_pct}%</strong> of divisions — '
            f'<strong>{rate_pct}%</strong> Aye&nbsp;·&nbsp;'
            f'<strong>{round(100-rate_pct,1)}%</strong> Níl when cast.</p>'
        )

    # ── Filter row 1: policy area (with "All topics") ────────────────────
    area_options  = ["All topics"] + _AREA_LABELS
    selected_area = st.pills(
        "Policy area",
        options=area_options,
        default="All topics",
        key="mo_vote_area",
        label_visibility="collapsed",
    ) or "All topics"

    # ── Filter row 2: year ──────────────────────────────────────────────
    available_years = _member_vote_years(conn, join_key)
    if available_years:
        year_opts     = ["All years"] + [str(y) for y in available_years]
        selected_year = st.radio(
            "Year",
            options=year_opts,
            index=0,
            horizontal=True,
            key="mo_vote_year",
            label_visibility="collapsed",
        ) or "All years"
    else:
        selected_year = "All years"

    # ── Resolve filters ─────────────────────────────────────────────────
    keyword = (
        None if selected_area == "All topics"
        else _AREA_LABEL_TO_KW.get(selected_area)
    )

    # Year pill takes precedence over the sidebar date range when set.
    eff_from = date_from
    eff_to   = date_to
    if selected_year != "All years":
        eff_from = f"{selected_year}-01-01"
        eff_to   = f"{selected_year}-12-31"

    topic_df = _votes_by_topic(conn, join_key, keyword, eff_from, eff_to)

    if topic_df.empty:
        scope = selected_area if selected_area != "All topics" else "any topic"
        year_note = (
            f" in {selected_year}" if selected_year != "All years"
            else " in this date range" if (eff_from or eff_to) else ""
        )
        empty_state(
            f"No votes on {scope}{year_note}",
            "Try widening the year, picking 'All topics', "
            "or clearing the date filter in the sidebar.",
        )
        return

    total      = len(topic_df)
    PAGE_SIZE  = 10
    # Pager key includes the active filter signature so changing any filter
    # resets to page 1 instead of leaving the user stranded past the new end.
    filter_sig = f"{keyword or 'all'}_{selected_year}_{eff_from or '_'}_{eff_to or '_'}"
    pager_key  = f"mo_vote_topic_{join_key}_{filter_sig}"
    page_idx   = paginate(total, key_prefix=pager_key, page_size=PAGE_SIZE)
    visible    = topic_df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    start = page_idx * PAGE_SIZE + 1
    end   = min((page_idx + 1) * PAGE_SIZE, total)
    scope_label = selected_area if selected_area != "All topics" else "all topics"
    year_label  = selected_year if selected_year != "All years" else "all years"
    st.caption(
        f"Showing {start:,}–{end:,} of {total:,} "
        f"division{'s' if total != 1 else ''} on {scope_label} · {year_label}"
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


def _section_legislation(conn, member_name: str) -> None:
    st.html('<p class="section-heading">Legislation sponsored</p>')
    st.html(
        '<div class="leg-todo-callout">'
        '<span class="leg-todo-label">TODO PIPELINE VIEW REQUIRED</span>'
        ' <code>sponsor_join_key</code> missing from <code>v_legislation_index</code>. '
        'Bills below matched by last name — may include false positives.</div>'
    )

    df = _legislation(conn, member_name)
    if df.empty:
        empty_state(
            "No bills found",
            f"No bills matching '{member_name.split()[-1]}' as sponsor in v_legislation_index.",
        )
        return

    n = len(df)
    st.caption(f"{n} bill{'s' if n != 1 else ''} matched by name")

    for _, row in df.iterrows():
        title  = str(row.get("bill_title",  "—"))
        status = str(row.get("bill_status", "—"))
        year   = str(row.get("bill_year",   "—"))
        url    = str(row.get("oireachtas_url", "") or "")

        sl = status.lower()
        status_css = (
            "leg-status-enacted" if ("enact" in sl or "sign" in sl)
            else "leg-status-lapsed" if sl in ("lapsed", "withdrawn", "defeated")
            else "leg-status-active"
        )
        if url in ("nan", "None"):
            url = ""
        url_html = source_link_html(
            url, "Oireachtas.ie",
            aria_label="Open this bill on oireachtas.ie",
        )
        st.html(
            f'<div class="leg-bill-card" style="margin-bottom:0.3rem;">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(year)}</span>'
            f'<span class="signal {status_css}">{_h(status)}</span>'
            f'</div>'
            f'<div class="leg-bill-card-title">{_h(title)}</div>'
            f'<div style="margin-top:0.2rem;">{url_html}</div>'
            f'</div>'
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


def _section_debates() -> None:
    st.html('<p class="section-heading">Debate contributions</p>')
    st.html(
        '<div class="leg-todo-callout">'
        '<span class="leg-todo-label">TODO PIPELINE VIEW REQUIRED</span>'
        ' <code>v_member_debate_speeches</code> — what a TD said in each debate.'
        ' The existing <code>v_legislation_debates</code> links debates to bills but'
        ' does not record speakers or utterances. Required columns:'
        ' <b>unique_member_code</b>, <b>debate_date</b>, <b>debate_title</b>,'
        ' <b>snippet</b>, <b>oireachtas_url</b>.'
        '</div>'
    )


def _section_committees() -> None:
    st.html('<p class="section-heading">Committees</p>')
    st.html(
        '<div class="leg-todo-callout">'
        '<span class="leg-todo-label">TODO PIPELINE VIEW REQUIRED</span>'
        ' Per-member committee membership is pending the committees-page refactor.'
        ' Required view: <code>v_committee_membership</code> with columns'
        ' <b>unique_member_code</b>, <b>committee_name</b>, <b>role</b> (Chair / Member),'
        ' <b>start_date</b>, <b>end_date</b>.'
        '</div>'
    )


def _section_lobbying(conn, member_name: str) -> None:
    st.html('<p class="section-heading">Lobbying &amp; revolving door</p>')
    rd_df = _lobbying_rd(conn, member_name)

    if not rd_df.empty:
        row      = rd_df.iloc[0]
        rc       = int(row.get("return_count",   0) or 0)
        firms    = int(row.get("distinct_firms", 0) or 0)
        pos      = str(row.get("former_position", "")).strip()
        pos_line = f"Former position: <strong>{_h(pos)}</strong>. " if pos else ""

        st.badge("Revolving door", icon=":material/warning:", color="orange")
        st.html(
            f'<div class="lob-revolving-callout">'
            f'<div class="lob-revolving-heading">Revolving door flag</div>'
            f'<p style="margin:0;font-size:0.88rem;color:var(--text-secondary);">'
            f'{pos_line}'
            f'Appears on <strong>{rc}</strong> lobbying return{"s" if rc != 1 else ""} '
            f'across <strong>{firms}</strong> distinct firm{"s" if firms != 1 else ""}. '
            f'Matched by display name — approximate.</p>'
            f'</div>'
        )
    else:
        st.html(
            '<div class="dt-callout">No revolving door flag found for this member '
            '(name-based lookup — approximate).</div>'
        )

    st.html(
        '<div class="leg-todo-callout" style="margin-top:0.75rem;">'
        '<span class="leg-todo-label">TODO PIPELINE VIEW REQUIRED</span>'
        ' Per-member lobbying contact table requires'
        ' <code>unique_member_code</code> on the lobbying contact view.'
        '</div>'
    )


_OTHER_PILL = "Other / Independent"
_OTHER_MIN  = 3  # parties with fewer TDs are grouped into Other


def _named_parties(df: pd.DataFrame) -> list[str]:
    """Parties with >= _OTHER_MIN members, sorted by size desc then name."""
    if df.empty or "party_name" not in df.columns:
        return []
    counts  = df["party_name"].value_counts()
    parties = df["party_name"].dropna().astype(str).unique().tolist()
    parties = [p for p in parties if p and p.lower() not in ("nan", "")]
    named   = [p for p in parties if int(counts.get(p, 0)) >= _OTHER_MIN]
    return sorted(named, key=lambda p: (-int(counts.get(p, 0)), p))


def _party_pill_options(df: pd.DataFrame) -> list[str]:
    named = _named_parties(df)
    if not named:
        return []
    counts     = df["party_name"].value_counts()
    in_named   = sum(int(counts.get(p, 0)) for p in named)
    has_other  = (len(df) - in_named) > 0
    return named + ([_OTHER_PILL] if has_other else [])


def _render_browse(conn) -> None:
    df = _member_list(conn)

    st.html(
        '<div class="dt-hero">'
        '<p class="dt-kicker">MEMBER OVERVIEW</p>'
        '<h1 style="margin:0.1rem 0 0.25rem;font-size:1.85rem;font-weight:700;'
        'font-family:\'Zilla Slab\',Georgia,serif;">Browse all TDs</h1>'
        '<p class="dt-dek">Pick a TD to open their accountability profile — '
        'attendance, votes by policy area, payments, lobbying, and legislation.</p>'
        '</div>'
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
        filtered  = filtered[
            filtered["party_name"].isna()
            | ~filtered["party_name"].isin(named_set)
        ]
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
    st.html(
        f'<p class="section-heading">{showing:,} TD{"s" if showing != 1 else ""}</p>'
    )

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
        name    = str(row.get("member_name", ""))
        party   = str(row.get("party_name", "") or "")
        constit = str(row.get("constituency", "") or "")
        code    = str(row["unique_member_code"])
        meta    = clean_meta(party, constit)
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
    cards.append('</div>')
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

def _render_stage2(
    conn,
    join_key: str,
    date_from: str | None = None,
    date_to: str | None = None,
) -> None:

    if back_button("← All TDs", key="mo_all", help="Return to the full TD list"):
        st.session_state.pop(_STAGE_KEY, None)
        st.query_params.clear()
        st.rerun()

    identity = _identity(conn, join_key)
    if not identity:
        browse_href = f"/{PAGES['member_overview']}"
        st.html(
            f'<div class="dt-callout">'
            f'<strong>This TD is not in the dataset</strong><br>'
            f'<span style="color:var(--text-meta)">No record matched <code>{_h(join_key)}</code> '
            f'in <code>v_attendance_member_year_summary</code>. The link you followed may be '
            f'out of date, or the pipeline has not yet ingested this member.</span><br>'
            f'<a class="dt-member-link" href="{_h(browse_href)}" target="_self" '
            f'style="margin-top:0.6rem;display:inline-block;">← Browse all TDs</a>'
            f'</div>'
        )
        return

    member_name  = str(identity.get("member_name",  ""))
    party        = str(identity.get("party_name",   ""))
    constituency = str(identity.get("constituency", ""))
    is_minister  = str(identity.get("is_minister", "false")).lower() == "true"
    meta         = clean_meta(party, constituency)

    role_html = (
        '<span style="display:inline-flex;align-items:center;padding:0.15rem 0.55rem;'
        'background:#dbeafe;border:1px solid #93c5fd;color:#1e40af;border-radius:2px;'
        'font-size:0.78rem;font-weight:600;font-family:\'Epilogue\',sans-serif;">Minister</span>'
        if is_minister else
        '<span style="display:inline-flex;align-items:center;padding:0.15rem 0.55rem;'
        'background:#fef3c7;border:1px solid #fcd34d;color:#92400e;border-radius:2px;'
        'font-size:0.78rem;font-weight:600;font-family:\'Epilogue\',sans-serif;">TD</span>'
    )

    rd_df   = _lobbying_rd(conn, member_name)
    rd_html = (
        '<span style="display:inline-flex;align-items:center;gap:0.25rem;'
        'padding:0.15rem 0.55rem;background:#fffbeb;border:1px solid #fcd34d;'
        'color:#92400e;border-radius:2px;font-size:0.78rem;font-weight:600;'
        'font-family:\'Epilogue\',sans-serif;margin-left:0.35rem;">'
        '⚠ Revolving door</span>'
        if not rd_df.empty else ""
    )

    photo_url    = avatar_data_url(member_name)
    photo_credit = avatar_credit_html(member_name)
    if photo_url:
        avatar_block = (
            f'<img class="dt-profile-avatar" src="{_h(photo_url)}" alt="" loading="lazy">'
        )
        caption_block = (
            f'<p class="dt-profile-avatar-credit">{photo_credit}</p>'
            if photo_credit else ""
        )
    else:
        avatar_block = (
            f'<span class="dt-profile-initials" aria-hidden="true">'
            f'{_h(_initials(member_name))}</span>'
        )
        caption_block = '<p class="dt-profile-avatar-empty">No photo available</p>'

    st.html(
        f'<div class="dt-hero">'
        f'  <p class="dt-kicker">TD ACCOUNTABILITY RECORD</p>'
        f'  <div class="dt-profile-header">'
        f'    <div class="dt-profile-avatar-col">{avatar_block}{caption_block}</div>'
        f'    <div class="dt-profile-meta-col">'
        f'      <h1 class="td-name" style="margin:0.15rem 0 0.2rem;">{_h(member_name)}</h1>'
        f'      <p class="td-meta" style="margin:0 0 0.55rem;">{_h(meta)}</p>'
        f'      <div style="display:flex;flex-wrap:wrap;gap:0.3rem;">{role_html}{rd_html}</div>'
        f'    </div>'
        f'  </div>'
        f'</div>'
    )

    # ── Headline stats — single source of truth, no duplication ──────────────
    att_df    = _att_all_years(conn, join_key)
    pay_total = _pay_grand_total(conn, join_key)
    vote_df   = _votes_summary(conn, join_key)

    if not att_df.empty:
        att_yr   = int(att_df.iloc[0]["year"])
        att_days = int(att_df.iloc[0]["attended_count"])
        is_min   = str(att_df.iloc[0].get("is_minister", "false")).lower() == "true"
        att_lbl  = f"Days in chamber ({att_yr})"
        att_val  = str(att_days)
        att_help = "Ministerial duties not captured in plenary records." if is_min else None
    else:
        att_lbl, att_val, att_help = "Days in chamber", "—", None

    if not vote_df.empty:
        vr        = vote_df.iloc[0]
        votes_cast = (
            int(vr.get("yes_count",       0) or 0)
            + int(vr.get("no_count",      0) or 0)
            + int(vr.get("abstained_count",0) or 0)
        )
        cast_val = f"{votes_cast:,}"
    else:
        cast_val = "—"

    pay_val = f"€{pay_total:,.0f}" if pay_total else "—"

    c1, c2, c3 = st.columns(3)
    c1.metric(att_lbl, att_val, help=att_help)
    c2.metric("Votes cast (total)", cast_val)
    c3.metric("Payments received", pay_val)

    st.divider()

    # ── 1. Voting record by issue ─────────────────────────────────────────────
    _section_votes(conn, join_key, date_from, date_to)

    st.html(entity_cta_html(member_votes_url(join_key), "Full voting history →"))

    st.divider()

    # ── 2. Legislation ────────────────────────────────────────────────────────
    _section_legislation(conn, member_name)

    st.divider()

    # ── 3. Debate contributions ───────────────────────────────────────────────
    _section_debates()

    st.divider()

    # ── 4. Committees ─────────────────────────────────────────────────────────
    _section_committees()

    st.divider()

    # ── 5. Lobbying & revolving door ──────────────────────────────────────────
    _section_lobbying(conn, member_name)


# ── Main entry point ───────────────────────────────────────────────────────────

def member_overview_page() -> None:
    inject_css()
    conn = get_member_overview_conn()

    url_jk = st.query_params.get("member")
    if url_jk:
        st.session_state[_STAGE_KEY] = url_jk

    join_key = st.session_state.get(_STAGE_KEY)

    date_from: str | None = None
    date_to:   str | None = None
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
