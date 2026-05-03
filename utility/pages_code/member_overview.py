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
from ui.components import (
    back_button,
    clean_meta,
    empty_state,
    member_card_html,
    pagination_controls,
    sidebar_page_header,
)
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
def _votes_by_topic(_conn, join_key: str, keyword: str) -> pd.DataFrame:
    pattern = f"%{keyword}%"
    return _q(
        _conn,
        "SELECT vote_date, debate_title, vote_type, vote_outcome, oireachtas_url"
        " FROM v_vote_member_detail"
        " WHERE member_id = ? AND LOWER(debate_title) LIKE LOWER(?)"
        " ORDER BY vote_date DESC LIMIT 200",
        [join_key, pattern],
    )


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

def _section_votes(conn, join_key: str) -> None:
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

    selected_area = st.pills(
        "Policy area",
        options=_AREA_LABELS,
        default="Housing",
        key="mo_vote_area",
        label_visibility="collapsed",
    )

    keyword  = _AREA_LABEL_TO_KW.get(selected_area or "Housing", "housing")
    topic_df = _votes_by_topic(conn, join_key, keyword)

    if topic_df.empty:
        empty_state(
            f"No votes on {selected_area}",
            "No divisions with this topic found in the debate title.",
        )
    else:
        st.caption(
            f"{len(topic_df)} division{'s' if len(topic_df) != 1 else ''} on {selected_area}"
        )
        for _, row in topic_df.iterrows():
            date    = str(row.get("vote_date",    ""))[:10]
            title   = str(row.get("debate_title", "—"))
            vtype   = str(row.get("vote_type",    "—"))
            outcome = str(row.get("vote_outcome", "—"))
            url     = str(row.get("oireachtas_url", "") or "")

            vl = vtype.lower()
            if "yes" in vl:
                vote_colour, vote_bg = "#1d4ed8", "#dbeafe"
            elif "no" in vl:
                vote_colour, vote_bg = "#92400e", "#fef3c7"
            else:
                vote_colour, vote_bg = "#6b7280", "#f3f4f6"

            url_html = (
                f'<a href="{_h(url)}" target="_blank" '
                f'style="font-size:0.78rem;color:var(--accent);text-decoration:none;'
                f'margin-left:auto;flex-shrink:0;">Oireachtas →</a>'
                if url and url not in ("nan", "None", "")
                else ""
            )
            st.html(
                f'<div style="padding:0.55rem 0.9rem;margin-bottom:0.25rem;'
                f'border:1px solid var(--border);border-left:3px solid {vote_colour};'
                f'border-radius:8px;background:#ffffff;">'
                f'<div style="display:flex;align-items:center;gap:0.55rem;margin-bottom:0.2rem;">'
                f'<span style="font-size:0.73rem;color:var(--text-meta);">{_h(date)}</span>'
                f'<span style="padding:0.1rem 0.5rem;background:{vote_bg};color:{vote_colour};'
                f'border-radius:4px;font-size:0.73rem;font-weight:700;">{_h(vtype)}</span>'
                f'<span style="font-size:0.73rem;color:var(--text-meta);">{_h(outcome)}</span>'
                f'{url_html}'
                f'</div>'
                f'<div style="font-family:\'Zilla Slab\',Georgia,serif;font-size:0.94rem;'
                f'font-weight:600;color:var(--text-primary);line-height:1.35;">'
                f'{_h(title)}</div>'
                f'</div>'
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
        url_html = (
            f'<a class="leg-bill-oireachtas-link" href="{_h(url)}" target="_blank">'
            f'Oireachtas.ie →</a>'
            if url and url not in ("nan", "None", "")
            else ""
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

    search = st.text_input(
        "Search TDs",
        placeholder="Search by name, party or constituency…",
        key="mo_browse_search",
        label_visibility="collapsed",
    )

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

    total   = len(df)
    showing = len(filtered)
    st.caption(f"{showing} of {total} TDs")

    if filtered.empty:
        empty_state(
            "No TDs match your filters",
            "Try clearing the search box or choosing a different party.",
        )
        return

    page_size, page_idx = pagination_controls(
        total=showing,
        key_prefix="mo_browse",
        page_sizes=(25, 50, 100),
        default_page_size=25,
        show_caption=False,
    )
    visible = filtered.iloc[page_idx * page_size : (page_idx + 1) * page_size]

    cards = ['<div class="mo-grid">']
    for _, row in visible.iterrows():
        name    = str(row.get("member_name", ""))
        party   = str(row.get("party_name", "") or "")
        constit = str(row.get("constituency", "") or "")
        code    = str(row["unique_member_code"])
        meta    = clean_meta(party, constit)
        cards.append(
            f'<a class="mo-grid-link" href="?member={_h(code)}" target="_self" '
            f'aria-label="View {_h(name)}">'
            f'{member_card_html(name=name, meta=meta)}'
            f'<span class="mo-grid-arrow" aria-hidden="true">→</span>'
            f'</a>'
        )
    cards.append('</div>')
    st.html("\n".join(cards))


# ── Profile ─────────────────────────────────────────────────────────────────────

def _render_stage2(conn, join_key: str) -> None:

    if back_button("← All TDs", key="mo_all", help="Return to the full TD list"):
        st.session_state.pop(_STAGE_KEY, None)
        st.query_params.clear()
        st.rerun()

    identity = _identity(conn, join_key)
    if not identity:
        st.error("Member not found — join_key has no match in v_attendance_member_year_summary.")
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

    st.html(
        f'<div class="dt-hero">'
        f'<p class="dt-kicker">TD ACCOUNTABILITY RECORD</p>'
        f'<h1 class="td-name" style="margin:0.15rem 0 0.2rem;">{_h(member_name)}</h1>'
        f'<p class="td-meta" style="margin:0 0 0.55rem;">{_h(meta)}</p>'
        f'<div style="display:flex;flex-wrap:wrap;gap:0.3rem;">{role_html}{rd_html}</div>'
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
    _section_votes(conn, join_key)

    st.html(
        f'<a href="/Votes?member={_h(join_key)}" target="_self"'
        f' style="display:inline-flex;align-items:center;gap:0.4rem;margin-top:0.5rem;'
        f'padding:0.5rem 1.1rem;background:var(--text-primary);color:#ffffff;'
        f'border-radius:2px;text-decoration:none;font-weight:700;'
        f'font-family:\'Epilogue\',sans-serif;font-size:0.82rem;letter-spacing:0.04em;'
        f'text-transform:uppercase;">'
        f'Full voting history →</a>'
    )

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

    with st.sidebar:
        sidebar_page_header("Member<br>Overview", "OIREACHTAS EXPLORER")

        df_all       = _member_list(conn)
        member_names = df_all["member_name"].tolist() if not df_all.empty else []

        st.html('<p class="sidebar-label">Find a member</p>')
        search = st.text_input(
            "Search", placeholder="Type a name…",
            key="mo_sidebar_search", label_visibility="collapsed",
        )
        sq             = search.strip().lower()
        filtered_names = [n for n in member_names if sq in n.lower()] if sq else member_names
        chosen         = st.selectbox(
            "Select member", ["— select —"] + filtered_names,
            key="mo_sidebar_select", label_visibility="collapsed",
        )
        if chosen and chosen != "— select —":
            jk = _join_key_by_name(conn, chosen)
            if jk and jk != join_key:
                st.session_state[_STAGE_KEY] = jk
                st.query_params["member"]    = jk
                st.rerun()

    if join_key:
        _render_stage2(conn, join_key)
    else:
        _render_browse(conn)
