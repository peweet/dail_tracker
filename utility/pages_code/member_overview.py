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

import datetime
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
    evidence_heading,
    subsection_heading,
    find_a_td_filter,
    field_label,
    glossary_strip,
    hide_sidebar,
    member_card_html,
    page_error_boundary,
    paginate,
    pagination_controls,
    party_colour,
    stat_strip,
)
from ui.entity_links import (
    PAGES,
    api_json_link,
    member_profile_url,
    oireachtas_profile_url,
    si_detail_url,
    social_icon_chip_html,
    source_link_html,
)
from ui.export_controls import export_button
from ui.vote_explorer import render_member_votes
from data_access.member_overview_data import get_member_overview_conn
from dail_tracker_core.queries import member_overview as moq
from ui.attendance_panel import render_member_attendance
from data_access.committees_data import fetch_committee_assignments, fetch_office_holders
from pages_code.committees import render_member_committees
from ui.interests_panel import render_member_interests
from pages_code.lobbying_3 import render_member_lobbying
from ui.payments_panel import render_member_payments
from data_access.payments_data import fetch_filter_options as _pay_filter_options
from data_access.payments_data import fetch_payments_summary as _pay_summary

_STAGE_KEY = "mo_join_key"

# ── Profile section IA (Phase 2 chrome) ────────────────────────────────────────
# Section order is "most politically potent first" per project_design_principles.
# (id, section label, ranking-page key in entity_links.PAGES). The id is the
# URL-fragment anchor (`/member-overview?member=<code>#<id>`).
_PROFILE_SECTIONS: list[tuple[str, str, str]] = [
    ("interests", "Interests", "interests"),
    ("lobbying", "Lobbying", "lobbying"),
    ("payments", "Payments", "payments"),
    ("attendance", "Attendance", "attendance"),
    ("votes", "Votes", "votes"),
    ("debates", "Debates", "votes"),  # promoted out of Votes 2026-05-31 — was buried
    ("questions", "Questions", "votes"),  # 2026-05-27: see _section_questions
    ("legislation", "Legislation", "legislation"),
    ("committees", "Committees", "committees"),
]


# ── Data retrieval ─────────────────────────────────────────────────────────────


# Retrieval SQL now lives in dail_tracker_core.queries.member_overview (imported
# as `moq`). These wrappers keep the SAME names/signatures the renderers call, so
# only the bodies changed: each delegates to a core QueryResult and applies the
# small dict/list/scalar/fallback *shaping* the UI expects. @st.cache_data is kept
# here (the cache layer is a Streamlit concern); core stays cache-free + UI-free.


@st.cache_data(ttl=300)
def _member_list(_conn) -> pd.DataFrame:
    return moq.member_list(_conn).data


@st.cache_data(ttl=300)
def _join_key_by_name(_conn, name: str, house: str | None = None) -> str | None:
    df = moq.join_key_by_name(_conn, name, house).data
    return str(df.iloc[0]["unique_member_code"]) if not df.empty else None


@st.cache_data(ttl=300)
def _member_house(_conn, join_key: str) -> str:
    """House ('Dáil'/'Seanad') for a member code. Defaults to 'Dáil'. The one
    cross-house code collision (Seán Kyne) resolves to his current house via
    the Seanad-last ordering of the registry; acceptable for a single edge case.
    """
    df = moq.member_house(_conn, join_key).data
    return str(df.iloc[0]["house"]) if not df.empty else "Dáil"


@st.cache_data(ttl=300)
def _identity(_conn, join_key: str) -> dict:
    # Attendance first — has year; fall back to canonical registry if no record.
    df = moq.identity_attendance(_conn, join_key).data
    if not df.empty:
        return df.iloc[0].to_dict()
    df = moq.identity_registry(_conn, join_key).data
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _att_all_years(_conn, join_key: str) -> pd.DataFrame:
    return moq.att_all_years(_conn, join_key).data


@st.cache_data(ttl=300)
def _att_rank_for_year(_conn, join_key: str, year: int, house: str = "Dáil") -> tuple[int | None, int | None]:
    """Member's attendance rank for a given year and the total ranked field size.
    Returns (rank_high, total). Both None on miss. Rank + total are scoped to the
    member's house (TDs ranked among TDs only)."""
    df = moq.att_rank(_conn, join_key, year).data
    if df.empty:
        return None, None
    total_df = moq.att_rank_total(_conn, year, house).data
    rank = int(df.iloc[0]["rank_high"]) if pd.notna(df.iloc[0]["rank_high"]) else None
    total = int(total_df.iloc[0]["n"]) if not total_df.empty else None
    return rank, total


@st.cache_data(ttl=300)
def _external_links(_conn, join_key: str) -> dict:
    """Wikidata-sourced socials + Wikipedia URL for the hero chips row. Empty
    dict when the view is missing or the member has no entry (both normal — the
    UI just renders fewer chips). Nulls dropped so the hero only iterates over
    populated platforms."""
    df = moq.external_links(_conn, join_key).data
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    return {k: v for k, v in row.items() if isinstance(v, str) and v.strip()}


@st.cache_data(ttl=300)
def _votes_summary(_conn, join_key: str) -> pd.DataFrame:
    return moq.votes_summary(_conn, join_key).data


@st.cache_data(ttl=300)
def _pay_overview(_conn, join_key: str) -> pd.DataFrame:
    return moq.pay_overview(_conn, join_key).data


@st.cache_data(ttl=300)
def _pay_grand_total(_conn, join_key: str) -> float:
    # SUM permitted as presentation-layer scalar — contract §headline_metrics_row note.
    # .df() yields NaN for a NULL SUM, so guard isna before float().
    df = moq.pay_grand_total(_conn, join_key).data
    if df.empty or pd.isna(df.iloc[0]["total"]):
        return 0.0
    return float(df.iloc[0]["total"])


@st.cache_data(ttl=300)
def _lobbying_rd(_conn, join_key: str) -> pd.DataFrame:
    return moq.lobbying_rd(_conn, join_key).data


@st.cache_data(ttl=300)
def _legislation(_conn, join_key: str) -> pd.DataFrame:
    return moq.legislation(_conn, join_key).data


@st.cache_data(ttl=300)
def _si_signed(_conn, join_key: str) -> pd.DataFrame:
    """SIs the member signed as a departmental minister (si_minister_member_code)."""
    return moq.si_signed(_conn, join_key).data


@st.cache_data(ttl=300)
def _ministerial_roles(_conn, join_key: str) -> pd.DataFrame:
    """Ministerial posts this member has held (Wikidata tenure spine; 2011→present).
    Wider history than _si_signed. Empty for members who never held office."""
    return moq.ministerial_roles(_conn, join_key).data


# Electoral Commission review deep link — surfaces the source report for citizen
# verification. This is the only external URL specific to constituency
# demographics; the rest of the provenance is captured inline in the SQL view header.
_EC_REVIEW_URL = "https://www.electoralcommission.ie/publications/constituency-review-reports/"


@st.cache_data(ttl=300)
def _constituency_context(_conn, constituency: str) -> dict:
    """v_member_constituency_demographics row for ``constituency``, or {} when the
    name has no row. The empty-dict branch is a defensive fallback (the Electoral
    Commission 2023-boundary source matches 43/43 current constituencies)."""
    if not constituency:
        return {}
    df = moq.constituency_context(_conn, constituency).data
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


# ── Questions section data helpers ─────────────────────────────────────────────
# Added 2026-05-27. All three views read silver/questions.parquet (264k rows
# post the May 2026 pagination-cap fix; see [[project-questions-cap-fix-2026-05-27]]).


@st.cache_data(ttl=300)
def _q_profile(_conn, join_key: str) -> dict:
    df = moq.question_profile(_conn, join_key).data
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _q_focus_shift(_conn, join_key: str) -> dict:
    df = moq.question_focus_shift(_conn, join_key).data
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _q_years(_conn, join_key: str) -> list[int]:
    df = moq.question_years(_conn, join_key).data
    return [int(y) for y in df["question_year"].dropna().tolist()] if not df.empty else []


@st.cache_data(ttl=300)
def _q_ministries(_conn, join_key: str) -> list[str]:
    """Per-TD distinct ministries ordered by COUNT desc (rollup is in the view)."""
    df = moq.question_ministries(_conn, join_key).data
    return df["ministry"].astype(str).tolist() if not df.empty else []


@st.cache_data(ttl=300)
def _q_top_topics(_conn, join_key: str) -> pd.DataFrame:
    """Top-3 topics for a TD. Rollup lives in v_member_question_top_topics."""
    return moq.question_top_topics(_conn, join_key).data


@st.cache_data(ttl=300)
def _q_feed(
    _conn,
    join_key: str,
    year: int | None = None,
    qtype: str | None = None,
    ministry: str | None = None,
    topic: str | None = None,
    search_text: str | None = None,
) -> pd.DataFrame:
    """Question feed query (filters AND together; free-text ILIKE %wrap; LIMIT
    10000, page paginates client-side)."""
    return moq.question_feed(_conn, join_key, year, qtype, ministry, topic, search_text).data


@st.cache_data(ttl=300)
def _debate_years(_conn, join_key: str) -> list[int]:
    df = moq.debate_years(_conn, join_key).data
    if df.empty or "debate_year" not in df.columns:
        return []
    return [int(y) for y in df["debate_year"].dropna().tolist()]


@st.cache_data(ttl=300)
def _debate_topics(_conn, join_key: str, year: int | None = None) -> list[str]:
    df = moq.debate_topics(_conn, join_key, year).data
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
    """Debate sections a TD raised a question in (retrieval-only filter)."""
    return moq.debate_sections(_conn, join_key, year, topic).data


# ── Speeches (floor contributions) section data helpers ────────────────────────


@st.cache_data(ttl=300)
def _speech_summary(_conn, join_key: str) -> dict:
    df = moq.speech_summary(_conn, join_key).data
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _speech_years(_conn, join_key: str) -> list[int]:
    df = moq.speech_years(_conn, join_key).data
    if df.empty or "year" not in df.columns:
        return []
    return [int(y) for y in df["year"].dropna().tolist()]


@st.cache_data(ttl=300)
def _speech_business(_conn, join_key: str) -> list[str]:
    df = moq.speech_business(_conn, join_key).data
    if df.empty or "business" not in df.columns:
        return []
    return [str(b) for b in df["business"].dropna().tolist()]


@st.cache_data(ttl=300)
def _member_speeches(
    _conn,
    join_key: str,
    year: int | None = None,
    contribution_type: str | None = None,
    business: str | None = None,
    irish_only: bool = False,
    search: str | None = None,
) -> pd.DataFrame:
    """Paginated floor-contribution feed (retrieval-only filters)."""
    return moq.member_speeches(_conn, join_key, year, contribution_type, business, irish_only, search).data


# ── Profile section renderers ──────────────────────────────────────────────────


def _section_legislation(conn, join_key: str, member_name: str) -> None:
    subsection_heading("Legislation sponsored")

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
            f'<div class="leg-bill-card mo-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(year)}</span>'
            f'<span class="signal {status_css}">{_h(status)}</span>'
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(title)}</div>'
            f'<div class="mo-bill-card-link-row">{url_html}</div>'
            f"</div>"
        )

    export_button(
        df,
        label="Export legislation (CSV)",
        filename=f"legislation_{member_name.replace(' ', '_')}.csv",
        key="mo_leg_export",
    )


def _fmt_tenure_days(days) -> str:
    """Humanise a tenure length in days → '2 yrs 10 mths'. Presentation only."""
    if days is None or pd.isna(days):
        return ""
    days = int(days)
    yrs, rem = divmod(days, 365)
    mths = rem // 30
    parts: list[str] = []
    if yrs:
        parts.append(f"{yrs} yr{'s' if yrs != 1 else ''}")
    if mths:
        parts.append(f"{mths} mth{'s' if mths != 1 else ''}")
    return " ".join(parts) or "< 1 mth"


def _section_ministerial_roles(conn, join_key: str) -> None:
    """Ministerial posts the member has held (Wikidata-sourced tenure spine).
    Conditional: rendered only when the member held office, so non-ministers see
    nothing. Wider than the SIs-signed section below — it covers earlier
    governments back to 2011, not just the current one."""
    df = _ministerial_roles(conn, join_key)
    if df.empty:
        return

    st.divider()
    subsection_heading("Ministerial roles")

    n = len(df)
    current_n = int(df["is_current"].fillna(False).astype(bool).sum())
    current_str = f", {current_n} held now" if current_n else ""
    st.caption(
        f"{n} ministerial post{'s' if n != 1 else ''} held{current_str} — "
        "departmental office history sourced from Wikidata. Dates are the "
        "appointment and departure recorded for each post."
    )

    for _, row in df.iterrows():
        start = row.get("start_date")
        end = row.get("end_date")
        is_current = bool(row.get("is_current"))
        start_txt = start.strftime("%b %Y") if pd.notna(start) else "—"
        if is_current or pd.isna(end):
            date_range = f"since {start_txt}"
            pill = '<span class="signal leg-status-active">Current</span>'
        else:
            date_range = f"{start_txt} – {end.strftime('%b %Y')}"
            pill = ""
        duration = _fmt_tenure_days(row.get("tenure_days"))
        dur_html = f"In post {_h(duration)}" if duration else ""
        st.html(
            f'<div class="leg-bill-card mo-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(date_range)}</span>'
            f"{pill}"
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(str(row.get("department_label", "—")))}</div>'
            f'<div class="mo-bill-card-link-row">{dur_html}</div>'
            f"</div>"
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
    subsection_heading("Statutory Instruments signed")

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
        si_id = str(row.get("si_id", "") or "")
        # Round-3 audit P3-3: was inline-style amber hex; now uses the
        # tokenised .signal-eu class so the EU palette lives in one place.
        eu_badge = '<span class="signal-eu">EU</span>' if bool(row.get("si_is_eu")) else ""
        eisb_html = (
            source_link_html(
                url,
                "irishstatutebook.ie",
                aria_label="Open this SI on irishstatutebook.ie",
            )
            if url.startswith("http")
            else ""
        )
        # Cross-page jump into the SI detail panel — adds the SI page's
        # taxonomy, parent legislation, and EU-relationship context that
        # don't fit in this sub-section card.
        si_page_html = (
            f'<a class="dt-source-link" href="{_h(si_detail_url(si_id))}" '
            f'target="_self" aria-label="Open SI {_h(si_id)} on /rankings-statutory-instruments">'
            f"Full SI detail</a>"
            if si_id
            else ""
        )
        links_html = " &nbsp;·&nbsp; ".join(p for p in (eisb_html, si_page_html) if p)
        st.html(
            f'<div class="leg-bill-card mo-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">SI {_h(si_id or "—")}</span>'
            f'<span class="signal leg-status-active">{op}</span>'
            f"{eu_badge}"
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(str(row.get("si_title", "—")))}</div>'
            f'<div class="mo-bill-card-link-row">{links_html}</div>'
            f"</div>"
        )

    export_button(
        df,
        label="Export statutory instruments (CSV)",
        filename=f"si_signed_{join_key}.csv",
        key="mo_si_export",
    )


def _section_questions(conn, join_key: str, member_name: str) -> None:
    """Parliamentary questions section. Three bands:
      1. Header strip with concentration % + total + top topics + shift subtitle.
      2. Filter bar: year pills, type segmented control, ministry selectbox.
      3. Paginated feed of question cards (date desc).
    Built on the post-cap-fix full history (264k rows, 2020-present).
    """
    profile = _q_profile(conn, join_key)
    total_qs = int(profile.get("total_qs", 0) or 0)

    if total_qs == 0:
        empty_state(
            "No parliamentary questions on file",
            f"{member_name} does not appear in the questions register (2020 onwards).",
        )
        return

    # ── Build header strip ───────────────────────────────────────────────────
    # Three columns: concentration / total / top topics, plus an optional
    # inline shift subtitle spanning the full width below.
    top_min = str(profile.get("top_ministry") or "").strip()
    top_count = int(profile.get("top_count", 0) or 0)
    top_pct = profile.get("top_pct")
    distinct_min = int(profile.get("distinct_ministries", 0) or 0)

    # Concentration cell. Suppress the percentage when total < 100 (the
    # ratio is unstable below that and would mislead).
    if total_qs >= 100 and top_pct is not None and not pd.isna(top_pct):
        conc_html = (
            f'<div class="q-strip-cell-label">Most-questioned ministry</div>'
            f'<div class="q-conc-pct">{float(top_pct):.1f}%</div>'
            f'<div class="q-conc-ministry">{_h(top_min)}</div>'
            f'<div class="q-conc-detail">{top_count:,} of {total_qs:,} questions</div>'
        )
    elif distinct_min >= 15:
        conc_html = (
            '<div class="q-strip-cell-label">Pattern</div>'
            f'<div class="q-conc-sparse">Questions across {distinct_min} ministries</div>'
            '<div class="q-conc-detail">Constituency generalist</div>'
        )
    else:
        conc_html = (
            '<div class="q-strip-cell-label">Recently elected</div>'
            f'<div class="q-conc-sparse">{total_qs} questions on record</div>'
        )

    # Middle panel: distinct ministries with cabinet-denominator sub-line.
    # Replaces the redundant "on file / total_qs" panel from v1 — total was
    # already in the concentration sub-line. Distinct ministries is the
    # genuine second-axis signal (specialist vs generalist).
    if distinct_min > 0:
        total_html = (
            '<div class="q-strip-cell-label">Ministries engaged</div>'
            f'<div class="q-total-num">{distinct_min}</div>'
            '<div class="q-total-sub">Out of around 26 in cabinet</div>'
        )
    else:
        total_html = (
            '<div class="q-strip-cell-label">Activity</div>'
            f'<div class="q-total-num">{total_qs:,}</div>'
            '<div class="q-total-sub">Questions, 2020 to present</div>'
        )

    # Top topics: small clickable chips that apply a topic filter to the feed.
    # Click handler is via st.query_params (?mo_q_topic=...) read at the top of
    # this section. Matches the feedback_css_card_pattern URL handler pattern.
    # Each chip has a trailing ▾ glyph + aria-label so the click-to-filter
    # affordance is recognisable. Cell label says "click to filter".
    topics_df = _q_top_topics(conn, join_key)
    if topics_df.empty:
        topics_inner = (
            '<div class="q-strip-cell-label">Top topics</div>'
            '<div class="q-conc-detail">No topic taxonomy on file.</div>'
        )
    else:
        chip_html_parts = []
        for _, row in topics_df.iterrows():
            t = str(row["topic"])
            n = int(row["n"])
            chip_html_parts.append(
                f'<a class="q-topic-chip" href="?member={_h(join_key)}&mo_q_topic={_h(t)}" '
                f'target="_self" aria-label="Filter feed to questions on {_h(t)} ({n} questions)">'
                f'{_h(t)}<span class="q-topic-chip-count">{n}</span>'
                '<span class="q-topic-chip-action" aria-hidden="true">▾</span>'
                "</a>"
            )
        topics_inner = (
            '<div class="q-strip-cell-label">Top topics <span class="q-strip-cell-hint">— click to filter</span></div>'
            '<div class="q-topic-list">' + "".join(chip_html_parts) + "</div>"
        )

    # Focus shift subtitle (only when present).
    shift = _q_focus_shift(conn, join_key)
    shift_html = ""
    if shift:
        shift_html = (
            '<div class="q-shift-subtitle">'
            f"Most-questioned ministry shifted from "
            f"<strong>{_h(str(shift['past_top']))}</strong> "
            f"({int(shift['past_year_min'])}–{int(shift['past_year_max'])}, "
            f"{int(shift['past_n'])} questions) to "
            f"<strong>{_h(str(shift['recent_top']))}</strong> "
            f"({int(shift['recent_year_min'])}–{int(shift['recent_year_max'])}, "
            f"{int(shift['recent_n'])} questions)."
            "</div>"
        )

    st.html(
        '<div class="q-header-strip">'
        f"<div>{conc_html}</div>"
        f"<div>{total_html}</div>"
        f"<div>{topics_inner}</div>"
        f"{shift_html}"
        "</div>"
    )

    # ── Filter bar ───────────────────────────────────────────────────────────
    # Topic comes from the chip URL handler; year, type, ministry from
    # controls; free-text search from a text input above the row.
    topic_filter = st.query_params.get("mo_q_topic")
    if topic_filter:
        # Render an active-filter chip (× removes the filter via URL).
        clear_href = f"?member={_h(join_key)}"
        st.html(
            '<div class="q-active-filter-bar">'
            '<span class="q-active-filter-label">Topic filter:</span>'
            f'<a class="q-active-chip" href="{_h(clear_href)}" target="_self" '
            f'aria-label="Clear topic filter {_h(topic_filter)}">'
            f"{_h(topic_filter)} "
            '<span class="q-active-chip-x" aria-hidden="true">×</span>'
            "</a></div>"
        )

    # Free-text search of question_text. Empty input matches everything.
    search_text = st.text_input(
        "Search question text",
        key=f"mo_q_search_{join_key}",
        placeholder="Search question text (e.g. 'cardiac services', 'endometriosis')",
        label_visibility="collapsed",
    )

    years = _q_years(conn, join_key)
    ministries = _q_ministries(conn, join_key)

    # Year pills
    year_opts = ["All years"] + [str(y) for y in years]
    selected_year_str = (
        st.pills(
            "Question year",
            options=year_opts,
            default="All years",
            key=f"mo_q_year_{join_key}",
            label_visibility="collapsed",
        )
        or "All years"
    )
    year_val: int | None = None if selected_year_str == "All years" else int(selected_year_str)

    # Type segmented control + ministry selectbox side by side
    c1, c2 = st.columns([1, 2])
    with c1:
        selected_type = st.segmented_control(
            "Question type",
            options=["All", "Written", "Oral"],
            default="All",
            key=f"mo_q_type_{join_key}",
            label_visibility="collapsed",
        )
    with c2:
        selected_ministry = st.selectbox(
            "Ministry",
            options=["All ministries"] + ministries,
            index=0,
            key=f"mo_q_min_{join_key}",
            label_visibility="collapsed",
        )

    qtype_val = None if not selected_type or selected_type == "All" else selected_type.lower()
    ministry_val = None if not selected_ministry or selected_ministry == "All ministries" else selected_ministry
    search_val = (search_text or "").strip() or None

    # ── Feed ─────────────────────────────────────────────────────────────────
    df = _q_feed(conn, join_key, year_val, qtype_val, ministry_val, topic_filter, search_val)
    if df.empty:
        empty_state(
            "No questions match these filters",
            "Try clearing the search box, the ministry, the year pill, or the topic filter.",
        )
        return

    total = len(df)
    PAGE_SIZE = 10
    filter_sig = (
        f"{year_val or 'all'}_{qtype_val or 'all'}_{ministry_val or 'all'}"
        f"_{topic_filter or 'all'}_{hash(search_val) if search_val else 'all'}"
    )
    pager_key = f"mo_q_{join_key}_{filter_sig}"
    page_idx = paginate(total, key_prefix=pager_key, page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    start = page_idx * PAGE_SIZE + 1
    end = min((page_idx + 1) * PAGE_SIZE, total)
    st.caption(f"Showing {start:,}–{end:,} of {total:,} question{'s' if total != 1 else ''}")

    # Render each card. The body uses <details> for "Read full text" expand
    # so toggling stays client-side (no Streamlit rerun per card).
    TRUNC = 280
    for _, row in visible.iterrows():
        raw_date = row.get("question_date")
        try:
            date_disp = pd.to_datetime(raw_date).strftime("%d %b %Y")
        except Exception:
            date_disp = str(raw_date or "")
        qtype = str(row.get("question_type", "") or "").lower()
        ministry = str(row.get("ministry", "") or "").strip()
        topic = str(row.get("topic", "") or "").strip()
        text = str(row.get("question_text", "") or "").strip()
        ref = str(row.get("question_ref", "") or "").strip()
        url = str(row.get("oireachtas_url", "") or "").strip()

        type_cls = "q-card-type-oral" if qtype == "oral" else "q-card-type-written"
        type_label = "Oral" if qtype == "oral" else "Written"

        # Build the head row as a series of flex children so the .q-card-head
        # flex gap rule actually spaces them. (Nesting separators inside the
        # kicker span squashes them visually.)
        # Ministry kicker is dropped when topic starts with the ministry word
        # (Oireachtas taxonomy regularly does this — "Health" + "Health
        # Services Waiting Lists" reads as "Health Health Services" otherwise).
        head_parts = [f'<span class="q-card-date">{_h(date_disp)}</span>']
        topic_dupes_ministry = bool(ministry and topic and topic.lower().startswith(ministry.lower()))
        if ministry and not topic_dupes_ministry:
            head_parts.append('<span class="q-card-sep">·</span>')
            head_parts.append(f'<span class="q-card-kicker">{_h(ministry)}</span>')
        if topic:
            head_parts.append('<span class="q-card-sep">·</span>')
            head_parts.append(f'<span class="q-card-kicker">{_h(topic)}</span>')
        head_parts.append(f'<span class="q-card-type {type_cls}">{type_label}</span>')

        # Body: truncate beyond TRUNC chars with <details> expand.
        if len(text) > TRUNC:
            short = text[:TRUNC].rstrip()
            body_html = (
                "<details>"
                f'<summary><span class="q-card-truncated">{_h(short)}…</span></summary>'
                f'<div class="q-card-fulltext">{_h(text)}</div>'
                "</details>"
            )
        else:
            body_html = _h(text)

        link_html = ""
        if url.startswith("http"):
            link_html = source_link_html(
                url,
                "Open on Oireachtas.ie",
                aria_label="Open this question on oireachtas.ie",
            )
        ref_html = f'<span class="q-card-ref">[{_h(ref)}]</span>' if ref else ""

        st.html(
            '<div class="q-card">'
            '<div class="q-card-head">' + "".join(head_parts) + "</div>"
            f'<div class="q-card-body">{body_html}</div>'
            '<div class="q-card-foot">'
            f"{link_html}"
            f"{ref_html}"
            "</div>"
            "</div>"
        )

    pagination_controls(
        total=total,
        key_prefix=pager_key,
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="questions",
        show_caption=False,
    )

    # Export
    export_button(
        df,
        label="Export filtered questions (CSV)",
        filename=f"questions_{member_name.replace(' ', '_')}.csv",
        key=f"mo_q_export_{join_key}",
    )

    st.caption("Source: oireachtas.ie/en/debates/questions/ · 2020 to present · complete history per TD.")


_SPEECH_EXCERPT_CHARS = 360


def _render_speech_card(row) -> None:
    """One floor-contribution 'transcript' card: date + badges, topic, spoken
    excerpt, word count + source. Full text follows in an expander when clamped."""
    date_raw = str(row.get("speech_date", "") or "")
    try:
        date_disp = pd.to_datetime(date_raw).strftime("%d %b %Y")
    except Exception:
        date_disp = date_raw
    chamber = str(row.get("house", "") or "").strip() or "—"
    business = str(row.get("business", "") or "").strip()
    topic = str(row.get("section_heading", "") or "").strip()
    ctype = str(row.get("contribution_type", "") or "")
    words = int(row.get("word_count", 0) or 0)
    text = str(row.get("speech_text", "") or "").strip()
    url = str(row.get("debate_url", "") or "")
    if url in ("nan", "None"):
        url = ""

    title = topic or business or "—"
    crumb = business if business and business != topic else ""

    badges = f'<span class="signal leg-status-active">{_h(chamber)}</span>'
    if bool(row.get("is_irish")):
        badges += '<span class="signal signal-gaeilge">As Gaeilge</span>'
    if ctype == "question":
        badges += '<span class="signal signal-neutral">Oral question</span>'

    clamped = len(text) > _SPEECH_EXCERPT_CHARS
    excerpt = (text[:_SPEECH_EXCERPT_CHARS].rsplit(" ", 1)[0] + "…") if clamped else text

    url_html = source_link_html(url, "Oireachtas.ie", aria_label="Open this debate on oireachtas.ie") if url else ""
    crumb_html = f'<div class="mo-speech-crumb">{_h(crumb)}</div>' if crumb else ""
    meta_tail = ("&nbsp;·&nbsp;" + url_html) if url_html else ""

    # Full text expands inline via <details> (same client-side pattern as the
    # Questions cards) — the old full-width st.expander below each 600px card
    # read as a separate, broken element.
    if clamped:
        excerpt_html = (
            "<details>"
            f'<summary><span class="mo-speech-excerpt mo-speech-truncated">{_h(excerpt)}</span> '
            '<span class="mo-speech-read-more">Read full contribution</span></summary>'
            f'<div class="mo-speech-excerpt">{_h(text)}</div>'
            "</details>"
        )
    else:
        excerpt_html = f'<div class="mo-speech-excerpt">{_h(text)}</div>'

    st.html(
        f'<div class="leg-bill-card mo-bill-card mo-speech-card">'
        f'<div class="leg-bill-card-header">'
        f'<span class="leg-bill-card-date">{_h(date_disp)}</span>'
        f'<span class="mo-speech-badges">{badges}</span>'
        f"</div>"
        f"{crumb_html}"
        f'<div class="leg-bill-card-title">{_h(title)}</div>'
        f"{excerpt_html}"
        f'<div class="mo-debate-card-meta">{words:,} word{"s" if words != 1 else ""}{meta_tail}</div>'
        f"</div>"
    )


def _section_debates(conn, join_key: str, member_name: str) -> None:
    """Floor contributions (speeches + oral questions) from the AKN debate
    transcript — the member's actual spoken words, with an As-Gaeilge flag and
    full-text search. Replaces the former question-derived debate-section proxy.
    """
    subsection_heading("Debates")

    summary = _speech_summary(conn, join_key)
    total = int(summary.get("total_contributions", 0) or 0)
    if total == 0:
        empty_state(
            "No floor contributions on record",
            f"{member_name} has no speeches or oral questions in the available debate transcript record.",
        )
        return

    house = str(summary.get("house") or "Dáil")
    role = "Senator" if house == "Seanad" else "TD"
    words = int(summary.get("total_words", 0) or 0)
    irish = int(summary.get("irish_count", 0) or 0)
    distinct_business = int(summary.get("distinct_business", 0) or 0)
    commencement = int(summary.get("commencement_count", 0) or 0)

    st.caption(
        f"What this {role} actually said on the floor — speeches and oral "
        "questions from the Oireachtas debate record (oireachtas.ie AKN "
        "transcripts). Contributions delivered in Irish are flagged."
    )

    # ── Header strip (reuses stat_strip) ─────────────────────────────────────
    stats: list[tuple[str, str, str, str]] = [
        (f"{total:,}", "Contributions", "var(--ink-strong)", f"≈{words:,} words spoken"),
    ]
    if irish > 0:
        stats.append((f"{irish:,}", "As Gaeilge", "var(--accent)", "delivered in Irish"))
    if commencement > 0:
        stats.append((f"{commencement:,}", "Commencement Matters", "var(--ink-strong)", "issues raised"))
    else:
        stats.append((f"{distinct_business}", "Items of business", "var(--ink-strong)", "distinct debates"))
    stat_strip(stats)

    # ── Filter bar ───────────────────────────────────────────────────────────
    years = _speech_years(conn, join_key)
    year_opts = ["All years"] + [str(y) for y in years]
    selected_year = (
        st.pills("Year", options=year_opts, default="All years", key="mo_speech_year", label_visibility="collapsed")
        or "All years"
    )
    year_val = None if selected_year == "All years" else int(selected_year)

    fcol1, fcol2 = st.columns([3, 1])
    with fcol1:
        type_label = (
            st.segmented_control(
                "Type",
                options=["All", "Speeches", "Questions"],
                default="All",
                key="mo_speech_type",
                label_visibility="collapsed",
            )
            or "All"
        )
    with fcol2:
        irish_only = st.toggle(
            "As Gaeilge",
            key="mo_speech_irish",
            help="Show only contributions identified as delivered in Irish.",
        )
    ctype = {"All": None, "Speeches": "speech", "Questions": "question"}.get(type_label)

    business_opts = _speech_business(conn, join_key)
    selected_business = (
        st.selectbox(
            "Item of business",
            options=["All business"] + business_opts,
            index=0,
            key="mo_speech_business",
            label_visibility="collapsed",
        )
        or "All business"
    )
    business_val = None if selected_business == "All business" else selected_business

    search = (
        st.text_input(
            "Search what they said",
            value="",
            key="mo_speech_search",
            placeholder="Search the words they spoke…",
            label_visibility="collapsed",
        ).strip()
        or None
    )

    df = _member_speeches(conn, join_key, year_val, ctype, business_val, bool(irish_only), search)
    if df.empty:
        if irish_only:
            empty_state(
                "No contributions in Irish match",
                f"No contributions by {member_name} were identified as delivered in Irish under these filters.",
            )
        else:
            empty_state(
                "No contributions match these filters",
                "Try a different year, type, item of business, or search term.",
            )
        return

    total_rows = len(df)
    PAGE_SIZE = 8
    filter_sig = f"{year_val or 'all'}_{ctype or 'all'}_{business_val or 'all'}_{int(bool(irish_only))}_{search or ''}"
    pager_key = f"mo_speech_{join_key}_{filter_sig}"
    page_idx = paginate(total_rows, key_prefix=pager_key, page_size=PAGE_SIZE)
    visible = df.iloc[page_idx * PAGE_SIZE : (page_idx + 1) * PAGE_SIZE]

    start = page_idx * PAGE_SIZE + 1
    end = min((page_idx + 1) * PAGE_SIZE, total_rows)
    st.caption(f"Showing {start:,}–{end:,} of {total_rows:,} contribution{'s' if total_rows != 1 else ''}")

    for _, row in visible.iterrows():
        _render_speech_card(row)

    pagination_controls(
        total=total_rows,
        key_prefix=pager_key,
        page_sizes=(PAGE_SIZE,),
        default_page_size=PAGE_SIZE,
        label="contributions",
        show_caption=False,
    )


def _section_committees(member_name: str, join_key: str) -> None:
    """Phase 8 lift: per-TD committee profile body.

    Backed by the v_committee_* analytical views via data_access.committees_data
    (same fetchers committees.py uses for its register and per-committee pages).
    """
    df_long = fetch_committee_assignments("Dáil")
    offices = fetch_office_holders("Dáil")
    if df_long.empty:
        st.html(
            '<div class="dt-callout">No committee data available — '
            "the committees pipeline scaffold returned no rows.</div>"
        )
        return
    render_member_committees(
        member_name,
        df_long,
        offices,
        chamber="Dáil",
        show_member_header=False,
        status_filter_key=f"mo_comm_status_{join_key}",
        export_key_suffix="_mo",
    )


_OTHER_PILL = "Other / Independent"
_OTHER_MIN = 3  # UI display threshold — parties with fewer TDs collapse into
# the "Other / Independent" pill. This is a chip-layout
# decision (keep the pill row scannable), not a civic metric:
# changing it shouldn't require a pipeline rebuild.


def _named_parties(df: pd.DataFrame) -> list[str]:
    """Parties with >= _OTHER_MIN members, sorted by size desc then name."""
    if df.empty or "party_name" not in df.columns:
        return []
    counts = df["party_name"].value_counts()  # logic_firewall: display_only
    parties = df["party_name"].dropna().astype(str).unique().tolist()
    parties = [p for p in parties if p and p.lower() not in ("nan", "")]
    named = [p for p in parties if int(counts.get(p, 0)) >= _OTHER_MIN]
    return sorted(named, key=lambda p: (-int(counts.get(p, 0)), p))


def _party_pill_options(df: pd.DataFrame) -> list[str]:
    named = _named_parties(df)
    if not named:
        return []
    counts = df["party_name"].value_counts()  # logic_firewall: display_only
    in_named = sum(int(counts.get(p, 0)) for p in named)
    has_other = (len(df) - in_named) > 0
    return named + ([_OTHER_PILL] if has_other else [])


def _render_browse(conn) -> None:
    df = _member_list(conn)

    # House scope — Dáil (default) or Seanad. Keeps the list, labels and glossary
    # coherent: a mixed 236-member list with a "TDs" heading would mislead.
    house = (
        st.segmented_control(
            "Chamber",
            options=["Dáil", "Seanad"],
            default="Dáil",
            key="mo_browse_house",
            label_visibility="collapsed",
        )
        or "Dáil"
    )
    is_seanad = house == "Seanad"
    term = "Senator" if is_seanad else "TD"
    terms = "Senators" if is_seanad else "TDs"
    place_word = "panel" if is_seanad else "constituency"

    st.html(
        '<div class="dt-hero">'
        '<p class="dt-kicker">MEMBER OVERVIEW</p>'
        f'<h1 class="mo-browse-h1">Browse all {_h(terms)}</h1>'
        f'<p class="dt-dek">Pick a {_h(term)} to open their accountability profile: '
        "attendance, votes by policy area, payments, lobbying, and legislation.</p>"
        "</div>"
    )
    glossary_strip(
        [
            (
                term,
                "Seanadóir, a member of the Seanad (Senate)" if is_seanad else "Teachta Dála, a member of the Dáil",
            ),
            ("Accountability profile", "attendance, votes, payments, lobbying, and legislation in one place"),
        ]
    )

    if df.empty:
        empty_state("No member data", "Run the pipeline to generate attendance parquet files.")
        return

    df = df[df["house"] == house].reset_index(drop=True)

    # v_member_registry is unique on unique_member_code (verified on the
    # silver parquet: 176 rows / 176 distinct codes). The page-side
    # drop_duplicates that used to live here was defensive against a
    # historical pipeline gap that no longer exists.
    member_names = df["member_name"].dropna().astype(str).tolist()
    search, picked = find_a_td_filter(
        member_names,
        key_prefix="mo_browse",
        label=f"Find a {term}",
        placeholder=f"Search by name, party or {place_word}…",
    )
    if picked:
        picked_jk = _join_key_by_name(conn, picked, house)
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
    evidence_heading(f"{showing:,} {term if showing == 1 else terms}")

    if filtered.empty:
        empty_state(
            f"No {terms} match your filters",
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
        # Audit P2-3: same party-swatch as the profile hero.
        swatch_html = (
            f'<span class="mo-party-swatch" style="background:{party_colour(party)};" aria-hidden="true"></span>'
            if party
            else ""
        )
        cards.append(
            clickable_card_link(
                href=member_profile_url(code),
                inner_html=member_card_html(
                    name=name,
                    meta=meta,
                    avatar_url=avatar_data_url(name),
                    avatar_initials=_initials(name),
                    meta_prefix_html=swatch_html,
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
        label=terms,
    )


# ── Constituency civic context (Census 2022 / Electoral Commission 2023) ──────
# Renders one info card under the hero stat strip showing the constituency's
# headline civic numbers, with an inline source attribution that double-clicks
# as a verification link to the Electoral Commission review.
#
# Source is now the Electoral Commission's 2023 review (2023 boundaries), which
# matches all 43 current constituencies. The empty-ctx branch below is retained
# only as a defensive fallback (e.g. an unexpected/renamed constituency string);
# in normal operation every constituency resolves to the clean-match branch.


def _render_constituency_context(constituency: str, ctx: dict) -> None:
    """Render the constituency civic-context strip with built-in provenance.

    A matched constituency (Electoral Commission row found) gets the headline
    figures. An unmatched constituency gets a transparent "no figure on file"
    caveat card. Never interpolate or estimate a population figure.
    """
    if not constituency:
        return

    from ui.components import info_card
    from ui.entity_links import source_link_html

    if ctx:
        pop22 = int(ctx.get("population_2022") or 0)
        per_td = int(ctx.get("population_per_td") or 0)
        seats = int(ctx.get("td_seats") or 0)
        boundary_caption = str(ctx.get("boundaries_label") or "Census 2022")

        body = (
            f'<div class="mo-cc-row">'
            f'  <span class="mo-cc-kicker">Constituency · {_h(constituency)}</span>'
            f"</div>"
            f'<div class="mo-cc-row">'
            f'  <strong class="mo-cc-headline">{pop22:,}</strong>'
            f'  <span class="mo-cc-headline-label">residents at Census 2022</span>'
            f"</div>"
            f'<div class="mo-cc-row mo-cc-row-secondary">'
            f"  <strong>{per_td:,}</strong> per TD"
            f'  <span class="mo-cc-sep">·</span>'
            f"  <strong>{seats}</strong> {'seat' if seats == 1 else 'seats'}"
            f"</div>"
        )
    else:
        # Defensive fallback only. Since the Electoral Commission source matches
        # all 43 current constituencies, this branch fires only for an
        # unexpected/unrecognised constituency string. Be transparent rather
        # than guess a figure.
        note = "No Census 2022 population figure is on file for this constituency."
        body = (
            f'<div class="mo-cc-row">'
            f'  <span class="mo-cc-kicker">Constituency · {_h(constituency)}</span>'
            f"</div>"
            # Use a block (not flex) container so inline <strong> in the
            # caveat copy doesn't force a flex-line break before/after it.
            f'<p class="mo-cc-caveat">{note}</p>'
        )
        boundary_caption = "Census 2022 (2023 boundaries)"

    info_card(body, border_left_color="var(--accent)", padding="0.7rem 1rem")

    # Inline verification footer — visible source attribution + deep link to the
    # Electoral Commission review so any reader can verify the figure
    # themselves. Project pattern: provenance is a first-class UI element,
    # not a hidden expander.
    source_chip = source_link_html(
        _EC_REVIEW_URL,
        "Verify in the Electoral Commission review",
        aria_label="Open the Electoral Commission Constituency Review Report 2023 in a new tab",
    )
    st.html(
        f'<div class="mo-cc-source">'
        f'<span class="mo-cc-source-label">Source · </span>'
        f'<span class="mo-cc-source-body">CSO Census 2022, via Electoral Commission Constituency Review 2023 · {_h(boundary_caption)}</span>'
        f'<span class="mo-cc-source-link"> · {source_chip or ""}</span>'
        f"</div>"
    )


# ── Profile ─────────────────────────────────────────────────────────────────────


def _prev_next_member(conn, join_key: str, house: str) -> tuple[dict | None, dict | None]:
    """Return (prev, next) member dicts in alphabetical-name order, or None at ends.

    Retrieval-only: reuses _member_list which already SELECTs from v_member_registry
    ORDER BY member_name. Wraps at the ends to None so the buttons can disable.

    Scoped to ``house`` so the walker stays within the same chamber the browse
    grid is filtered to — otherwise a TD's "next" could land on a Senator
    (the registry interleaves both houses alphabetically).
    """
    df = _member_list(conn)
    if df.empty:
        return None, None
    df = df[df["house"] == house]
    # v_member_registry is unique on unique_member_code — no in-page dedup
    # needed (see comment at the browse-list above).
    df = df.reset_index(drop=True)
    idx_match = df.index[df["unique_member_code"] == join_key]
    if len(idx_match) == 0:
        return None, None
    i = int(idx_match[0])
    prev_row = df.iloc[i - 1].to_dict() if i > 0 else None
    next_row = df.iloc[i + 1].to_dict() if i < len(df) - 1 else None
    return prev_row, next_row


def _render_profile_nav(conn, join_key: str, house: str, term: str, terms: str) -> None:
    """Top-of-profile nav: [← All TDs] [← prev TD] [next TD →].

    ``term``/``terms`` adapt the labels and help text to the member's chamber
    (TD/TDs for the Dáil, Senator/Senators for the Seanad). ``house`` scopes
    the prev/next walker to the same chamber.

    Round-3 audit P2-1: previously rendered 3 full-width stretched buttons.
    Audit 2026-05-27 P1-3: Streamlit columns collapse one-per-row on mobile,
    so the 4-column layout became 4 stacked rows wasting ~140px above the
    hero. Now wraps the three Streamlit buttons in a `.mo-prof-nav` flex
    container so they stay on one horizontal row at every viewport (the
    `:has()` CSS selector grabs the stHorizontalBlock around them).
    """
    prev_row, next_row = _prev_next_member(conn, join_key, house)
    c_back, c_prev, c_next, _spacer = st.columns([1.4, 2.2, 2.2, 6])
    with c_back:
        # Marker INSIDE the first column so the parent stHorizontalBlock's
        # :has(.mo-prof-nav-marker) descendant selector matches and forces
        # the row to stay horizontal on mobile (Streamlit columns otherwise
        # stack one-per-row under 640px).
        st.html('<div class="mo-prof-nav-marker"></div>')
        if back_button(f"← All {terms}", key="mo_all", help=f"Return to the full {term} list"):
            st.session_state.pop(_STAGE_KEY, None)
            st.query_params.clear()
            st.rerun()
    with c_prev:
        if prev_row is not None:
            label = f"← {prev_row['member_name']}"
            if st.button(
                label,
                key="mo_prev_td",
                help=f"Previous {term} alphabetically: {prev_row['member_name']}",
            ):
                st.session_state[_STAGE_KEY] = str(prev_row["unique_member_code"])
                st.query_params.clear()
                st.query_params["member"] = str(prev_row["unique_member_code"])
                st.rerun()
        else:
            st.button("← (start)", key="mo_prev_td_disabled", disabled=True)
    with c_next:
        if next_row is not None:
            label = f"{next_row['member_name']} →"
            if st.button(
                label,
                key="mo_next_td",
                help=f"Next {term} alphabetically: {next_row['member_name']}",
            ):
                st.session_state[_STAGE_KEY] = str(next_row["unique_member_code"])
                st.query_params.clear()
                st.query_params["member"] = str(next_row["unique_member_code"])
                st.rerun()
        else:
            st.button("(end) →", key="mo_next_td_disabled", disabled=True)


def _render_stage2(
    conn,
    join_key: str,
) -> None:

    # House drives a handful of label/section differences (Senator vs TD badge,
    # panel vs constituency, no PQs/constituency-demographics for Senators) and
    # scopes the prev/next walker + back-button wording to the right chamber.
    house = _member_house(conn, join_key)
    is_seanad = house == "Seanad"
    term = "Senator" if is_seanad else "TD"
    terms = "Senators" if is_seanad else "TDs"

    _render_profile_nav(conn, join_key, house, term, terms)

    identity = _identity(conn, join_key)
    if not identity:
        browse_href = f"/{PAGES['member_overview']}"
        st.html(
            f'<div class="mo-not-found-callout">'
            f"<strong>We couldn't find this member</strong><br>"
            f'<span class="mo-not-found-body">'
            f"The link you followed may be out of date, or this member "
            f"hasn't been added yet — the Oireachtas roster updates as the "
            f"membership changes.</span><br>"
            f'<a class="mo-not-found-cta" href="{_h(browse_href)}" target="_self">'
            f"&larr; Browse all members</a>"
            f"</div>"
        )
        return

    member_name = str(identity.get("member_name", ""))
    party = str(identity.get("party_name", ""))
    constituency = str(identity.get("constituency", ""))
    is_minister = str(identity.get("is_minister", "false")).lower() == "true"
    meta = clean_meta(party, constituency)
    # Audit P2-3: party-colour swatch as a small dot in front of the
    # party text so the affiliation reads at a glance, not in prose.
    # Reuses the committees colour map via ui.components.party_colour.
    party_swatch_html = (
        f'<span class="mo-party-swatch" style="background:{party_colour(party)};" aria-hidden="true"></span>'
        if party
        else ""
    )

    role_html = (
        '<span class="dt-badge dt-badge-minister">Minister</span>'
        if is_minister
        else f'<span class="dt-badge dt-badge-td">{"Senator" if is_seanad else "TD"}</span>'
    )

    rd_df = _lobbying_rd(conn, join_key)
    # Audit P1-4: guard against the "former position = TD" misfire. The
    # v_lobbying_revolving_door view records ANY prior position including
    # "TD" for re-elected members, so every sitting TD was getting the
    # warning chip. Genuine cases (former Minister, former Senator, etc.)
    # survive this guard. Pipeline-side cleanup is tracked separately.
    rd_is_real = False
    if not rd_df.empty:
        _pos = str(rd_df.iloc[0].get("former_position", "")).strip()
        rd_is_real = bool(_pos) and _pos.upper() != "TD"
    rd_html = '<span class="dt-badge dt-badge-revolving">Revolving door</span>' if rd_is_real else ""

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
    meta_row = '<div class="dt-hero-meta-row">' + "".join(badge_parts) + sep_html + "".join(link_parts) + "</div>"

    st.html(
        f'<div class="dt-hero">'
        f'  <div class="dt-profile-header">'
        f'    <div class="dt-profile-avatar-col">{avatar_block}{caption_block}</div>'
        f'    <div class="dt-profile-meta-col">'
        f'      <h1 class="td-name mo-profile-h1">{_h(member_name)}</h1>'
        f'      <p class="td-meta mo-profile-meta">{party_swatch_html}{_h(meta)}</p>'
        f"      {meta_row}"
        f"    </div>"
        f"  </div>"
        f"</div>"
    )

    # ── Headline stats — single source of truth, no duplication ──────────────
    att_df = _att_all_years(conn, join_key)
    pay_total = _pay_grand_total(conn, join_key)
    vote_df = _votes_summary(conn, join_key)

    # Round-3 audit P1-F: for ministers (especially the Taoiseach), the
    # plenary-attendance and TAA-payments data sources legitimately have
    # NO rows — but the unguarded stat strip rendered two em-dashes, which
    # looked broken rather than deliberate. When both are empty AND we
    # know it's a minister, replace the strip with a single explanatory
    # caption (with the votes summary inlined, since votes are still
    # tracked for ministers). This is the "every row tells a story"
    # principle from PRODUCT.md applied to the empty rows.
    att_empty = att_df.empty
    pay_empty = not pay_total

    if not vote_df.empty:
        vr = vote_df.iloc[0]
        votes_cast = (
            int(vr.get("yes_count", 0) or 0) + int(vr.get("no_count", 0) or 0) + int(vr.get("abstained_count", 0) or 0)
        )
        votes_div = int(vr.get("division_count", 0) or 0)
    else:
        votes_cast = votes_div = 0

    if att_empty and pay_empty:
        # Round-3 audit P1-F: when BOTH attendance and payments are empty,
        # show a single explanatory line instead of two em-dashes in the
        # stat strip. The is_minister flag from v_member_registry isn't
        # reliable for cabinet members (returns False even for the
        # Taoiseach), so we don't gate on it — empty-on-both is itself
        # the strongest signal that the regular plenary/TAA framing
        # doesn't apply.
        # Audit P2-2: "1,318 votes cast across 1,318 divisions" reads
        # tautologically when the numbers match (a TD who voted in every
        # division). Collapse to the one-number form in that case.
        if votes_cast and votes_div and votes_cast == votes_div:
            votes_phrase = f"voted in all <strong>{votes_div:,}</strong> divisions"
        elif votes_cast:
            votes_phrase = f"<strong>{votes_cast:,}</strong> votes cast across <strong>{votes_div:,}</strong> divisions"
        else:
            votes_phrase = "votes record not on file"
        headline = "Cabinet member." if is_minister else "Different rules apply."
        st.html(
            f'<div class="dt-callout mo-cabinet-callout">'
            f"<strong>{headline}</strong> &nbsp;"
            f'<span class="mo-cabinet-callout-body">'
            f"Plenary-attendance and Parliamentary Standard Allowance figures "
            f"aren't on file for this member &nbsp;·&nbsp; "
            f"{votes_phrase}.</span>"
            f"</div>"
        )
    else:
        if not att_df.empty:
            # Skip the in-progress calendar year on the stat strip — it makes
            # every TD look like an absentee from Jan-May (audit P1-6, mirrors
            # attendance P1-1 and payments P1-1). Pick the first completed
            # year; if the dataset only contains the in-progress year, fall
            # back to it and label "(so far)" so the framing stays honest.
            this_year = datetime.date.today().year
            completed = att_df[att_df["year"] < this_year]
            row = completed.iloc[0] if not completed.empty else att_df.iloc[0]
            att_yr = int(row["year"])
            att_days = int(row["attended_count"])
            so_far = " (so far)" if att_yr >= this_year else ""
            att_lbl = f"Days in chamber · {att_yr}{so_far}"
            att_val = str(att_days)
            if is_minister:
                att_sub = "Minister · plenary record only"
            else:
                rank, total = _att_rank_for_year(conn, join_key, att_yr, house)
                att_sub = f"Rank {rank} of {total} {'Senators' if is_seanad else 'TDs'}" if rank and total else ""
        else:
            att_lbl, att_val, att_sub = "Days in chamber", "—", ""

        cast_val = f"{votes_cast:,}" if votes_cast else "—"
        cast_sub = f"across {votes_div:,} divisions" if votes_div else ""
        # Audit P1-1: drop the em-dash for a single empty stat. The TAA
        # parquet only covers ministers + a small subset of TDs, so the
        # bare "—" was the rule not the exception for ~150 of 176 members
        # and read as broken data. "Not on file" + sub-label explanation
        # mirrors the round-3 P1-F cabinet-member fallback pattern.
        if pay_total:
            pay_val = f"€{pay_total:,.0f}"
            # Expand TAA on first use (it appears unexpanded nowhere else above
            # the fold); the Payments section below repeats it once known.
            pay_sub = "Travel & Accommodation Allowance (TAA), all years on record"
        else:
            pay_val = "Not on file"
            pay_sub = "Travel & Accommodation Allowance (TAA) not tracked for this member"

        stat_strip(
            [
                (att_val, att_lbl, "var(--text-primary)", att_sub),
                (cast_val, "Votes cast", "var(--signal-good)", cast_sub),
                (pay_val, "Payments received", "var(--text-meta)" if not pay_total else "var(--text-primary)", pay_sub),
            ]
        )

    # ── Constituency civic context (Census 2022 / Electoral Commission 2023) ─
    # Sits between the TD-axis stat strip (about this TD) and the section nav
    # (about this TD's record). Anchors the page to the constituency the TD
    # represents — population, seats, per-TD on the current 2023 boundaries
    # (43/43 match; unmatched names get a transparent caveat).
    # Seanad seats are filled by vocational panels / university / Taoiseach
    # nomination, not geographic constituencies — there is no Census population
    # denominator to show, so this card is Dáil-only.
    if not is_seanad:
        ctx = _constituency_context(conn, constituency)
        _render_constituency_context(constituency, ctx)

    # ── Section nav chip row (Phase 2 chrome, audit P0-2 finally rendered) ──
    # Anchors jump to #mo-section-<sid> divs emitted alongside each expander
    # below. CSS at shared_css.py:.mo-section-nav / .mo-section-chip shipped
    # with Phase 2 but the markup was missing — citizens had no wayfinding
    # past the hero on the longest page in the app.
    chip_html = ['<nav class="mo-section-nav" aria-label="Profile sections">']
    for sid, label, _ in _PROFILE_SECTIONS:
        chip_html.append(f'<a class="mo-section-chip" href="#mo-section-{sid}">{_h(label)}</a>')
    chip_html.append("</nav>")
    st.html("\n".join(chip_html))

    # Hash-scroll shim — Streamlit doesn't honour `#anchor` on cold-load (the
    # browser scrolls before sections render) or on chip click (the iframe
    # boundaries swallow the default hashchange behaviour). This polls for the
    # target inside the parent document after each rerun and scrolls when found.
    # Lives inside an st.components.v1.html iframe so the <script> isn't
    # stripped by st.markdown's DOMPurify (per feedback_streamlit_css_and_state).
    import streamlit.components.v1 as components  # local import keeps top tidy

    components.html(
        """
        <script>
        (function() {
          const D = window.parent.document;
          const scrollTo = (id) => {
            const el = D.getElementById(id);
            if (!el) return false;
            el.scrollIntoView({ behavior: 'smooth', block: 'start' });
            return true;
          };
          const honourHash = () => {
            const h = window.parent.location.hash;
            if (!h || !h.startsWith('#mo-section-')) return;
            const id = h.slice(1);
            // Poll for up to 3s — sections render after this script fires.
            let tries = 0;
            const t = setInterval(() => {
              if (scrollTo(id) || ++tries > 30) clearInterval(t);
            }, 100);
          };
          // Run once on load + whenever the chip-click changes the hash.
          honourHash();
          window.parent.addEventListener('hashchange', honourHash);
        })();
        </script>
        """,
        height=0,
    )

    # ── Profile sections (always-rendered, flat headings) ────────────────────
    # Every section's body runs on every view — no expand/collapse, no lazy
    # gating. Trades ~5 cold-load SQL queries for ~25 to make the page
    # scannable end-to-end (TheyWorkForYou pattern, PRODUCT.md principle #3).
    for sid, label, _page_key in _PROFILE_SECTIONS:
        # Anchor lives outside the heading so #mo-section-<sid> jumps land
        # at the right scroll offset (CSS uses negative top to clear any
        # sticky bits above).
        st.html(f'<div id="mo-section-{sid}" class="mo-section-anchor"></div>')
        st.html(f'<h2 class="section-heading">{_h(label)}</h2>')

        if sid == "interests":
            # Phase 3 lift: full body rendered here without the per-page
            # member header (the hero above already shows it). House-aware —
            # the Register of Interests is published per chamber.
            render_member_interests(
                house,
                member_name,
                show_member_header=False,
                year_pill_key=f"mo_int_year_{join_key}",
            )
        elif sid == "lobbying":
            # Revolving-door callout (member-overview-local — built from
            # v_lobbying_revolving_door_member, which lobbying_2.py does
            # not query directly). Renders above the lifted body so the
            # most politically potent flag is the first thing visible.
            # Audit P1-4: same "former position = TD" guard as the hero
            # badge — without it, every sitting TD shows the warning.
            rd_df = _lobbying_rd(conn, join_key)
            if not rd_df.empty:
                rd_row = rd_df.iloc[0]
                pos = str(rd_row.get("former_position", "")).strip()
                if pos and pos.upper() != "TD":
                    rc = int(rd_row.get("return_count", 0) or 0)
                    firms = int(rd_row.get("distinct_firms", 0) or 0)
                    pos_line = f"Former position: <strong>{_h(pos)}</strong>. "
                    st.badge("Revolving door", icon=":material/warning:", color="orange")
                    st.html(
                        f'<div class="lob-revolving-callout">'
                        f'<div class="lob-revolving-heading">Revolving door flag</div>'
                        f'<p class="lob-revolving-body">'
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
                    unique_member_code=join_key,
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
                house=house,
                show_member_header=False,
                year_pill_key=f"mo_att_year_{join_key}",
                export_key_suffix="_mo",
            )
        elif sid == "votes":
            # Phase 7 lift: shared `render_member_votes` wrapper fetches
            # td_vote_summary + history + year summary and calls
            # `render_td_panel(show_header=False)`. Same render path the
            # stand-alone /rankings-votes page used to take in Mode B —
            # vote_explorer was already shared, so this is the lightest
            # cross-page lift. Debates stay as a sub-section (their data
            # comes from member_debate_sections, unrelated to votes).
            # Vote date range (relocated from the old sidebar secondary slot)
            # now sits with the votes section it actually filters.
            field_label("Vote date range")
            _dv = st.date_input(
                "Vote date range",
                value=(),
                label_visibility="collapsed",
                key="mo_vote_date",
            )
            _v_from, _v_to = (
                (str(_dv[0]), str(_dv[1])) if isinstance(_dv, (list, tuple)) and len(_dv) == 2 else (None, None)
            )
            render_member_votes(
                conn,
                join_key,
                show_header=False,
                date_from=_v_from,
                date_to=_v_to,
                key_suffix=f"_mo_{join_key}",
            )
        elif sid == "debates":
            # 2026-05-31: promoted out of the Votes section into its own chip
            # so a reporter looking for "what has this TD spoken on?" finds it
            # in the section nav rather than buried under a 1000-row vote list.
            _section_debates(conn, join_key, member_name)
        elif sid == "questions":
            # 2026-05-27: full-history (264k row) Questions section.
            # Header strip + filter bar + paginated feed. See contract
            # member_overview.yaml -> section_content.questions.
            # Parliamentary Questions are a Dáil instrument — Senators raise
            # Commencement Matters instead. Those now live in the Debates
            # section (speeches_fact), so point there rather than dead-ending.
            if is_seanad:
                empty_state(
                    "Not applicable to Senators",
                    "Parliamentary Questions are tabled by TDs. Senators raise "
                    "Commencement Matters in the Seanad — see the **Debates** "
                    "section above (filter the item of business to "
                    "“Commencement Matters”).",
                )
            else:
                _section_questions(conn, join_key, member_name)
        elif sid == "legislation":
            _section_legislation(conn, join_key, member_name)
            _section_ministerial_roles(conn, join_key)
            _section_statutory_instruments(conn, join_key)
        elif sid == "committees":
            _section_committees(member_name, join_key)

    # Quiet developer affordance: this whole dossier as JSON on the open-data API.
    # Renders nothing until DAIL_API_BASE_URL is configured (config-gated).
    from urllib.parse import quote

    _api = api_json_link(f"/v1/members/{quote(str(join_key), safe='')}/dossier", "This profile as JSON")
    if _api:
        st.html(f'<div class="dt-api-footer">{_api}</div>')


# ── Main entry point ───────────────────────────────────────────────────────────


@page_error_boundary
def member_overview_page() -> None:
    inject_css()
    conn = get_member_overview_conn()

    url_jk = st.query_params.get("member")
    if url_jk:
        st.session_state[_STAGE_KEY] = url_jk

    join_key = st.session_state.get(_STAGE_KEY)

    # Sidebar→filter-bar migration: identity is carried by the top-nav tab +
    # each view's own hero. The only sidebar control was a vote-date filter,
    # now relocated into the Votes section it filters (see _render_stage2).
    hide_sidebar()

    if join_key:
        _render_stage2(conn, join_key)
    else:
        _render_browse(conn)
