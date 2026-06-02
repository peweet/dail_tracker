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
    evidence_heading,
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
    member_profile_url,
    oireachtas_profile_url,
    si_detail_url,
    social_icon_chip_html,
    source_link_html,
)
from ui.vote_explorer import render_member_votes
from data_access.member_overview_data import get_member_overview_conn
from ui.attendance_panel import render_member_attendance
from data_access.committees_data import fetch_committee_assignments, fetch_office_holders
from pages_code.committees import render_member_committees
from ui.interests_panel import render_member_interests
from pages_code.lobbying_3 import render_member_lobbying
from ui.payments_panel import render_member_payments
from data_access.payments_data import fetch_filter_options as _pay_filter_options
from data_access.payments_data import fetch_payments_summary as _pay_summary

_log = logging.getLogger(__name__)
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
        "SELECT unique_member_code, member_name, party_name, constituency, house"
        " FROM v_member_registry ORDER BY member_name",
    )


@st.cache_data(ttl=300)
def _join_key_by_name(_conn, name: str, house: str | None = None) -> str | None:
    if house:
        df = _q(
            _conn,
            "SELECT unique_member_code FROM v_member_registry WHERE member_name = ? AND house = ? LIMIT 1",
            [name, house],
        )
    else:
        df = _q(
            _conn,
            "SELECT unique_member_code FROM v_member_registry WHERE member_name = ? LIMIT 1",
            [name],
        )
    return str(df.iloc[0]["unique_member_code"]) if not df.empty else None


@st.cache_data(ttl=300)
def _member_house(_conn, join_key: str) -> str:
    """House ('Dáil'/'Seanad') for a member code. Defaults to 'Dáil'. The one
    cross-house code collision (Seán Kyne) resolves to his current house via
    the Seanad-last ordering of the registry; acceptable for a single edge case.
    """
    df = _q(
        _conn,
        "SELECT house FROM v_member_registry WHERE unique_member_code = ? ORDER BY house DESC LIMIT 1",
        [join_key],
    )
    return str(df.iloc[0]["house"]) if not df.empty else "Dáil"


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
def _att_rank_for_year(_conn, join_key: str, year: int, house: str = "Dáil") -> tuple[int | None, int | None]:
    """Member's attendance rank for a given year and the total ranked field size.
    Returns (rank_high, total). Both None on miss. Retrieval-only.
    Rank + total are scoped to the member's house (TDs ranked among TDs only)."""
    df = _q(
        _conn,
        "SELECT rank_high FROM v_attendance_year_rank WHERE unique_member_code = ? AND year = ? LIMIT 1",
        [join_key, year],
    )
    if df.empty:
        return None, None
    total_df = _q(
        _conn,
        "SELECT COUNT(*) AS n FROM v_attendance_year_rank WHERE year = ? AND house = ?",
        [year, house],
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


# Electoral Commission review deep link — surfaces the source report for citizen
# verification. This is the only external URL specific to constituency
# demographics; the rest of the provenance is captured inline in the SQL view header.
_EC_REVIEW_URL = "https://www.electoralcommission.ie/publications/constituency-review-reports/"


@st.cache_data(ttl=300)
def _constituency_context(_conn, constituency: str) -> dict:
    """Return the v_member_constituency_demographics row for ``constituency``,
    or an empty dict when the name has no row. With the Electoral Commission
    (2023-boundary) source every current constituency matches 43/43, so the
    empty-dict branch is now a defensive fallback rather than the common
    boundary-split case it used to guard."""
    if not constituency:
        return {}
    df = _q(
        _conn,
        "SELECT population_2022, population_per_td, td_seats,"
        " boundaries_label, source_key"
        " FROM v_member_constituency_demographics"
        " WHERE constituency_name = ?",
        [constituency],
    )
    if df.empty:
        return {}
    return df.iloc[0].to_dict()


# ── Questions section data helpers ─────────────────────────────────────────────
# Added 2026-05-27. All three views read silver/questions.parquet (264k rows
# post the May 2026 pagination-cap fix; see [[project-questions-cap-fix-2026-05-27]]).


@st.cache_data(ttl=300)
def _q_profile(_conn, join_key: str) -> dict:
    df = _q(
        _conn,
        "SELECT total_qs, distinct_ministries, top_ministry, top_count, top_pct"
        " FROM v_member_question_profile WHERE unique_member_code = ? LIMIT 1",
        [join_key],
    )
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _q_focus_shift(_conn, join_key: str) -> dict:
    df = _q(
        _conn,
        "SELECT past_top, past_n, past_year_min, past_year_max,"
        " recent_top, recent_n, recent_year_min, recent_year_max"
        " FROM v_member_question_focus_shift WHERE unique_member_code = ? LIMIT 1",
        [join_key],
    )
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _q_years(_conn, join_key: str) -> list[int]:
    df = _q(
        _conn,
        "SELECT DISTINCT question_year FROM v_member_questions"
        " WHERE unique_member_code = ? AND question_year IS NOT NULL"
        " ORDER BY question_year DESC",
        [join_key],
    )
    return [int(y) for y in df["question_year"].dropna().tolist()] if not df.empty else []


@st.cache_data(ttl=300)
def _q_ministries(_conn, join_key: str) -> list[str]:
    """Per-TD distinct ministries ordered by COUNT desc.

    Rollup lives in v_member_question_ministries; this is retrieval-only.
    """
    df = _q(
        _conn,
        "SELECT ministry FROM v_member_question_ministries WHERE unique_member_code = ? ORDER BY n DESC, ministry ASC",
        [join_key],
    )
    return df["ministry"].astype(str).tolist() if not df.empty else []


@st.cache_data(ttl=300)
def _q_top_topics(_conn, join_key: str) -> pd.DataFrame:
    """Top-3 topics for a TD. Rollup lives in v_member_question_top_topics."""
    return _q(
        _conn,
        "SELECT topic, n FROM v_member_question_top_topics"
        " WHERE unique_member_code = ?"
        " ORDER BY n DESC, topic ASC LIMIT 3",
        [join_key],
    )


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
    """Question feed query. Filters AND together. Free-text search uses
    ILIKE with %wrap on the user's input, case-insensitive. We pull up to
    10k rows and paginate client-side via paginate(), matching
    _section_debates. The 10k ceiling is above any plausible per-TD-per-
    filter slice (Cullinane's full 7,052 is the only case that approaches
    it; any filter narrows well below)."""
    clauses = ["unique_member_code = ?"]
    params: list = [join_key]
    if year is not None:
        clauses.append("question_year = ?")
        params.append(year)
    if qtype:
        clauses.append("question_type = ?")
        params.append(qtype)
    if ministry:
        clauses.append("ministry = ?")
        params.append(ministry)
    if topic:
        clauses.append("topic = ?")
        params.append(topic)
    if search_text:
        clauses.append("question_text ILIKE ?")
        params.append(f"%{search_text}%")
    return _q(
        _conn,
        "SELECT question_date, question_type, ministry, topic, question_text,"
        " question_ref, oireachtas_url"
        f" FROM v_member_questions WHERE {' AND '.join(clauses)}"
        " ORDER BY question_date DESC LIMIT 10000",
        params,
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


def _section_legislation(conn, join_key: str, member_name: str) -> None:
    evidence_heading("Legislation sponsored")

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
    evidence_heading("Statutory Instruments signed")

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

    st.download_button(
        label="Export statutory instruments (CSV)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"si_signed_{join_key}.csv",
        mime="text/csv",
        key="mo_si_export",
        width="stretch",
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
                f'<div style="margin-top: 0.4rem;">{_h(text)}</div>'
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
    st.download_button(
        label="Export filtered questions (CSV)",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"questions_{member_name.replace(' ', '_')}.csv",
        mime="text/csv",
        key=f"mo_q_export_{join_key}",
        width="stretch",
    )

    st.caption("Source: oireachtas.ie/en/debates/questions/ · 2020 to present · complete history per TD.")


def _section_debates(conn, join_key: str, member_name: str) -> None:
    evidence_heading("Debate participation")
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
            f'<div class="leg-bill-card mo-bill-card">'
            f'<div class="leg-bill-card-header">'
            f'<span class="leg-bill-card-date">{_h(date_disp)}</span>'
            f'<span class="signal leg-status-active">{_h(chamber)}</span>'
            f"</div>"
            f'<div class="leg-bill-card-title">{_h(topic)}</div>'
            f'<div class="mo-debate-card-meta">'
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
    house = st.segmented_control(
        "House",
        options=["Dáil", "Seanad"],
        default="Dáil",
        key="mo_browse_house",
        label_visibility="collapsed",
    ) or "Dáil"
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


def _prev_next_member(conn, join_key: str) -> tuple[dict | None, dict | None]:
    """Return (prev, next) member dicts in alphabetical-name order, or None at ends.

    Retrieval-only: reuses _member_list which already SELECTs from v_member_registry
    ORDER BY member_name. Wraps at the ends to None so the buttons can disable.
    """
    df = _member_list(conn)
    if df.empty:
        return None, None
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


def _render_profile_nav(conn, join_key: str) -> None:
    """Top-of-profile nav: [← All TDs] [← prev TD] [next TD →].

    Round-3 audit P2-1: previously rendered 3 full-width stretched buttons.
    Audit 2026-05-27 P1-3: Streamlit columns collapse one-per-row on mobile,
    so the 4-column layout became 4 stacked rows wasting ~140px above the
    hero. Now wraps the three Streamlit buttons in a `.mo-prof-nav` flex
    container so they stay on one horizontal row at every viewport (the
    `:has()` CSS selector grabs the stHorizontalBlock around them).
    """
    prev_row, next_row = _prev_next_member(conn, join_key)
    c_back, c_prev, c_next, _spacer = st.columns([1.4, 2.2, 2.2, 6])
    with c_back:
        # Marker INSIDE the first column so the parent stHorizontalBlock's
        # :has(.mo-prof-nav-marker) descendant selector matches and forces
        # the row to stay horizontal on mobile (Streamlit columns otherwise
        # stack one-per-row under 640px).
        st.html('<div class="mo-prof-nav-marker"></div>')
        if back_button("← All TDs", key="mo_all", help="Return to the full TD list"):
            st.session_state.pop(_STAGE_KEY, None)
            st.query_params.clear()
            st.rerun()
    with c_prev:
        if prev_row is not None:
            label = f"← {prev_row['member_name']}"
            if st.button(
                label,
                key="mo_prev_td",
                help=f"Previous TD alphabetically: {prev_row['member_name']}",
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
                help=f"Next TD alphabetically: {next_row['member_name']}",
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

    _render_profile_nav(conn, join_key)

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

    # House drives a handful of label/section differences (Senator vs TD badge,
    # panel vs constituency, no PQs/constituency-demographics for Senators).
    house = _member_house(conn, join_key)
    is_seanad = house == "Seanad"
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
            pay_sub = "TAA · all years on record"
        else:
            pay_val = "Not on file"
            pay_sub = "TAA figures aren't tracked for this member"

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
            # Commencement Matters instead, which this dataset does not yet
            # cover. State that plainly rather than showing an empty feed.
            if is_seanad:
                empty_state(
                    "Not applicable to Senators",
                    "Parliamentary Questions are tabled by TDs. Senators raise "
                    "Commencement Matters in the Seanad, which are not yet tracked here.",
                )
            else:
                _section_questions(conn, join_key, member_name)
        elif sid == "legislation":
            _section_legislation(conn, join_key, member_name)
            _section_statutory_instruments(conn, join_key)
        elif sid == "committees":
            _section_committees(member_name, join_key)


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
