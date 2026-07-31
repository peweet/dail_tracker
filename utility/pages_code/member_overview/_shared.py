from __future__ import annotations

import datetime

import pandas as pd
import streamlit as st

from dail_tracker_core.queries import member_overview as moq

_STAGE_KEY = "mo_join_key"

# ── Profile section IA (Phase 2 chrome) ────────────────────────────────────────
# Section order is "most politically potent first" per project_design_principles.
# (id, section label, ranking-page key in entity_links.PAGES). The id is the
# URL-fragment anchor (`/member-overview?member=<code>#<id>`).
_PROFILE_SECTIONS: list[tuple[str, str, str]] = [
    ("interests", "Interests", "interests"),
    ("lobbying", "Lobbying", "lobbying"),
    ("payments", "Salary & expenses", "payments"),
    ("attendance", "Attendance", "attendance"),
    ("votes", "Votes", "votes"),
    ("debates", "Debates", "votes"),  # promoted out of Votes 2026-05-31 — was buried
    ("questions", "Questions", "votes"),  # 2026-05-27: see _section_questions
    ("legislation", "Legislation", "legislation"),
    ("committees", "Committees", "committees"),
]

# ── Section router IA (2026-06-22) ─────────────────────────────────────────────
# The profile moved from an all-sections-rendered flat scroll (too long; adjacent
# section cards bled into the viewport) to a single-section router. "overview" is
# the default landing — a one-screen summary-card grid; every other tab renders
# exactly one domain body. Tab order promotes Votes to first detail tab. The
# section is carried in ?section=<sid> (bookmarkable); see _render_stage2 for the
# session-state fallback that keeps in-section filter chips from kicking the
# reader back to Overview.
_SECTION_LABELS: dict[str, str] = {
    "overview": "Overview",
    "votes": "Votes",
    "interests": "Interests",
    "lobbying": "Lobbying",
    "payments": "Salary & expenses",
    "attendance": "Attendance",
    "questions": "Questions",
    "debates": "Debates",
    "legislation": "Legislation",
    "committees": "Committees",
}
_SECTION_TABS: list[str] = list(_SECTION_LABELS.keys())


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
def _member_list_all(_conn) -> pd.DataFrame:
    """Current + historic members (for the 'Include historic TDs' toggle). Falls
    back to the current-only list (tagged is_current) if the historic view is
    unavailable, so the browse page degrades gracefully."""
    df = moq.member_list_all(_conn).data
    if df.empty:
        df = moq.member_list(_conn).data.copy()
        if not df.empty:
            df["is_current"] = True
            df["dails_served"] = ""
            df["served_from_year"] = pd.NA
            df["served_to_year"] = pd.NA
    return df


@st.cache_data(ttl=300)
def _member_codes_for_dail(_conn, dail: str) -> set[str]:
    """Member codes who served in one Dáil/Seanad term — the comma-list
    dails_served is split in SQL (v_member_registry_all), so the browse page's
    term filter is a plain isin() on these codes."""
    df = moq.member_codes_for_dail(_conn, dail).data
    return set() if df.empty else set(df["unique_member_code"].astype(str))


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
    if not df.empty:
        return df.iloc[0].to_dict()
    # Former member (not in the current roster) — resolve from the historic view.
    df = moq.identity_registry_all(_conn, join_key).data
    return df.iloc[0].to_dict() if not df.empty else {}


@st.cache_data(ttl=300)
def _att_all_years(_conn, join_key: str) -> pd.DataFrame:
    return moq.att_all_years(_conn, join_key).data


@st.cache_data(ttl=300)
def _att_headline_row(_conn, join_key: str) -> pd.DataFrame:
    """One-row frame for the hero stat strip (most recent completed year, else the
    in-progress year). The year-pick rule lives in moq.att_headline_year."""
    return moq.att_headline_year(_conn, join_key, datetime.date.today().year).data


@st.cache_data(ttl=300)
def _att_chamber_sitting_days(_conn, house: str = "Dáil") -> dict[int, int]:
    """{year: distinct chamber plenary sitting days} for the house — the
    denominator for the hero plenary-attendance figure. Empty on miss (the
    stat then falls back to the bare plenary count with no rate)."""
    df = moq.att_chamber_sitting_days(_conn, house).data
    if df.empty:
        return {}
    return {int(y): int(s) for y, s in zip(df["year"], df["sitting_days"], strict=True)}


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
def _contact_details(_conn, join_key: str) -> dict:
    """Official office contact details (address / phone / email / website) from
    the member's oireachtas.ie profile. Empty dict when there's no row or the
    view is missing; nulls dropped so the hero only iterates populated fields."""
    df = moq.contact_details(_conn, join_key).data
    if df.empty:
        return {}
    row = df.iloc[0].to_dict()
    return {k: v for k, v in row.items() if isinstance(v, str) and v.strip()}


@st.cache_data(ttl=300)
def _news_mentions(_conn, join_key: str) -> pd.DataFrame:
    """Recent news mentions (per-member Google-News search), most-recent first. Empty when the
    member has no recent coverage or the view is missing."""
    return moq.news_mentions(_conn, join_key).data


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
def _salary(_conn, join_key: str, house: str) -> pd.DataFrame:
    """Statutory salary RATE row (basic + highest current office allowance)."""
    return moq.salary(_conn, join_key, house).data


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
