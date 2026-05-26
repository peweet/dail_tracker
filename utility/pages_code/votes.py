import logging
import sys
from pathlib import Path

_UTIL = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_UTIL))

import pandas as pd
import streamlit as st

_log = logging.getLogger(__name__)

from html import escape as _h

from shared_css import inject_css
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    member_moved_callout,
    page_error_boundary,
    sidebar_date_range,
    sidebar_member_filter,
    sidebar_page_header,
    todo_callout,
    year_selector,
)
from ui.entity_links import division_url, member_profile_url
from ui.export_controls import export_button
from ui.source_pdfs import provenance_expander
from ui.vote_explorer import render_division_panel, vt_division_card_html
from data_access.votes_data import get_votes_conn

_REQUIRED_INDEX_COLS: frozenset[str] = frozenset(
    {"vote_id", "vote_date", "debate_title", "vote_outcome", "yes_count", "no_count"}
)

_VOTE_INDEX_LIMIT = 500
_DIVISION_MEMBERS_LIMIT = 5000

# Topical seed terms used for the "Find a TD" landing page. Each one matches a
# debate_title substring; presentation-only filter, not modelling.
_TD_PICKER_TOPICS: tuple[tuple[str, str], ...] = (
    ("Housing", "%housing%"),
    ("Health", "%health%"),
    ("Disability", "%disab%"),
    ("Climate", "%climate%"),
    ("Energy", "%energy%"),
    ("Palestine", "%palestin%"),
    ("Neutrality", "%neutral%"),
    ("Education", "%education%"),
    ("Childcare", "%child%"),
)
_TD_PICKER_CARD_COUNT = 4


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
        "SELECT division_count, member_count, first_vote_date, last_vote_date FROM v_vote_result_summary LIMIT 1",
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
def _fetch_td_row_by_name(_conn, member_name: str) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT member_id, member_name, party_name, constituency,"
        " yes_count, no_count, abstained_count, division_count, yes_rate_pct"
        " FROM td_vote_summary WHERE member_name = ? LIMIT 1",
        (member_name,),
    )


@st.cache_data(ttl=300)
def _fetch_td_name_by_id(_conn, member_id: str) -> str:
    """Look up TD display name from member_id for the legacy redirect.

    Used by `_render_mode_b_redirect` so the moved-callout link can show
    the member's actual name rather than the bare ID slug.
    """
    df = _safe_query(
        _conn,
        "SELECT member_name FROM td_vote_summary WHERE member_id = ? LIMIT 1",
        (member_id,),
    )
    if df.empty or "member_name" not in df.columns:
        return ""
    val = df.iloc[0]["member_name"]
    return str(val) if val is not None else ""


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
        " yes_count, no_count, abstained_count, margin, oireachtas_url"
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
        "SELECT party_name, vote_type, member_count, vote_pct FROM party_vote_breakdown WHERE vote_id = ? LIMIT 500",
        (vote_id,),
    )


@st.cache_data(ttl=300)
def _fetch_division_members(_conn, vote_id) -> pd.DataFrame:
    return _safe_query(
        _conn,
        "SELECT member_id, member_name, party_name, constituency, vote_type"
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
def _fetch_topical_votes(_conn) -> pd.DataFrame:
    """Recent member votes on hot-topic debates. Used to seed the TD picker cards.

    Retrieval-only: SELECT with WHERE/ORDER BY/LIMIT against the approved view.
    """
    likes = " OR ".join(["debate_title ILIKE ?" for _ in _TD_PICKER_TOPICS])
    sql = (
        "SELECT vote_date, member_id, member_name, party_name, constituency,"
        " vote_type, debate_title, vote_outcome"
        " FROM v_vote_member_detail"
        " WHERE vote_type IN ('Voted Yes', 'Voted No')"
        " AND member_name IS NOT NULL"
        f" AND ({likes})"
        " ORDER BY vote_date DESC LIMIT 2000"
    )
    params = [pat for _label, pat in _TD_PICKER_TOPICS]
    return _safe_query(_conn, sql, params)



# ── TD picker (landing for the TDs view) ───────────────────────────────────────


def _pick_diverse_cards(df: pd.DataFrame, n: int) -> list[dict]:
    """Pick up to ``n`` rows with distinct members, balanced Yes/No.

    Presentation-layer selection only — no aggregation, no joins. Walks the
    already-fetched, already-sorted result and trims it for display.

    Pass 1: distinct member + distinct debate title, balance Yes/No.
    Pass 2: relax title-uniqueness to fill remaining slots when the recent
    set is dominated by a single debate.
    """
    if df.empty:
        return []
    rows = df.to_dict("records")

    def _candidates(distinct_titles: bool) -> list[dict]:
        seen_m: set[str] = set()
        seen_t: set[str] = set()
        picks: list[dict] = []
        for r in rows:
            m = str(r.get("member_name") or "").strip()
            t = str(r.get("debate_title") or "").strip()
            if not m or not t or m in seen_m:
                continue
            if distinct_titles and t in seen_t:
                continue
            seen_m.add(m)
            seen_t.add(t)
            picks.append(r)
        return picks

    candidates = _candidates(distinct_titles=True)
    yes_pool = [r for r in candidates if r.get("vote_type") == "Voted Yes"]
    no_pool = [r for r in candidates if r.get("vote_type") == "Voted No"]

    half = n // 2
    out: list[dict] = no_pool[:half] + yes_pool[:half]

    if len(out) < n:
        leftover = no_pool[half:] + yes_pool[half:]
        out += leftover[: n - len(out)]

    if len(out) < n:
        seen_keys = {(r.get("member_name"), r.get("debate_title")) for r in out}
        for r in _candidates(distinct_titles=False):
            key = (r.get("member_name"), r.get("debate_title"))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            out.append(r)
            if len(out) >= n:
                break

    return out[:n]


def _td_pick_card_html(row: dict) -> str:
    name = str(row.get("member_name") or "")
    party = str(row.get("party_name") or "")
    const = str(row.get("constituency") or "")
    vote_type = str(row.get("vote_type") or "")
    title = str(row.get("debate_title") or "")
    # P2-8: lift the upstream "[Private Members]" tag out of the title so
    # the prose statement doesn't carry trailing jargon. The flag becomes
    # a small pill rendered alongside the meta line below.
    _pm_tag = "[Private Members]"
    is_private = title.rstrip().endswith(_pm_tag)
    if is_private:
        title = title[: title.rfind(_pm_tag)].rstrip().rstrip(":")
    vote_date = row.get("vote_date")
    date_str = ""
    if vote_date is not None:
        try:
            date_str = vote_date.strftime("%d %b %Y")
        except AttributeError:
            date_str = str(vote_date)[:10]

    # P1-5: the previous "✓ Voted Yes" / "✗ Voted No" badges used red/green
    # signal colours that read as the bill's *outcome* rather than the TD's
    # vote. Reframe: one quiet pill carrying the TD's vote, prose-inlined
    # into a single readable sentence ("<Name> voted YES on <bill>"). The
    # bill's outcome belongs on Mode C, not on the TD picker.
    if vote_type == "Voted Yes":
        vote_text = "voted YES"
        vote_cls = "td-pick-vote td-pick-vote-yes"
    elif vote_type == "Voted No":
        vote_text = "voted NO"
        vote_cls = "td-pick-vote td-pick-vote-no"
    else:
        vote_text = "abstained"
        vote_cls = "td-pick-vote td-pick-vote-abs"

    meta_parts = [p for p in (party, const, date_str) if p]
    meta_html = " · ".join(_h(p) for p in meta_parts)
    private_pill = (
        '<span class="vt-card-private" title="Private Members’ motion or bill '
        '— tabled by a TD/Senator who is not a government minister">Private Members</span>'
        if is_private
        else ""
    )

    # Single-sentence framing — no orphan "on" word, no separate badge
    # competing for attention with the title.
    statement_html = (
        f'<span class="td-pick-name">{_h(name)}</span> '
        f'<span class="{vote_cls}">{vote_text}</span> on '
        f'<span class="td-pick-title">{_h(title)}</span>'
    )

    return (
        f'<div class="td-pick-card">'
        f'<p class="td-pick-statement">{statement_html}</p>'
        f'<div class="td-pick-meta">{meta_html}{(" " + private_pill) if private_pill else ""}</div>'
        f"</div>"
    )


def _render_td_picker(conn) -> None:
    """Editorial 'Find a TD' landing page with curated suggestion cards."""
    st.html(
        '<p class="dt-kicker">Dáil Tracker · Voting Record</p>'
        '<h1 class="dt-hero">Find a TD</h1>'
        '<p class="td-pick-dek">'
        "Search for a TD on the left, or jump straight in: here are recent "
        "votes on housing, health and other crucial legislation."
        "</p>"
    )

    topical = _fetch_topical_votes(conn)
    picks = _pick_diverse_cards(topical, _TD_PICKER_CARD_COUNT)

    if not picks:
        empty_state(
            "Pick a TD from the sidebar",
            "Use the search box on the left to find an individual TD and view "
            "their full voting record across every published division.",
        )
        return

    # Two-column grid of suggestion cards. Each card is itself the click
    # target (stretched-link via clickable_card_link) — the previous
    # round had a redundant ALL-CAPS "VIEW <NAME>'S RECORD →" button below
    # every card competing with a small "Profile ↗" pill inside the card.
    # One affordance per card is the rule.
    for row_pair_start in range(0, len(picks), 2):
        pair = picks[row_pair_start : row_pair_start + 2]
        cols = st.columns(2, gap="small")
        for j, pick in enumerate(pair):
            with cols[j]:
                target = member_profile_url(str(pick["member_id"]), section="votes")
                st.html(
                    clickable_card_link(
                        href=target,
                        inner_html=_td_pick_card_html(pick),
                        aria_label=f"View {pick['member_name']}'s voting record",
                    )
                )

    st.html(
        '<p class="td-pick-foot">Showing recent topical votes — selection updates as new divisions are published.</p>'
    )


# ── Mode A: Divisions index ────────────────────────────────────────────────────


@st.fragment
def _card_list_fragment(conn, date_from, date_to, outcome_filter) -> None:
    """Fragment: year pills + card list. Reruns only this block when year changes."""
    years = _fetch_vote_years(conn)
    eff_from = date_from
    eff_to = date_to

    if years:
        year_strs = [str(y) for y in years]
        sel_year = year_selector(year_strs, key="v_year")
        if not eff_from:
            eff_from = f"{sel_year}-01-01"
            eff_to = f"{sel_year}-12-31"

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

    total = len(vote_df)
    show_all = st.session_state.get("v_show_all", False)
    visible = vote_df if show_all else vote_df.head(25)
    # Caption echoes the active filter scope so users keep their bearings
    # while scrolling. The outcome filter lives in the sidebar; without
    # this users would scroll through results with no breadcrumb back to
    # "you are filtered". Year is implicit in the pills above.
    outcome_word = f" {outcome_filter.lower()}" if outcome_filter else ""
    suffix = " · showing first 25" if not show_all and total > 25 else ""
    st.html(
        f'<p class="vt-index-caption">'
        f'{total:,}{outcome_word} division{"s" if total != 1 else ""}{suffix}'
        f'</p>'
    )

    cards_html: list[str] = []
    for _, row in visible.iterrows():
        vote_id = str(row.get("vote_id") or "")
        title = str(row.get("debate_title") or "View division")
        cards_html.append(
            clickable_card_link(
                href=division_url(vote_id),
                inner_html=vt_division_card_html(row),
                aria_label=f"View division: {title}",
            )
        )
    st.html("\n".join(cards_html))

    if not show_all and total > 25 and st.button(f"Show all {total:,} divisions", key="v_show_all_btn"):
        st.session_state["v_show_all"] = True
        st.rerun()

    display_cols = [
        c
        for c in [
            "vote_date",
            "vote_id",
            "debate_title",
            "vote_outcome",
            "yes_count",
            "no_count",
            "abstained_count",
            "margin",
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
    st.html('<p class="dt-kicker">Dáil Tracker · Voting Record</p><h1 class="dt-hero">Dáil Divisions</h1>')
    _card_list_fragment(conn, date_from, date_to, outcome_filter)

    # P2-9: surface the provenance headline above the expander so the
    # data-source story isn't buried behind a closed disclosure. The
    # full expander still carries the long-form text + corpus counts.
    hero = _fetch_hero_stats(conn)
    sections: list[str] = [
        "Divisions data sourced from the Oireachtas Open Data API. Votes are as published in the official record."
    ]
    if not hero.empty:
        r = hero.iloc[0]
        dc = r.get("division_count")
        mc = r.get("member_count")
        if dc:
            sections.insert(0, f"{int(dc):,} total divisions on record · {int(mc or 0):,} TDs recorded.")
    st.caption(
        "Sourced from the Oireachtas Open Data API — as published in the official record."
    )
    provenance_expander(sections=sections)
    # P2-6: removed the "Source link quality" todo_callout. The external
    # Oireachtas link on every card was the only thing it warned about,
    # and that link is now demoted to a quiet footer chip (P1-4/P2-2),
    # so a static "some links may not work" banner under every visit
    # is no longer earning its place.


# ── Mode B (legacy) — redirect to canonical /member-overview profile ──────────


def _render_mode_b_redirect(conn, member_id: str) -> None:
    """Mode B's in-page TD profile was lifted into the member-overview Votes
    expander in Phase 7. Renders the shared ``member_moved_callout`` so the
    moved notice is stylistically consistent with the other dimension
    pages (attendance / interests / payments / committees) — previously
    used a bespoke inline callout that read "Open profile →" generically.
    The shared helper looks up the canonical member code and renders
    "Open <Name>'s profile →" with a working href; it also calls
    ``st.stop()`` so the rest of the page body doesn't render under it."""
    name = _fetch_td_name_by_id(conn, member_id)
    member_moved_callout(
        name=name or member_id,
        section="votes",
        section_label="Per-TD voting",
        legacy_param="member",
        state_keys=("v_sel_member_id", "v_from"),
    )


# ── Mode C: Division evidence ──────────────────────────────────────────────────


def _render_mode_c(conn, vote_id: str, v_from: str) -> None:
    back_label = "← Back to TD record" if v_from == "td" else "← Back to divisions"
    if back_button(back_label, key="v_c"):
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
        # P2-7: drop the "ID:" dev-concept prefix. Don't echo the literal
        # user-supplied vote_id when it's clearly malformed — say so plainly.
        empty_state(
            "Division not found",
            "No division matches that bookmark or link. The vote may have "
            "been re-published with a different identifier, or the URL is "
            "malformed. Try the divisions index.",
        )
        return

    vote_row = vote_df.iloc[0]
    members_df = _fetch_division_members(conn, vote_id)
    sources_df = _fetch_sources(conn, vote_id)
    breakdown_df = _fetch_party_breakdown(conn, vote_id)

    render_division_panel(vote_row, members_df, sources_df, breakdown_df)

    provenance_expander(sections=["Division record sourced from the Oireachtas Open Data API."])


# ── Page entry point ───────────────────────────────────────────────────────────


@page_error_boundary
def votes_page() -> None:
    inject_css()
    conn = get_votes_conn()

    # Resolve pending back-navigation flags before any widgets are instantiated
    if st.session_state.get("_v_clear_member"):
        st.session_state.pop("_v_clear_member", None)
        st.session_state.pop("v_sel_member_id", None)
        st.session_state.pop("_v_last_sel_name", None)
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

    sel_vote_id = st.session_state.get("v_sel_vote_id")
    sel_member_id = st.session_state.get("v_sel_member_id")
    v_from = st.session_state.get("v_from", "index")

    # Default view: Dáil (Mode A — divisions index). If a TD is already
    # selected via URL or prior interaction, surface the TDs view so the
    # sidebar member search is visible.
    if "v_view" not in st.session_state:
        st.session_state["v_view"] = "TDs" if sel_member_id else "Dáil"
    # Migrate legacy session state from the previous "Divisions" label so users
    # who toggled before this rename don't get stuck on a value that no longer
    # matches an option.
    if st.session_state["v_view"] == "Divisions":
        st.session_state["v_view"] = "Dáil"

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        sidebar_page_header("Dáil<br>Divisions")

        hero_df = _fetch_hero_stats(conn)
        if not hero_df.empty:
            r = hero_df.iloc[0]
            fd = r.get("first_vote_date")
            ld = r.get("last_vote_date")
            if fd and ld:
                st.caption(f"Data covers {str(fd)[:7]} to {str(ld)[:7]}")

        st.divider()

        # ── View toggle ────────────────────────────────────────────────────
        st.html('<p class="sidebar-label">View</p>')
        prev_view = st.session_state["v_view"]
        view_sel = st.segmented_control(
            "View",
            options=["Dáil", "TDs"],
            default=prev_view,
            key="v_view_widget",
            label_visibility="collapsed",
        )
        view = view_sel or prev_view
        if view != prev_view:
            st.session_state["v_view"] = view
            # Switching to Dáil clears any selected TD so the index reappears.
            if view == "Dáil" and sel_member_id:
                st.session_state["_v_clear_member"] = True
                st.query_params.pop("member", None)
            st.rerun()

        st.divider()

        outcome_filter = None
        sel_party = ""
        sel_name = None
        date_from = date_to = None

        if view == "Dáil":
            # Outcome + party filters live with the divisions index.
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
        else:  # TDs view — search a single TD
            if sel_member_id:
                date_from, date_to = sidebar_date_range("Date range", key="v_date_range")

            member_names = _fetch_member_names(conn, sel_party)
            sel_name = sidebar_member_filter(
                "Find a TD",
                member_names,
                key_search="v_member_search",
                key_select="v_member_select",
            )

            # Track the dropdown's last-applied name so the auto-clear branch
            # only fires when the user explicitly clears the dropdown — not
            # when the picker (or a URL param) sets sel_member_id directly.
            last_applied = st.session_state.get("_v_last_sel_name", "")
            if sel_name:
                if sel_name != last_applied:
                    td_lkp = _fetch_td_row_by_name(conn, sel_name)
                    if not td_lkp.empty:
                        new_mid = str(td_lkp.iloc[0]["member_id"])
                        st.session_state["v_sel_member_id"] = new_mid
                        st.session_state["_v_last_sel_name"] = sel_name
                        st.session_state.pop("v_sel_vote_id", None)
                        st.query_params["member"] = new_mid
                        st.query_params.pop("vote", None)
                        st.rerun()
            elif last_applied and sel_member_id:
                st.session_state["_v_clear_member"] = True
                st.query_params.clear()
                st.rerun()

    # ── Mode routing ──────────────────────────────────────────────────────────
    if sel_vote_id:
        _render_mode_c(conn, sel_vote_id, v_from)
    elif view == "TDs":
        if sel_member_id:
            # Phase 7: Mode B's in-page TD profile was lifted into
            # member-overview's Votes expander. Redirect bookmarks / sidebar
            # selections to the canonical profile.
            _render_mode_b_redirect(conn, sel_member_id)
        else:
            _render_td_picker(conn)
    else:
        _render_mode_a(conn, date_from, date_to, outcome_filter)
