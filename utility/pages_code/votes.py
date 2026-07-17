import sys
from pathlib import Path

_UTIL = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_UTIL))

import pandas as pd
import streamlit as st

from html import escape as _h

from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    field_label,
    filter_bar,
    hide_sidebar,
    member_moved_callout,
    dt_page,
    todo_callout,
    year_selector,
)
from data_access.freshness_data import freshness_line
from ui.entity_links import api_json_link, division_url, member_profile_url
from ui.export_controls import export_button
from ui.source_pdfs import provenance_expander
from ui.vote_explorer import render_division_panel, vt_division_card_html
from data_access.votes_data import (
    fetch_division_members,
    fetch_hero_stats,
    fetch_member_names,
    fetch_party_breakdown,
    fetch_party_names,
    fetch_sources,
    fetch_td_name_by_id,
    fetch_td_row_by_name,
    fetch_topical_votes,
    fetch_vote_by_id,
    fetch_vote_index,
    fetch_vote_years,
)

_REQUIRED_INDEX_COLS: frozenset[str] = frozenset(
    {"vote_id", "vote_date", "debate_title", "vote_outcome", "yes_count", "no_count"}
)

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


# ── Data fetch lives in data_access.votes_data (imported above) ───────────────


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


def _render_td_picker(house: str = "Dáil") -> None:
    """Editorial 'Find a member' landing page with curated suggestion cards."""
    term = "Senator" if house == "Seanad" else "TD"
    st.html(
        '<p class="dt-kicker">Dáil Tracker · Voting Record</p>'
        f'<h1 class="dt-hero">Find a {_h(term)}</h1>'
        '<p class="td-pick-dek">'
        f"Search for a {_h(term)} above, or jump straight in: here are recent "
        "votes on housing, health and other crucial legislation."
        "</p>"
    )

    topical = fetch_topical_votes(tuple(pat for _label, pat in _TD_PICKER_TOPICS), house)
    picks = _pick_diverse_cards(topical, _TD_PICKER_CARD_COUNT)

    if not picks:
        empty_state(
            f"Pick a {term} above",
            f"Use the search box to find an individual {term} and view "
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
def _card_list_fragment(date_from, date_to, outcome_filter, house: str = "Dáil") -> None:
    """Fragment: year pills + card list. Reruns only this block when year changes."""
    years = fetch_vote_years(house)
    eff_from = date_from
    eff_to = date_to

    if years:
        year_strs = [str(y) for y in years]
        sel_year = year_selector(year_strs, key="v_year")
        if not eff_from:
            eff_from = f"{sel_year}-01-01"
            eff_to = f"{sel_year}-12-31"

    vote_df = fetch_vote_index(eff_from, eff_to, outcome_filter, house)

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
    st.html(f'<p class="vt-index-caption">{total:,}{outcome_word} division{"s" if total != 1 else ""}{suffix}</p>')

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
        filename=f"{house.lower()}_divisions.csv",
        key="exp_vote_idx",
    )


def _render_mode_a(date_from, date_to, outcome_filter, house: str = "Dáil") -> None:
    term = "Senator" if house == "Seanad" else "TD"
    st.html(f'<p class="dt-kicker">Dáil Tracker · Voting Record</p><h1 class="dt-hero">{_h(house)} Divisions</h1>')
    _card_list_fragment(date_from, date_to, outcome_filter, house)

    # P2-9: surface the provenance headline above the expander so the
    # data-source story isn't buried behind a closed disclosure. The
    # full expander still carries the long-form text + corpus counts.
    hero = fetch_hero_stats(house)
    sections: list[str] = [
        "Divisions data sourced from the Oireachtas Open Data API. Votes are as published in the official record."
    ]
    if not hero.empty:
        r = hero.iloc[0]
        dc = r.get("division_count")
        mc = r.get("member_count")
        if dc:
            sections.insert(0, f"{int(dc):,} total divisions on record · {int(mc or 0):,} {term}s recorded.")
    st.caption("Sourced from the Oireachtas Open Data API — as published in the official record.")
    provenance_expander(sections=sections, freshness=freshness_line("votes"))
    # P2-6: removed the "Source link quality" todo_callout. The external
    # Oireachtas link on every card was the only thing it warned about,
    # and that link is now demoted to a quiet footer chip (P1-4/P2-2),
    # so a static "some links may not work" banner under every visit
    # is no longer earning its place.


# ── Mode B (legacy) — redirect to canonical /member-overview profile ──────────


def _render_mode_b_redirect(member_id: str) -> None:
    """Mode B's in-page TD profile was lifted into the member-overview Votes
    expander in Phase 7. Renders the shared ``member_moved_callout`` so the
    moved notice is stylistically consistent with the other dimension
    pages (attendance / interests / payments / committees) — previously
    used a bespoke inline callout that read "Open profile →" generically.
    The shared helper looks up the canonical member code and renders
    "Open <Name>'s profile →" with a working href; it also calls
    ``st.stop()`` so the rest of the page body doesn't render under it."""
    name = fetch_td_name_by_id(member_id)
    member_moved_callout(
        name=name or member_id,
        section="votes",
        section_label="Per-TD voting",
        legacy_param="member",
        state_keys=("v_sel_member_id", "v_from"),
    )


# ── Mode C: Division evidence ──────────────────────────────────────────────────


def _render_mode_c(vote_id: str, v_from: str) -> None:
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

    vote_df = fetch_vote_by_id(vote_id)
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
    members_df = fetch_division_members(vote_id)
    sources_df = fetch_sources(vote_id)
    breakdown_df = fetch_party_breakdown(vote_id)

    render_division_panel(vote_row, members_df, sources_df, breakdown_df)

    provenance_expander(sections=["Division record sourced from the Oireachtas Open Data API."])

    from urllib.parse import quote

    _api = api_json_link(f"/v1/votes/{quote(str(vote_id), safe='')}", "This division as JSON")
    if _api:
        st.html(f'<div class="dt-api-footer">{_api}</div>')


# ── Page entry point ───────────────────────────────────────────────────────────


@dt_page
def votes_page() -> None:
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

    # Default view: Divisions (Mode A — divisions index). If a member is already
    # selected via URL or prior interaction, surface the Members view so the
    # member search is visible.
    if "v_view" not in st.session_state:
        st.session_state["v_view"] = "Members" if sel_member_id else "Divisions"
    # Migrate legacy view labels. The toggle was "Dáil"/"TDs" (and earlier
    # "Divisions") before the chamber toggle was added; "Dáil" now names the
    # CHAMBER, so the mode toggle is "Divisions"/"Members".
    _legacy_view = {"Dáil": "Divisions", "TDs": "Members"}
    if st.session_state["v_view"] in _legacy_view:
        st.session_state["v_view"] = _legacy_view[st.session_state["v_view"]]

    # ── Controls (was the sidebar) ──────────────────────────────────────────────
    # Sidebar→filter-bar migration: the View switch + filters move into a
    # main-panel bar. The View toggle drives mode routing, so it sits above the
    # per-mode hero; identity is carried by the top-nav tab + each mode's hero.
    hide_sidebar()

    # Chamber scope — Dáil (default) or Seanad. Divisions, member search and the
    # hero corpus counts all scope to the picked house (v_vote_base tags every
    # row with `house`). Switching chamber clears any selected member, whose
    # member_id belongs to a single house.
    house = (
        st.segmented_control(
            "Chamber",
            options=["Dáil", "Seanad"],
            default="Dáil",
            key="v_house",
            label_visibility="collapsed",
        )
        or "Dáil"
    )
    if st.session_state.get("_v_house_applied", "Dáil") != house:
        st.session_state["_v_house_applied"] = house
        if sel_member_id:
            st.session_state["_v_clear_member"] = True
            st.query_params.pop("member", None)
            st.rerun()
    is_seanad = house == "Seanad"
    term = "Senator" if is_seanad else "TD"

    prev_view = st.session_state["v_view"]
    outcome_filter = None
    sel_party = ""
    sel_name = None
    # date_from/date_to feed Mode A (divisions index); they stay None now — the
    # old Members-view date range fed the lifted-out in-page profile (Phase 7).
    date_from = date_to = None

    if prev_view == "Divisions":
        party_names = fetch_party_names(house)
        with filter_bar([2, 2, 3, 5] if party_names else [2, 2, 8]) as cols:
            with cols[0]:
                field_label("View")
                view_sel = st.segmented_control(
                    "View",
                    options=["Divisions", "Members"],
                    default=prev_view,
                    key="v_view_widget",
                    label_visibility="collapsed",
                )
            with cols[1]:
                field_label("Outcome")
                outcome_sel = st.selectbox(
                    "Outcome",
                    ["All", "Carried", "Lost"],
                    key="v_outcome",
                    label_visibility="collapsed",
                )
            outcome_filter = None if outcome_sel == "All" else outcome_sel
            if party_names:
                with cols[2]:
                    field_label("Party")
                    party_sel = st.selectbox(
                        "Party",
                        ["All parties"] + party_names,
                        key="v_party",
                        label_visibility="collapsed",
                    )
                sel_party = "" if party_sel == "All parties" else party_sel
    else:  # Members view — search a single member
        member_names = fetch_member_names(sel_party, house)
        with filter_bar([2, 4, 6]) as cols:
            with cols[0]:
                field_label("View")
                view_sel = st.segmented_control(
                    "View",
                    options=["Divisions", "Members"],
                    default=prev_view,
                    key="v_view_widget",
                    label_visibility="collapsed",
                )
            with cols[1]:
                field_label(f"Find a {term}")
                _opts = ["Search a member…"] + list(member_names)
                _chosen = st.selectbox(
                    f"Find a {term}",
                    _opts,
                    index=0,
                    key="v_member_select",
                    label_visibility="collapsed",
                )
                sel_name = _chosen if _chosen and _chosen != "Search a member…" else None

    view = view_sel or prev_view
    if view != prev_view:
        st.session_state["v_view"] = view
        # Switching to Divisions clears any selected member so the index reappears.
        if view == "Divisions" and sel_member_id:
            st.session_state["_v_clear_member"] = True
            st.query_params.pop("member", None)
        st.rerun()

    # ── Member-selection handler (mirrors the old sidebar logic) ────────────────
    if view == "Members":
        # Track the dropdown's last-applied name so the auto-clear branch only
        # fires when the user explicitly clears the dropdown — not when the
        # picker (or a URL param) sets sel_member_id directly.
        last_applied = st.session_state.get("_v_last_sel_name", "")
        if sel_name:
            if sel_name != last_applied:
                td_lkp = fetch_td_row_by_name(sel_name, house)
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
        _render_mode_c(sel_vote_id, v_from)
    elif view == "Members":
        if sel_member_id:
            # Phase 7: Mode B's in-page profile was lifted into
            # member-overview's Votes expander. Redirect bookmarks / picker
            # selections to the canonical profile.
            _render_mode_b_redirect(sel_member_id)
        else:
            _render_td_picker(house)
    else:
        _render_mode_a(date_from, date_to, outcome_filter, house)
