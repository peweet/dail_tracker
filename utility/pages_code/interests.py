"""
Register of Members' Interests — Dáil Tracker.

Data source: silver CSV via in-memory DuckDB (registered view simulation).
All retrieval follows SELECT / WHERE / ORDER BY / LIMIT only — no aggregation in Streamlit.

Genuinely-open pipeline gaps (verified against sql_views/ on 2026-06-04):

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_yearly_summary
    Year + interest_category + declarations_count.
    Needed for the year-responsive category breakdown chart. (Not yet built.)

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_sources
    Per-declaration source-document view: source_page_number, oireachtas_url
    (v_member_interests_detail already carries source_pdf_url, but the
    per-declaration sources view itself is not built).

TODO_PIPELINE_VIEW_REQUIRED: unique_member_code — stable join key on v_member_interests_*
    views. Required for cross-page member-name links via
    utility/ui/entity_links.member_link_html. Until then the page links out via the
    exact-match data_access.identity_resolver.resolve_member_code workaround.

SHIPPED — tokens cleared 2026-06-04 (verified present in sql_views/ AND consumed by
interests_data.py): v_member_interests_index, v_member_interests_detail,
directorship_flag, shareholding_flag, source_pdf_url.
"""

from __future__ import annotations

from html import escape as _h
import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.components import (
    clean_meta,
    clickable_card_link,
    empty_state,
    evidence_heading,
    field_label,
    hero_banner,
    hide_sidebar,
    main_member_jump,
    member_moved_callout,
    page_error_boundary,
    pagination_controls,
    pill,
    ranked_member_card,
    render_notable_chips,
    year_selector,
)
from data_access.identity_resolver import resolve_member_code
from ui.entity_links import member_profile_url
from ui.source_pdfs import interests_pdf_links, provenance_expander

from config import (
    NOTABLE_SENATORS,
    NOTABLE_TDS,
)
from data_access.interests_data import (
    fetch_interests_availability,
    fetch_interests_filter_options,
    fetch_member_index,
)

# ── Data access ───────────────────────────────────────────────────────────────
# All retrieval lives in data_access.interests_data and the two registered
# views v_member_interests_detail + v_member_interests_index (sql_views/
# member_interests_detail.sql, sql_views/member_zz_interests_index.sql).
# This module is rendering only.


def _int_member_card_html(row) -> str:
    rank = int(row["rank"])
    name = str(row["member_name"])
    party = str(row.get("party_name") or "")
    constit = str(row.get("constituency") or "")
    total = int(row.get("total_declarations") or 0)
    d_count = int(row.get("directorship_count") or 0)
    p_count = int(row.get("property_count") or 0)
    s_count = int(row.get("share_count") or 0)
    landlord = bool(row.get("is_landlord", False))
    is_prop = bool(row.get("is_property_owner", False))

    # Emoji icons intentionally dropped per project convention — text +
    # colour-by-pill-class carries enough signal in this editorial register.
    parts = [pill(f"{total} declarations", "decl")]
    if landlord:
        parts.append(pill("Landlord", "accent"))
    elif is_prop:
        parts.append(pill("Property owner", "owner"))
    if p_count:
        parts.append(pill(f"{p_count} propert{'ies' if p_count != 1 else 'y'}", "prop"))
    if s_count:
        parts.append(pill(f"Shareholder · {s_count}", "shares"))
    if d_count:
        parts.append(pill(f"{d_count} compan{'ies' if d_count != 1 else 'y'}", "company"))

    return ranked_member_card(
        name=name,
        meta=clean_meta(party, constit),
        rank=rank,
        pills_html="".join(parts),
    )


def _render_leaderboard(ranking_df: pd.DataFrame) -> None:
    """Render ranked member cards (all members, paginated). Cards are full-card links."""
    if ranking_df.empty:
        empty_state(
            "No members found",
            "Adjust the name filter or choose a different year.",
        )
        return

    total = len(ranking_df)
    page_size, page_idx = pagination_controls(
        total=total,
        key_prefix="int_ranking",
        page_sizes=(10,),
        default_page_size=10,
        label="members",
        show_caption=False,
    )
    visible = ranking_df.iloc[page_idx * page_size : (page_idx + 1) * page_size]

    cards: list[str] = []
    for _, row in visible.iterrows():
        name = str(row["member_name"])
        code = resolve_member_code(name)
        if code:
            cards.append(
                clickable_card_link(
                    href=member_profile_url(code, section="interests"),
                    inner_html=_int_member_card_html(row),
                    aria_label=f"View {name}'s declarations",
                )
            )
        else:
            # Member not in v_member_registry — render unwrapped (no link).
            cards.append(_int_member_card_html(row))
    st.html("\n".join(cards))


# ── Pure helpers ───────────────────────────────────────────────────────────────


# ── Provenance footer ──────────────────────────────────────────────────────────


def _render_provenance(house: str) -> None:
    provenance_expander(
        sections=[
            "Declarations are extracted from published Oireachtas PDF documents. "
            "Flags (landlord, property) are pipeline navigation aids, not legal conclusions. "
            "Office holders (Ministers, Ceann Comhairle) may be exempt from filing — "
            "records can be incomplete. "
            "A high declaration count reflects transparency, not wrongdoing."
        ],
        source_caption="Data: Oireachtas Register of Members' Interests (data.oireachtas.ie)",
        pdf_links=interests_pdf_links(house),
    )


# ── Page entry point ───────────────────────────────────────────────────────────


@page_error_boundary
def interests_page() -> None:
    inject_css()

    # Sidebar→filter-bar migration: identity via top-nav + hero; the Chamber
    # scope + member search + notable chips move into the main panel under the
    # hero. Chamber stays near the top because every data fetch depends on it.
    hide_sidebar()

    # ── Page header ───────────────────────────────────────────────────────────
    hero_banner(
        kicker="REGISTER OF MEMBERS' INTERESTS",
        title="What has your TD declared?",
    )

    # ── Chamber scope (was the sidebar) ────────────────────────────────────────
    field_label("Chamber")
    house: str = (
        st.segmented_control(
            "Chamber",
            ["Dáil", "Seanad"],
            default="Dáil",
            key="interests_house",
            label_visibility="collapsed",
        )
        or "Dáil"
    )
    # Clear year pill and member selection on chamber switch
    if st.session_state.get("_interests_last_house") != house:
        for k in ("int_profile_year", "selected_td", "int_member_sel", "int_member_q"):
            st.session_state.pop(k, None)
        st.session_state["_interests_last_house"] = house

    # ── Guard ─────────────────────────────────────────────────────────────────
    # The column contract is now enforced by v_member_interests_detail itself
    # (registration fails if a silver column is missing). The only remaining
    # guard the page needs is "does the view have any rows for this house".
    if not fetch_interests_availability(house):
        empty_state(
            "Register data not available",
            f"Source data for {house} not found. Run the pipeline to populate "
            "data/silver/ and re-register v_member_interests_detail.",
        )
        return

    # Legacy ?member=<name> URLs (from before Phase 3) redirect to the
    # canonical /member-overview?member=<code>#interests profile. The
    # shared helper resolves the actual unique_member_code, scrubs the
    # legacy param, and calls st.stop() so the page body doesn't render
    # below the callout (round-3 audit P0-3).
    qp_member = st.query_params.get("member")
    if qp_member:
        member_moved_callout(
            qp_member,
            section="interests",
            section_label="The Interests section",
            legacy_param="member",
        )

    opts = fetch_interests_filter_options(house)

    # ── Member jump (search + notable chips) ────────────────────────────────────
    # Navigate straight to the canonical /member-overview profile (Phase 3
    # lifted the per-TD profile there). st.markdown not st.html — st.html
    # iframes the meta-refresh so it would redirect the iframe, not the parent
    # page (see [[feedback-streamlit-css-and-state]]).
    chosen = main_member_jump(
        opts["members"],
        key_prefix="int",
        label="Find a TD",
        placeholder="Type a name…",
    )
    if chosen:
        code = resolve_member_code(chosen)
        if code:
            target = member_profile_url(code, section="interests")
            st.markdown(
                f'<meta http-equiv="refresh" content="0;url={_h(target)}">',
                unsafe_allow_html=True,
            )
            st.stop()

    notable = NOTABLE_TDS if house == "Dáil" else NOTABLE_SENATORS
    if notable and render_notable_chips(notable, opts["members"], "chip_int", "selected_td", cols=6):
        picked = st.session_state.pop("selected_td", None)
        if picked:
            code = resolve_member_code(picked)
            if code:
                target = member_profile_url(code, section="interests")
                st.markdown(
                    f'<meta http-equiv="refresh" content="0;url={_h(target)}">',
                    unsafe_allow_html=True,
                )
                st.stop()
        st.rerun()

    # P0-1 audit fix: the typeahead + Notable Members chips both write
    # `selected_td` into session state, but Phase 3 lifted the per-TD
    # profile to /member-overview without rewiring the readers — so the
    # primary CTA used to be dead. Mirror the legacy `?member=` redirect
    # contract: when a TD has been selected via either control, render the
    # shared "Member profiles have moved" callout (with a working profile
    # link) and stop. The cards on this page already navigate via
    # `clickable_card_link(href=member_profile_url(code, section="interests"))`
    # so this branch only fires for typeahead / chip selections.
    selected_td = st.session_state.get("selected_td")
    if selected_td:
        member_moved_callout(
            selected_td,
            section="interests",
            section_label="The Interests section",
            state_keys=("selected_td",),
        )

    # Year pills — main content, newest first
    year_opts = [str(y) for y in opts["years"]]
    if not year_opts:
        empty_state("No year data found", "v_member_interests_detail returned no years.")
        _render_provenance(house)
        return

    selected_year = year_selector(year_opts, key="int_year")

    # ── Leaderboard — one card per member, ranked by declarations ─────────────
    # Data: v_member_interests_index (sql_views/member_zz_interests_index.sql).
    # The same rank, counts and flag rollup that used to live in the page's
    # _fetch_member_index_fallback now lives in that view.
    members_df = fetch_member_index(house, selected_year)
    if members_df.empty:
        empty_state(
            f"No declarations for {selected_year}",
            "No interest declarations on record for this year and chamber.",
        )
        _render_provenance(house)
        return

    # Audit fix (2026-05-26, P1-5): the previous heading
    # "Members · 2025 · 174" was terse and didn't say WHY this list
    # was showing or that switching the year pill above changed it.
    # Rephrase to verbalise the year context so a year-pill switch
    # has visible textual feedback.
    evidence_heading(f"Declarations for {selected_year} · {len(members_df)} members")
    # P2-1: demoted "Coming soon" callout to a single caption so the
    # data isn't preceded by a heavy notice; P2-3: pill colour legend
    # — declarations (blue), landlord (orange), property (green),
    # shareholder (purple) — so the encoding is documented inline.
    st.caption(
        "Ranked leaderboard coming when the pipeline view lands — "
        "showing all members in name order for now.   "
        "Pill colours: "
        "**declarations** (blue) · "
        "**landlord** (orange) · "
        "**property owner** (green) · "
        "**shareholder** (purple)."
    )

    # Pagination state (read-only here; nav widgets render below the cards).
    page_size = 12
    page_key = "int_fb_page"
    cur = int(st.session_state.get(page_key, 1))
    total_pages = max(1, (len(members_df) + page_size - 1) // page_size)
    if cur > total_pages:
        cur = 1
        st.session_state[page_key] = 1
    page_idx = cur - 1
    visible = members_df.iloc[page_idx * page_size : (page_idx + 1) * page_size]

    cards: list[str] = []
    for _, row in visible.iterrows():
        name = str(row["member_name"])
        code = resolve_member_code(name)
        if code:
            cards.append(
                clickable_card_link(
                    href=member_profile_url(code, section="interests"),
                    inner_html=_int_member_card_html(row),
                    aria_label=f"View {name}'s declarations",
                )
            )
        else:
            cards.append(_int_member_card_html(row))
    st.html("\n".join(cards))

    pagination_controls(
        total=len(members_df),
        key_prefix="int_fb",
        page_sizes=(page_size,),
        default_page_size=page_size,
        label="members",
        show_caption=False,
    )

    _render_provenance(house)
    return
