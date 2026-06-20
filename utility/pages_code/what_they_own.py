"""
What They Own — Dáil Tracker.

The citizen-first front door to the Register of Members' Interests. Where the
/interests page is a year-by-year league table, this page leads with the
question most people actually arrive with — *what do the people who govern us
own?* — and answers it across the **whole record**: every declaration year on
file, sitting and former TDs/senators alike (the historic backfill, 2011–2025).

It is deliberately the left-most page in the nav: plain language, ownership
first (property, shares, companies), one card per member, ranked by lifetime
declarations. Selecting a member jumps to their canonical /member-overview
profile (the Interests section).

Logic boundary: this module is rendering only. It reuses the existing,
firewall-clean data-access fetcher ``fetch_member_index_alltime`` (over the
registered view ``v_member_interests_index_alltime`` —
sql_views/member/member_zz_interests_index_alltime.sql). The ownership category
control is a display-only filter over boolean/count columns already present on
the returned rows (SELECT/WHERE-equivalent, no aggregation in Streamlit).
"""

from __future__ import annotations

from html import escape as _h

import pandas as pd
import streamlit as st

from config import NOTABLE_SENATORS, NOTABLE_TDS
from data_access.identity_resolver import resolve_member_code
from data_access.interests_data import (
    fetch_interests_availability,
    fetch_interests_filter_options,
    fetch_member_index,
    fetch_member_index_alltime,
)
from shared_css import inject_css
from ui.components import (
    clean_meta,
    clickable_card_link,
    empty_state,
    evidence_heading,
    hero_banner,
    hide_sidebar,
    main_member_jump,
    member_moved_callout,
    page_error_boundary,
    pagination_controls,
    pill,
    ranked_member_card,
    render_notable_chips,
)
from ui.entity_links import member_profile_url
from ui.source_pdfs import interests_pdf_links, provenance_expander

# ── Ownership category filters ─────────────────────────────────────────────────
# Each entry is a display-only predicate over columns that the all-time index
# view already returns. No aggregation happens here — these select which of the
# already-ranked rows are shown, the same shape as the /interests landlord_only
# filter. "All" carries no predicate (renders the full ranked leaderboard).
_CATEGORIES: dict[str, object] = {
    "Everyone": None,
    "Landlords": lambda df: df["is_landlord"].fillna(False).astype(bool),
    "Property owners": lambda df: df["is_property_owner"].fillna(False).astype(bool),
    "Shareholders": lambda df: df["share_count"].fillna(0).astype(int) > 0,
}


def _own_card_html(row, *, show_rank: bool) -> str:
    """One member card framed around what they own.

    Mirrors the /interests card encoding (same pill colours) so the two pages
    read as one family, but leads with the ownership pills rather than the raw
    declaration count.
    """
    name = str(row["member_name"])
    party = str(row.get("party_name") or "")
    constit = str(row.get("constituency") or "")
    total = int(row.get("total_declarations") or 0)
    p_count = int(row.get("property_count") or 0)
    s_count = int(row.get("share_count") or 0)
    landlord = bool(row.get("is_landlord", False))
    is_prop = bool(row.get("is_property_owner", False))

    parts: list[str] = []
    if landlord:
        parts.append(pill("Landlord", "accent"))
    elif is_prop:
        parts.append(pill("Property owner", "owner"))
    if p_count:
        parts.append(pill(f"{p_count} propert{'ies' if p_count != 1 else 'y'}", "prop"))
    if s_count:
        parts.append(pill(f"Shareholder · {s_count}", "shares"))
    # Always close with the lifetime declaration total so the ranking metric is
    # visible on every card, even ones with no ownership pills.
    parts.append(pill(f"{total} declarations", "decl"))

    return ranked_member_card(
        name=name,
        meta=clean_meta(party, constit),
        rank=int(row["rank"]) if show_rank else None,
        pills_html="".join(parts),
    )


def _render_leaderboard(df: pd.DataFrame, *, show_rank: bool) -> None:
    page_size = 12
    page_key = "wto_page"
    cur = int(st.session_state.get(page_key, 1))
    total_pages = max(1, (len(df) + page_size - 1) // page_size)
    if cur > total_pages:
        cur = 1
        st.session_state[page_key] = 1
    page_idx = cur - 1
    visible = df.iloc[page_idx * page_size : (page_idx + 1) * page_size]

    cards: list[str] = []
    for _, row in visible.iterrows():
        name = str(row["member_name"])
        # Prefer the canonical code the view now carries (works for FORMER members
        # like Seán Haughey, who are absent from the current-roster registry the
        # name resolver reads). Fall back to the name lookup only when it is null.
        code = str(row.get("member_id") or "").strip() or resolve_member_code(name)
        inner = _own_card_html(row, show_rank=show_rank)
        if code:
            cards.append(
                clickable_card_link(
                    href=member_profile_url(code, section="interests"),
                    inner_html=inner,
                    aria_label=f"See what {name} has declared",
                )
            )
        else:
            cards.append(inner)
    st.html("\n".join(cards))

    pagination_controls(
        total=len(df),
        key_prefix="wto",
        page_sizes=(page_size,),
        default_page_size=page_size,
        label="members",
        show_caption=False,
    )


def _redirect_to_profile(name: str) -> None:
    """Jump to a member's canonical interests profile (meta-refresh, not st.html
    — see the /interests page for why an iframe would swallow the redirect)."""
    code = resolve_member_code(name)
    if code:
        target = member_profile_url(code, section="interests")
        st.markdown(
            f'<meta http-equiv="refresh" content="0;url={_h(target)}">',
            unsafe_allow_html=True,
        )
        st.stop()


def _render_provenance(house: str) -> None:
    provenance_expander(
        sections=[
            "This page draws on every published year of the Register of Members' Interests "
            "on record — sitting and former TDs/senators alike. The register is restated "
            "in full each year, so figures are shown for a single declaration year (the "
            "member's most recent by default, or the year you pick) and are never summed "
            "across years. Members are ranked by their declaration count in that year. "
            "Flags (landlord, property owner, shareholder) are pipeline navigation aids, "
            "not legal conclusions. Office holders (Ministers, Ceann Comhairle) may be "
            "exempt from filing, so records can be incomplete. A high count reflects "
            "transparency, not wrongdoing."
        ],
        source_caption="Data: Oireachtas Register of Members' Interests (data.oireachtas.ie)",
        pdf_links=interests_pdf_links(house),
    )


@page_error_boundary
def what_they_own_page() -> None:
    inject_css()
    hide_sidebar()

    hero_banner(
        kicker="REGISTER OF MEMBERS' INTERESTS",
        title="What they own",
        dek=(
            "Property, shares and company interests declared by the people who govern "
            "Ireland — every year on record, sitting and former members alike."
        ),
    )

    house: str = (
        st.segmented_control(
            "Chamber",
            ["Dáil", "Seanad"],
            default="Dáil",
            key="wto_house",
            label_visibility="collapsed",
        )
        or "Dáil"
    )
    if st.session_state.get("_wto_last_house") != house:
        for k in ("wto_page", "wto_member_sel", "wto_member_q", "wto_selected", "wto_year"):
            st.session_state.pop(k, None)
        st.session_state["_wto_last_house"] = house

    if not fetch_interests_availability(house):
        empty_state(
            "Register data not available",
            f"Source data for {house} not found. Run the pipeline to populate "
            "data/silver/ and re-register v_member_interests_detail.",
        )
        return

    # ── Member jump (search + notable chips) → canonical profile ────────────────
    opts = fetch_interests_filter_options(house)
    chosen = main_member_jump(
        opts["members"],
        key_prefix="wto_member",
        label="Find a TD or senator",
        placeholder="Type a name…",
    )
    if chosen:
        _redirect_to_profile(chosen)

    notable = NOTABLE_TDS if house == "Dáil" else NOTABLE_SENATORS
    if notable and render_notable_chips(notable, opts["members"], "chip_wto", "wto_selected", cols=6):
        picked = st.session_state.pop("wto_selected", None)
        if picked:
            _redirect_to_profile(picked)
        st.rerun()
    if st.session_state.get("wto_selected"):
        member_moved_callout(
            st.session_state["wto_selected"],
            section="interests",
            section_label="the member's declarations",
            state_keys=("wto_selected",),
        )

    # ── Ownership category filter (display-only) ────────────────────────────────
    category = (
        st.segmented_control(
            "What they own",
            list(_CATEGORIES),
            default="Everyone",
            key="wto_category",
            label_visibility="collapsed",
        )
        or "Everyone"
    )

    # ── Historic year picker ────────────────────────────────────────────────────
    # "Most recent on file" shows each member at their latest declaration year
    # (the all-time snapshot view). Picking a year shows that year's register —
    # surfacing whoever sat then, including members who have since left.
    _LATEST = "Most recent on file"
    years = [int(y) for y in opts.get("years", [])]
    year_choice = (
        st.selectbox(
            "Declaration year",
            [_LATEST, *[str(y) for y in years]],
            index=0,
            key="wto_year",
            label_visibility="collapsed",
        )
        or _LATEST
    )
    selected_year: int | None = None if year_choice == _LATEST else int(year_choice)

    # The Seanad register is only published from 2020 — there is no earlier Seanad
    # history on record (the Dáil reaches back to 2011). Flag it so a short year
    # list doesn't read as missing data.
    if house == "Seanad" and years:
        st.caption(
            f"ℹ️ Seanad declarations are only on record from **{min(years)}** onward — "
            "there is no earlier Seanad register published. (The Dáil register reaches back to 2011.)"
        )

    members_df = (
        fetch_member_index_alltime(house) if selected_year is None else fetch_member_index(house, selected_year)
    )
    if members_df.empty:
        empty_state(
            "No declarations on record",
            "No interest declarations on record for this chamber.",
        )
        _render_provenance(house)
        return

    predicate = _CATEGORIES[category]
    if predicate is not None:
        members_df = members_df[predicate(members_df)].reset_index(drop=True)

    member_label = "TDs and senators" if house == "Dáil" else "senators"
    # Scope phrase shared by every caption — makes clear the count is a single
    # year's snapshot, never a running total across years.
    if selected_year is None:
        scope = "each member's **most recent declaration year** on file (includes former members at their last year)"
        year_tag = "most recent year"
    else:
        scope = f"the **{selected_year}** register"
        year_tag = str(selected_year)
    pill_legend = (
        "Pill colours: **landlord** (orange) · **property owner** (green) · "
        "**shareholder** (purple) · **declarations** (blue). Counts are for that "
        "year only — not summed across years."
    )
    if category == "Everyone":
        evidence_heading(f"Every member · {year_tag} · {len(members_df)} {member_label}")
        st.caption(f"Ranked by declarations in {scope}. {pill_legend}")
    else:
        evidence_heading(f"{category} · {year_tag} · {len(members_df)} members")
        st.caption(f"Members declaring **{category.lower()}** in {scope}. {pill_legend}")

    if members_df.empty:
        empty_state(
            f"No {category.lower()} on record",
            "No members match this category for the selected chamber.",
        )
        _render_provenance(house)
        return

    # Keep the leaderboard rank only on the unfiltered "Everyone" view, where it
    # reads as a true 1..N ranking. Filtered views would otherwise show gapped
    # ranks (1, 4, 9…), which looks like missing data rather than a filter.
    _render_leaderboard(members_df, show_rank=(category == "Everyone"))

    _render_provenance(house)
