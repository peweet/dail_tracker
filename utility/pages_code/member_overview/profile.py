from __future__ import annotations

import datetime
from html import escape as _h
import re

import pandas as pd
import streamlit as st

from ui.avatars import avatar_credit_html, avatar_data_url, initials as _initials
from ui.components import (
    back_button,
    clean_meta,
    empty_state,
    field_label,
    party_colour,
    stat_strip,
)
from ui.entity_links import (
    PAGES,
    api_json_link,
    oireachtas_profile_url,
    social_icon_chip_html,
    source_link_html,
)
from ui.source_pdfs import provenance_expander
from ui.vote_explorer import render_member_votes
from ui.attendance_panel import render_member_attendance
from ui.interests_panel import render_member_interests
from pages_code.lobbying_3 import render_member_lobbying
from ui.payments_panel import render_member_payments
from data_access.payments_data import fetch_filter_options as _pay_filter_options
from data_access.payments_data import fetch_payments_summary as _pay_summary

from ._shared import (
    _EC_REVIEW_URL,
    _STAGE_KEY,
    _SECTION_LABELS,
    _SECTION_TABS,
    _att_all_years,
    _att_chamber_sitting_days,
    _att_headline_row,
    _constituency_context,
    _contact_details,
    _external_links,
    _identity,
    _lobbying_rd,
    _member_house,
    _member_list,
    _pay_grand_total,
    _votes_summary,
)
from .overview import _render_overview, _render_pay_summary_tiles
from .sections import (
    _section_committees,
    _section_debates,
    _section_legislation,
    _section_ministerial_roles,
    _section_questions,
    _section_statutory_instruments,
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


def _tel_href(display: str) -> str:
    """Build a `tel:` href from a display phone string. Keeps a leading '+' and
    digits only — '(01) 230 3020' → 'tel:012303020'. Presentation only."""
    digits = re.sub(r"[^\d+]", "", display or "")
    return f"tel:{digits}" if digits else ""


_NEWS_TIER = {
    "national": ("#1d4ed8", "National"),
    "specialist": ("#0f766e", "Specialist"),
    "local_paper": ("#b45309", "Local paper"),
    "local_radio": ("#7c3aed", "Local radio"),
    "partisan": ("#9f1239", "Partisan"),
}


def _render_news_mentions_block(df: pd.DataFrame, member_name: str) -> None:
    """Collapsible 'Recent media mentions' section: headline → publisher link, outlet + date +
    tier badge, most-recent first. Pure presentation — the rows (incl. the name match) are produced
    by the extractor/view; this only renders them. A mention is not an assertion by this site."""
    n = 0 if df is None or df.empty else len(df)
    # Open by default when there IS coverage so the count isn't hidden behind a
    # click; stay collapsed (honest empty state) for members with nothing recent.
    with st.expander(f"📰 Recent media mentions ({n})", expanded=n > 0):
        if n == 0:
            st.caption(
                f"No recent media mentions matched {_h(member_name)} in the last 30 days of Irish "
                "news searched. Coverage is name-matched and skews to higher-profile members."
            )
            return
        cards = []
        for r in df.itertuples(index=False):
            colour, label = _NEWS_TIER.get(getattr(r, "outlet_tier", ""), ("#6b7280", "News"))
            dt = getattr(r, "published_at", None)
            date_str = dt.strftime("%d %b %Y") if dt is not None and pd.notna(dt) else ""
            body_note = (
                ""
                if getattr(r, "match_in_title", False)
                else "<span style='color:#9ca3af;font-style:italic'>· named in article body</span>"
            )
            url = _h(str(getattr(r, "article_url", "") or "#"))
            cards.append(
                "<div style='background:#ffffff;border:1px solid #e7e2d8;border-radius:10px;"
                "padding:12px 14px;margin-bottom:9px'>"
                f"<a href='{url}' target='_blank' rel='noopener' "
                "style='color:#111827;text-decoration:none;font-weight:600;line-height:1.35'>"
                f"{_h(str(getattr(r, 'article_title', '')))}</a>"
                "<div style='margin-top:6px;font-size:0.82rem;color:#6b7280;display:flex;gap:8px;"
                "align-items:center;flex-wrap:wrap'>"
                f"<span style='color:#fff;border-radius:999px;padding:1px 9px;font-size:0.7rem;"
                f"font-weight:600;background:{colour}'>{label}</span>"
                f"<span>{_h(str(getattr(r, 'outlet', '') or ''))}</span>"
                f"<span>· {date_str}</span>{body_note}</div></div>"
            )
        st.html("".join(cards))
        st.caption(
            "Name-matched from a public news search. A mention is not an assertion by this site "
            "and does not imply involvement; headlines link to the publisher."
        )


def _render_contact_block(contact: dict, member_name: str, profile_href: str | None) -> None:
    """Official office contact details card: address, phone(s), email — each a
    real `tel:` / `mailto:` link where present — with a verification link to the
    member's oireachtas.ie profile (the source). Renders nothing only when there
    is genuinely no contact data AND no profile link; otherwise it always shows
    the profile link so a reader is never dead-ended.

    Every field is data-anchored to the scraped profile page — nothing is
    imputed. Missing fields are simply omitted (no em-dashes).
    """
    address = str(contact.get("address", "") or "").strip()
    phone_all = str(contact.get("phone_all", "") or "").strip()
    email = str(contact.get("email", "") or "").strip()

    rows: list[str] = []
    if address:
        rows.append(
            '<div class="mo-contact-row">'
            '<span class="mo-contact-ico" aria-hidden="true">📍</span>'
            f'<span class="mo-contact-val">{_h(address)}</span>'
            "</div>"
        )
    if phone_all:
        phones = [p.strip() for p in phone_all.split("/") if p.strip()]
        phone_links = []
        for p in phones:
            href = _tel_href(p)
            phone_links.append(f'<a class="mo-contact-link" href="{_h(href)}">{_h(p)}</a>' if href else _h(p))
        rows.append(
            '<div class="mo-contact-row">'
            '<span class="mo-contact-ico" aria-hidden="true">📞</span>'
            f'<span class="mo-contact-val">{" &nbsp;·&nbsp; ".join(phone_links)}</span>'
            "</div>"
        )
    if email:
        rows.append(
            '<div class="mo-contact-row">'
            '<span class="mo-contact-ico" aria-hidden="true">✉️</span>'
            f'<span class="mo-contact-val">'
            f'<a class="mo-contact-link" href="mailto:{_h(email)}">{_h(email)}</a>'
            "</span></div>"
        )

    # Nothing useful to show and no profile to point at → render nothing.
    if not rows and not profile_href:
        return

    source_chip = (
        source_link_html(
            profile_href,
            "View on Oireachtas.ie",
            aria_label=f"Open {member_name}'s official Oireachtas profile (source of these contact details) in a new tab",
        )
        if profile_href
        else ""
    )

    if rows:
        body = "".join(rows)
        footer = (
            f'<div class="mo-contact-source">Source · oireachtas.ie member profile · {source_chip}</div>'
            if source_chip
            else ""
        )
    else:
        # No scraped fields — be transparent and still offer the official page.
        body = (
            '<div class="mo-contact-row mo-contact-empty">'
            "No office address, phone or email is published for this member on "
            "their Oireachtas profile.</div>"
        )
        footer = f'<div class="mo-contact-source">{source_chip}</div>' if source_chip else ""

    st.html(f'<div class="mo-contact-card"><div class="mo-contact-title">Contact</div>{body}{footer}</div>')


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


def _render_section_switcher(join_key: str, active: str) -> None:
    """Profile section tabs. Each chip is a soft-nav link into one domain.

    CONSTRAINT: spa_links replaces the WHOLE query string on a ?-anchor click,
    so every chip MUST carry ``member=`` or the profile bounces back to browse.
    The active chip gets aria-current for the filled-accent state + a11y.
    """
    chips = ['<nav class="mo-section-nav" aria-label="Profile sections">']
    for sid in _SECTION_TABS:
        cur = ' aria-current="true"' if sid == active else ""
        chips.append(
            f'<a class="mo-section-chip" href="?member={_h(join_key)}&section={sid}"{cur}>'
            f"{_h(_SECTION_LABELS[sid])}</a>"
        )
    chips.append("</nav>")
    st.html("\n".join(chips))


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

    # ── Official contact details (scraped from the oireachtas.ie profile) ─────
    # Office address, phone(s) and @oireachtas.ie email, each a real tel:/mailto:
    # link. Sits directly under the identity hero — how a citizen reaches their
    # representative is identity-level information, not buried in a section.
    _render_contact_block(_contact_details(conn, join_key), member_name, profile_href)

    # NOTE: the per-member "Recent media mentions" card is parked while the news
    # feature is tested further (see pipeline_sandbox/news_mentions/). The render
    # helper + query (moq.news_mentions) remain for one-line reinstatement.

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
        # stat strip. We gate on empty-on-both rather than is_minister: it's
        # the strongest signal that the regular plenary/TAA framing doesn't
        # apply, and it also covers ex-ministers / edge cases. (is_minister
        # itself is now corrected in v_member_registry from the member feed's
        # office slots — Taoiseach included — 2026-06-22.)
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
            # Skip the in-progress calendar year on the stat strip (audit P1-6,
            # mirrors attendance P1-1 and payments P1-1); only-year-on-file falls
            # back to it, labelled "(so far)". The pick itself is a query
            # (moq.att_headline_year) — the page just renders the row.
            row = _att_headline_row(conn, join_key).iloc[0]
            att_yr = int(row["year"])
            so_far = " (so far)" if att_yr >= datetime.date.today().year else ""
            # Lead with PLENARY sitting days (days actually in the chamber),
            # rated against the chamber's own sitting-day count. The headline
            # used to show attended_count = sitting + "other" days (committee /
            # non-sitting business per the TAA report footnote) labelled "Days
            # in chamber" — but that total can exceed the days the chamber sat
            # (e.g. 120 vs 94 in 2025), which reads as broken/stale data. Other
            # days are surfaced separately, never summed into the headline.
            sitting = int(row["sitting_days"]) if pd.notna(row.get("sitting_days")) else 0
            other = int(row["other_days"]) if pd.notna(row.get("other_days")) else 0
            denom = _att_chamber_sitting_days(conn, house).get(att_yr)
            att_lbl = f"Sitting days · {att_yr}{so_far}"
            att_val = f"{sitting} of {denom}" if denom else str(sitting)
            # No rank here: the only rank we have (v_attendance_year_rank) is
            # computed on the combined total, which is capped at the 120-day TAA
            # limit — 125 of 155 members tie at 120 in 2025, so "rank 1 of 155"
            # is meaningless. The "other days" are committee / non-sitting business
            # (TAA report footnote: "includes non-sitting days on which committees
            # may have sat") and are shown separately, never folded into the figure.
            sub_parts: list[str] = []
            if other:
                sub_parts.append(f"+{other} other (non-sitting) days")
            if is_minister:
                sub_parts.append("Minister")
            att_sub = " · ".join(sub_parts)
        else:
            att_lbl, att_val, att_sub = "Sitting days", "—", ""

        cast_val = f"{votes_cast:,}" if votes_cast else "—"
        cast_sub = f"across {votes_div:,} divisions" if votes_div else ""
        # Audit P1-1: drop the em-dash for a single empty stat. The TAA
        # parquet only covers ministers + a small subset of TDs, so the
        # bare "—" was the rule not the exception for ~150 of 176 members
        # and read as broken data. "Not on file" + sub-label explanation
        # mirrors the round-3 P1-F cabinet-member fallback pattern.
        if pay_total:
            pay_val = f"€{pay_total:,.0f}"
            # Make the framing honest above the fold: this is reimbursed expense
            # money (the PSA / TAA travel allowance), not salary or income. The
            # Salary & expenses section below breaks both out in full.
            pay_sub = "Expense allowances (PSA/TAA) — not salary · all years on record"
        else:
            pay_val = "Not on file"
            pay_sub = "Expense allowances (PSA/TAA) not tracked for this member"

        stat_strip(
            [
                (att_val, att_lbl, "var(--text-primary)", att_sub),
                (cast_val, "Votes cast", "var(--signal-good)", cast_sub),
                (
                    pay_val,
                    "Expenses & allowances",
                    "var(--text-meta)" if not pay_total else "var(--text-primary)",
                    pay_sub,
                ),
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

    # ── Section router (2026-06-22) ──────────────────────────────────────────
    # The profile was an all-sections-rendered flat scroll: too long, and the
    # next section's cards bled into the viewport. Now one domain renders at a
    # time, chosen by ?section=<sid> (bookmarkable; default "overview").
    #
    # spa_links replaces the WHOLE query string on any ?-anchor click, so an
    # in-section filter chip (?mo_q_topic, ?payyr) drops ?section. We hold the
    # active section in session_state as a fallback so those clicks don't kick
    # the reader back to Overview, and reset to Overview when the member changes.
    url_section = st.query_params.get("section")
    if st.session_state.get("mo_active_member") != join_key:
        st.session_state["mo_active_member"] = join_key
        active = url_section or "overview"
    else:
        active = url_section or st.session_state.get("mo_active_section", "overview")
    if active not in _SECTION_LABELS:
        active = "overview"
    st.session_state["mo_active_section"] = active

    _render_section_switcher(join_key, active)

    if active == "overview":
        _render_overview(conn, join_key, house, member_name, is_minister, is_seanad)
    else:
        st.html(f'<h2 class="section-heading">{_h(_SECTION_LABELS[active])}</h2>')

        if active == "interests":
            # Phase 3 lift: full body rendered here without the per-page
            # member header (the hero above already shows it). House-aware —
            # the Register of Interests is published per chamber.
            render_member_interests(
                house,
                member_name,
                show_member_header=False,
                year_pill_key=f"mo_int_year_{join_key}",
            )
        elif active == "lobbying":
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
        elif active == "payments":
            # Two compact tiles — statutory salary | reimbursed expenses —
            # replace the old salary card + divider + lead paragraph: the
            # salary≠expenses point is stated once, cleanly, above the body.
            _render_pay_summary_tiles(conn, join_key, house)
            # Phase 5 lift: full payments body (year metrics + card-based
            # all-years summary + card-based payment records) without the
            # per-page identity strip, back button, or provenance footer.
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
        elif active == "attendance":
            # Phase 6 lift: year metrics + card-based year breakdown + a
            # sitting-calendar toggle. No inner `st.expander` (nested
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
        elif active == "votes":
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
        elif active == "debates":
            # 2026-05-31: promoted out of the Votes section into its own chip
            # so a reporter looking for "what has this TD spoken on?" finds it
            # in the section nav rather than buried under a 1000-row vote list.
            _section_debates(conn, join_key, member_name)
        elif active == "questions":
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
                    "section (filter the item of business to "
                    "“Commencement Matters”).",
                )
            else:
                _section_questions(conn, join_key, member_name)
        elif active == "legislation":
            _section_legislation(conn, join_key, member_name)
            _section_ministerial_roles(conn, join_key)
            _section_statutory_instruments(conn, join_key)
        elif active == "committees":
            _section_committees(member_name, join_key)

    # ── Unified provenance footer ────────────────────────────────────────────
    # The flagship dossier fuses ~10 public sources; each section above carries
    # its own per-record source ↗ link (Layer B). This expander is the page-level
    # roll-up (Layer A) — one place that names every source the profile draws on.
    provenance_expander(
        sections=[
            "**A profile built from the public record.** This dossier fuses the "
            "Houses of the Oireachtas open data — the member's official profile, "
            "**votes**, **debates & speeches**, **parliamentary questions**, "
            "**bills & committee** membership, **Register of Interests** declarations, "
            "and **Parliamentary Standard Allowance** expense figures — with the "
            "**Register of Lobbying** (lobbying.ie), the published salary set rate, and "
            "constituency context from the **CSO Census 2022** and **Electoral "
            "Commission 2023** boundary review.",
            "**Validate any figure at source.** Every section carries its own "
            "**source ↗** links straight to the underlying record (the division on "
            "oireachtas.ie, the bill, the lobbying return, the register entry). The hero "
            "links to the member's official Oireachtas profile and Wikipedia.",
            "**A record, not a judgement.** Figures are the bodies' own published "
            "values; expense allowances (PSA/TAA) are reimbursed costs of doing the job, "
            "**not** salary or income, and are never added to it. Appearing on a "
            "lobbying return or in a payment record is a public-record fact, not "
            "evidence of influence or wrongdoing.",
        ],
        source_caption=(
            "Data: Houses of the Oireachtas open data · Register of Lobbying "
            "(lobbying.ie) · CSO Census 2022 · Electoral Commission 2023."
        ),
    )

    # Quiet developer affordance: this whole dossier as JSON on the open-data API.
    # Renders nothing until DAIL_API_BASE_URL is configured (config-gated).
    from urllib.parse import quote

    _api = api_json_link(f"/v1/members/{quote(str(join_key), safe='')}/dossier", "This profile as JSON")
    if _api:
        st.html(f'<div class="dt-api-footer">{_api}</div>')
