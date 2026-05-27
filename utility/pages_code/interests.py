"""
Register of Members' Interests — Dáil Tracker.

Data source: silver CSV via in-memory DuckDB (registered view simulation).
All retrieval follows SELECT / WHERE / ORDER BY / LIMIT only — no aggregation in Streamlit.

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_index
    Member-level summary per year: member_name, party_name, constituency,
    declaration_count, categories_count, landlord_flag, property_flag.
    Needed to replace the flat declaration browse table with a clean member index.

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_detail
    Replace _load_interests() silver CSV simulation with a registered view.

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_yearly_summary
    Year + interest_category + declarations_count.
    Needed for the year-responsive category breakdown chart.

TODO_PIPELINE_VIEW_REQUIRED: v_member_interests_sources
    Per-declaration source PDF links: source_pdf_url, source_page_number, oireachtas_url.

TODO_PIPELINE_VIEW_REQUIRED: directorship_flag — derive from interest_category in pipeline
TODO_PIPELINE_VIEW_REQUIRED: shareholding_flag — derive from interest_category in pipeline
TODO_PIPELINE_VIEW_REQUIRED: source_pdf_url — add PDF URL to silver output
TODO_PIPELINE_VIEW_REQUIRED: unique_member_code — stable join key on v_member_interests_*
    views. Required for cross-page member-name links via
    utility/ui/entity_links.member_link_html. Until then this page cannot link member
    names out to /member-overview without an in-Streamlit name lookup (forbidden).
TODO_PIPELINE_VIEW_REQUIRED: mart_version, code_version, latest_fetch_timestamp_utc
    on v_member_interests_detail
"""

from __future__ import annotations

import datetime
from html import escape as _h
import pandas as pd
import streamlit as st

from shared_css import inject_css
from ui.avatars import avatar_credit_html, avatar_data_url, initials as _initials
from ui.components import (
    clean_meta,
    clickable_card_link,
    empty_state,
    evidence_heading,
    hero_banner,
    interest_declaration_item,
    main_member_jump,
    member_moved_callout,
    member_profile_header,
    page_error_boundary,
    pagination_controls,
    pill,
    ranked_member_card,
    render_notable_chips,
    sidebar_divider,
    sidebar_page_header,
    sidebar_subtitle,
    todo_callout,
    year_selector,
)
from data_access.identity_resolver import resolve_member_code
from ui.entity_links import member_profile_url, source_link_html
from ui.export_controls import export_button
from ui.source_pdfs import interests_pdf_url, provenance_expander

from config import (
    INTEREST_CATEGORY_LABELS,
    INTEREST_CATEGORY_ORDER,
    NOTABLE_SENATORS,
    NOTABLE_TDS,
)
from data_access.interests_data import (
    fetch_interests_availability,
    fetch_interests_filter_options,
    fetch_member_index,
    fetch_td_interests,
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


def _real_descriptions(rows: pd.DataFrame) -> list[str]:
    """Return non-empty, non-boilerplate interest_text entries, deduplicated."""
    if rows.empty or "interest_text" not in rows.columns:
        return []
    seen: dict[str, None] = {}
    for d in rows["interest_text"].tolist():
        s = str(d).strip()
        if s and s.lower() not in ("no interests declared", "", "nan"):
            seen[s] = None
    return list(seen)


# ── Profile view ───────────────────────────────────────────────────────────────


def render_member_interests(
    house: str,
    td_name: str,
    *,
    show_member_header: bool = True,
    year_pill_key: str = "int_profile_year",
) -> None:
    """Render the per-TD interests profile body.

    Public so :mod:`pages_code.member_overview` can embed it inside the
    Interests expander. When ``show_member_header=False``, the avatar/name/
    meta header is omitted (the embedding page already shows it via the
    member-overview hero) and the year-responsive Landlord / Property /
    Shareholder badges render as a compact strip above the year pills.

    ``year_pill_key`` is overridable so the embedded copy can use a key that
    doesn't collide with the stand-alone interests-page state.
    """
    td_df = fetch_td_interests(house, td_name)
    if td_df.empty:
        empty_state(
            "No records found",
            f"No interest declarations found for {td_name}. Try a different name.",
        )
        return

    info = td_df.iloc[0]
    party = str(info.get("party_name", "") or "")
    constit = str(info.get("constituency", "") or "")
    meta = clean_meta(party, constit)

    td_years = sorted(td_df["declaration_year"].dropna().astype(int).unique(), reverse=True)

    # ── Identity strip — header reserved here (when shown), filled below
    #    once we know the selected year so badges reflect that year.
    header_slot = st.empty() if show_member_header else None

    # ── Year pills (profile-scoped key) ───────────────────────────────────────
    year_opts = [str(y) for y in td_years]
    selected_year = year_selector(year_opts, key=year_pill_key, skip_current=False)

    year_df = td_df[td_df["declaration_year"] == selected_year].copy()
    prior_year = selected_year - 1
    prior_df = td_df[td_df["declaration_year"] == prior_year].copy()
    has_prior = not prior_df.empty

    # ── Year-responsive identity badges ───────────────────────────────────────
    is_landlord_year = bool(year_df["landlord_flag"].any()) if not year_df.empty else False
    is_property_year = bool(year_df["property_flag"].any()) if not year_df.empty else False
    prop_count = (
        len(_real_descriptions(year_df[year_df["interest_category"] == "Land (including property)"]))
        if not year_df.empty
        else 0
    )
    share_count = len(_real_descriptions(year_df[year_df["interest_category"] == "Shares"])) if not year_df.empty else 0

    parts: list[str] = []
    if is_landlord_year:
        parts.append(pill("Landlord declared", "accent"))
    elif is_property_year:
        parts.append(pill("Property owner", "owner"))
    if prop_count:
        parts.append(pill(f"{prop_count} propert{'ies' if prop_count != 1 else 'y'}", "prop"))
    if share_count:
        parts.append(pill(f"Shareholder · {share_count}", "shares"))
    badges_html = " ".join(parts)

    if header_slot is not None:
        with header_slot.container():
            member_profile_header(
                td_name,
                meta,
                badges_html,
                avatar_url=avatar_data_url(td_name),
                avatar_initials=_initials(td_name),
                avatar_credit_html=avatar_credit_html(td_name),
            )
    elif badges_html:
        # Embedded mode: hero is shown by the parent page, so render the
        # year-aware badges on their own as a compact strip.
        st.html(f'<div class="int-embedded-badge-strip">{badges_html}</div>')

    # ── Editorial callout ──────────────────────────────────────────────────────
    name_short = _h(td_name.split()[-1])
    if year_df.empty:
        glance = f"No declarations recorded for {selected_year}."
    else:
        descs_all = _real_descriptions(year_df)
        n_entries = len(descs_all)
        n_cats = len(year_df["interest_category"].dropna().unique())
        parts: list[str] = [
            f"In {selected_year}, {name_short} filed "
            f"<strong>{n_entries}</strong> declaration{'s' if n_entries != 1 else ''} "
            f"across <strong>{n_cats}</strong> "
            f"categor{'ies' if n_cats != 1 else 'y'}."
        ]
        if has_prior:
            prior_all = set(_real_descriptions(prior_df))
            current_all = set(descs_all)
            n_new = len(current_all - prior_all)
            n_removed = len(prior_all - current_all)
            if n_new:
                parts.append(f"<strong>{n_new} new</strong> since {prior_year}.")
            if n_removed:
                parts.append(f"<strong>{n_removed} removed</strong> since {prior_year}.")
        glance = " ".join(parts)

    st.html(
        f'<div class="dt-callout" style="margin:0.5rem 0 0.9rem;">'
        f'<p style="margin:0;font-size:0.95rem;line-height:1.65;">{glance}</p>'
        f"</div>"
    )

    # ── Diff toggle — prominent, above category sections ──────────────────────
    show_diff = False
    if has_prior:
        show_diff = st.toggle(
            f"Show changes since {prior_year}",
            value=True,
            key=f"int_diff_{td_name}_{selected_year}",
        )
    else:
        st.caption(f"No {prior_year} declarations on record. Year-on-year comparison unavailable.")

    st.divider()

    pdf_url = interests_pdf_url(house, selected_year)
    if pdf_url:
        link = source_link_html(
            pdf_url,
            f"Register of Members' Interests · {house} · {selected_year} (Oireachtas.ie PDF)",
            aria_label=f"Open the {house} {selected_year} register of interests PDF",
        )
        st.html(
            f'<div class="dt-provenance-box" style="margin-bottom:0.75rem">'
            f'<span style="font-size:0.68rem;font-weight:700;letter-spacing:0.08em;'
            f'text-transform:uppercase;color:var(--text-meta)">Source document</span><br>'
            f"{link}"
            f"</div>"
        )

    evidence_heading(f"Declarations · {selected_year}")

    # ── Category sections — non-empty only ────────────────────────────────────
    # Pre-compute descriptions per category once to avoid repeated calls inside the loop.
    all_cats = list(INTEREST_CATEGORY_ORDER)
    if not year_df.empty:
        for cat in year_df["interest_category"].dropna().unique():
            if cat not in INTEREST_CATEGORY_ORDER:
                all_cats.append(cat)

    year_descs_by_cat: dict[str, list[str]] = {
        cat: _real_descriptions(year_df[year_df["interest_category"] == cat] if not year_df.empty else pd.DataFrame())
        for cat in all_cats
    }
    prior_descs_by_cat: dict[str, list[str]] = {
        cat: (_real_descriptions(prior_df[prior_df["interest_category"] == cat]) if has_prior else [])
        for cat in all_cats
    }

    cats_with_data = [cat for cat in all_cats if year_descs_by_cat[cat]]
    cats_empty = [cat for cat in INTEREST_CATEGORY_ORDER if not year_descs_by_cat.get(cat)]

    if not cats_with_data:
        empty_state(
            f"Nothing declared for {selected_year}",
            "No interest declarations recorded for this member in this year.",
        )
    else:
        for cat in cats_with_data:
            descs = year_descs_by_cat[cat]
            prior_cat_set = set(prior_descs_by_cat[cat])
            current_cat_set = set(descs)
            label = INTEREST_CATEGORY_LABELS.get(cat, cat)

            st.html(f'<p class="int-category-section">{_h(label)}&nbsp;&nbsp;·&nbsp;&nbsp;{len(descs)}</p>')

            if show_diff and has_prior:
                for d in descs:
                    interest_declaration_item(d, "new" if d not in prior_cat_set else "unchanged")
                for d in sorted(prior_cat_set - current_cat_set):
                    interest_declaration_item(d, "removed")
            else:
                for d in descs:
                    interest_declaration_item(d, "unchanged")

        # Categories that existed in prior year but have nothing in current year
        if show_diff and has_prior:
            for cat in INTEREST_CATEGORY_ORDER:
                if cat in cats_with_data:
                    continue
                prior_descs = prior_descs_by_cat.get(cat, [])
                if not prior_descs:
                    continue
                label = INTEREST_CATEGORY_LABELS.get(cat, cat)
                st.html(
                    f'<p class="int-category-section">{_h(label)}&nbsp;&nbsp;·&nbsp;&nbsp;'
                    f'0 <span style="font-weight:400;text-transform:none;font-size:0.75rem;">'
                    f"(all removed)</span></p>"
                )
                for d in sorted(prior_descs):
                    interest_declaration_item(d, "removed")

    # ── Empty categories — single collapsed summary ────────────────────────────
    if cats_empty:
        empty_labels = [INTEREST_CATEGORY_LABELS.get(c, c) for c in cats_empty]
        with st.expander(f"Nothing declared · {len(cats_empty)} categories", expanded=False):
            st.html('<p class="int-empty-cats">' + " · ".join(_h(lbl) for lbl in empty_labels) + "</p>")

    st.divider()

    # ── Source links (pipeline gap) ────────────────────────────────────────────
    # Pipeline detail (dev): per-declaration source_pdf_url + oireachtas_url
    # needed on v_member_interests_sources before each declaration can link
    # to its scanned page on oireachtas.ie.
    todo_callout(
        "Source PDFs — direct links from each declaration to the official "
        "Oireachtas register PDF page will appear here in a future release."
    )

    # ── Export ─────────────────────────────────────────────────────────────────
    today = datetime.date.today().isoformat()
    export_button(
        year_df,
        label=f"Export {td_name} · {selected_year} · {len(year_df)} rows",
        filename=f"dail_tracker_interests_{td_name.replace(' ', '_')}_{selected_year}_{today}.csv",
        key="int_td_export",
    )


# ── Provenance footer ──────────────────────────────────────────────────────────


def _render_provenance() -> None:
    provenance_expander(
        sections=[
            "Declarations are extracted from published Oireachtas PDF documents. "
            "Flags (landlord, property) are pipeline navigation aids, not legal conclusions. "
            "Office holders (Ministers, Ceann Comhairle) may be exempt from filing — "
            "records can be incomplete. "
            "A high declaration count reflects transparency, not wrongdoing."
        ],
        source_caption="Data: Oireachtas Register of Members' Interests (data.oireachtas.ie)",
    )


# ── Page entry point ───────────────────────────────────────────────────────────


@page_error_boundary
def interests_page() -> None:
    inject_css()

    # ── Sidebar (P1-3 grammar) ────────────────────────────────────────────────
    with st.sidebar:
        sidebar_page_header("Register of<br>Members&rsquo; Interests")
        sidebar_subtitle("Declared interests by TD or Senator")
        sidebar_divider()

        house: str = (
            st.segmented_control(
                "Chamber",
                ["Dáil", "Seanad"],
                default="Dáil",
                key="interests_house",
            )
            or "Dáil"
        )

        # Clear year pill and member selection on chamber switch
        if st.session_state.get("_interests_last_house") != house:
            for k in ("int_profile_year", "selected_td", "int_member_sel", "int_member_q"):
                st.session_state.pop(k, None)
            st.session_state["_interests_last_house"] = house

        opts = fetch_interests_filter_options(house)

        notable = NOTABLE_TDS if house == "Dáil" else NOTABLE_SENATORS
        # P0-1 fix: previously this wrote selected_td and called st.rerun(),
        # but nothing read selected_td (Phase 3 lifted the per-TD profile
        # to /member-overview without rewiring the picker). Navigate
        # directly to the canonical profile via the same contract the
        # cards already use.
        if notable and render_notable_chips(notable, opts["members"], "chip_int", "selected_td"):
            picked = st.session_state.pop("selected_td", None)
            if picked:
                code = resolve_member_code(picked)
                if code:
                    target = member_profile_url(code, section="interests")
                    # Use st.markdown(unsafe_allow_html=True) — NOT st.html()
                    # — because st.html iframes its content and a meta-refresh
                    # inside an iframe redirects the iframe only, not the
                    # parent page. See [[feedback-streamlit-css-and-state]]
                    # and the same pattern in lobbying_3.py:274,340.
                    st.markdown(
                        f'<meta http-equiv="refresh" content="0;url={_h(target)}">',
                        unsafe_allow_html=True,
                    )
                    st.stop()
            st.rerun()

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

    # ── Page header ───────────────────────────────────────────────────────────
    hero_banner(
        kicker="REGISTER OF MEMBERS' INTERESTS",
        title="What has your TD declared?",
    )

    # ── Browse mode ───────────────────────────────────────────────────────────

    # Main-panel member search — primary call-to-action under the hero.
    # P0-1 fix: this used to write selected_td + rerun, but no branch ever
    # read it (Phase 3 lifted the per-TD profile to /member-overview).
    # Navigate directly to the canonical profile via the same contract
    # the cards already use.
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
            # st.markdown not st.html — see notable-chip handler above.
            st.markdown(
                f'<meta http-equiv="refresh" content="0;url={_h(target)}">',
                unsafe_allow_html=True,
            )
            st.stop()

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
        _render_provenance()
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
        _render_provenance()
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

    _render_provenance()
    return
