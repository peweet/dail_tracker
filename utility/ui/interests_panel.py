"""Per-TD interests panel — embedded in the /member-overview Interests section.

Extracted from ``pages_code/interests.py`` (2026-06-01) so member-overview no
longer imports a render body out of another page. Pure rendering + data-access
retrieval, no business logic — mirrors ``ui/vote_explorer.py``.
"""

from __future__ import annotations

import datetime
from html import escape as _h

import pandas as pd
import streamlit as st
from data_access.interests_data import fetch_td_interests
from ui.avatars import avatar_credit_html, avatar_data_url
from ui.avatars import initials as _initials
from ui.components import (
    clean_meta,
    empty_state,
    evidence_heading,
    interest_declaration_item,
    member_profile_header,
    pill,
    todo_callout,
    year_selector,
)
from ui.entity_links import source_link_html
from ui.export_controls import export_button
from ui.source_pdfs import interests_pdf_url

from config import INTEREST_CATEGORY_LABELS, INTEREST_CATEGORY_ORDER


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
