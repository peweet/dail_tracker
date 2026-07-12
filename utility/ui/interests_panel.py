"""Per-TD interests panel — embedded in the /member-overview Interests section.

Extracted from ``pages_code/interests.py`` (2026-06-01) so member-overview no
longer imports a render body out of another page. Pure rendering + data-access
retrieval, no business logic — mirrors ``ui/vote_explorer.py``.
"""

from __future__ import annotations

import datetime
from html import escape as _h

import streamlit as st
from data_access.interests_data import (
    fetch_td_interest_declarations,
    fetch_td_interest_year_summary,
    fetch_td_interests,
    fetch_td_supplements,
)
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

    # The de-dup, year-on-year diff, and category/new/removed counting now live
    # in the pipeline views (v_member_interests_declarations /
    # v_member_interests_member_year_summary). This panel only renders.
    decl_df = fetch_td_interest_declarations(house, td_name)
    summary_df = fetch_td_interest_year_summary(house, td_name)

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

    # Per-year summary row from the view (counts + diff totals + badge inputs).
    _sy = summary_df[summary_df["declaration_year"] == selected_year] if not summary_df.empty else summary_df
    syr = _sy.iloc[0] if not _sy.empty else None
    has_prior = bool(syr["has_prior_year"]) if syr is not None else False

    # ── Year-responsive identity badges ───────────────────────────────────────
    is_landlord_year = bool(syr["is_landlord"]) if syr is not None else False
    is_property_year = bool(syr["is_property_owner"]) if syr is not None else False
    prop_count = int(syr["property_count"]) if syr is not None else 0
    share_count = int(syr["share_count"]) if syr is not None else 0

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
    if syr is None or int(syr["total_declarations"]) == 0 and int(syr["category_count"]) == 0:
        glance = f"No declarations recorded for {selected_year}."
    else:
        n_entries = int(syr["total_declarations"])
        n_cats = int(syr["category_count"])
        parts: list[str] = [
            f"In {selected_year}, {name_short} filed "
            f"<strong>{n_entries}</strong> declaration{'s' if n_entries != 1 else ''} "
            f"across <strong>{n_cats}</strong> "
            f"categor{'ies' if n_cats != 1 else 'y'}."
        ]
        if has_prior:
            n_new = int(syr["new_count"])
            n_removed = int(syr["removed_count"])
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

    # ── Section 29 supplements — member-level, not year-scoped ────────────────
    # Late filings / corrections to the register (the annual register is a full
    # restatement, so the correction event only exists in the supplement).
    # Display-only: the rows arrive pre-aggregated from
    # v_member_interests_supplements; an empty frame (none on file, or view
    # unavailable) simply omits the strip. Factual copy with the lead-not-verdict
    # rail — a late filing is a matter of record, not a finding.
    suppl_df = fetch_td_supplements(house, td_name)
    if not suppl_df.empty:
        n_filings = len(suppl_df)
        lines_html: list[str] = []
        for _, s in suppl_df.iterrows():
            yrs = str(s.get("years_declared", "") or "").replace(";", ", ")
            cats = str(s.get("categories", "") or "").replace(";", " · ")
            filed = str(s.get("supplement_date", "") or "")[:10]  # date only, not the 00:00:00 tail
            bits = [f"<strong>Filed {_h(filed)}</strong>"]
            if yrs:
                n_yrs = int(s.get("n_years", 0) or 0)
                bits.append(f"covering {_h(yrs)}" + (f" ({n_yrs} years)" if n_yrs > 1 else ""))
            if cats:
                bits.append(_h(cats))
            lines_html.append(
                f'<p style="margin:0.15rem 0 0;font-size:0.88rem;line-height:1.55;">{" — ".join(bits)}</p>'
            )
        st.html(
            f'<div class="dt-callout" style="margin:0 0 0.9rem;">'
            f'<span style="font-size:0.68rem;font-weight:700;letter-spacing:0.08em;'
            f'text-transform:uppercase;color:var(--text-meta)">'
            f"Section 29 supplement{'s' if n_filings != 1 else ''} · {n_filings} on file</span>"
            f"{''.join(lines_html)}"
            f"</div>"
        )
        st.caption(
            "A supplement is a late filing or correction to the Register, made under "
            "Section 29 of the Ethics Acts. It is a matter of public record, not a "
            "finding — first or consolidated filings can also cover several years."
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

    # ── Category sections — reshaped from the diff-tagged declarations view ────
    # present_by_cat / removed_by_cat are a pure presentation regrouping of the
    # already-deduped, already-diff-tagged rows the view returns. No dedup, set
    # maths, or counting happens here any more.
    year_decls = decl_df[decl_df["declaration_year"] == selected_year] if not decl_df.empty else decl_df
    present_by_cat: dict[str, list[tuple[str, str]]] = {}
    removed_by_cat: dict[str, list[str]] = {}
    for _, r in year_decls.iterrows():
        cat = r["interest_category"]
        text = r["interest_text"]
        if r["change_status"] == "removed":
            removed_by_cat.setdefault(cat, []).append(text)
        else:
            present_by_cat.setdefault(cat, []).append((text, r["change_status"]))

    ordered_cats = list(INTEREST_CATEGORY_ORDER) + [c for c in present_by_cat if c not in INTEREST_CATEGORY_ORDER]
    cats_with_data = [cat for cat in ordered_cats if present_by_cat.get(cat)]
    cats_empty = [cat for cat in INTEREST_CATEGORY_ORDER if not present_by_cat.get(cat)]

    if not cats_with_data:
        empty_state(
            f"Nothing declared for {selected_year}",
            "No interest declarations recorded for this member in this year.",
        )
    else:
        for cat in cats_with_data:
            items = present_by_cat[cat]
            label = INTEREST_CATEGORY_LABELS.get(cat, cat)

            st.html(f'<p class="int-category-section">{_h(label)}&nbsp;&nbsp;·&nbsp;&nbsp;{len(items)}</p>')

            if show_diff and has_prior:
                for text, status in items:
                    interest_declaration_item(text, status)
                for text in sorted(removed_by_cat.get(cat, [])):
                    interest_declaration_item(text, "removed")
            else:
                for text, _status in items:
                    interest_declaration_item(text, "unchanged")

        # Categories that existed in prior year but have nothing in current year
        if show_diff and has_prior:
            for cat in INTEREST_CATEGORY_ORDER:
                if cat in cats_with_data:
                    continue
                rem = removed_by_cat.get(cat, [])
                if not rem:
                    continue
                label = INTEREST_CATEGORY_LABELS.get(cat, cat)
                st.html(
                    f'<p class="int-category-section">{_h(label)}&nbsp;&nbsp;·&nbsp;&nbsp;'
                    f'0 <span style="font-weight:400;text-transform:none;font-size:0.75rem;">'
                    f"(all removed)</span></p>"
                )
                for text in sorted(rem):
                    interest_declaration_item(text, "removed")

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
