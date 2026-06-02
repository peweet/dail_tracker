"""
TD Payments — payments.py

Retrieval-only Streamlit page. All parsing, aggregation, and ranking live in
sql_views/payments_*.sql (pipeline layer). All data access functions live in
utility/data_access/payments_data.py.

This file: layout, controls, HTML card rendering, and navigation only.
No groupby, merge, pivot, or metric definitions here.

TODO_PIPELINE_VIEW_REQUIRED: per-year source PDF URL on v_payments_sources
TODO_PIPELINE_VIEW_REQUIRED: canonical unique_member_code on payments views — required
    for cross-page member-name links via utility/ui/entity_links.member_link_html.
    Until then this page cannot link member names out to /member-overview.
TODO_PIPELINE_VIEW_REQUIRED: party_name and constituency — not present in payments source CSV
"""

from __future__ import annotations

import datetime as _dt
import re
import sys
from html import escape as _h
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_access.payments_data import (
    fetch_alltime_ranking,
    fetch_filter_options,
    fetch_payments_summary,
    fetch_since_2020_summary,
    fetch_year_ranking,
)
from shared_css import inject_css
from ui.components import (
    clean_meta,
    clickable_card_link,
    empty_state,
    glossary_strip,
    hero_banner,
    hide_sidebar,
    member_jump_panel,
    page_error_boundary,
    ranked_member_card,
    totals_strip,
)
from data_access.identity_resolver import resolve_member_code
from ui.components import member_moved_callout
from ui.entity_links import member_profile_url
from ui.export_controls import export_button
from ui.source_pdfs import PAYMENTS, provenance_expander

from config import NOTABLE_TDS, TAA_BAND_TABLE, TAA_DEDUCTIONS_NOTE

# ── Constants ──────────────────────────────────────────────────────────────────

_CAVEAT = (
    "Parliamentary Standard Allowance (PSA) payments cover the cost of carrying out "
    "parliamentary duties. The amount a TD receives is primarily determined by their "
    "TAA distance band — the measured road distance from their normal place of residence "
    "to Leinster House. A higher total does not imply wrongdoing; it reflects living "
    "farther from Dublin. Totals shown here cover both the TAA-banded travel allowance "
    "and the vouched Public Representation Allowance (PRA), including the ministerial "
    "PRA rate where applicable. Data sourced from official Oireachtas payment records."
)

_QUARANTINE_NOTE = (
    "**Data quality notice:** A small number of payment rows fall outside the expected "
    "schema (malformed cells, illegible amounts) and are excluded from this view. The "
    "excluded count is published in `payments_full_psa_quarantine.parquet` and is "
    "typically under 1% of all rows."
)


def _flip_name(raw: str) -> str:
    """'Collins, Michael' → 'Michael Collins'. Pass-through if no comma."""
    if ", " in raw:
        last, first = raw.split(", ", 1)
        return f"{first.strip()} {last.strip()}"
    return raw


def _clean_taa_label(raw: str) -> tuple[str, bool]:
    """Strip the internal '(unmapped)' / '(unknown)' parentheticals from
    TAA band labels so citizens don't see system jargon. Returns
    ``(clean_label, is_unmapped)`` — the second flag drives a small
    caveat pill on the card so users know the distance band isn't
    derived from the current registry (P1-6 audit fix)."""
    is_unmapped = bool(re.search(r"\((?:unmapped|unknown)\)", raw))
    clean = re.sub(r"\s*\((?:unmapped|unknown)\)\s*$", "", raw).strip() or raw
    return clean, is_unmapped


def _pay_card_html(row: pd.Series) -> str:
    """Member name card for the payments ranked list.

    Data ships names "Last, First" (sortable but unidiomatic) and TAA labels
    with "(unmapped)" / "(unknown)" parentheticals (internal pipeline
    metadata). Both are normalised here for display. Unmapped bands carry a
    small caveat glyph + tooltip so the uncertainty is visible without dev
    jargon; mapped bands stay clean.
    """
    name = _flip_name(str(row.get("member_name", "—")))
    pos = str(row.get("position", "Deputy"))
    party = str(row.get("party_name", "") or "")
    constit = str(row.get("constituency", "") or "")
    taa_label, taa_unmapped = _clean_taa_label(str(row.get("taa_band_label", "—")))
    taa = _h(taa_label)
    count = int(row.get("payment_count", 0) or 0)
    total_str = f"€{float(row.get('total_paid', 0) or 0):,.0f}"
    taa_pill_cls = "pay-taa-pill pay-taa-pill-unmapped" if taa_unmapped else "pay-taa-pill"
    taa_caveat = (
        '<span class="pay-taa-caveat" title="Distance band not mapped in the '
        'current registry — verified using the recorded TAA value instead.">?</span>'
        if taa_unmapped
        else ""
    )
    pills = (
        f'<span class="{taa_pill_cls}">{taa}{taa_caveat}</span>'
        f'<span class="pay-count-pill-accent">{count} payments</span>'
    )
    badge = (
        f'<div class="pay-total-badge">'
        f'<span class="pay-total-badge-num">{total_str}</span>'
        f'<span class="pay-total-badge-lbl">total</span>'
        f"</div>"
    )
    return ranked_member_card(
        name=name,
        meta=clean_meta(party, constit) or pos,
        rank=int(row.get("rank_high", 0)),
        pills_html=pills,
        badge_html=badge,
    )


# ── Provenance footer ──────────────────────────────────────────────────────────


def _render_provenance(summary: pd.Series, year: int | None = None) -> None:
    first_year = summary.get("first_year", "2020")
    last_year = summary.get("last_year", "—")
    year_str = str(year) if year else None
    links = [(lbl, url) for lbl, url in PAYMENTS if not year_str or year_str in lbl]
    provenance_expander(
        sections=[
            _CAVEAT,
            "**TAA distance bands**\n\n" + TAA_BAND_TABLE,
            TAA_DEDUCTIONS_NOTE,
            _QUARANTINE_NOTE,
        ],
        source_caption=(
            f"Data: Oireachtas Parliamentary Standard Allowance records · {first_year}–{last_year}"
            + (f" · Showing {year}" if year else "")
        ),
        pdf_links=links,
    )


# ── Rankings view (all-time since 2020) ───────────────────────────────────────


def _render_rankings(since_2020: dict, summary: pd.Series) -> None:
    total = since_2020["total"]
    members = since_2020["members"]
    avg = since_2020["avg_per_td"]

    totals_strip(
        [
            (f"€{total:,.0f}", "Total since 2020"),
            (f"{members:,}", "TDs with payments"),
            (f"€{avg:,.0f}", "Avg per TD since 2020"),
        ]
    )

    alltime = fetch_alltime_ranking()
    if alltime.empty:
        empty_state(
            "All-time rankings not yet available",
            "v_payments_alltime_ranking returned no rows. Re-run the pipeline if you expect data here.",
        )
        _render_provenance(summary)
        return

    st.caption(f"All-time rankings · since 2020 · {len(alltime)} members")

    # The new view exposes the same columns the year-view card already
    # reads, with two name aliases: total_paid_since_2020 → total_paid and
    # payment_count_since_2020 → payment_count. Aliasing here keeps the
    # _pay_card_html helper unchanged.
    top_10 = alltime.head(10).copy()
    next_10 = alltime.iloc[10:20].copy()
    for chunk in (top_10, next_10):
        chunk["total_paid"] = chunk["total_paid_since_2020"]
        chunk["payment_count"] = chunk["payment_count_since_2020"]

    col_l, col_r = st.columns(2)
    for col, chunk in ((col_l, top_10), (col_r, next_10)):
        with col:
            cards: list[str] = []
            for _, row in chunk.iterrows():
                # unique_member_code comes straight from the view (pipeline
                # join), so we don't need to round-trip through a name lookup
                # against v_member_registry — payment names are "Last, First"
                # and accent-stripped, which never exact-matches the registry.
                raw_name = str(row["member_name"])
                display_name = _flip_name(raw_name)
                code = str(row.get("unique_member_code", "") or "").strip() or resolve_member_code(raw_name)
                inner = _pay_card_html(row)
                if code:
                    cards.append(
                        clickable_card_link(
                            href=member_profile_url(code, section="payments"),
                            inner_html=inner,
                            aria_label=f"View {display_name}'s payments profile",
                        )
                    )
                else:
                    cards.append(inner)
            st.html("\n".join(cards))

    _render_provenance(summary)


# ── Stage 1 — Primary ranked view ─────────────────────────────────────────────


def _render_primary(year_options: list[str], summary: pd.Series) -> None:
    # Hero + glossary are rendered by the caller (payments_page) so the
    # main-panel member jump can sit between them and the view controls.
    all_views = ["Rankings"] + year_options
    # Default to the most-recent COMPLETED year, not the current YTD year
    # (audit P1-1). year_options is sorted DESC, so skip the first option
    # if it matches the current calendar year.
    current_year_str = str(_dt.date.today().year)
    default_year = year_options[1] if len(year_options) > 1 and year_options[0] == current_year_str else year_options[0]
    selected_view = (
        st.segmented_control(
            "View",
            all_views,
            default=default_year,
            key="pay_view",
            label_visibility="collapsed",
        )
        or default_year
    )

    since_2020 = fetch_since_2020_summary()

    if selected_view == "Rankings":
        _render_rankings(since_2020, summary)
        return

    selected_year = int(selected_view)
    ranking = fetch_year_ranking(selected_year)

    if ranking.empty:
        empty_state(
            "No payment data for this year",
            "The selected year has no records in the current dataset.",
        )
        _render_provenance(summary, selected_year)
        return

    total_yr = float(ranking.iloc[0]["year_total_paid"])
    yr_count = int(ranking.iloc[0]["year_member_count"])
    avg_yr = float(ranking.iloc[0]["year_avg_per_td"])

    totals_strip(
        [
            (f"€{total_yr:,.0f}", f"Total · {selected_year}"),
            (f"€{avg_yr:,.0f}", f"Avg per TD · {selected_year}"),
        ]
    )

    st.caption(f"Ranked by total PSA received · {selected_year} · {yr_count} members")

    top_10 = ranking.head(10)
    next_10 = ranking.iloc[10:20]

    col_l, col_r = st.columns(2)
    for col, chunk in ((col_l, top_10), (col_r, next_10)):
        with col:
            cards: list[str] = []
            for _, row in chunk.iterrows():
                name = str(row["member_name"])
                # Pipeline ships unique_member_code on v_payments_yearly_evolution
                # for ~97% of TDs; name-based resolver is the last-resort fallback
                # (and is broken for the "Last, First" + accent-stripped format
                # the payments parquet uses — every card was non-clickable before).
                code = str(row.get("unique_member_code", "") or "").strip() or resolve_member_code(name)
                if code:
                    cards.append(
                        clickable_card_link(
                            href=member_profile_url(code, section="payments"),
                            inner_html=_pay_card_html(row),
                            aria_label=f"View {_flip_name(name)}'s payments profile",
                        )
                    )
                else:
                    # Member not in v_member_registry — render unwrapped.
                    cards.append(_pay_card_html(row))
            st.html("\n".join(cards))

    export_df = ranking[
        ["rank_high", "member_name", "position", "taa_band_label", "total_paid", "payment_count"]
    ].copy()
    export_df.columns = ["Rank", "Member", "Position", "TAA Band", "Total Paid (€)", "Payments"]
    export_button(
        export_df,
        f"Download {selected_year} payments CSV",
        f"td_payments_{selected_year}.csv",
        key="pay_export_primary",
    )

    _render_provenance(summary, selected_year)


# ── Entry point ────────────────────────────────────────────────────────────────


@page_error_boundary
def payments_page() -> None:
    inject_css()

    summary = fetch_payments_summary()
    opts = fetch_filter_options()

    year_options = opts.get("years", [])
    if not year_options:
        st.error(
            "No payment data available. "
            "Ensure sql_views/payments_*.sql are present and the DuckDB connection is loaded."
        )
        return

    # ── Page header ────────────────────────────────────────────────────────────
    # Sidebar→filter-bar migration: identity via top-nav + hero; the member
    # picker + notable chips move into a main-panel jump under the hero.
    hide_sidebar()
    hero_banner(
        kicker="PUBLIC SPENDING · PARLIAMENTARY ALLOWANCES",
        title="TD Payments",
        dek="Parliamentary Standard Allowance (PSA): the official record of payments to Dáil members.",
    )
    glossary_strip(
        [
            ("TD", "Teachta Dála, a member of the Dáil"),
            ("TAA", "Travel & Accommodation Allowance, reimbursed mileage and overnight stays"),
            ("PRA", "Public Representation Allowance, an unvouched flat allowance for constituency work"),
            ("PSA", "Parliamentary Standard Allowance, the umbrella term for TAA plus PRA"),
        ]
    )

    # ── Member jump (was the sidebar) ───────────────────────────────────────────
    picked = member_jump_panel(
        opts["members"],
        search_key_prefix="pay",
        session_key="selected_td_pay",
        label="Browse all members",
        placeholder="e.g. Mary Lou McDonald",
        notable=NOTABLE_TDS,
        chip_key_prefix="pay_notable",
    )
    if picked and st.session_state.get("selected_td_pay") != picked:
        st.session_state["selected_td_pay"] = picked
        st.rerun()

    # Legacy ?member=<name> URLs AND member-jump selections both redirect
    # to the canonical /member-overview?member=<code>#payments profile.
    # Shared helper resolves the real unique_member_code, scrubs state, and
    # calls st.stop() so the rankings page body doesn't render under the
    # callout (round-3 audit P0-3).
    qp_member = st.query_params.get("member")
    if qp_member:
        member_moved_callout(
            qp_member,
            section="payments",
            section_label="Per-TD payments",
            legacy_param="member",
            state_keys=("selected_td_pay",),
        )

    selected_td = st.session_state.get("selected_td_pay")
    if selected_td:
        member_moved_callout(
            selected_td,
            section="payments",
            section_label="Per-TD payments",
            state_keys=("selected_td_pay",),
        )

    _render_primary(year_options, summary)
