"""
TD Payments — payments.py

Retrieval-only Streamlit page. All parsing, aggregation, and ranking live in
sql_views/payments_*.sql (pipeline layer). All data access functions live in
utility/data_access/payments_data.py.

This file: layout, controls, HTML card rendering, and navigation only.
No groupby, merge, pivot, or metric definitions here.

TODO_PIPELINE_VIEW_REQUIRED: per-year source PDF URL on v_payments_sources
TODO_PIPELINE_VIEW_REQUIRED: fix malformed/shifted-column rows in aggregated_payment_tables.csv
TODO_PIPELINE_VIEW_REQUIRED: normalise all TAA_Band values (Vouched, MIN, NoTAA, combined codes)
TODO_PIPELINE_VIEW_REQUIRED: canonical member_id for cross-page linking
TODO_PIPELINE_VIEW_REQUIRED: party_name and constituency — not present in payments source CSV
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_access.payments_data import (
    fetch_filter_options,
    fetch_member_all_years,
    fetch_member_payments,
    fetch_member_year_summary,
    fetch_payments_summary,
    fetch_since_2020_summary,
    fetch_year_ranking,
)
from shared_css import inject_css
from ui.components import empty_state, hero_banner
from ui.export_controls import export_button
from ui.source_pdfs import PAYMENTS, render_pdf_source_links

# ── Constants ──────────────────────────────────────────────────────────────────

_NOTABLE_TDS: list[str] = [
    "Mary Lou McDonald",
    "Micheál Martin",
    "Simon Harris",
    "Leo Varadkar",
    "Pearse Doherty",
    "Eamon Ryan",
    "Michael Healy-Rae",
    "Danny Healy-Rae",
    "Michael Collins",
    "Michael Lowry",
    "Marian Harkin",
    "Holly Cairns",
]

_CAVEAT = (
    "Parliamentary Standard Allowance (PSA) payments cover the cost of carrying out "
    "parliamentary duties. The amount a TD receives is primarily determined by their "
    "TAA distance band — the measured road distance from their normal place of residence "
    "to Leinster House. A higher total does not imply wrongdoing; it reflects living "
    "farther from Dublin. The Public Representation Allowance (PRA) component is the same "
    "for all members. Data sourced from official Oireachtas payment records."
)

_TAA_TABLE = """\
| Band | Distance from Leinster House |
|---|---|
| Dublin | Under 25 km — no Travel & Accommodation Allowance |
| Band 1 | 25–60 km |
| Band 2 | 60–80 km |
| Band 3 | 80–100 km |
| Band 4 | 100–130 km |
| Band 5 | 130–160 km |
| Band 6 | 160–190 km |
| Band 7 | 190–210 km |
| Band 8 | Over 210 km — highest TAA rate |
"""

_DEDUCTIONS_NOTE = (
    "PSA payments are linked to attendance. Under Oireachtas rules, members must attend "
    "a minimum of **120 sitting days per year** to receive the full TAA. For each day "
    "below that threshold, **1% of the annual TAA is deducted**. Certain absences are "
    "excused — committee work, official duties abroad, certified ill-health — so a lower "
    "TAA does not necessarily mean a member was absent."
)

_QUARANTINE_NOTE = (
    "**Data quality notice:** A portion of payment records in the source data contain "
    "unrecognised or legacy TAA band codes (for example, combined codes from older PDF "
    "parsing runs). These rows have been quarantined and excluded from this view pending "
    "pipeline correction. Totals shown may be slightly lower than the full official record "
    "for some members. This is a known pipeline issue and will be resolved."
)

_YEAR_SOURCE_NOTE = (
    "Payment records will link to the official Oireachtas source document for each year "
    "once the pipeline exposes per-year source URLs."
)


# ── Card / badge HTML helpers ──────────────────────────────────────────────────


def _pay_name_pill_html(row: pd.Series, rank: int) -> str:
    """Compact rank card with amount badge embedded — mirrors interests page layout."""
    name      = str(row.get("member_name",    "—"))
    pos       = str(row.get("position",       "Deputy"))
    taa       = str(row.get("taa_band_label", "—"))
    count     = int(row.get("payment_count",  0) or 0)
    total_str = f"€{float(row.get('total_paid', 0) or 0):,.0f}"
    return (
        f'<div class="pay-name-row">'
        f'<span class="pay-name-rank">#{rank}</span>'
        f'<div class="pay-name-body">'
        f'<div class="pay-name-body-name">{name}</div>'
        f'<div class="pay-name-body-pos">{pos}</div>'
        f'<span class="pay-taa-pill">{taa}</span>'
        f'<span class="pay-count-pill">{count} payments</span>'
        f'</div>'
        f'<div class="pay-amount-badge">'
        f'<span class="pay-amount-badge-num">{total_str}</span>'
        f'<span class="pay-amount-badge-label">total</span>'
        f'</div>'
        f'</div>'
    )


# ── Provenance footer ──────────────────────────────────────────────────────────

def _render_provenance(summary: pd.Series, year: int | None = None) -> None:
    source     = str(summary.get("source_summary")             or "Oireachtas Payment Records")
    first_year = summary.get("first_year", "—")
    last_year  = summary.get("last_year",  "—")

    with st.expander("About & data provenance", expanded=False):
        st.markdown(_CAVEAT)

        st.divider()
        st.markdown("**TAA distance bands**")
        st.markdown(_TAA_TABLE)

        st.divider()
        st.markdown(_DEDUCTIONS_NOTE)

        st.divider()
        st.markdown(_QUARANTINE_NOTE)

        if year:
            st.caption(f"Showing data for: {year}. {_YEAR_SOURCE_NOTE}")

        st.caption(
            f"Source: {source}  ·  Dataset covers: {first_year}–{last_year}"
        )

        st.divider()
        year_str = str(year) if year else None
        links = [(lbl, url) for lbl, url in PAYMENTS if not year_str or year_str in lbl]
        st.markdown(
            f"**Source PDFs** — {len(links)} document{'s' if len(links) != 1 else ''} "
            f"({'filtered to ' + str(year) if year_str else 'all years'})"
        )
        render_pdf_source_links(links)
        # TODO_PIPELINE_VIEW_REQUIRED: per-year source PDF URL, fetch timestamp, mart version, code version


# ── Stage 1 — Primary ranked view ─────────────────────────────────────────────

def _render_primary(year_options: list[str], summary: pd.Series) -> None:
    hero_banner(
        kicker="PUBLIC SPENDING · PARLIAMENTARY ALLOWANCES",
        title="TD Payments",
        dek="Parliamentary Standard Allowance (PSA) — the official record of payments to Dáil members.",
    )

    selected_year_str = st.pills(
        "Year",
        options=year_options,
        default=year_options[0],
        key="pay_year",
        label_visibility="collapsed",
    )
    selected_year = int(selected_year_str) if selected_year_str else int(year_options[0])

    ranking    = fetch_year_ranking(selected_year)
    since_2020 = fetch_since_2020_summary()
    total_yr   = float(ranking["total_paid"].sum()) if not ranking.empty else 0.0
    yr_count   = len(ranking)

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total since 2020",      f"€{since_2020['total']:,.0f}")
    c2.metric("TDs with payments",     since_2020["members"])
    c3.metric("Avg per TD since 2020", f"€{since_2020['avg_per_td']:,.0f}")
    c4.metric(f"Total — {selected_year}", f"€{total_yr:,.0f}")

    if ranking.empty:
        empty_state(
            "No payment data for this year",
            "The selected year has no records in the current dataset.",
        )
        _render_provenance(summary, selected_year)
        return

    st.caption(f"Ranked by total PSA received · {selected_year} · {yr_count} members")

    for i, (_, row) in enumerate(ranking.iterrows()):
        c1, c2 = st.columns([5, 1])
        c1.markdown(
            _pay_name_pill_html(row, int(row["rank_high"])),
            unsafe_allow_html=True,
        )
        c2.markdown('<div class="dt-nav-anchor"></div>', unsafe_allow_html=True)
        if c2.button("→", key=f"pay_row_{i}"):
            st.session_state["selected_td_pay"] = str(row["member_name"])
            st.rerun()

    st.markdown("")
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


# ── Stage 2 — Member profile ───────────────────────────────────────────────────

def _render_profile(
    td_name: str,
    year_options: list[str],
    summary: pd.Series,
) -> None:
    if st.button("← Back to all members", key="pay_back"):
        st.session_state.pop("selected_td_pay", None)
        st.rerun()

    selected_year_str = st.pills(
        "Year",
        options=year_options,
        default=year_options[0],
        key="pay_profile_year",
        label_visibility="collapsed",
    )
    selected_year = int(selected_year_str) if selected_year_str else int(year_options[0])

    all_years = fetch_member_all_years(td_name)

    if all_years.empty:
        empty_state("No data found", f"No payment records found for {td_name}.")
        _render_provenance(summary)
        return

    latest    = all_years.iloc[0]
    taa_label = str(latest.get("taa_band_label", "—"))
    position  = str(latest.get("position",       "Deputy"))

    # Identity strip
    st.markdown(
        f'<div class="pay-identity-card">'
        f'<div class="pay-identity-card-name">{td_name}</div>'
        f'<div class="pay-identity-card-meta">'
        f'{position} &nbsp;·&nbsp; '
        f'<span class="pay-taa-pill">{taa_label}</span>'
        f'</div></div>',
        unsafe_allow_html=True,
    )

    # Summary metrics
    alltime_total = float(all_years["total_paid"].sum())
    yr_df = fetch_member_year_summary(td_name, selected_year)

    if not yr_df.empty:
        yr = yr_df.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total received", f"€{float(yr['total_paid']):,.0f}")
        col2.metric("Payments",       int(yr["payment_count"]))
        col3.metric("Year rank",      f"#{int(yr['rank_high'])}")
        col4.metric("All-time total", f"€{alltime_total:,.0f}")
    else:
        st.info(f"No payment records for {td_name} in {selected_year}.")
        col1, col2 = st.columns(2)
        col1.metric("TAA band",       taa_label)
        col2.metric("All-time total", f"€{alltime_total:,.0f}")

    # All-years summary table
    st.markdown("**All years**")
    years_display = all_years.rename(columns={
        "payment_year":  "Year",
        "total_paid":    "Total received (€)",
        "payment_count": "Payments",
        "rank_high":     "Rank that year",
        "taa_band_label":"TAA Band",
    })[["Year", "Total received (€)", "Payments", "Rank that year", "TAA Band"]]
    st.dataframe(
        years_display,
        hide_index=True,
        use_container_width=True,
        column_config={
            "Year":             st.column_config.NumberColumn("Year",             format="%d"),
            "Total received (€)":st.column_config.NumberColumn("Total received (€)", format="€%.2f"),
            "Payments":         st.column_config.NumberColumn("Payments"),
            "Rank that year":   st.column_config.NumberColumn("Rank that year",   format="#%d"),
            "TAA Band":         st.column_config.TextColumn(  "TAA Band"),
        },
    )

    # Payment record table (audit trail)
    payments = fetch_member_payments(td_name, selected_year)

    if payments.empty:
        empty_state(
            "No payment records",
            f"No individual records for {td_name} in {selected_year}.",
        )
    else:
        st.markdown(
            f"**Payment records — {selected_year}** "
            f"<span style='font-size:0.8rem;color:#6b7280;font-weight:400;'>"
            f"({len(payments)} transactions — add them up to verify the total above)</span>",
            unsafe_allow_html=True,
        )
        st.dataframe(
            payments.rename(
                columns={
                    "date_paid":     "Date",
                    "narrative":     "Description",
                    "amount_num":    "Amount (€)",
                    "taa_band_label":"TAA Band",
                }
            ),
            hide_index=True,
            use_container_width=True,
            column_config={
                "Date":       st.column_config.DateColumn(   "Date",        format="D MMM YYYY"),
                "Amount (€)": st.column_config.NumberColumn( "Amount (€)",  format="€%.2f"),
                "Description":st.column_config.TextColumn(   "Description"),
                "TAA Band":   st.column_config.TextColumn(   "TAA Band"),
            },
        )
        export_df = payments.rename(
            columns={
                "date_paid":     "Date",
                "narrative":     "Description",
                "amount_num":    "Amount (€)",
                "taa_band_label":"TAA Band",
            }
        )
        export_button(
            export_df,
            f"Download {td_name} {selected_year} payments CSV",
            f"{td_name.replace(' ', '_')}_payments_{selected_year}.csv",
            key="pay_export_profile",
        )

    st.caption("Source PDF for this year will link directly to the official Oireachtas payment record once available.")

    _render_provenance(summary, selected_year)


# ── Entry point ────────────────────────────────────────────────────────────────

def payments_page() -> None:
    inject_css()

    summary = fetch_payments_summary()
    opts    = fetch_filter_options()

    year_options = opts.get("years", [])
    if not year_options:
        st.error(
            "No payment data available. "
            "Ensure sql_views/payments_*.sql are present and the DuckDB connection is loaded."
        )
        return

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        pay_search: str = st.text_input(
            "",
            placeholder="e.g. Mary Lou McDonald",
            key="pay_sidebar_search",
            label_visibility="collapsed",
        )
        sq = pay_search.strip().lower()
        filtered_members = [m for m in opts["members"] if sq in m.lower()] if sq else opts["members"]
        chosen = st.selectbox(
            "Browse all members",
            ["— select a member —"] + filtered_members,
            key="pay_member_sel",
            label_visibility="collapsed",
        )
        if chosen != "— select a member —" and st.session_state.get("selected_td_pay") != chosen:
            st.session_state["selected_td_pay"] = chosen
            st.rerun()

        st.divider()
        st.markdown("**Notable members**")
        nb_cols = st.columns(2)
        for i, name in enumerate(_NOTABLE_TDS):
            if name in opts["members"] and nb_cols[i % 2].button(
                name.split()[-1],
                key=f"pay_notable_{i}",
                use_container_width=True,
            ):
                st.session_state["selected_td_pay"] = name
                st.rerun()

    # ── Route to Stage 1 or Stage 2 ──────────────────────────────────────────
    selected_td = st.session_state.get("selected_td_pay")

    if selected_td:
        _render_profile(selected_td, year_options, summary)
    else:
        _render_primary(year_options, summary)
