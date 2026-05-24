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

import sys
from html import escape as _h
from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).parent.parent))

from data_access.payments_data import (
    fetch_alltime_ranking,
    fetch_filter_options,
    fetch_member_all_years,
    fetch_member_payments,
    fetch_member_year_summary,
    fetch_payments_summary,
    fetch_since_2020_summary,
    fetch_year_ranking,
)
from shared_css import inject_css
from ui.avatars import avatar_data_url, initials as _initials
from ui.components import (
    back_button,
    clean_meta,
    clickable_card_link,
    empty_state,
    glossary_strip,
    hero_banner,
    member_card_html,
    page_error_boundary,
    render_notable_chips,
    sidebar_member_filter,
    year_selector,
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


def _clean_taa_label(raw: str) -> str:
    """Strip the internal '(unmapped)' / '(unknown)' parentheticals from
    TAA band labels so citizens don't see system jargon. Returns "Band X"
    or the original string if no parenthetical to strip."""
    import re
    return re.sub(r"\s*\((?:unmapped|unknown)\)\s*$", "", raw).strip() or raw


def _pay_card_html(row: pd.Series) -> str:
    """Member name card for the payments ranked list, built on the canonical
    dt-name-card pattern.

    Round-3 audit P1-B fix: names / pos / party / constituency are passed
    as RAW strings to ``member_card_html``, which applies ``_h()`` once.
    Previously every field was pre-escaped here AND again inside the
    component → ``O'Sullivan`` rendered as ``O&#x27;Sullivan`` on screen.
    Only ``taa`` is escaped because it goes into ``pills_html`` (a raw
    HTML slot) below.

    Round-3 audit P2-3 / P2-4: data ships names "Last, First" (sortable
    but unidiomatic) and TAA labels with "(unmapped)" parentheticals
    (internal pipeline metadata). Both are normalised here for display.
    """
    name = _flip_name(str(row.get("member_name", "—")))
    pos = str(row.get("position", "Deputy"))
    party = str(row.get("party_name", "") or "")
    constit = str(row.get("constituency", "") or "")
    taa = _h(_clean_taa_label(str(row.get("taa_band_label", "—"))))
    count = int(row.get("payment_count", 0) or 0)
    total_str = f"€{float(row.get('total_paid', 0) or 0):,.0f}"
    meta = clean_meta(party, constit) or pos
    pills = f'<span class="pay-taa-pill">{taa}</span><span class="pay-count-pill-accent">{count} payments</span>'
    badge = (
        f'<div class="pay-total-badge">'
        f'<span class="pay-total-badge-num">{total_str}</span>'
        f'<span class="pay-total-badge-lbl">total</span>'
        f"</div>"
    )
    return member_card_html(
        name=name,
        meta=meta,
        rank=int(row.get("rank_high", 0)),
        pills_html=pills,
        badge_html=badge,
        avatar_url=avatar_data_url(name),
        avatar_initials=_initials(name),
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

    c1, c2, c3 = st.columns(3)
    c1.metric("Total since 2020", f"€{total:,.0f}")
    c2.metric("TDs with payments", members)
    c3.metric("Avg per TD since 2020", f"€{avg:,.0f}")

    alltime = fetch_alltime_ranking()
    if alltime.empty:
        empty_state(
            "All-time rankings not yet available",
            "current_td_payment_rankings.parquet not found — run the pipeline to generate it.",
        )
        _render_provenance(summary)
        return

    st.caption(f"All-time rankings · since 2020 · {len(alltime)} members")

    name_col = "member_name" if "member_name" in alltime.columns else None
    amt_col = "total_amount_paid_since_2020"

    # TODO_PIPELINE_VIEW_REQUIRED: taa_band_label in current_td_payment_rankings.parquet
    # — parquet currently has ['rank','member_name','join_key','total_amount_paid_since_2020'] only
    has_taa = "taa_band_label" in alltime.columns

    top_10 = alltime.head(10)
    next_10 = alltime.iloc[10:20]

    col_l, col_r = st.columns(2)
    for col, chunk, offset in ((col_l, top_10, 0), (col_r, next_10, 10)):
        with col:
            for i, (_, row) in enumerate(chunk.iterrows()):
                rank = offset + i + 1
                name = _h(str(row.get(name_col, "—")) if name_col else "—")
                amt = float(row.get(amt_col, 0) or 0)
                rank_cls = "dt-name-card-rank-top" if rank <= 3 else "dt-name-card-rank"
                taa_pill = f'<span class="pay-taa-pill">{_h(str(row["taa_band_label"]))}</span>' if has_taa else ""
                card_html = (
                    f'<div class="dt-name-card">'
                    f'<div class="dt-name-card-left"><span class="{rank_cls}">#{rank}</span></div>'
                    f'<div class="dt-name-card-body">'
                    f'<div class="dt-name-card-name">{name}</div>'
                    f'<div class="dt-name-card-meta">{taa_pill}</div>'
                    f"</div>"
                    f'<div class="dt-name-card-badge dt-name-card-badge-metric">'
                    f'<span class="dt-name-card-badge-num">€{amt:,.0f}</span>'
                    f'<span class="dt-name-card-badge-lbl">total</span>'
                    f"</div>"
                    f"</div>"
                )
                st.markdown(card_html, unsafe_allow_html=True)

    _render_provenance(summary)


# ── Stage 1 — Primary ranked view ─────────────────────────────────────────────


def _render_primary(year_options: list[str], summary: pd.Series) -> None:
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

    all_views = ["Rankings"] + year_options
    selected_view = (
        st.segmented_control(
            "View",
            all_views,
            default=year_options[0],
            key="pay_view",
            label_visibility="collapsed",
        )
        or year_options[0]
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

    st.html(
        f'<div class="pay-totals-strip">'
        f'<div class="pay-totals-item">'
        f'<span class="pay-totals-num">€{total_yr:,.0f}</span>'
        f'<span class="pay-totals-lbl">Total · {selected_year}</span>'
        f"</div>"
        f'<div class="pay-totals-divider"></div>'
        f'<div class="pay-totals-item">'
        f'<span class="pay-totals-num">€{avg_yr:,.0f}</span>'
        f'<span class="pay-totals-lbl">Avg per TD · {selected_year}</span>'
        f"</div>"
        f"</div>"
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
                code = resolve_member_code(name)
                if code:
                    cards.append(
                        clickable_card_link(
                            href=member_profile_url(code, section="payments"),
                            inner_html=_pay_card_html(row),
                            aria_label=f"View {name}'s payments profile",
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


# ── Member profile body (lifted into member-overview Payments expander) ───────


def render_member_payments(
    td_name: str,
    year_options: list[str],
    summary: pd.Series,
    *,
    show_member_header: bool = True,
    year_pill_key: str = "pay_profile_year",
) -> None:
    """Render the per-TD payments body.

    Public so :mod:`pages_code.member_overview` can embed it inside the
    Payments expander. When ``show_member_header=False``, the back button,
    payments-specific identity card and provenance footer are skipped (the
    embedding page provides those), and the two ``st.dataframe`` views are
    converted to card lists (``.pay-record-card`` + a compact ``.pay-year-row``)
    — required by feedback_member_overview_no_dataframes.
    """
    if show_member_header and back_button("← Back to all members", key="pay"):
        st.session_state.pop("selected_td_pay", None)
        st.query_params.pop("member", None)
        st.rerun()

    selected_year = year_selector(year_options, key=year_pill_key, skip_current=False)

    all_years = fetch_member_all_years(td_name)

    if all_years.empty:
        empty_state("No data found", f"No payment records found for {td_name}.")
        if show_member_header:
            _render_provenance(summary)
        return

    latest = all_years.iloc[0]
    taa_label = str(latest.get("taa_band_label", "—"))
    position = str(latest.get("position", "Deputy"))
    party = str(latest.get("party_name", "") or "")
    constit = str(latest.get("constituency", "") or "")
    meta_str = clean_meta(party, constit) or position

    if show_member_header:
        # Identity strip — duplicates the member-overview hero when embedded,
        # so only rendered on the stand-alone /rankings-payments page.
        st.markdown(
            f'<div class="pay-identity-card">'
            f'<div class="pay-identity-card-name">{_h(td_name)}</div>'
            f'<div class="pay-identity-card-meta">'
            f"{_h(meta_str)} &nbsp;·&nbsp; "
            f'<span class="pay-taa-pill">{_h(taa_label)}</span>'
            f"</div></div>",
            unsafe_allow_html=True,
        )

    # Summary metrics
    alltime_total = float(all_years.iloc[0]["member_alltime_total"])
    yr_df = fetch_member_year_summary(td_name, selected_year)

    if not yr_df.empty:
        yr = yr_df.iloc[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total received", f"€{float(yr['total_paid']):,.0f}")
        col2.metric("Payments", int(yr["payment_count"]))
        col3.metric("Year rank", f"#{int(yr['rank_high'])}")
        col4.metric("All-time total", f"€{alltime_total:,.0f}")
    else:
        empty_state("No payment records", f"No payment records for {td_name} in {selected_year}.")
        col1, col2 = st.columns(2)
        col1.metric("TAA band", taa_label)
        col2.metric("All-time total", f"€{alltime_total:,.0f}")

    # Yearly evolution chart — chronological, left-to-right
    chart_df = all_years[["payment_year", "total_paid"]].sort_values("payment_year")
    bars = (
        alt.Chart(chart_df)
        .mark_bar(color="#1e40af", cornerRadiusTopLeft=3, cornerRadiusTopRight=3)
        .encode(
            x=alt.X("payment_year:O", title=None, axis=alt.Axis(labelAngle=0)),
            y=alt.Y("total_paid:Q", title="Total received (€)", axis=alt.Axis(format=",.0f")),
            tooltip=[
                alt.Tooltip("payment_year:O", title="Year"),
                alt.Tooltip("total_paid:Q", title="Total received (€)", format=",.0f"),
            ],
        )
        .properties(height=180)
    )
    st.altair_chart(bars, use_container_width=True)

    # ── All-years summary ────────────────────────────────────────────────
    st.markdown("**All years**")
    if show_member_header:
        # Stand-alone page: full dataframe (drill-down + export adjacency OK
        # on /rankings-payments per feedback_dataframes_secondary_only).
        years_display = all_years.rename(
            columns={
                "payment_year": "Year",
                "total_paid": "Total received (€)",
                "payment_count": "Payments",
                "rank_high": "Rank that year",
                "taa_band_label": "TAA Band",
            }
        )[["Year", "Total received (€)", "Payments", "Rank that year", "TAA Band"]]
        st.dataframe(
            years_display,
            hide_index=True,
            use_container_width=True,
            column_config={
                "Year": st.column_config.NumberColumn("Year", format="%d"),
                "Total received (€)": st.column_config.NumberColumn("Total received (€)", format="€%.2f"),
                "Payments": st.column_config.NumberColumn("Payments"),
                "Rank that year": st.column_config.NumberColumn("Rank that year", format="#%d"),
                "TAA Band": st.column_config.TextColumn("TAA Band"),
            },
        )
    else:
        # Embedded: card-based per feedback_member_overview_no_dataframes —
        # one compact row per year, total + payments + rank.
        rows_html: list[str] = []
        for _, row in all_years.iterrows():
            yr_num = int(row["payment_year"])
            tot = float(row["total_paid"])
            cnt = int(row["payment_count"])
            rk = row.get("rank_high")
            rk_html = (
                f'<span class="pay-year-rank">#{int(rk)}</span>'
                if pd.notna(rk)
                else '<span class="pay-year-rank pay-year-rank-missing">—</span>'
            )
            rows_html.append(
                f'<div class="pay-year-row">'
                f'<span class="pay-year-yr">{yr_num}</span>'
                f'<span class="pay-year-amount">€{tot:,.0f}</span>'
                f'<span class="pay-year-payments">{cnt} payment{"s" if cnt != 1 else ""}</span>'
                f"{rk_html}"
                f"</div>"
            )
        st.html(f'<div class="pay-year-list">{"".join(rows_html)}</div>')

    # ── Payment records (audit trail) ────────────────────────────────────
    payments = fetch_member_payments(td_name, selected_year)

    if payments.empty:
        empty_state(
            "No payment records",
            f"No individual records for {td_name} in {selected_year}.",
        )
    else:
        st.html(
            f"<p style='margin:0.75rem 0 0.4rem;'><strong>Payment records — {selected_year}</strong> "
            f"<span style='font-size:0.8rem;color:var(--text-meta);font-weight:400;'>"
            f"({len(payments)} transactions — add them up to verify the total above)</span></p>"
        )

        if show_member_header:
            # Stand-alone page keeps the full sortable dataframe.
            st.dataframe(
                payments.rename(
                    columns={
                        "date_paid": "Date",
                        "narrative": "Description",
                        "amount_num": "Amount (€)",
                        "taa_band_label": "TAA Band",
                    }
                ),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "Date": st.column_config.DateColumn("Date", format="D MMM YYYY"),
                    "Amount (€)": st.column_config.NumberColumn("Amount (€)", format="€%.2f"),
                    "Description": st.column_config.TextColumn("Description"),
                    "TAA Band": st.column_config.TextColumn("TAA Band"),
                },
            )
        else:
            # Embedded: card list. Truncate to first 50 to keep the expander
            # body light; CSV export below remains the full set.
            cards_html: list[str] = []
            for _, row in payments.head(50).iterrows():
                date_raw = row.get("date_paid")
                try:
                    date_disp = pd.to_datetime(date_raw).strftime("%d %b %Y")
                except Exception:
                    date_disp = str(date_raw or "—")
                desc = _h(str(row.get("narrative", "") or "—"))
                amount = float(row.get("amount_num", 0) or 0)
                band = _h(str(row.get("taa_band_label", "") or ""))
                band_html = f'<span class="signal leg-status-active">{band}</span>' if band else ""
                cards_html.append(
                    f'<div class="pay-record-card">'
                    f'<div class="pay-record-card-header">'
                    f'<span class="pay-record-card-date">{_h(date_disp)}</span>'
                    f'<span class="pay-record-card-amount">€{amount:,.2f}</span>'
                    f"{band_html}"
                    f"</div>"
                    f'<div class="pay-record-card-desc">{desc}</div>'
                    f"</div>"
                )
            st.html("".join(cards_html))
            if len(payments) > 50:
                st.caption(f"Showing the most recent 50 of {len(payments)} transactions. Full set in the CSV below.")

        export_df = payments.rename(
            columns={
                "date_paid": "Date",
                "narrative": "Description",
                "amount_num": "Amount (€)",
                "taa_band_label": "TAA Band",
            }
        )
        export_button(
            export_df,
            f"Download {td_name} {selected_year} payments CSV",
            f"{td_name.replace(' ', '_')}_payments_{selected_year}.csv",
            key=f"pay_export_profile{'_mo' if not show_member_header else ''}",
        )

    st.caption("Source PDF for this year will link directly to the official Oireachtas payment record once available.")

    if show_member_header:
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

    # ── Sidebar ───────────────────────────────────────────────────────────────
    with st.sidebar:
        chosen = sidebar_member_filter(
            "Browse all members",
            opts["members"],
            key_search="pay_sidebar_search",
            key_select="pay_member_sel",
            placeholder="e.g. Mary Lou McDonald",
        )
        if chosen and st.session_state.get("selected_td_pay") != chosen:
            st.session_state["selected_td_pay"] = chosen
            st.rerun()

        st.divider()
        if render_notable_chips(NOTABLE_TDS, opts["members"], "pay_notable", "selected_td_pay"):
            st.rerun()

    # Legacy ?member=<name> URLs AND sidebar-driven selections both redirect
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
