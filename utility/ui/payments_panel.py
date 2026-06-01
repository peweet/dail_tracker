"""Per-TD payments panel — embedded in the /member-overview Payments section.

Extracted from ``pages_code/payments.py`` (2026-06-01) so member-overview no
longer imports a render body out of another page. Pure rendering + data-access
retrieval, no business logic — mirrors ``ui/vote_explorer.py``.
"""

from __future__ import annotations

from html import escape as _h

import altair as alt
import pandas as pd
import streamlit as st
from data_access.payments_data import (
    fetch_member_all_years,
    fetch_member_payments,
    fetch_member_year_summary,
)
from ui.components import empty_state, year_selector
from ui.export_controls import export_button


def render_member_payments(
    td_name: str,
    year_options: list[str],
    summary: pd.Series,
    *,
    show_member_header: bool = False,
    year_pill_key: str = "pay_profile_year",
    unique_member_code: str | None = None,
) -> None:
    """Render the per-TD payments body embedded inside /member-overview.

    The ``show_member_header`` kwarg is retained for API compatibility but
    is no longer load-bearing: every reachable caller (member_overview)
    passes False, and the legacy True paths — back button, identity strip,
    full-width ``st.dataframe`` views, provenance footer — were dead code
    that also violated ``feedback_dataframes_secondary_only``. Removed
    2026-05-27.
    """
    selected_year = year_selector(year_options, key=year_pill_key, skip_current=False)

    all_years = fetch_member_all_years(td_name, unique_member_code=unique_member_code)

    if all_years.empty:
        empty_state("No data found", f"No payment records found for {td_name}.")
        return

    latest = all_years.iloc[0]
    _taa_raw = latest.get("taa_band_label")
    taa_label = str(_taa_raw) if pd.notna(_taa_raw) and str(_taa_raw).strip() else "—"

    # Summary metrics
    alltime_total = float(all_years.iloc[0]["member_alltime_total"])
    yr_df = fetch_member_year_summary(td_name, selected_year, unique_member_code=unique_member_code)

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
    st.altair_chart(bars, width="stretch")

    # ── All-years summary (card-based per feedback_member_overview_no_dataframes) ──
    st.markdown("**All years**")
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
    payments = fetch_member_payments(td_name, selected_year, unique_member_code=unique_member_code)

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

        # Card list. Truncate to first 50 to keep the expander body light;
        # the CSV export below ships the full set.
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
            key="pay_export_profile_mo",
        )

    st.caption("Source PDF for this year will link directly to the official Oireachtas payment record once available.")
