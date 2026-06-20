"""Accommodation spending — what the State pays providers to accommodate people
seeking international protection (asylum) and Ukrainian beneficiaries of temporary
protection.

DISPLAY ONLY. Figures arrive pre-aggregated from registered v_accommodation_spend_*
views via dail_tracker_core.queries.housing; the classification (which spend-categories
count) and the sums live in sql_views/housing/*. This page only renders and labels.

Neutral by design: provider, amount, year, stream, source — no editorialising, no
inference, and NO juxtaposition with the social-housing waiting list (a different
question). The C&AG 2024 denominator is shown so the figures are never read as the
whole, and the coverage gap (2020-2024) is stated plainly.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.housing_data import (
    fetch_accommodation_spend_by_year_result,
    fetch_accommodation_spend_providers_result,
)
from ui.components import empty_state, evidence_heading, hero_banner, hide_sidebar, page_error_boundary, totals_strip

# Authoritative denominator (C&AG 2024, Ch.10) — so our register-based figure is never
# read as the full spend. https://www.audit.gov.ie (Management of IP Accommodation Contracts).
_CAG_2024_COMMERCIAL_EUR = 978_000_000
_CAG_2024_TOTAL_EUR = 1_100_000_000


def _eur(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    v = float(v)
    a = abs(v)
    if a >= 1e9:
        return f"€{v / 1e9:.2f}bn"
    if a >= 1e6:
        return f"€{v / 1e6:.1f}m"
    if a >= 1e3:
        return f"€{v / 1e3:.0f}k"
    return f"€{v:,.0f}"


def _render_by_year(df) -> None:
    total = df["total_eur"].sum()
    yrs = [int(y) for y in df["year"].tolist()]
    totals_strip(
        [
            (_eur(total), "committed to providers (covered years)"),
            (str(int(df["n_providers"].max())), "providers (peak year)"),
            (f"{min(yrs)}–{max(yrs)}", "years with data"),
        ]
    )
    st.caption(
        "Committed = purchase-order amounts from the published over-€20k registers "
        "(PO-committed, not confirmed cash). For 2024 the Comptroller & Auditor General "
        f"put IP accommodation at ~{_eur(_CAG_2024_COMMERCIAL_EUR)} paid to commercial "
        f"providers (~{_eur(_CAG_2024_TOTAL_EUR)} total) — far above what the registers "
        "capture, so the 2020–2024 figures here are UNDER-COUNTED (that spend sat under "
        "the Dept of Children, whose register is not yet ingested)."
    )
    chart = df.set_index("year")[["ip_eur", "ukraine_eur"]].rename(
        columns={"ip_eur": "International protection", "ukraine_eur": "Ukraine"}
    )
    st.bar_chart(chart, color=["#3d719c", "#dba43c"], height=300)


def _render_providers(df) -> None:
    evidence_heading("Who the money goes to")
    st.html(
        '<p class="con-section-note">Providers ranked by total committed accommodation '
        "spend across the covered years. Names are as published; some single operators "
        "still appear under more than one spelling, so treat the order as indicative.</p>"
    )
    show = pd.DataFrame(
        {
            "Provider": df["provider"],
            "Total committed": df["total_eur"],
            "Intl. protection": df["ip_eur"],
            "Ukraine": df["ukraine_eur"],
            "Years": [f"{int(a)}–{int(b)}" if a != b else f"{int(a)}" for a, b in zip(df["first_year"], df["last_year"])],
        }
    )
    st.dataframe(
        show,
        hide_index=True,
        width="stretch",
        column_config={
            "Total committed": st.column_config.NumberColumn(format="€%d"),
            "Intl. protection": st.column_config.NumberColumn(format="€%d"),
            "Ukraine": st.column_config.NumberColumn(format="€%d"),
        },
    )


@page_error_boundary
def accommodation_spend_page() -> None:
    hide_sidebar()
    hero_banner(
        kicker="THE MONEY",
        title="Asylum & Ukraine accommodation spending",
        dek="What the State pays private providers — hotels, former hostels, emergency "
        "centres and others — to accommodate people seeking international protection and "
        "Ukrainian beneficiaries of temporary protection, from the published over-€20,000 "
        "purchase-order registers.",
    )

    yr = fetch_accommodation_spend_by_year_result()
    if not yr.ok or yr.data.empty:
        empty_state(
            "Accommodation spending unavailable",
            "The provider-payment figures could not be loaded. Try refreshing.",
        )
        return
    evidence_heading("Spend by year")
    _render_by_year(yr.data)

    prov = fetch_accommodation_spend_providers_result(40)
    if prov.ok and not prov.data.empty:
        _render_providers(prov.data)

    st.caption(
        "Source: departmental & agency purchase-orders-over-€20,000 registers (gov.ie), "
        "via the project's payments dataset. Scale benchmark: C&AG 2024 Report, Ch.10. "
        "Most accommodation is procured by direct/emergency award, so it does NOT appear "
        "on eTenders/TED. Figures are committed purchase orders, not audited final cash."
    )
