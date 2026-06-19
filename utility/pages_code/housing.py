"""Housing — the national social-housing waiting list ("Who's on the list").

The demand side of housing at national scale: who is on the social-housing waiting
list (Housing Agency SSHA 2025), how long they wait, and how it varies by county.

DISPLAY ONLY. Every figure arrives pre-aggregated from a registered
``v_ssha_waiting_list_*`` view via ``dail_tracker_core.queries.housing``; this page
never JOINs, GROUPs, unpivots, or derives a metric — the composition, the LA→county
rollup and the per-capita all live in ``sql_views/housing/*``.

Grain: national by default; ``?county=`` soft-navigates to one county's breakdown.
The county league table can also be viewed at local-authority grain (per-capita is
only shown where a real population denominator exists — county/national).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.housing_data import (
    fetch_waiting_list_composition_result,
    fetch_waiting_list_totals_result,
)
from ui.components import (
    empty_state,
    evidence_heading,
    hero_banner,
    hide_sidebar,
    page_error_boundary,
    proportion_stripe_html,
    totals_strip,
)

# Dimensions rendered as "who is waiting" cards (time-on-list is rendered separately
# above, with the sequential ramp). Citizenship carries the sensitivity caption.
_DIM_CARDS = [
    ("tenure", "Where they live now"),
    ("employment", "Employment"),
    ("household", "Household"),
    ("citizenship", "Citizenship of main applicant"),
]


def _int(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return f"{int(round(float(v))):,}"
    except (TypeError, ValueError):
        return "—"


def _pct(v, dp: int = 1) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    return f"{float(v):.{dp}f}%"


def _dim_segments(cdf, dimension: str) -> list[tuple[str, float]]:
    """Ordered (category, count) pairs for one dimension — ord, then count desc."""
    d = cdf[cdf["dimension"] == dimension].sort_values(
        ["ord", "count"], ascending=[True, False], na_position="last"
    )
    return [(str(r["category"]), float(r["count"])) for _, r in d.iterrows()]


def _render_hero_stats(row) -> None:
    yoy = row.get("waiting_yoy_pct")
    yoy_txt = "—"
    if yoy is not None and not pd.isna(yoy):
        yoy_txt = f"{'+' if yoy >= 0 else ''}{yoy:.1f}%"
    items = [
        (_int(row.get("waiting_total")), "households on the list"),
        (yoy_txt, "vs 2024"),
        (_pct(row.get("over_7yr_pct"), 1), "waiting 7+ years"),
    ]
    per_k = row.get("waiters_per_1000")
    if per_k is not None and not pd.isna(per_k):
        items.append((f"{per_k:.1f}", "per 1,000 people"))
    totals_strip(items)


def _render_composition(grain: str, area: str) -> None:
    res = fetch_waiting_list_composition_result(grain, area)
    if not res.ok or res.data.empty:
        return
    cdf = res.data

    # How long people wait — single ordered stripe, sequential ramp (long tail = dark)
    time_segs = _dim_segments(cdf, "time_on_list")
    if time_segs:
        evidence_heading("How long people wait")
        st.html(proportion_stripe_html(time_segs, palette="sequential"))
        st.caption("Length of time on the Record of Qualified Households · SSHA 2025")

    # Who is waiting — demographic stripes, two per row
    evidence_heading("Who is waiting")
    cards = [(dim, title, _dim_segments(cdf, dim)) for dim, title in _DIM_CARDS]
    cards = [c for c in cards if c[2]]
    for i in range(0, len(cards), 2):
        cols = st.columns(2)
        for col, (dim, title, segs) in zip(cols, cards[i : i + 2]):
            with col:
                st.html(
                    f'<p class="hou-dim-title">{title}</p>'
                    + proportion_stripe_html(segs, palette="categorical")
                )
                if dim == "citizenship":
                    st.caption(
                        "Citizenship of the main applicant, as a share of qualified "
                        "households — not a measure of who is housed. SSHA 2025."
                    )


def _render_county_table() -> None:
    """National view only: the county league table + a county/LA grain toggle and a
    drill-into-a-county control (soft-navs via ?county=)."""
    evidence_heading("By county")
    grain = "county"
    choice = st.segmented_control(
        "Grain", ["County", "Local authority"], default="County", key="hou_grain",
        label_visibility="collapsed",
    )
    if choice == "Local authority":
        grain = "la"
    res = fetch_waiting_list_totals_result(grain)
    if not res.ok or res.data.empty:
        return
    df = res.data.copy()
    show = pd.DataFrame(
        {
            ("County" if grain == "county" else "Local authority"): df["area"],
            "On the list": df["waiting_total"],
            "Per 1,000": df["waiters_per_1000"],
            "% 7yr+": df["over_7yr_pct"],
            "YoY %": df["waiting_yoy_pct"],
        }
    )
    st.dataframe(
        show,
        hide_index=True,
        width="stretch",
        column_config={
            "On the list": st.column_config.NumberColumn(format="%d"),
            "Per 1,000": st.column_config.NumberColumn(
                format="%.1f", help="Households on the list per 1,000 people (CSO PEA08 population)"
            ),
            "% 7yr+": st.column_config.NumberColumn(format="%.1f%%"),
            "YoY %": st.column_config.NumberColumn(format="%.1f%%"),
        },
    )
    if grain == "la":
        st.caption("Per-1,000 is blank at local-authority grain — CSO population is county-level.")

    counties = res.data["area"].tolist() if grain == "county" else []
    if counties:
        sel = st.selectbox(
            "Explore one county’s breakdown", ["—", *counties], key="hou_drill"
        )
        if sel and sel != "—":
            st.query_params["county"] = sel
            st.rerun()


@page_error_boundary
def housing_page() -> None:
    hide_sidebar()
    hero_banner(
        kicker="HOUSING",
        title="Who's on the social housing list",
        dek="The social-housing waiting list (net need) from the Housing Agency's 2025 "
        "Summary of Social Housing Assessments — how many households are waiting, for "
        "how long, who they are, and how it differs across the country.",
    )

    county = st.query_params.get("county")
    grain = "county" if county else "national"
    area = county if county else "Ireland"

    totals_res = fetch_waiting_list_totals_result(grain)
    if not totals_res.ok or totals_res.data.empty:
        empty_state(
            "Housing data unavailable",
            "The social-housing waiting-list figures could not be loaded. Try refreshing.",
        )
        return
    match = totals_res.data[totals_res.data["area"] == area]
    if match.empty:
        # unknown ?county — fall back to national
        st.query_params.pop("county", None)
        st.rerun()
    row = match.iloc[0]

    if county:
        st.html(
            f'<p class="hou-crumb"><a href="?">Ireland</a> ▸ <strong>{county}</strong></p>'
        )
    _render_hero_stats(row)
    _render_composition(grain, area)
    if not county:
        _render_county_table()

    st.caption(
        "Source: Housing Agency, Summary of Social Housing Assessments 2025 (per local "
        "authority; counties roll up the city/county authorities). Population: CSO PEA08. "
        "Council-area figures — the area is not a constituency."
    )
