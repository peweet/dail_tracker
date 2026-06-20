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
    fetch_housing_completions_trend_result,
    fetch_housing_hap_national_result,
    fetch_housing_rent_by_county_result,
    fetch_housing_supply_national_result,
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

# "Who is waiting" demographic stripes, split for progressive disclosure: the three
# highest-signal show by default; the rest sit behind a "More" expander (citizenship
# stays in there with its sensitivity caption).
_LEAD_DIMS = [
    ("main_need", "Main need for housing"),
    ("tenure", "Where they live now"),
    ("employment", "Employment"),
]
_MORE_DIMS = [
    ("age", "Age of main applicant"),
    ("household", "Household"),
    ("income", "Household income"),
    ("accom_need", "Specific accommodation needs"),
    ("citizenship", "Citizenship of main applicant"),
]
# County-detail view shows a compact subset (keeps the drill light).
_COUNTY_DIMS = [
    ("main_need", "Main need for housing"),
    ("tenure", "Where they live now"),
    ("employment", "Employment"),
    ("household", "Household"),
]

# Source links (verified live).
_SRC_SSHA = "https://www.housingagency.ie/housing-information/summary-social-housing-assessments-ssha"
_SRC_CSO = "https://data.cso.ie/"
_SRC_PEA08 = "https://data.cso.ie/table/PEA08"


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
    d = cdf[cdf["dimension"] == dimension].sort_values(["ord", "count"], ascending=[True, False], na_position="last")
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


def _fetch_cdf(grain: str, area: str):
    """The composition rows for one area, or None."""
    res = fetch_waiting_list_composition_result(grain, area)
    return res.data if res.ok and not res.data.empty else None


def _render_time_bar(cdf) -> None:
    """How long people wait — the single ordered stripe (sequential ramp, long tail dark)."""
    time_segs = _dim_segments(cdf, "time_on_list")
    if not time_segs:
        return
    evidence_heading("How long people wait")
    st.html(proportion_stripe_html(time_segs, palette="sequential"))
    st.caption("Length of time on the Record of Qualified Households · SSHA 2025.")


def _render_demo_grid(cdf, dims) -> None:
    """A 2-per-row grid of demographic proportion stripes for the given dimensions."""
    cards = [(dim, title, _dim_segments(cdf, dim)) for dim, title in dims]
    cards = [c for c in cards if c[2]]
    for i in range(0, len(cards), 2):
        cols = st.columns(2)
        for col, (dim, title, segs) in zip(cols, cards[i : i + 2], strict=False):
            with col:
                st.html(f'<p class="hou-dim-title">{title}</p>' + proportion_stripe_html(segs, palette="categorical"))
                if dim == "citizenship":
                    st.caption(
                        "Citizenship of the main applicant, as a share of qualified "
                        "households — not a measure of who is housed."
                    )


def _lead_sentence(cdf) -> str:
    """One plain-language line of the sharpest facts for the lead section."""

    def top(dim: str) -> str:
        d = cdf[cdf["dimension"] == dim].sort_values("count", ascending=False)
        return str(d.iloc[0]["category"]).lower() if not d.empty else ""

    age, emp, need = top("age"), top("employment"), top("main_need")
    bits = [b for b in (f"aged {age}" if age else "", emp, f"needing housing because: {need}" if need else "") if b]
    return "Most applicants are " + ", ".join(bits) + "." if bits else ""


def _render_supply() -> None:
    """National supply & affordability context — the other side of the waiting list.
    Periods differ by source and are labelled; no causal link is drawn."""
    res = fetch_housing_supply_national_result()
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    evidence_heading("Supply & affordability")
    items: list[tuple[str, str]] = []
    if not pd.isna(r.get("vacant_dwellings")):
        items.append((_int(r["vacant_dwellings"]), f"homes vacant ({r['vacancy_period']})"))
    if not pd.isna(r.get("vacancy_rate")):
        items.append((_pct(r["vacancy_rate"], 1), "of housing stock"))
    if not pd.isna(r.get("avg_weekly_private_rent")):
        items.append((f"€{r['avg_weekly_private_rent']:.0f}", f"avg weekly private rent (Census {r['rent_period']})"))
    if not pd.isna(r.get("hap_households")):
        items.append((_int(r["hap_households"]), f"households in HAP ({r['hap_period']})"))
    if items:
        totals_strip(items)
        st.caption(
            "Vacancy: CSO metered-electricity stock. Rent: Census 2022, private-landlord "
            "tenancies. HAP: CSO Housing Assistance Payment. Periods differ by source — "
            "shown as published, not blended, and not linked to the waiting-list figures above."
        )

    # New homes completed per year — the supply trend.
    ctres = fetch_housing_completions_trend_result()
    if ctres.ok and not ctres.data.empty:
        st.html('<p class="hou-dim-title" style="margin-top:0.8rem">New homes completed per year</p>')
        trend = ctres.data.set_index("year")["completions"]
        st.bar_chart(trend, color="#3d719c", height=240)
        st.caption("CSO new dwelling completions (NDQ09), complete calendar years.")


def _render_hap() -> None:
    """National HAP profile — the state's private-rental subsidy (latest CSO = 2022)."""
    res = fetch_housing_hap_national_result()
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    period = r.get("hap_period") or "2022"
    evidence_heading("Housing Assistance Payment (HAP)")
    items: list[tuple[str, str]] = []
    if not pd.isna(r.get("hap_households")):
        items.append((_int(r["hap_households"]), f"households on HAP ({period})"))
    if not pd.isna(r.get("pct_working")):
        items.append((_pct(r["pct_working"], 0), "of HAP tenants in employment"))
    if not pd.isna(r.get("rent_pct_of_disposable_income")):
        items.append((_pct(r["rent_pct_of_disposable_income"], 0), "of tenant disposable income on rent"))
    if not pd.isna(r.get("median_years_to_social_housing")):
        items.append((f"{r['median_years_to_social_housing']:.0f} yrs", "median wait + HAP to social housing"))
    if items:
        totals_strip(items)
        st.caption(
            f"HAP = the state's main private-rental subsidy. CSO HAP statistics, {period} "
            "(latest available). National figures."
        )


def _render_county_table() -> None:
    """National view only: the county league table + a county/LA grain toggle and a
    drill-into-a-county control (soft-navs via ?county=)."""
    evidence_heading("By county")
    grain = "county"
    choice = st.segmented_control(
        "Grain",
        ["County", "Local authority"],
        default="County",
        key="hou_grain",
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
            # comma-delimited (rows already ranked by size in the query)
            "On the list": [f"{int(v):,}" if pd.notna(v) else "—" for v in df["waiting_total"]],
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
        sel = st.selectbox("Explore one county’s breakdown", ["—", *counties], key="hou_drill")
        if sel and sel != "—":
            st.query_params["county"] = sel
            st.rerun()


def _render_county_rent(county: str) -> None:
    """One county's average private rent (Census 2022). Silent for Dublin/Galway,
    which F2023B splits into multiple areas with no single county total."""
    res = fetch_housing_rent_by_county_result(county)
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    st.caption(
        f"Average private rent in {county}: €{r['avg_weekly_private_rent']:.0f} per week "
        f"(Census {r['rent_period']}, private-landlord tenancies)."
    )


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
        st.html(f'<p class="hou-crumb"><a href="?">Ireland</a> ▸ <strong>{county}</strong></p>')
    _render_hero_stats(row)
    if county:
        _render_county_rent(county)
    _render_composition(grain, area)
    if not county:
        _render_supply()
        _render_hap()
        _render_county_table()

    st.caption(
        f"**Sources:** [Housing Agency — Summary of Social Housing Assessments 2025]({_SRC_SSHA}) "
        f"(waiting list, per local authority; counties roll up the city/county authorities) · "
        f"[CSO PxStat]({_SRC_CSO}) (population [PEA08]({_SRC_PEA08}), completions NDQ09, vacancy "
        "VAC14, rent F2023B, HAP). Council-area figures — the area is not a constituency."
    )
