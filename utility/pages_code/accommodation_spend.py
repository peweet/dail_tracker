"""Accommodation spending — what the State pays providers to accommodate people
seeking international protection (asylum) and Ukrainian beneficiaries of temporary
protection, who runs those centres, how they inspect, and what a person is owed.

DISPLAY ONLY. Every figure arrives pre-aggregated from registered v_accommodation_spend_*
and v_ipas_* views via dail_tracker_core.queries.housing; the classification, sums, joins
and identity-gating live in sql_views/housing/*. This page only renders and labels.

The four tabs answer four questions with four grains that MUST NOT be mixed:
  · The money        — v_accommodation_spend_* (PO-committed € — a money flow)
  · Where            — v_ipas_la_profile        (a point-in-time headcount per LA)
  · Who runs it      — v_ipas_operators + v_ipas_centre_compliance (audit narrative)
  · What you're owed — v_ipas_entitlements       (the law vs the audited finding)

Hard rails (see sql_views/housing/*.sql and doc/archive/REVIEW_SYNTHESIS.md):
  * NEVER CAUSAL — an operator's compliance record and the money it was paid cover
    DIFFERENT windows; the page states co-occurrence, never "paid because" / "failed
    because". The caveat is shown, not implied.
  * IDENTITY-GATED — only operators resolved exactly on the house normaliser are named.
  * UNKNOWNS SHOWN — where the State publishes no figure, the page says so.
  * NEVER-SUM — audit-narrative figures are value_safe_to_sum=False and are never added
    to, or unioned with, the payment figures.
  * TONE — state the law and the audited finding; never editorialise about migration;
    never name, age, locate or quote an individual resident.
"""

from __future__ import annotations

import base64
import html
import sys
from functools import partial
from pathlib import Path

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.housing_data import (
    fetch_accommodation_spend_by_year_result,
    fetch_accommodation_spend_providers_result,
    fetch_ipas_centre_compliance_result,
    fetch_ipas_entitlements_result,
    fetch_ipas_la_profile_result,
    fetch_ipas_operators_result,
    fetch_ipas_property_rates_result,
)
from ui.components import dt_page, empty_state, evidence_heading, hero_banner, totals_strip
from ui.format import eur, eur_full

# Authoritative denominator (C&AG 2024, Ch.10) — so our register-based figure is never
# read as the full spend.
_CAG_2024_COMMERCIAL_EUR = 978_000_000
_CAG_2024_TOTAL_EUR = 1_100_000_000

# Source links (shown under the page, and the C&AG benchmark inline).
_SRC_CAG = (
    "https://www.audit.gov.ie/en/find-report/publications/2025/"
    "10-management-of-international-protection-accommodation-contracts.pdf"
)
_SRC_DCEDIY = (
    "https://www.gov.ie/en/department-of-children-disability-and-equality/collections/"
    "department-of-children-equality-disability-integration-and-youth-purchase-orders-for-20000-or-above/"
)
_SRC_JUSTICE = (
    "https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/"
    "department-of-justice-purchase-orders-issued-over-20000-in-value/"
)
_SRC_IPAS = "https://www.gov.ie/en/international-protection-accommodation-services-ipas/"
_SRC_HIQA = "https://www.hiqa.ie/areas-we-work/international-protection-accommodation-services"

# C&AG Figure 10.2 banding — the auditor's own choropleth bands, low→high, on a blue ramp.
_BAND_ORDER = ["0-2", "3-5", "6-8", "9-11", "12+"]
_BAND_COLOUR = {"0-2": "#cfe0ec", "3-5": "#9dc0da", "6-8": "#6a9fc7", "9-11": "#3d719c", "12+": "#234a68"}


# Canonical formatters (ui.format, 2026-07 consolidation).
_eur_full = partial(eur_full, dash_zero=True)  # €113,863,982; — for none/zero
_eur = eur


def _html_table(headers: list[str], rows: list[list], numeric_cols: tuple[int, ...] = ()) -> str:
    """A civic HTML table (st.html) — the sanctioned replacement for st.dataframe
    on a primary view (cards/inline only; st.dataframe is drill-down/export only).
    Every cell is escaped here, so callers pass plain strings."""

    def cell(tag: str, content: str, i: int, extra: str) -> str:
        align = "right" if i in numeric_cols else "left"
        return f'<{tag} style="text-align:{align};padding:0.4rem 0.65rem;{extra}">{html.escape(str(content))}</{tag}>'

    head = "".join(
        cell(
            "th",
            h,
            i,
            "border-bottom:2px solid #d6d3d1;font-size:0.74rem;text-transform:uppercase;"
            "letter-spacing:0.04em;color:#5b6b73;font-weight:600;",
        )
        for i, h in enumerate(headers)
    )
    body = "".join(
        "<tr>"
        + "".join(
            cell("td", c, i, "border-bottom:1px solid #ebe6da;font-variant-numeric:tabular-nums;color:#14232b;")
            for i, c in enumerate(r)
        )
        + "</tr>"
        for r in rows
    )
    return (
        '<table style="width:100%;border-collapse:collapse;font-size:0.9rem;margin:0.3rem 0 1rem;">'
        f"<thead><tr>{head}</tr></thead><tbody>{body}</tbody></table>"
    )


def _caveat(text_html: str) -> None:
    """A quiet, non-alarm caveat rail — grey, never a warning colour. Used for the
    never-causal and coverage disclaimers so they are visible but not editorialising."""
    st.html(
        '<div style="border-left:3px solid #b9c3c7;background:#f4f6f7;padding:0.6rem 0.85rem;'
        'margin:0.4rem 0 1rem;border-radius:0 4px 4px 0;font-size:0.86rem;color:#3f5259;'
        f'line-height:1.5">{text_html}</div>'
    )


# ─────────────────────────── The money ───────────────────────────
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
        "Committed = purchase-order amounts (PO-committed, not confirmed cash) from the "
        "published over-€20,000 registers — including the Dept of Children (DCEDIY) register "
        "for 2023–2024, the years IPAS sat under that department. For 2024 the "
        f"[Comptroller & Auditor General]({_SRC_CAG}) put IP accommodation at "
        f"~{_eur(_CAG_2024_COMMERCIAL_EUR)} paid to commercial providers "
        f"(~{_eur(_CAG_2024_TOTAL_EUR)} total incl. capital) — our 2024 IP figure is now in "
        "that range. 2020–2022 remain thin (pre-surge; not separately published in a "
        "parsable register)."
    )
    chart = df.set_index("year")[["ip_eur", "ukraine_eur"]].rename(
        columns={"ip_eur": "International protection", "ukraine_eur": "Ukraine"}
    )
    st.bar_chart(chart, color=["#3d719c", "#dba43c"], height=300, x_label="Year", y_label="€ committed")

    yt = df.sort_values("year", ascending=False)
    rows = [
        [
            str(int(y)),
            _eur_full(ip),
            _eur_full(uk),
            _eur_full(tot),
            f"{int(n):,}" if pd.notna(n) else "—",
        ]
        for y, ip, uk, tot, n in zip(
            yt["year"], yt["ip_eur"], yt["ukraine_eur"], yt["total_eur"], yt["n_providers"], strict=False
        )
    ]
    st.html(
        _html_table(
            ["Year", "Intl. protection", "Ukraine", "Total committed", "Providers"],
            rows,
            numeric_cols=(1, 2, 3, 4),
        )
    )


def _render_providers(df) -> None:
    evidence_heading("Who the money goes to")
    st.html(
        '<p class="con-section-note">Providers ranked by total committed accommodation '
        "spend across the covered years. Names are as published; some single operators "
        "still appear under more than one spelling, so treat the order as indicative.</p>"
    )
    rows = [
        [
            prov,
            _eur_full(tot),
            _eur_full(ip),
            _eur_full(uk),
            f"{int(a)}–{int(b)}" if a != b else f"{int(a)}",
        ]
        for prov, tot, ip, uk, a, b in zip(
            df["provider"],
            df["total_eur"],
            df["ip_eur"],
            df["ukraine_eur"],
            df["first_year"],
            df["last_year"],
            strict=False,
        )
    ]
    st.html(
        _html_table(
            ["Provider", "Total committed", "Intl. protection", "Ukraine", "Years"],
            rows,
            numeric_cols=(1, 2, 3),
        )
    )


def _render_money_tab() -> None:
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
        f"**Sources:** [Dept of Children (DCEDIY) purchase orders over €20k]({_SRC_DCEDIY}) · "
        f"[Dept of Justice purchase orders over €20k]({_SRC_JUSTICE}) · "
        f"[C&AG 2024 Report, Ch.10]({_SRC_CAG}) · [IPAS]({_SRC_IPAS}). "
        "Most accommodation is procured by direct/emergency award, so it does NOT appear on "
        "eTenders/TED. Figures are committed purchase orders (PO-committed), not audited final cash."
    )


# ─────────────────────────── Where people are housed ───────────────────────────
def _render_where_tab() -> None:
    res = fetch_ipas_la_profile_result()
    if not res.ok or res.data.empty:
        empty_state("Distribution unavailable", "The per-local-authority figures could not be loaded.")
        return
    df = res.data.sort_values("ip_applicants", ascending=False)
    total = int(df["ip_applicants"].sum())
    snap = str(df["snapshot_date"].iloc[0]) if "snapshot_date" in df.columns else ""

    evidence_heading("Where people are housed")
    st.html(
        '<p class="con-section-note">People seeking international protection in State-provided '
        "accommodation, by <strong>local authority</strong>, as a share of each area's population. "
        "The rate — applicants per 1,000 residents — is the measure the Comptroller &amp; Auditor "
        "General's own map uses; the colour band is the auditor's. Population is Census 2022.</p>"
    )
    per_capita = df[df["ip_per_1000_population"].notna()]
    if not per_capita.empty:
        top = per_capita.sort_values("ip_per_1000_population", ascending=False).iloc[0]
        totals_strip(
            [
                (f"{total:,}", "people accommodated"),
                (f"{len(df)}", "local authorities"),
                (
                    f"{float(top['ip_per_1000_population']):.1f}",
                    f"per 1,000 — highest ({html.escape(str(top['local_authority']))})",
                ),
            ]
        )

    # Ranked horizontal bars, coloured by the C&AG band — a choropleth read as a league table.
    rate_max = float(per_capita["ip_per_1000_population"].max()) if not per_capita.empty else 1.0
    bars: list[str] = []
    for _, r in df.iterrows():
        rate = r["ip_per_1000_population"]
        band = r.get("cag_band")
        colour = _BAND_COLOUR.get(str(band), "#b9c3c7")
        if pd.notna(rate):
            width = max(2.0, float(rate) / rate_max * 100.0)
            rate_txt = f"{float(rate):.1f}"
        else:
            width, rate_txt = 0.0, "—"
        bars.append(
            '<div style="display:flex;align-items:center;gap:0.6rem;margin:0.18rem 0">'
            f'<div style="width:150px;flex:none;font-size:0.85rem;color:#14232b;text-align:right">'
            f"{html.escape(str(r['local_authority']))}</div>"
            '<div style="flex:1;background:#eef1f2;border-radius:3px;height:1.1rem;position:relative">'
            f'<div style="width:{width:.1f}%;background:{colour};height:100%;border-radius:3px"></div>'
            "</div>"
            f'<div style="width:112px;flex:none;font-size:0.82rem;color:#3f5259;'
            f'font-variant-numeric:tabular-nums">{rate_txt} <span style="color:#8a979d">/1k</span> · '
            f"{int(r['ip_applicants']):,}</div>"
            "</div>"
        )
    legend = "".join(
        f'<span style="display:inline-flex;align-items:center;gap:0.3rem;margin-right:0.8rem">'
        f'<span style="width:0.8rem;height:0.8rem;border-radius:2px;background:{_BAND_COLOUR[b]};'
        f'display:inline-block"></span>{b}</span>'
        for b in _BAND_ORDER
    )
    st.html(
        f'<div style="margin:0.3rem 0 0.4rem">{"".join(bars)}</div>'
        f'<div style="font-size:0.76rem;color:#5b6b73;margin-bottom:0.5rem">'
        f"Applicants per 1,000 residents · band (C&amp;AG Fig 10.2): {legend}</div>"
    )

    unknown = df[df["ip_per_1000_population"].isna()]
    if not unknown.empty:
        names = ", ".join(html.escape(str(x)) for x in unknown["local_authority"].tolist())
        _caveat(f"Population could not be mapped for: {names}. Their rate is left blank rather than estimated.")
    _caveat(
        f"Snapshot {html.escape(snap)}. The count is a point-in-time headcount, not a money flow — it is "
        "never summed with the spending figures. The rate divides a Dec-2024 headcount by the Apr-2022 "
        "Census; the auditor's own map does the same (no later local-authority census exists)."
    )
    src = str(df["source_url_ip_applicants"].iloc[0]) if "source_url_ip_applicants" in df.columns else _SRC_IPAS
    st.caption(f"**Source:** [IPAS weekly accommodation & arrivals statistics]({src}) · population: CSO Census 2022.")


# ─────────────────────────── Who runs the centres ───────────────────────────
def _compliance_colour(pct: float) -> str:
    if pct >= 25:
        return "#b23c3c"
    if pct >= 10:
        return "#c67a1e"
    return "#3f7d54"


def _render_operators_tab() -> None:
    res = fetch_ipas_operators_result()
    if not res.ok or res.data.empty:
        empty_state("Operator records unavailable", "The operator compliance/payment figures could not be loaded.")
        return
    df = res.data

    evidence_heading("Who runs the centres")
    st.html(
        '<p class="con-section-note">The C&amp;AG anonymised its accommodation suppliers; HIQA\'s '
        "overview report names none. But HIQA's individual inspection reports name the operator of "
        "every centre — so each operator here can be set beside its inspection record and the public "
        "money it received. Only operators whose identity resolves with certainty are named.</p>"
    )
    _caveat(
        "<strong>These are not linked as cause and effect.</strong> The inspection record (2024–2026) "
        "and the payments (Dept of Children 2023–24; Dept of Justice from 2025) cover different periods. "
        "This is what an operator was paid <em>and</em>, separately, how its centres inspected — never "
        "&ldquo;paid because&rdquo; or &ldquo;failed because&rdquo;."
    )

    rows = []
    for _, r in df.iterrows():
        pct = float(r["pct_not_compliant"])
        chip = (
            f'<span style="display:inline-block;min-width:3.2rem;text-align:center;padding:0.1rem 0.4rem;'
            f'border-radius:3px;background:{_compliance_colour(pct)};color:#fff;font-size:0.8rem;'
            f'font-variant-numeric:tabular-nums">{pct:.1f}%</span>'
        )
        dcediy = r.get("ip_paid_dcediy_eur")
        doj = r.get("ip_paid_justice_eur")
        rows.append(
            "<tr>"
            f'<td style="padding:0.4rem 0.65rem;border-bottom:1px solid #ebe6da;color:#14232b">'
            f"{html.escape(str(r['operator']))}</td>"
            f'<td style="padding:0.4rem 0.65rem;border-bottom:1px solid #ebe6da;text-align:right;'
            f'font-variant-numeric:tabular-nums">{int(r["centres"])}</td>'
            f'<td style="padding:0.4rem 0.65rem;border-bottom:1px solid #ebe6da;text-align:right">{chip}</td>'
            f'<td style="padding:0.4rem 0.65rem;border-bottom:1px solid #ebe6da;text-align:right;'
            f'font-variant-numeric:tabular-nums;color:#14232b">{_eur(dcediy)}</td>'
            f'<td style="padding:0.4rem 0.65rem;border-bottom:1px solid #ebe6da;text-align:right;'
            f'font-variant-numeric:tabular-nums;color:#14232b">{_eur(doj)}</td>'
            "</tr>"
        )
    head = "".join(
        f'<th style="text-align:{a};padding:0.4rem 0.65rem;border-bottom:2px solid #d6d3d1;'
        f'font-size:0.74rem;text-transform:uppercase;letter-spacing:0.04em;color:#5b6b73;font-weight:600">{h}</th>'
        for h, a in [
            ("Operator", "left"),
            ("Centres", "right"),
            ("Inspections not compliant", "right"),
            ("Paid — Dept of Children (IP, 2023–24)", "right"),
            ("Paid — Dept of Justice (2025+)", "right"),
        ]
    )
    st.html(
        '<table style="width:100%;border-collapse:collapse;font-size:0.9rem;margin:0.3rem 0 1rem">'
        f"<thead><tr>{head}</tr></thead><tbody>{''.join(rows)}</tbody></table>"
    )

    # County drill-down into the inspection record + nightly rates.
    comp = fetch_ipas_centre_compliance_result()
    if comp.ok and not comp.data.empty:
        counties = sorted({str(c) for c in comp.data["county"].dropna().tolist()})
        st.space(1)
        evidence_heading("Inspection findings by county")
        pick = st.segmented_control(
            "County", counties, key="ipas_county_pick", label_visibility="collapsed"
        )
        if pick:
            _render_county_detail(pick)

    st.caption(
        f"**Sources:** [HIQA inspection reports]({_SRC_HIQA}) · "
        f"[Dept of Children (DCEDIY) purchase orders]({_SRC_DCEDIY}) · "
        f"[Dept of Justice purchase orders]({_SRC_JUSTICE}). Provider identity resolved on the "
        "project name-normaliser; unresolved operators are omitted rather than shown on a guess."
    )


def _render_county_detail(county: str) -> None:
    comp = fetch_ipas_centre_compliance_result(county)
    if not comp.ok or comp.data.empty:
        st.caption("No inspection findings recorded for this county.")
        return
    d = comp.data
    centres = sorted({str(c) for c in d["centre_name"].dropna().tolist()})
    not_comp = d[d["judgment"].astype(str).str.lower().str.startswith("not")]

    totals_strip(
        [
            (str(len(centres)), "centres inspected"),
            (str(int(d.shape[0])), "standard judgments"),
            (str(int(not_comp.shape[0])), "not compliant"),
        ]
    )
    # The specific not-compliant findings — centre, the standard's meaning, risk.
    rows = []
    for _, r in not_comp.head(40).iterrows():
        statement = str(r.get("standard_statement") or r.get("standard_ref") or "")
        risk = r.get("risk_rating")
        rows.append(
            [
                str(r.get("centre_name") or ""),
                str(r.get("operator") or ""),
                f"{r.get('standard_ref', '')} · {statement[:70]}",
                str(risk) if pd.notna(risk) else "—",
            ]
        )
    if rows:
        st.html(_html_table(["Centre", "Operator", "Standard not met", "Risk"], rows))
    else:
        st.caption("Every standard judged in this county's inspected centres was met.")

    rates = fetch_ipas_property_rates_result(county)
    if rates.ok and not rates.data.empty:
        rr = rates.data
        st.html('<p class="con-section-note">What a bed cost in the C&amp;AG\'s sampled properties here '
                "(per person, per night). Direct award means no competitive tender.</p>")
        rows = [
            [
                str(x.get("accommodation_type") or ""),
                str(x.get("procurement_route") or ""),
                (f"€{float(x['contracted_rate_eur_per_person_night']):.0f}"
                 if x.get("rate_known") and pd.notna(x.get("contracted_rate_eur_per_person_night"))
                 else "not disclosed"),
            ]
            for _, x in rr.iterrows()
        ]
        st.html(_html_table(["Type", "Procurement route", "Per person / night"], rows, numeric_cols=(2,)))


# ─────────────────────────── What you're entitled to ───────────────────────────
_REALITY_STYLE = {
    "NOT_DELIVERED_TO_ALL": ("#b23c3c", "Not delivered to all"),
    "NOT_PERFORMED": ("#b23c3c", "Not performed"),
    "SERIOUS_GAPS": ("#b23c3c", "Serious gaps"),
    "WORST_PERFORMING_STANDARD": ("#b23c3c", "Worst-performing standard"),
    "GAP_FOUND": ("#c67a1e", "Gap found"),
    "GATED_BY_DELAY": ("#c67a1e", "Gated by delay"),
    "DEPENDS_ON_UNPERFORMED_ASSESSMENT": ("#c67a1e", "Depends on an assessment not done"),
    "UNENFORCEABLE_AND_MOSTLY_UNMONITORED": ("#c67a1e", "Unenforceable, mostly unmonitored"),
    "AMOUNT_NOT_IN_THIS_SOURCE": ("#7c8a90", "Rate set administratively"),
    "NOT_PUBLISHED": ("#7c8a90", "Not published"),
    "IN_USE": ("#3f7d54", "In use"),
}

# A muted, low-opacity walking-figure outline — humanises the number without othering,
# and avoids any photograph of an identifiable person. Decorative only.
# st.html strips inline <svg>, so embed as a base64 data-URI <img> — the same technique
# the choropleths use (see local_government.py / constituency.py).
_SILHOUETTE_RAW = (
    "<svg xmlns='http://www.w3.org/2000/svg' width='46' height='72' viewBox='0 0 46 72'>"
    "<circle cx='24' cy='9' r='7' fill='none' stroke='#2c4048' stroke-width='2.2'/>"
    "<path d='M24 17 L21 39 M24 24 L36 31 M24 22 L11 30 M21 39 L12 62 M21 39 L31 60' "
    "fill='none' stroke='#2c4048' stroke-width='2.2' stroke-linecap='round' stroke-linejoin='round'/>"
    "</svg>"
)
_SILHOUETTE = (
    '<img src="data:image/svg+xml;base64,'
    + base64.b64encode(_SILHOUETTE_RAW.encode("utf-8")).decode("ascii")
    + '" width="54" height="84" alt="" style="opacity:0.5;flex:none"/>'
)


def _render_person_tab() -> None:
    res = fetch_ipas_entitlements_result()
    if not res.ok or res.data.empty:
        empty_state("Entitlements unavailable", "The entitlements record could not be loaded.")
        return
    df = res.data.sort_values("display_order")

    st.html(
        '<div style="display:flex;align-items:center;gap:1rem;margin:0.2rem 0 0.4rem">'
        f"{_SILHOUETTE}"
        '<div><h2 class="section-heading" style="margin:0">What a person is entitled to</h2>'
        '<p style="margin:0.15rem 0 0;color:#5b6b73;font-size:0.9rem;max-width:44rem">'
        "What Irish and EU law guarantees a person seeking international protection — set beside what "
        "the auditor and the inspector actually found. The law is quoted from the Reception Conditions "
        "Regulations; the reality from the C&amp;AG, HIQA and the Government's own strategy.</p></div>"
        "</div>"
    )

    for _, r in df.iterrows():
        status = str(r.get("reality_status") or "")
        colour, label = _REALITY_STYLE.get(status, ("#7c8a90", status.replace("_", " ").title()))
        st.html(
            '<div style="border:1px solid #e6e2d8;border-radius:6px;background:#ffffff;'
            'padding:0.85rem 1rem;margin:0.5rem 0">'
            '<div style="display:flex;justify-content:space-between;align-items:baseline;gap:0.8rem;'
            'flex-wrap:wrap">'
            f'<div style="font-weight:700;font-size:1.02rem;color:#14232b">'
            f"{html.escape(str(r['entitlement']))}</div>"
            f'<span style="flex:none;padding:0.12rem 0.5rem;border-radius:3px;background:{colour};'
            f'color:#fff;font-size:0.76rem;font-weight:600">{html.escape(label)}</span></div>'
            f'<div style="font-size:0.78rem;color:#8a7a2a;margin:0.15rem 0 0.5rem">'
            f"⏱ {html.escape(str(r.get('timeframe') or ''))}</div>"
            '<div style="display:grid;grid-template-columns:1fr 1fr;gap:0.9rem">'
            '<div><div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em;'
            'color:#5b6b73;font-weight:600;margin-bottom:0.2rem">What the law says</div>'
            f'<div style="font-size:0.88rem;color:#233">{html.escape(str(r.get("what_the_law_says") or ""))}</div></div>'
            '<div><div style="font-size:0.72rem;text-transform:uppercase;letter-spacing:0.04em;'
            'color:#5b6b73;font-weight:600;margin-bottom:0.2rem">What was found</div>'
            f'<div style="font-size:0.88rem;color:#233">{html.escape(str(r.get("reality_finding") or ""))}</div></div>'
            "</div></div>"
        )

    st.caption(
        "**Sources:** the entitlement is quoted from [SI 230/2018 as amended]"
        "(https://www.irishstatutebook.ie/eli/2018/si/230/made/en/print) (the Reception Conditions "
        f"Regulations, transposing EU Directive 2013/33); the finding from the [C&AG]({_SRC_CAG}), "
        f"[HIQA]({_SRC_HIQA}) and the Government's Comprehensive Accommodation Strategy. Where the State "
        "publishes no measure of its own performance, this is stated rather than inferred."
    )


# ─────────────────────────── Page shells ───────────────────────────
def _render_accommodation_tabs() -> None:
    """The four-concern dossier: the money, where people are housed, who runs the
    centres, and what a person is entitled to. Shared by the standalone page and the
    Public Payments hub embed so both surface the SAME accountability tabs — the embed
    used to render the money block only, which left the other three (and the C&AG
    distribution / entitlements) unreachable from the visible nav."""
    money, where, who, person = st.tabs(
        ["The money", "Where people are housed", "Who runs the centres", "What you're entitled to"]
    )
    with money:
        _render_money_tab()
    with where:
        _render_where_tab()
    with who:
        _render_operators_tab()
    with person:
        _render_person_tab()


def render_accommodation_body(*, embedded: bool = False) -> None:
    """The whole accommodation dossier, composable so the Public Payments hub can render
    it inline (Money nav declutter Phase 3). Embedded = a compact evidence heading instead
    of the page hero; the accountability tabs render either way so the hub is the full
    front door, not a money-only teaser."""
    if embedded:
        evidence_heading("Asylum & Ukraine accommodation")
        st.caption(
            "What the State pays to house people seeking international protection and Ukrainian "
            "beneficiaries of temporary protection, where they are accommodated, who runs those "
            "centres and how they are inspected, and what a person is entitled to in law."
        )
    else:
        hero_banner(
            kicker="THE MONEY",
            title="Asylum & Ukraine accommodation spending",
            dek="What the State pays private providers — hotels, former hostels, emergency "
            "centres and others — to accommodate people seeking international protection and "
            "Ukrainian beneficiaries of temporary protection, from the published over-€20,000 "
            "purchase-order registers.",
        )
    _render_accommodation_tabs()


@dt_page
def accommodation_spend_page() -> None:
    hero_banner(
        kicker="ASYLUM ACCOMMODATION",
        title="Asylum & Ukraine accommodation",
        dek="What the State pays to house people seeking international protection, who runs those "
        "centres and how they inspect, and what a person is entitled to in law.",
    )
    _render_accommodation_tabs()
