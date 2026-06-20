"""Who runs your county — the unelected executive layer of local government.

In Irish local government most power sits with the appointed **Chief Executive**
(the former county/city manager; "Director General" in Limerick since its 2024
directly-elected-mayor reform), NOT the elected councillors. By law councillors
hold only a short list of *reserved functions*; everything else — planning
permissions, contracts, day-to-day spend, derelict-site enforcement, housing
allocation — is an *executive function* the CE performs. This page names that
office per council and shows how the council performs on published, attributed
indicators.

DISPLAY ONLY. Every figure arrives pre-aggregated from a registered ``v_la_*``
view via ``dail_tracker_core.queries.local_government``; this page never JOINs,
GROUPs, or derives a metric. Framing is published-indicators-only: council values
beside the national benchmark, attributed to NOAC / An Bord Pleanála / DHLGH — no
composite "score", no editorial verdict.

Index → dossier is a soft ``?la=`` rerun.
"""

from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.local_government_data import (
    fetch_chief_executive_result,
    fetch_chief_executives_result,
    fetch_collection_rates_result,
    fetch_derelict_sites_levy_result,
    fetch_housing_performance_result,
    fetch_national_summary_result,
    fetch_planning_overturn_result,
)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    evidence_heading,
    find_a_td_filter,
    hero_banner,
    hide_sidebar,
    page_error_boundary,
    search_matches,
    subsection_heading,
    totals_strip,
)

_SALARY_BAND = "€132,511–€189,301"  # national CE pay scale (not published per-council)
_LGA_URL = "https://www.irishstatutebook.ie/eli/2001/act/37/enacted/en/html"


# ── display-only formatting (no derivation) ───────────────────────────────────
def _eur(v) -> str:
    if v is None or pd.isna(v):
        return "—"
    v = float(v)
    if abs(v) >= 1_000_000:
        return f"€{v / 1_000_000:.2f}m"
    if abs(v) >= 1_000:
        return f"€{v / 1_000:.0f}k"
    return f"€{v:,.0f}"


def _eur_full(v) -> str:
    return "—" if v is None or pd.isna(v) else f"€{float(v):,.0f}"


def _pct(v, dp: int = 0) -> str:
    return "—" if v is None or pd.isna(v) else f"{float(v):.{dp}f}%"


def _int(v) -> str:
    return "—" if v is None or pd.isna(v) else f"{int(v):,}"


def _num1(v) -> str:
    return "—" if v is None or pd.isna(v) else f"{float(v):.1f}"


def _bench(value, national, fmt: str, higher_is_better: bool | None) -> str:
    """The 'national X ▲/▼' benchmark sub-line for a metric. The arrow is a neutral
    direction marker vs the national median (▲ above / ▼ below), tinted only when a
    direction-of-good is defined; higher_is_better=None → no arrow (ambiguous metric).
    Firewall-safe: published value + published benchmark, no 'good/bad' word."""
    if value is None or pd.isna(value) or national is None or pd.isna(national):
        return ""
    above = float(value) >= float(national)
    arrow = "▲" if above else "▼"
    cls = ""
    if higher_is_better is not None:
        good = above if higher_is_better else not above
        cls = "lg-arrow-up" if good else "lg-arrow-down"
    arrow_html = f'<span class="{cls}">{arrow}</span>' if cls else arrow
    return f"national {fmt.format(national)} {arrow_html}"


def _metric(value: str, label: str, bench: str = "") -> str:
    return (
        '<div class="lg-metric"><div class="lg-metric-main">'
        f'<span class="lg-metric-value">{value}</span>'
        f'<span class="lg-metric-label">{label}</span></div>'
        f'<div class="lg-metric-bench">{bench}</div></div>'
    )


def _stat_card(title: str, rows: list[str], source: str, extra: str = "") -> str:
    body = "".join(r for r in rows if r)
    if not body:
        return ""
    return (
        f'<div class="lg-card"><div class="lg-card-title">{_h(title)}</div>'
        f"{body}{extra}"
        f'<div class="lg-card-src">{_h(source)}</div></div>'
    )


# ── INDEX ─────────────────────────────────────────────────────────────────────
def _council_card_inner(row) -> str:
    council = _h(str(row["council_name"]))
    ce = _h(str(row.get("chief_executive") or "—"))
    title = _h(str(row.get("head_title") or "Chief Executive"))
    return (
        f'<div class="con-card-inner">'
        f'<div class="con-card-name">{council}</div>'
        f'<div class="con-card-meta">{title}: <strong>{ce}</strong></div>'
        f'<div class="con-card-sub">Appointed, not elected</div>'
        f"</div>"
    )


def _render_national_summary() -> None:
    res = fetch_national_summary_result()
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    n_nil = r.get("n_councils_levied_nothing")
    n_all = r.get("n_councils")
    totals_strip(
        [
            ("Derelict-site levies uncollected", _eur(r.get("derelict_outstanding_eur"))),
            (
                "Councils that levied €0 (2024)",
                f"{_int(n_nil)} of {_int(n_all)}" if not pd.isna(n_nil) else "—",
            ),
            ("Planning decisions overturned on appeal", _pct(r.get("national_overturn_rate_pct"), 1)),
        ]
    )


def _render_index() -> None:
    hero_banner(
        kicker="LOCAL GOVERNMENT",
        title="Who runs your county",
        dek="Most power in your county is held by an appointed official — the Chief Executive — "
        "not by the councillors you elect. Pick a council to see who runs it and how it performs.",
    )
    _render_national_summary()

    res = fetch_chief_executives_result()
    if not res.ok or res.data.empty:
        empty_state("Councils unavailable", "The Chief Executive roster could not be loaded. Try refreshing.")
        return
    df = res.data

    query, _ = find_a_td_filter(
        df["local_authority"].tolist(),
        key_prefix="lg_idx",
        label="Find your council",
        placeholder="Search by council name…",
        show_picker=False,
    )
    shown = df[[search_matches(query, n) for n in df["local_authority"]]] if query else df
    if shown.empty:
        empty_state("No match", f"No council matches “{query}”.")
        return

    cards = [
        clickable_card_link(
            href=f"?la={quote(str(r['local_authority']))}",
            inner_html=_council_card_inner(r),
            aria_label=f"Open the {r['council_name']} dossier",
        )
        for _, r in shown.iterrows()
    ]
    st.html(f'<div class="con-card-grid">{"".join(cards)}</div>')
    st.caption(
        f"{len(shown)} of {len(df)} local authorities · Chief Executives verified against each "
        "council's own site / the CCMA (data/_meta/la_chief_executives.csv)."
    )


# ── DOSSIER ───────────────────────────────────────────────────────────────────
def _render_ce_hero(name: str, row) -> None:
    ce = _h(str(row.get("chief_executive") or "—"))
    title = _h(str(row.get("head_title") or "Chief Executive"))
    council = _h(str(row.get("council_name") or name))
    appointed = row.get("appointed_year")
    appt_txt = (
        f" · In post since <strong>{int(appointed)}</strong>"
        if appointed is not None and not pd.isna(appointed)
        else ""
    )
    src = str(row.get("source_url") or "")
    st.html(
        f'<div class="con-hero">'
        f'<div class="con-hero-text">'
        f'<p class="dt-kicker">{title.upper()}</p>'
        f'<h1 class="con-hero-title">{ce}</h1>'
        f'<p class="con-hero-meta">{title}, {council}<br>'
        f"Appointed by the Public Appointments Service — <strong>not elected</strong> · "
        f"7-year term · Salary band {_SALARY_BAND} (national scale){appt_txt}</p>"
        f"</div>"
        f"</div>"
    )
    if src:
        st.html(f'<a class="dt-source-link" href="{_h(src)}" target="_blank" rel="noopener">Source ↗</a>')


def _render_power_explainer(council: str) -> None:
    subsection_heading("What this office controls — and what it doesn't")
    exec_card = (
        '<div class="con-council-card">'
        '<div class="con-council-name">The executive (this office)</div>'
        '<div class="con-grain-row">'
        '<span class="con-grain">Planning permissions</span>'
        '<span class="con-grain">Contracts &amp; spending</span>'
        '<span class="con-grain">Council staff</span>'
        '<span class="con-grain">Derelict-site enforcement</span>'
        '<span class="con-grain">Housing allocation</span>'
        '<span class="con-grain">…everything not reserved</span>'
        "</div></div>"
    )
    reserved_card = (
        '<div class="con-council-card">'
        '<div class="con-council-name">Your councillors (elected)</div>'
        '<div class="con-grain-row">'
        '<span class="con-grain con-grain-rev">Adopt the budget</span>'
        '<span class="con-grain con-grain-rev">Adopt the development plan</span>'
        '<span class="con-grain con-grain-rev">Borrow money</span>'
        '<span class="con-grain con-grain-rev">Appoint the Chief Executive</span>'
        "</div></div>"
    )
    st.html(f'<div class="con-council-grid">{exec_card}{reserved_card}</div>')
    st.html(
        f'<a class="dt-source-link" href="{_LGA_URL}" target="_blank" rel="noopener">'
        "Legal basis: Local Government Act 2001, Part 14 (as amended 2014) ↗</a>"
    )


def _card_money_collected(name: str) -> str:
    col = fetch_collection_rates_result(name)
    if not col.ok or col.data.empty:
        return ""
    c = col.data.iloc[0]
    rows = [
        _metric(_pct(c.get("commercial_rates_pct")), "Commercial rates collected",
                _bench(c.get("commercial_rates_pct"), c.get("nat_commercial_rates_pct"), "{:.0f}%", True)),
        _metric(_pct(c.get("rent_annuities_pct")), "Rent &amp; annuities collected",
                _bench(c.get("rent_annuities_pct"), c.get("nat_rent_annuities_pct"), "{:.0f}%", True)),
        _metric(_pct(c.get("housing_loans_pct")), "Housing loans collected",
                _bench(c.get("housing_loans_pct"), c.get("nat_housing_loans_pct"), "{:.0f}%", True)),
    ]
    return _stat_card("Money collected", rows, "NOAC Performance Indicator Report 2024")


def _card_housing(name: str) -> str:
    h = fetch_housing_performance_result(name)
    if not h.ok or h.data.empty:
        return ""
    r = h.data.iloc[0]
    rows = [
        _metric(_pct(r.get("vacancy_pct"), 1), "Council homes lying vacant",
                _bench(r.get("vacancy_pct"), r.get("nat_vacancy_pct"), "{:.1f}%", False)),
        _metric(f'{_num1(r.get("reletting_weeks"))} wks', "Time to re-let an empty home",
                _bench(r.get("reletting_weeks"), r.get("nat_reletting_weeks"), "{:.0f} wks", False)),
        _metric(_eur_full(r.get("maintenance_eur_per_dwelling")), "Upkeep spend per home",
                _bench(r.get("maintenance_eur_per_dwelling"), r.get("nat_maintenance_eur_per_dwelling"), "€{:,.0f}", None)),
        _metric(_pct(r.get("retrofit_pct_of_stock"), 1), "Stock retrofitted (2024)",
                _bench(r.get("retrofit_pct_of_stock"), r.get("nat_retrofit_pct_of_stock"), "{:.1f}%", True)),
        _metric(_pct(r.get("longterm_homeless_pct")), "Homeless adults long-term",
                _bench(r.get("longterm_homeless_pct"), r.get("nat_longterm_homeless_pct"), "{:.0f}%", False)),
    ]
    return _stat_card("Social housing management", rows, "NOAC Performance Indicator Report 2024 (H-series)")


def _card_derelict(name: str) -> str:
    der = fetch_derelict_sites_levy_result(name)
    if not der.ok or der.data.empty:
        return ""
    d = der.data.iloc[0]
    rows = [
        _metric(_int(d.get("sites_on_register")), "Sites on the Derelict Sites Register"),
        _metric(_eur_full(d.get("amount_levied_eur")), "Levied in 2024"),
        _metric(_eur_full(d.get("total_received_eur")), "Actually collected"),
        _metric(_eur(d.get("cumulative_outstanding_eur")), "Cumulative outstanding"),
    ]
    badge = '<div class="lg-badge">⚠ Levied nothing in 2024</div>' if bool(d.get("levied_nothing")) else ""
    return _stat_card("Derelict sites", rows, "Dept of Housing Derelict Sites annual return 2024", extra=badge)


def _card_planning(name: str) -> str:
    ov = fetch_planning_overturn_result(name)
    if not ov.ok or ov.data.empty:
        return ""
    o = ov.data.iloc[0]
    rows = [
        _metric(_pct(o.get("overturn_rate_pct"), 1), "Decisions overturned by An Bord Pleanála",
                _bench(o.get("overturn_rate_pct"), o.get("national_overturn_rate_pct"), "{:.1f}%", False)),
        _metric(_int(o.get("n_appeals")), "Appeals decided (2016 on)"),
    ]
    return _stat_card("Planning decisions", rows, "An Bord Pleanála appeal outcomes")


def _render_performance(name: str) -> None:
    cards = [
        _card_money_collected(name),
        _card_housing(name),
        _card_derelict(name),
        _card_planning(name),
    ]
    cards = [c for c in cards if c]

    evidence_heading(f"How {name} performs")
    if not cards:
        empty_state("No indicators yet", "No published performance indicators are mapped for this council.")
        return
    st.html(
        '<p class="con-section-note">Published indicators, each beside the <strong>national '
        "benchmark</strong> (median across the 31 councils). These are <strong>executive</strong> "
        "responsibilities — the Chief Executive's administration, not the elected councillors. "
        "▲/▼ shows where the council sits relative to the benchmark; no judgement is implied.</p>"
    )
    st.html(f'<div class="lg-perf-grid">{"".join(cards)}</div>')


def _render_dossier(name: str) -> None:
    res = fetch_chief_executive_result(name)
    if back_button("← All councils", key="lg_back"):
        st.query_params.pop("la", None)
        st.rerun()
    if not res.ok or res.data.empty:
        empty_state("Unknown council", f"“{name}” is not one of the 31 local authorities.")
        return

    row = res.data.iloc[0]
    _render_ce_hero(name, row)
    _render_power_explainer(name)
    _render_performance(name)

    st.caption(
        "Chief Executive: each council's own site / the CCMA. Performance figures are each council's "
        "published whole-area numbers, shown beside the national benchmark; not apportioned and never "
        "summed across measures. Sources: NOAC Performance Indicator Report 2024 · An Bord Pleanála · "
        "Dept of Housing Derelict Sites annual return 2024."
    )


@page_error_boundary
def local_government_page() -> None:
    hide_sidebar()
    selected = st.query_params.get("la")
    if selected:
        _render_dossier(selected)
    else:
        _render_index()
