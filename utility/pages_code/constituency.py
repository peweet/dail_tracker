"""Constituencies — a per-constituency civic dossier ("Your Area").

The unit citizens vote in. Pick one of the 43 Dáil constituencies (2023 Electoral
Commission boundaries) and see who represents it, the Census-2022 demographics,
and — as CONTEXT — what the local authority(ies) serving the area spend.

DISPLAY ONLY. Every figure arrives pre-aggregated from a registered
``v_constituency_*`` view via ``dail_tracker_core.queries.constituency``; this
page never JOINs, GROUPs, or derives a metric. The crosswalk + grain guards live
in ``sql_views/constituency/*``.

⚠️ Council money is the council's WHOLE-AREA figure shown because the council
serves (part of) this constituency — it is NEVER apportioned into a per-
constituency number, and the four grains (revenue / capital / ordered / paid) are
never summed. The page states both caveats in the UI.

Index → dossier is a soft ``?constituency=`` rerun (utility/ui/spa_links).
House-work sections (questions / votes / speeches / interests) land in Phase 2;
housing (CSO completions / vacancy) in Phase 3.
"""

from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from data_access.constituency_data import (
    fetch_constituency_council_context_result,
    fetch_constituency_header_result,
    fetch_constituency_house_work_result,
    fetch_constituency_housing_context_result,
    fetch_constituency_list_result,
    fetch_constituency_members_result,
    fetch_constituency_party_breakdown_result,
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
    party_stripe_html,
    ranked_member_card,
    search_matches,
    subsection_heading,
    totals_strip,
)
from ui.entity_links import member_profile_url

_EC_REVIEW_URL = "https://www.electoralcommission.ie/constituency-reviews/"


# ── display-only formatting (no derivation) ───────────────────────────────────
def _eur(v) -> str:
    """Compact euro label: €1.44bn / €478m / €212k / €950."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        v = float(v)
    except (TypeError, ValueError):
        return "—"
    a = abs(v)
    if a >= 1e9:
        return f"€{v / 1e9:.2f}bn"
    if a >= 1e6:
        return f"€{v / 1e6:.0f}m"
    if a >= 1e3:
        return f"€{v / 1e3:.0f}k"
    return f"€{v:,.0f}"


def _int(v) -> str:
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return f"{int(v):,}"
    except (TypeError, ValueError):
        return "—"


def _yr(v) -> str:
    """Year with no thousands separator."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return str(int(v))
    except (TypeError, ValueError):
        return "—"


# ── INDEX ─────────────────────────────────────────────────────────────────────
def _constituency_card_inner(row) -> str:
    name = _h(str(row["constituency_name"]))
    pop = _int(row.get("population_2022"))
    seats = _int(row.get("td_seats"))
    n_tds = _int(row.get("n_tds_current"))
    per_td = _int(row.get("population_per_td"))
    return (
        f'<div class="con-card-inner">'
        f'<div class="con-card-name">{name}</div>'
        f'<div class="con-card-meta">{seats} seats · {n_tds} TDs · {pop} people</div>'
        f'<div class="con-card-sub">{per_td} people per TD (Census 2022)</div>'
        f"</div>"
    )


def _render_index() -> None:
    hero_banner(
        kicker="YOUR AREA",
        title="Constituencies",
        dek="The 43 Dáil constituencies on the current boundaries — who represents each "
        "area, and what the councils serving it spend. Pick your constituency.",
    )

    res = fetch_constituency_list_result()
    if not res.ok or res.data.empty:
        empty_state(
            "Constituencies are unavailable",
            "The constituency registry could not be loaded. Try refreshing.",
        )
        return
    df = res.data

    query, _ = find_a_td_filter(
        df["constituency_name"].tolist(),
        key_prefix="con_idx",
        label="Find your constituency",
        placeholder="Search by constituency name…",
        show_picker=False,
    )
    shown = df[[search_matches(query, n) for n in df["constituency_name"]]] if query else df
    if shown.empty:
        empty_state("No match", f"No constituency matches “{query}”.")
        return

    cards = [
        clickable_card_link(
            href=f"?constituency={quote(str(r['constituency_name']))}",
            inner_html=_constituency_card_inner(r),
            aria_label=f"Open the {r['constituency_name']} dossier",
        )
        for _, r in shown.iterrows()
    ]
    st.html(f'<div class="con-card-grid">{"".join(cards)}</div>')
    st.caption(
        f"{len(shown)} of {len(df)} constituencies · "
        "Population: Census 2022 on the 2023 Electoral Commission boundaries."
    )


# ── DOSSIER ───────────────────────────────────────────────────────────────────
def _render_header(name: str, header_row) -> None:
    pop = _int(header_row.get("population_2022"))
    per_td = _int(header_row.get("population_per_td"))
    seats = _int(header_row.get("td_seats"))
    n_tds = _int(header_row.get("n_tds_current"))
    st.html(
        f'<div class="con-hero">'
        f'<p class="dt-kicker">CONSTITUENCY</p>'
        f'<h1 class="con-hero-title">{_h(name)}</h1>'
        f'<p class="con-hero-meta">{seats} seats · {n_tds} TDs · '
        f"Population <strong>{pop}</strong> · <strong>{per_td}</strong> per TD "
        f"(Census 2022)</p>"
        f"</div>"
    )


def _render_party_bar(name: str) -> None:
    res = fetch_constituency_party_breakdown_result(name)
    if not res.ok or res.data.empty:
        return
    parties = [(str(r["party_name"]), int(r["n_seats"])) for _, r in res.data.iterrows()]
    stripe = party_stripe_html(parties, show_legend=True)
    if stripe:
        st.html(f'<div class="con-party-bar">{stripe}</div>')


def _render_roster(name: str) -> None:
    res = fetch_constituency_members_result(name)
    subsection_heading("Who represents you")
    if not res.ok or res.data.empty:
        empty_state("No TDs found", "The member registry has no current TDs for this constituency.")
        return
    rows = list(res.data.iterrows())
    cols = st.columns(2)
    for i, (_, r) in enumerate(rows):
        party = str(r.get("party_name") or "Independent")
        is_min = str(r.get("is_minister")).lower() == "true"
        meta = party + (" · Minister" if is_min else "")
        href = member_profile_url(str(r["unique_member_code"]))
        card = ranked_member_card(
            name=str(r["member_name"]),
            meta=meta,
            profile_href=href,
        )
        with cols[i % 2]:
            st.html(card)


def _render_house_work(name: str) -> None:
    res = fetch_constituency_house_work_result(name)
    if not res.ok or res.data.empty:
        return
    r = res.data.iloc[0]
    evidence_heading("What its TDs do in the Dáil")
    st.html(
        '<p class="con-section-note">Work by this constituency’s current TDs since the '
        "2024 general election — a count of activity, not a quality judgement. Open any TD "
        "above for the detail behind these totals.</p>"
    )
    items = [
        (_int(r.get("n_questions")), "Parliamentary questions"),
        (_int(r.get("n_speeches")), "Floor contributions"),
        (_int(r.get("n_votes_cast")), "Votes cast"),
        (_int(r.get("n_words")), "Words spoken"),
    ]
    totals_strip(items)
    landlords = int(r.get("n_landlords") or 0)
    owners = int(r.get("n_property_owners") or 0)
    if landlords or owners:
        bits = []
        if owners:
            bits.append(f"<strong>{owners}</strong> declared property")
        if landlords:
            bits.append(f"<strong>{landlords}</strong> declared as a landlord")
        st.html(
            '<p class="con-section-note" style="margin-top:0.5rem">Register of interests: '
            + " · ".join(bits)
            + " (latest declarations).</p>"
        )


def _housing_card(row) -> str:
    council = _h(str(row["local_authority"]))
    partial = str(row.get("link_type")) == "partial"
    vac = row.get("vacant_dwellings")
    rate = row.get("vacancy_rate")
    price = row.get("median_price_eur")
    pills: list[str] = []
    if vac is not None and not pd.isna(vac):
        rate_txt = f" ({rate:.1f}%)" if rate is not None and not pd.isna(rate) else ""
        pills.append(f'<span class="con-grain con-grain-vac">{_int(vac)} vacant homes{rate_txt}</span>')
    if price is not None and not pd.isna(price):
        pills.append(f'<span class="con-grain con-grain-price">Median price {_eur(price)}</span>')
    if not pills:
        return ""
    flag = ' <span class="con-council-partial">partial</span>' if partial else ""
    return (
        f'<div class="con-council-card{" con-council-card-partial" if partial else ""}">'
        f'<div class="con-council-name">{council}{flag}</div>'
        f'<div class="con-grain-row">{"".join(pills)}</div>'
        f"</div>"
    )


def _render_housing(name: str) -> None:
    res = fetch_constituency_housing_context_result(name)
    if not res.ok or res.data.empty:
        return
    df = res.data
    evidence_heading("Housing in this area")
    vac_period = next((str(p) for p in df["vac_period"] if p), "")
    med_period = next((str(p) for p in df["med_period"] if p), "")
    st.html(
        '<p class="con-section-note">Residential vacancy and median house price for the '
        "local-authority area(s) serving this constituency — <strong>council-area</strong> "
        "figures (the area is not the constituency).</p>"
    )
    cards = [c for c in (_housing_card(r) for _, r in df.iterrows()) if c]
    if cards:
        st.html(f'<div class="con-council-grid">{"".join(cards)}</div>')
    src = " · ".join(
        x for x in [
            f"Vacancy: CSO metered-electricity vacancy {vac_period}" if vac_period else "",
            f"Median price: CSO RPPI {med_period}" if med_period else "",
        ] if x
    )
    if src:
        st.caption(src)


def _council_card(row) -> str:
    council = _h(str(row["local_authority"]))
    partial = str(row.get("link_type")) == "partial"
    shared = bool(row.get("la_serves_multiple_constituencies"))

    pills: list[str] = []
    rev = row.get("afs_revenue_gross_eur")
    if rev is not None and not pd.isna(rev):
        yr = _yr(row.get("afs_revenue_year"))
        pills.append(f'<span class="con-grain con-grain-rev">Revenue account {_eur(rev)} <em>({yr})</em></span>')
    cap = row.get("capital_expenditure_eur")
    if cap is not None and not pd.isna(cap):
        yr = _yr(row.get("capital_year"))
        pills.append(f'<span class="con-grain con-grain-cap">Capital invested {_eur(cap)} <em>({yr})</em></span>')
    ordered = row.get("ordered_safe_eur")
    if ordered is not None and not pd.isna(ordered) and float(ordered) > 0:
        pills.append(f'<span class="con-grain con-grain-po">Ordered &gt;€20k {_eur(ordered)}</span>')
    paid = row.get("paid_safe_eur")
    if paid is not None and not pd.isna(paid) and float(paid) > 0:
        pills.append(f'<span class="con-grain con-grain-po">Paid &gt;€20k {_eur(paid)}</span>')

    if not pills:
        body = '<div class="con-council-empty">No published spending data yet.</div>'
    else:
        body = f'<div class="con-grain-row">{"".join(pills)}</div>'

    notes: list[str] = []
    if partial:
        notes.append("covers part of this constituency")
    if shared:
        notes.append("this council also serves other constituencies")
    note_html = f'<div class="con-council-note">{_h(" · ".join(notes))}</div>' if notes else ""

    flag = ' <span class="con-council-partial">partial</span>' if partial else ""
    return (
        f'<div class="con-council-card{" con-council-card-partial" if partial else ""}">'
        f'<div class="con-council-name">{council}{flag}</div>'
        f"{body}{note_html}"
        f"</div>"
    )


def _render_council_context(name: str) -> None:
    res = fetch_constituency_council_context_result(name)
    evidence_heading("Council spending in this area")
    if not res.ok or res.data.empty:
        empty_state("No council mapping", "No serving local authority is mapped for this constituency.")
        return
    st.html(
        '<p class="con-section-note">The local authority(ies) serving this area, each with its '
        "<strong>own</strong> published money. These are <strong>council-area</strong> figures — "
        "the council area is not the constituency, and the totals are <strong>never apportioned</strong> "
        "to it. Revenue (running services), capital (building), and orders/payments are different "
        "stages of council money and are <strong>never added together</strong>.</p>"
    )
    df = res.data
    primary = df[df["link_type"] == "primary"]
    partial = df[df["link_type"] == "partial"]
    cards = [_council_card(r) for _, r in primary.iterrows()]
    if cards:
        st.html(f'<div class="con-council-grid">{"".join(cards)}</div>')
    if not partial.empty:
        st.caption("Also partly covered by:")
        pcards = [_council_card(r) for _, r in partial.iterrows()]
        st.html(f'<div class="con-council-grid">{"".join(pcards)}</div>')


def _render_dossier(name: str) -> None:
    header_res = fetch_constituency_header_result(name)
    if not header_res.ok or header_res.data.empty:
        if back_button("← All constituencies", key="cons_back_miss"):
            st.query_params.pop("constituency", None)
            st.rerun()
        empty_state("Unknown constituency", f"“{name}” is not one of the 43 Dáil constituencies.")
        return

    if back_button("← All constituencies", key="cons_back"):
        st.query_params.pop("constituency", None)
        st.rerun()

    header_row = header_res.data.iloc[0]
    _render_header(name, header_row)
    _render_party_bar(name)
    _render_roster(name)
    _render_house_work(name)
    _render_housing(name)
    _render_council_context(name)

    st.caption(
        "Population: Electoral Commission Constituency Review 2023, Appendix 2 (Census 2022, "
        "2023 boundaries). Council–constituency mapping: same report, Appendix 1. Council "
        "spending: councils' audited Annual Financial Statements + their published "
        "purchase-order/payment lists."
    )
    st.html(
        f'<a class="dt-source-link" href="{_EC_REVIEW_URL}" target="_blank" '
        'rel="noopener">Electoral Commission constituency reviews</a>'
    )


@page_error_boundary
def constituency_page() -> None:
    hide_sidebar()
    selected = st.query_params.get("constituency")
    if selected:
        _render_dossier(selected)
    else:
        _render_index()
