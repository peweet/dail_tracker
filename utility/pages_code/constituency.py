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

import base64
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
    fetch_constituency_council_housing_performance_result,
    fetch_constituency_housing_with_ssha_result,
    fetch_constituency_waiting_composition_result,
    fetch_constituency_list_result,
    fetch_constituency_map_layers_result,
    fetch_constituency_members_result,
    fetch_constituency_outlines,
    fetch_constituency_party_breakdown_result,
    fetch_council_capital_divisions_result,
    fetch_council_revenue_divisions_result,
)
from ui.entity_links import council_accountability_url, council_spending_url
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
    proportion_stripe_html,
    ranked_member_card,
    search_matches,
    subsection_heading,
    totals_strip,
)
from ui.entity_links import member_profile_url

_EC_REVIEW_URL = "https://www.electoralcommission.ie/constituency-reviews/"

# Housing-section source links (verified live).
_SRC_SSHA = "https://www.housingagency.ie/housing-information/summary-social-housing-assessments-ssha"
_SRC_NOAC = "https://noac.ie/"
_SRC_DERELICT = (
    "https://www.gov.ie/en/department-of-housing-local-government-and-heritage/publications/"
    "annual-returns-for-2024-received-from-local-authorities-under-the-derelict-sites-act-1990/"
)


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


def _pct(v) -> str:
    """Whole-number percent label: 23% (display only)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return f"{float(v):.0f}%"
    except (TypeError, ValueError):
        return "—"


def _num1(v) -> str:
    """One-decimal number label: 12.4 (display only)."""
    if v is None or (isinstance(v, float) and pd.isna(v)):
        return "—"
    try:
        return f"{float(v):,.1f}"
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


# ── NATIONAL CHOROPLETH ───────────────────────────────────────────────────────
# A "colour every constituency by one measure" overview on the index. Display only:
# v_constituency_map_layers ships the value AND a precomputed NTILE(5) quintile per
# layer; this code only maps quintile → palette colour (no derivation). Rendered as a
# single <img> data-URI (st.html strips bare <svg>) — the searchable card grid below
# is the reliable selector, mirroring "colour the map, or search below".

# 5-step warm sequential ramp (light beige → brand terracotta). Index 0 = lowest fifth.
_CHORO_PALETTE = ["#f3ead9", "#e4c39c", "#d29a64", "#bd6e3e", "#a5431c"]
_CHORO_NODATA = "#e9e2d4"
# Fixed pixel height for the rendered map. Image-map <area> coords are in image
# pixels and DON'T rescale when the <img> is CSS-resized, so the image is rendered
# at a fixed size (CSS must not stretch it) and the coords are scaled to match.
_CHORO_PX_H = 460

# label → (quintile column, raw-value column, value formatter, caption phrase)
_MAP_LAYERS: dict[str, tuple[str, str, object, str]] = {
    "Population": ("q_population", "population_2022", _int, "residents (Census 2022)"),
    "People per TD": ("q_population_per_td", "population_per_td", _int, "people per TD"),
    "% of TDs who are landlords": (
        "q_pct_landlord_tds",
        "pct_landlord_tds",
        _pct,
        "of current TDs declared as landlords (latest register)",
    ),
    "Dáil questions per TD": (
        "q_questions_per_td",
        "questions_per_td",
        _num1,
        "parliamentary questions per TD (34th Dáil, since 29 Nov 2024)",
    ),
}


def _path_subpaths(d: str) -> list[list[tuple[float, float]]]:
    """Parse an M/L/Z-only path 'd' into its subpath polygons (point lists)."""
    subs: list[list[tuple[float, float]]] = []
    for chunk in d.split("M"):
        chunk = chunk.strip().replace("Z", "").replace("z", "")
        if not chunk:
            continue
        pts: list[tuple[float, float]] = []
        for tok in chunk.split("L"):
            tok = tok.strip()
            if not tok:
                continue
            try:
                xs, ys = tok.split(",")
                pts.append((float(xs), float(ys)))
            except ValueError:
                continue
        if len(pts) >= 3:
            subs.append(pts)
    return subs


def _poly_area(pts: list[tuple[float, float]]) -> float:
    """Absolute shoelace area — used to pick the mainland (largest subpath)."""
    s = 0.0
    n = len(pts)
    for i in range(n):
        x1, y1 = pts[i]
        x2, y2 = pts[(i + 1) % n]
        s += x1 * y2 - x2 * y1
    return abs(s) / 2.0


def _area_coords(d: str, scale: float) -> str:
    """The largest subpath of a constituency, as a scaled image-map 'coords' string.
    Mapping only the mainland keeps each constituency to ONE clickable polygon (some
    have 150+ island subpaths); the islands stay coloured but aren't click targets."""
    subs = _path_subpaths(d)
    if not subs:
        return ""
    best = max(subs, key=_poly_area)
    nums: list[str] = []
    for x, y in best:
        nums.append(f"{x * scale:.1f}")
        nums.append(f"{y * scale:.1f}")
    return ",".join(nums)


def _choropleth_html(quintile_by_name: dict, alt: str) -> str:
    """All 43 constituencies filled by quintile, as a FIXED-SIZE <img> data-URI with a
    clickable <map> overlay (each mainland → ?constituency= soft-nav via spa_links).
    '' if no map geometry (page then just shows the grid)."""
    outlines = fetch_constituency_outlines()
    paths = outlines.get("constituencies", {})
    if not paths:
        return ""
    vb = outlines.get("viewbox", "0 0 621 1000")
    try:
        _, _, vw, vh = (float(t) for t in vb.split())
    except ValueError:
        vw, vh = 621.1, 1000.0
    scale = _CHORO_PX_H / vh
    px_w = round(vw * scale)
    body, areas = [], []
    for name, d in paths.items():
        q = quintile_by_name.get(name)
        try:
            fill = _CHORO_PALETTE[int(q) - 1] if q is not None and 1 <= int(q) <= 5 else _CHORO_NODATA
        except (TypeError, ValueError):
            fill = _CHORO_NODATA
        body.append(f'<path d="{d}" fill="{fill}" stroke="#fbf8f2" stroke-width="1.2"/>')
        coords = _area_coords(d, scale)
        if coords:
            areas.append(
                f'<area shape="poly" coords="{coords}" '
                f'href="?constituency={quote(name)}" alt="{_h(name)}" title="{_h(name)}">'
            )
    svg = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}">{"".join(body)}</svg>'
    b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return (
        f'<img class="con-choropleth" width="{px_w}" height="{_CHORO_PX_H}" '
        f'usemap="#con-choro-map" src="data:image/svg+xml;base64,{b64}" '
        f'alt="{_h(alt)}" loading="lazy">'
        f'<map name="con-choro-map">{"".join(areas)}</map>'
    )


def _choro_legend() -> str:
    swatches = "".join(f'<span class="con-choro-sw" style="background:{c}"></span>' for c in _CHORO_PALETTE)
    return (
        f'<div class="con-choro-legend">'
        f'<span class="con-choro-end">Lower</span>{swatches}'
        f'<span class="con-choro-end">Higher</span>'
        f"</div>"
    )


def _render_choropleth() -> None:
    res = fetch_constituency_map_layers_result()
    if not res.ok or res.data.empty:
        return  # silent — the searchable grid below remains the reliable selector
    df = res.data
    subsection_heading("Compare every constituency")
    choice = st.radio(
        "Colour the map by",
        list(_MAP_LAYERS.keys()),
        horizontal=True,
        key="con_map_layer",
    )
    qcol, _vcol, _fmt, phrase = _MAP_LAYERS[choice]
    quint = {str(r["constituency_name"]): r[qcol] for _, r in df.iterrows() if pd.notna(r[qcol])}
    map_html = _choropleth_html(quint, alt=f"Map of the 43 Dáil constituencies shaded by {choice}")
    if not map_html:
        return
    st.html(f'<div class="con-choro">{map_html}{_choro_legend()}</div>')
    st.caption(
        f"Each of the 43 constituencies shaded into fifths by {phrase}. "
        "Click a constituency to open its dossier, or pick a card below. "
        "Boundaries: 2023 Electoral Commission review (Census 2022)."
    )


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

    # Exploratory choropleth now sits BELOW the search box + card grid so the grid
    # (the reliable selector) leads first paint (audit 2026-06-21). Kept inline rather
    # than in a collapsed expander — a folium/iframe map renders at 0 width when hidden.
    _render_choropleth()


# ── DOSSIER ───────────────────────────────────────────────────────────────────
def _locator_svg(name: str) -> str:
    """A discreet Ireland thumbnail with this constituency highlighted, returned as an
    <img> with an inline SVG data-URI (st.html strips bare <svg>, but allows <img>).
    '' if no map data."""
    outlines = fetch_constituency_outlines()
    paths = outlines.get("constituencies", {})
    if name not in paths:
        return ""
    vb = outlines.get("viewbox", "0 0 621 1000")
    others = "".join(
        f'<path d="{paths[c]}" fill="#e3ddd1" stroke="#fbf8f2" stroke-width="1.2"/>' for c in paths if c != name
    )
    here = f'<path d="{paths[name]}" fill="#b04a26" stroke="#fbf8f2" stroke-width="1.2"/>'
    svg = f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{vb}">{others}{here}</svg>'
    b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return (
        f'<img class="con-locator" src="data:image/svg+xml;base64,{b64}" '
        f'alt="Location of {_h(name)} within Ireland" loading="lazy">'
    )


def _render_header(name: str, header_row) -> None:
    pop = _int(header_row.get("population_2022"))
    per_td = _int(header_row.get("population_per_td"))
    seats = _int(header_row.get("td_seats"))
    n_tds = _int(header_row.get("n_tds_current"))
    locator = _locator_svg(name)
    map_html = f'<div class="con-hero-map">{locator}</div>' if locator else ""
    st.html(
        f'<div class="con-hero">'
        f'<div class="con-hero-text">'
        f'<p class="dt-kicker">CONSTITUENCY</p>'
        f'<h1 class="con-hero-title">{_h(name)}</h1>'
        f'<p class="con-hero-meta">{seats} seats · {n_tds} TDs · '
        f"Population <strong>{pop}</strong> · <strong>{per_td}</strong> per TD "
        f"(Census 2022)</p>"
        f"</div>"
        f"{map_html}"
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
    comp = row.get("completions")
    comp_period = row.get("comp_period")
    pills: list[str] = []
    if comp is not None and not pd.isna(comp):
        per = f" <em>({_h(str(comp_period))})</em>" if comp_period is not None and not pd.isna(comp_period) else ""
        pills.append(f'<span class="con-grain con-grain-comp">{_int(comp)} new homes{per}</span>')
    if vac is not None and not pd.isna(vac):
        rate_txt = f" ({rate:.1f}%)" if rate is not None and not pd.isna(rate) else ""
        pills.append(f'<span class="con-grain con-grain-vac">{_int(vac)} vacant homes{rate_txt}</span>')
    if price is not None and not pd.isna(price):
        pills.append(f'<span class="con-grain con-grain-price">Median price {_eur(price)}</span>')
    # Demand side — social-housing waiting list (SSHA), merged in by local_authority.
    waiting = row.get("waiting_total_2025")
    if waiting is not None and not pd.isna(waiting):
        yoy = row.get("waiting_yoy_pct")
        yoy_txt = ""
        if yoy is not None and not pd.isna(yoy):
            arrow = "↑" if yoy > 0 else ("↓" if yoy < 0 else "→")
            yoy_txt = f" <em>({arrow}{abs(yoy):.1f}%)</em>"
        pills.append(f'<span class="con-grain con-grain-ssha">{_int(waiting)} on housing list{yoy_txt}</span>')
    long_wait = row.get("long_wait_pct")
    if long_wait is not None and not pd.isna(long_wait):
        over7 = row.get("over_7yr_pct")
        o7 = f" <em>({over7:.0f}% over 7 yrs)</em>" if over7 is not None and not pd.isna(over7) else ""
        pills.append(f'<span class="con-grain con-grain-wait">{long_wait:.0f}% waiting 4 yrs+{o7}</span>')
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
    # Supply (vacancy / price / completions) LEFT-joined with demand (SSHA waiting list) in the
    # core query, so each council card shows both from ONE result; supply-only if SSHA is absent.
    res = fetch_constituency_housing_with_ssha_result(name)
    if not res.ok or res.data.empty:
        return
    df = res.data
    has_ssha = "waiting_total_2025" in df.columns
    evidence_heading("Housing in this area")
    vac_period = next((str(p) for p in df["vac_period"] if p), "")
    med_period = next((str(p) for p in df["med_period"] if p), "")
    comp_period = next((str(p) for p in df["comp_period"] if p), "")
    st.html(
        '<p class="con-section-note">New homes completed, residential vacancy, median house '
        "price and the social-housing waiting list for the local-authority area(s) serving "
        "this constituency — <strong>council-area</strong> figures (the area is not the "
        "constituency).</p>"
    )
    cards = [c for c in (_housing_card(r) for _, r in df.iterrows()) if c]
    if cards:
        st.html(f'<div class="con-council-grid">{"".join(cards)}</div>')
    src = " · ".join(
        x
        for x in [
            f"New homes: CSO new dwelling completions {comp_period}" if comp_period else "",
            f"Vacancy: CSO metered-electricity vacancy {vac_period}" if vac_period else "",
            f"Median price: CSO RPPI {med_period}" if med_period else "",
            "Waiting list: Housing Agency SSHA 2025" if has_ssha else "",
        ]
        if x
    )
    if src:
        st.caption(src)
    _render_waiting_breakdown(name)


# Dimensions shown in the "who's waiting here" expander (sequential time stripe first).
_WAIT_DIMS = [
    ("time_on_list", "How long they’ve waited", "sequential"),
    ("tenure", "Where they live now", "categorical"),
    ("employment", "Employment", "categorical"),
]


def _render_waiting_breakdown(name: str) -> None:
    """Expander: the social-housing waiting-list composition for the serving council(s),
    reusing the national composition view (grain='la'). Collapsed by default."""
    res = fetch_constituency_waiting_composition_result(name)
    if not res.ok or res.data.empty:
        return
    df = res.data
    with st.expander("Who’s waiting here — breakdown by serving council"):
        for council in df["local_authority"].drop_duplicates():
            sub = df[df["local_authority"] == council]
            st.html(f'<p class="hou-dim-title" style="font-size:0.95rem">{_h(str(council))}</p>')
            for dim, title, palette in _WAIT_DIMS:
                d = sub[sub["dimension"] == dim].sort_values(
                    ["ord", "count"], ascending=[True, False], na_position="last"
                )
                if d.empty:
                    continue
                segs = [(str(r["category"]), float(r["count"])) for _, r in d.iterrows()]
                st.html(
                    f'<p class="con-section-note" style="margin:0.3rem 0 0.2rem">{title}</p>'
                    + proportion_stripe_html(segs, palette=palette)
                )
        st.caption(
            f"[Housing Agency SSHA 2025]({_SRC_SSHA}) · council-area figures (the area is not the constituency)."
        )


def _perf_pill(value, fmt: str, label: str, national, nat_fmt: str | None = None) -> str:
    """One council-performance pill: the LA value + the national-median benchmark,
    presented factually (no good/bad framing — direction-of-good varies by metric)."""
    if value is None or pd.isna(value):
        return ""
    nat_txt = ""
    if national is not None and not pd.isna(national):
        nat_txt = f" <em>(median {(nat_fmt or fmt).format(national)})</em>"
    return f'<span class="con-grain con-grain-perf">{fmt.format(value)} {label}{nat_txt}</span>'


def _council_perf_card(row) -> str:
    council = _h(str(row["local_authority"]))
    partial = str(row.get("link_type")) == "partial"
    pills = [
        _perf_pill(row.get("vacancy_pct"), "{:.1f}%", "stock vacant", row.get("nat_vacancy_pct")),
        _perf_pill(row.get("reletting_weeks"), "{:.0f}", "wks to re-let", row.get("nat_reletting_weeks")),
        _perf_pill(
            row.get("maintenance_eur_per_dwelling"),
            "€{:,.0f}",
            "upkeep/home",
            row.get("nat_maintenance_eur_per_dwelling"),
        ),
        _perf_pill(
            row.get("retrofit_pct_of_stock"), "{:.1f}%", "of stock retrofitted", row.get("nat_retrofit_pct_of_stock")
        ),
        _perf_pill(
            row.get("longterm_homeless_pct"),
            "{:.0f}%",
            "homeless adults long-term",
            row.get("nat_longterm_homeless_pct"),
        ),
        _perf_pill(
            row.get("rent_collection_pct"), "{:.0f}%", "rent collected", row.get("nat_rent_collection_pct")
        ),
    ]
    # Derelict Sites Levy enforcement gap — the cumulative amount still uncollected.
    der_out = row.get("derelict_outstanding_eur")
    if der_out is not None and not pd.isna(der_out) and der_out > 0:
        lev = row.get("derelict_levied_eur")
        lev_txt = f" <em>({_eur(lev)} levied 2024)</em>" if lev is not None and not pd.isna(lev) else ""
        pills.append(
            f'<span class="con-grain con-grain-wait">{_eur(der_out)} derelict levy outstanding{lev_txt}</span>'
        )
    pills = [p for p in pills if p]
    if not pills:
        return ""
    flag = ' <span class="con-council-partial">partial</span>' if partial else ""
    return (
        f'<div class="con-council-card{" con-council-card-partial" if partial else ""}">'
        f'<div class="con-council-name">{council}{flag}</div>'
        f'<div class="con-grain-row">{"".join(pills)}</div>'
        f"</div>"
    )


def _render_council_housing_performance(name: str) -> None:
    res = fetch_constituency_council_housing_performance_result(name)
    if not res.ok or res.data.empty:
        return
    df = res.data
    cards = [c for c in (_council_perf_card(r) for _, r in df.iterrows()) if c]
    if not cards:
        return
    evidence_heading("Council housing performance")
    st.html(
        '<p class="con-section-note">How the local-authority area(s) serving this constituency '
        "manage their social housing, collect what they’re owed, and enforce dereliction — most "
        "figures beside the <strong>national median</strong> across all 31 councils. "
        "<strong>Council-area</strong> figures (the area is not the constituency).</p>"
    )
    st.html(f'<div class="con-council-grid">{"".join(cards)}</div>')
    st.caption(
        f"**Sources:** [NOAC Local Authority Performance Indicator Report 2024]({_SRC_NOAC}) "
        f"(performance + rent collection) · [Dept of Housing Derelict Sites annual return 2024]"
        f"({_SRC_DERELICT}) (levy outstanding)."
    )


def _council_card(row, constituency: str) -> str:
    council = str(row["local_authority"])
    council_h = _h(council)
    partial = str(row.get("link_type")) == "partial"
    shared = bool(row.get("la_serves_multiple_constituencies"))

    pills: list[str] = []
    has_afs = False
    rev = row.get("afs_revenue_gross_eur")
    if rev is not None and not pd.isna(rev):
        has_afs = True
        yr = _yr(row.get("afs_revenue_year"))
        pills.append(f'<span class="con-grain con-grain-rev">Revenue account {_eur(rev)} <em>({yr})</em></span>')
    cap = row.get("capital_expenditure_eur")
    if cap is not None and not pd.isna(cap):
        has_afs = True
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
    # A by-division drill-down is available only where we have audited (AFS) accounts.
    if has_afs:
        notes.append("see spending by service →")
    note_html = f'<div class="con-council-note">{_h(" · ".join(notes))}</div>' if notes else ""

    flag = ' <span class="con-council-partial">partial</span>' if partial else ""
    inner = (
        f'<div class="con-council-card{" con-council-card-partial" if partial else ""}">'
        f'<div class="con-council-name">{council_h}{flag}</div>'
        f"{body}{note_html}"
        f"</div>"
    )
    if not has_afs:
        return inner  # nothing more to drill into — leave it non-clickable
    href = f"?constituency={quote(constituency)}&council={quote(council)}"
    return clickable_card_link(
        href=href,
        inner_html=inner,
        aria_label=f"See {council} spending by service division",
    )


def _div_bar_rows(rows, value_key: str, label: str) -> str:
    """Horizontal value bars for a council's by-division spend (display-only scaling)."""
    vals = [
        (str(r["division"]), float(r[value_key]))
        for _, r in rows.iterrows()
        if r.get(value_key) is not None and not pd.isna(r.get(value_key)) and float(r[value_key]) > 0
    ]
    if not vals:
        return ""
    top = max(v for _, v in vals) or 1
    out = []
    for div, v in vals:
        pct = max(2.0, v / top * 100)
        out.append(
            f'<div class="con-div-row">'
            f'<div class="con-div-name">{_h(div)}</div>'
            f'<div class="con-div-track"><div class="con-div-bar" style="width:{pct:.1f}%"></div></div>'
            f'<div class="con-div-val">{_eur(v)}</div>'
            f"</div>"
        )
    return f'<div class="con-div-block"><div class="con-div-head">{_h(label)}</div>{"".join(out)}</div>'


def _render_council_detail(constituency: str, council: str) -> None:
    rev = fetch_council_revenue_divisions_result(council)
    cap = fetch_council_capital_divisions_result(council)
    if back_button(f"← All councils serving {constituency}", key="con_council_back"):
        st.query_params.pop("council", None)
        st.rerun()
    rev_year = _yr(rev.data["year"].iloc[0]) if rev.ok and not rev.data.empty else ""
    cap_year = _yr(cap.data["year"].iloc[0]) if cap.ok and not cap.data.empty else ""
    subsection_heading(f"{council} — spending by service")
    st.html(
        '<p class="con-section-note">The council\'s audited Annual Financial Statement, broken down '
        "by service division — its <strong>whole council area</strong>, not just this constituency. "
        "Revenue (running services) and capital (building/acquiring) are separate accounts, "
        "<strong>never summed</strong>.</p>"
    )
    blocks = ""
    if rev.ok and not rev.data.empty:
        blocks += _div_bar_rows(rev.data, "gross_expenditure_eur", f"Revenue account, gross by service ({rev_year})")
    if cap.ok and not cap.data.empty:
        blocks += _div_bar_rows(cap.data, "capital_expenditure_eur", f"Capital invested by service ({cap_year})")
    if blocks:
        st.html(f'<div class="con-div-wrap">{blocks}</div>')
    else:
        empty_state("No by-division detail", "This council's audited accounts aren't parsed by service yet.")
    # conduit to the source document + the fuller council dossier
    src = ""
    if rev.ok and not rev.data.empty:
        url = rev.data["source_file_url"].iloc[0]
        if isinstance(url, str) and url.startswith("http"):
            src = f'<a class="dt-source-link" href="{_h(url)}" target="_blank" rel="noopener">Audited accounts (PDF)</a> · '
    st.html(
        f'<p class="con-section-note" style="margin-top:0.6rem">{src}'
        f'<a class="dt-source-link" href="{_h(council_spending_url(council, "COMMITTED"))}" '
        f'target="_self">Full {_h(council)} dossier (suppliers, multi-year)</a> · '
        f'<a class="dt-source-link" href="{_h(council_accountability_url(council))}" '
        f'target="_self">Who runs {_h(council)} →</a></p>'
    )


def _render_council_context(name: str) -> None:
    res = fetch_constituency_council_context_result(name)
    evidence_heading("Council spending in this area")
    if not res.ok or res.data.empty:
        empty_state("No council mapping", "No serving local authority is mapped for this constituency.")
        return

    # Drill-down: a council card sets ?council=. Render that council's by-division detail in
    # place of the grid (only if the council actually serves this constituency).
    sel = st.query_params.get("council")
    if sel and sel in set(res.data["local_authority"]):
        _render_council_detail(name, sel)
        return

    st.html(
        '<p class="con-section-note">The local authority(ies) serving this area, each with its '
        "<strong>own</strong> published money. These are <strong>council-area</strong> figures — "
        "the council area is not the constituency, and the totals are <strong>never apportioned</strong> "
        "to it. Revenue (running services), capital (building), and orders/payments are different "
        "stages of council money and are <strong>never added together</strong>. Click a council with "
        "audited accounts to see its spending by service.</p>"
    )
    df = res.data
    primary = df[df["link_type"] == "primary"]
    partial = df[df["link_type"] == "partial"]
    cards = [_council_card(r, name) for _, r in primary.iterrows()]
    if cards:
        st.html(f'<div class="con-council-grid">{"".join(cards)}</div>')
    if not partial.empty:
        st.caption("Also partly covered by:")
        pcards = [_council_card(r, name) for _, r in partial.iterrows()]
        st.html(f'<div class="con-council-grid">{"".join(pcards)}</div>')

    # Contextual edge → the "Who runs your county" dossier per serving council (the
    # appointed Chief Executive + accountability indicators). Rendered as standalone
    # links: the spend cards above are themselves anchors, so this can't nest inside
    # them. Carries the council entity instead of dropping the user on a generic index.
    councils = list(dict.fromkeys(str(la) for la in df["local_authority"]))
    if councils:
        links = " · ".join(
            f'<a class="dt-source-link" href="{_h(council_accountability_url(la))}" '
            f'target="_self">{_h(la)}</a>'
            for la in councils
        )
        whose = "these councils" if len(councils) > 1 else "this council"
        st.html(
            f'<p class="con-section-note" style="margin-top:0.6rem">'
            f"<strong>Who runs {whose}?</strong> See the appointed Chief Executive "
            f"and how the council performs: {links}</p>"
        )


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
    _render_council_housing_performance(name)
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
