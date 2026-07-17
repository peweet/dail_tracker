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

import base64
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
    fetch_council_money_result,
    fetch_derelict_levy_ranking_result,
    fetch_derelict_sites_levy_result,
    fetch_housing_performance_result,
    fetch_la_map_layers_result,
    fetch_lgas_audit_result,
    fetch_la_outlines,
    fetch_national_summary_result,
    fetch_noac_indicators_result,
    fetch_noac_scorecard_history_result,
    fetch_noac_scorecard_result,
    fetch_planning_overturn_result,
)
from ui.components import (
    back_button,
    clickable_card_link,
    empty_state,
    evidence_heading,
    find_a_td_filter,
    hero_banner,
    dt_page,
    search_matches,
    subsection_heading,
    totals_strip,
)
from ui.entity_links import council_spending_url
from ui.format import eur, eur_full, fmt_int, pct

_SALARY_BAND = "€132,511–€189,301"  # national CE pay scale (not published per-council)
_LGA_URL = "https://www.irishstatutebook.ie/eli/2001/act/37/enacted/en/html"


# ── display-only formatting (no derivation) ───────────────────────────────────
# Canonical formatters (ui.format, 2026-07 consolidation): €1.4bn / €918.5m / €212k.
_eur = eur
_eur_full = eur_full
_pct = pct
_int = fmt_int


def _num1(v) -> str:
    return "—" if v is None or pd.isna(v) else f"{float(v):.1f}"


def _bench(value, national, fmt: str, higher_is_better: bool | None = None) -> str:
    """The 'national X ▲/▼' benchmark sub-line for a metric. The arrow is a NEUTRAL
    position marker vs the national median (▲ above / ▼ below) — no good/bad colour.
    Firewall-safe: published value + published benchmark, no 'good/bad' word, and the
    direction-of-good is deliberately NOT encoded in colour (it would put the verdict in
    colour alone — lost under deuteranopia — and contradict 'no judgement implied'). The
    ``higher_is_better`` arg is retained for call-site compatibility but unused."""
    if value is None or pd.isna(value) or national is None or pd.isna(national):
        return ""
    arrow = "▲" if float(value) >= float(national) else "▼"
    return f'national {fmt.format(national)} <span class="lg-arrow-neutral">{arrow}</span>'


def _metric(value: str, label: str, bench: str = "", doc_url: str = "", doc_page: int | None = None) -> str:
    """One metric row. Optional ``doc_url`` renders a small source deep-link after the label
    (-> the exact NOAC report page)."""
    doc = (
        (
            f' <a class="lg-metric-doc" href="{_h(doc_url)}" target="_blank" rel="noopener" '
            f'title="NOAC report, page {doc_page}">NOAC p.{doc_page} ↗</a>'
        )
        if doc_url
        else ""
    )
    return (
        '<div class="lg-metric"><div class="lg-metric-main">'
        f'<span class="lg-metric-value">{value}</span>'
        f'<span class="lg-metric-label">{label}{doc}</span></div>'
        f'<div class="lg-metric-bench">{bench}</div></div>'
    )


def _stat_card(title: str, rows: list[str], source: str, extra: str = "", src_url: str = "") -> str:
    body = "".join(r for r in rows if r)
    if not body:
        return ""
    src = f'<a href="{_h(src_url)}" target="_blank" rel="noopener">{_h(source)} ↗</a>' if src_url else _h(source)
    return (
        f'<div class="lg-card"><div class="lg-card-title">{_h(title)}</div>'
        f"{body}{extra}"
        f'<div class="lg-card-src">{src}</div></div>'
    )


# ── NATIONAL CHOROPLETH (index "Every council, compared") ─────────────────────
# Same technique as the constituency index map: one fixed-size <img> data-URI (st.html
# strips bare <svg>) with a clickable <map> overlay. The view ships the value AND a
# precomputed NTILE(5) quintile per layer; this code only maps quintile → palette colour.
_CHORO_PALETTE = ["#f3ead9", "#e4c39c", "#d29a64", "#bd6e3e", "#a5431c"]  # low → high fifth
_CHORO_NODATA = "#e9e2d4"
_CHORO_PX_H = 540  # image-map <area> coords are in image pixels and DON'T rescale with CSS

# label → (quintile column, caption phrase). All four are executive-function signals,
# reused from the dossier views so the map and the per-council cards never disagree.
_MAP_LAYERS: dict[str, tuple[str, str]] = {
    "Commercial rates collected": ("q_commercial_rates", "of commercial rates collected (NOAC 2024)"),
    "Derelict-levy uncollected": (
        "q_derelict_outstanding",
        "in derelict-site levies left uncollected (cumulative, 2024)",
    ),
    "Planning overturned on appeal": (
        "q_planning_overturn",
        "of planning decisions overturned by An Bord Pleanála (2016 on)",
    ),
    "Council homes vacant": ("q_housing_vacancy", "of council homes lying vacant (NOAC 2024)"),
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


# Discrete zoom regions: the small urban authorities are unclickable at national scale
# (Galway City ≈15×9px, the Dublin cluster ≈15–25px), and image-map <area> coords are
# fixed pixels that DON'T rescale with CSS — so a scroll/CSS zoom would misalign every
# target. Instead each region re-renders the map CROPPED to its bounds at full size, so
# the same ?la= click targets become large. label → (member LAs defining the crop, pad).
_ZOOM_W = 520  # max width budget for a zoomed (cropped) render
_ZOOMS: dict[str, tuple[set[str] | None, float]] = {
    "Ireland": (None, 0.0),
    "Dublin": ({"Dublin City", "Fingal", "South Dublin", "Dun Laoghaire-Rathdown"}, 0.12),
    "Cork city": ({"Cork City"}, 0.9),
    "Galway city": ({"Galway City"}, 1.3),
}


def _paths_bbox(paths: dict, names: set[str]) -> tuple[float, float, float, float] | None:
    xs: list[float] = []
    ys: list[float] = []
    for n in names:
        for sub in _path_subpaths(paths.get(n, "")):
            for x, y in sub:
                xs.append(x)
                ys.append(y)
    if not xs:
        return None
    return min(xs), min(ys), max(xs), max(ys)


def _choropleth_html(quintile_by_name: dict, alt: str, zoom: str = "Ireland", *, link_key: str = "la") -> str:
    """All 31 authorities filled by quintile, as a FIXED-SIZE <img> data-URI with a
    clickable <map> overlay (each → ?<link_key>= soft-nav). When ``zoom`` names a region the
    SVG viewBox is cropped to that region's bounds and the image re-scaled to fill,
    enlarging otherwise-unclickable city targets. '' if no map geometry.

    ``link_key`` is the query param each area links to — "la" for this page's own dossier
    (the default), "council" when the Your Council hub reuses the same map."""
    outlines = fetch_la_outlines()
    paths = outlines.get("local_authorities", {})
    if not paths:
        return ""
    try:
        _, _, vw, vh = (float(t) for t in outlines.get("viewbox", "0 0 689 1000").split())
    except ValueError:
        vw, vh = 688.7, 1000.0

    names, pad_frac = _ZOOMS.get(zoom, (None, 0.0))
    bb = _paths_bbox(paths, names) if names else None
    if bb:
        x0, y0, x1, y1 = bb
        pad = pad_frac * max(x1 - x0, y1 - y0)
        cx0, cy0 = max(0.0, x0 - pad), max(0.0, y0 - pad)
        cx1, cy1 = min(vw, x1 + pad), min(vh, y1 + pad)
    else:
        cx0, cy0, cx1, cy1 = 0.0, 0.0, vw, vh
    cw, ch = cx1 - cx0, cy1 - cy0
    scale = min(_ZOOM_W / cw, _CHORO_PX_H / ch)
    px_w, px_h = round(cw * scale), round(ch * scale)

    # Two cities are ENCLAVES inside their county (Cork City ⊂ Cork County, Galway City ⊂
    # Galway County) — exterior-only outlines mean the county polygon overlaps the city. So
    # order by polygon size: draw largest-first (the small city paints ON TOP, its colour
    # shows), and list click <area>s smallest-first (the enclave city wins the overlapping
    # click instead of resolving to the surrounding county).
    items = []  # (poly_area, path_html, area_html | None)
    for name, d in paths.items():
        q = quintile_by_name.get(name)
        try:
            fill = _CHORO_PALETTE[int(q) - 1] if q is not None and 1 <= int(q) <= 5 else _CHORO_NODATA
        except (TypeError, ValueError):
            fill = _CHORO_NODATA
        path_html = f'<path d="{d}" fill="{fill}" stroke="#fbf8f2" stroke-width="1.2"/>'
        subs = _path_subpaths(d)
        best = max(subs, key=_poly_area) if subs else None
        size = _poly_area(best) if best else 0.0
        area_html = None
        if best:
            bxs = [x for x, _ in best]
            bys = [y for _, y in best]
            if not (max(bxs) < cx0 or min(bxs) > cx1 or max(bys) < cy0 or min(bys) > cy1):
                coords = ",".join(f"{(x - cx0) * scale:.1f},{(y - cy0) * scale:.1f}" for x, y in best)
                area_html = (
                    f'<area shape="poly" coords="{coords}" '
                    f'href="?{link_key}={quote(name)}" alt="{_h(name)}" title="{_h(name)}">'
                )
        items.append((size, path_html, area_html))
    body = [p for _, p, _ in sorted(items, key=lambda t: -t[0])]  # big first → enclave on top
    areas = [a for _, _, a in sorted(items, key=lambda t: t[0]) if a]  # small first → enclave wins click
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="{cx0:.1f} {cy0:.1f} {cw:.1f} {ch:.1f}">{"".join(body)}</svg>'
    )
    b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return (
        f'<img class="con-choropleth" width="{px_w}" height="{px_h}" '
        f'usemap="#lg-choro-map" src="data:image/svg+xml;base64,{b64}" '
        f'alt="{_h(alt)}" loading="lazy">'
        f'<map name="lg-choro-map">{"".join(areas)}</map>'
    )


def _choro_legend() -> str:
    swatches = "".join(f'<span class="con-choro-sw" style="background:{c}"></span>' for c in _CHORO_PALETTE)
    return (
        f'<div class="con-choro-legend">'
        f'<span class="con-choro-end">Lower</span>{swatches}'
        f'<span class="con-choro-end">Higher</span>'
        f"</div>"
    )


def _render_choropleth(*, link_key: str = "la") -> None:
    """Clickable national choropleth. ``link_key`` is the dossier query param each council links to —
    "la" for this page (default), "council" when the Your Council hub reuses the same map."""
    res = fetch_la_map_layers_result()
    if not res.ok or res.data is None or res.data.empty:
        return  # silent — the searchable grid below remains the reliable selector
    df = res.data
    subsection_heading("Every council, compared")
    c_layer, c_zoom = st.columns([3, 2])
    with c_layer:
        choice = st.radio("Shade the map by", list(_MAP_LAYERS.keys()), horizontal=True, key="lg_map_layer")
    with c_zoom:
        zoom = st.radio("Zoom in on", list(_ZOOMS.keys()), horizontal=True, key="lg_map_zoom")
    qcol, phrase = _MAP_LAYERS[choice]
    quint = {str(r["local_authority"]): r[qcol] for _, r in df.iterrows() if pd.notna(r[qcol])}
    map_html = _choropleth_html(
        quint, alt=f"Map of the 31 local authorities shaded by {choice}", zoom=zoom, link_key=link_key
    )
    if not map_html:
        return
    st.html(f'<div class="con-choro">{map_html}{_choro_legend()}</div>')
    zoom_note = "" if zoom == "Ireland" else f"Zoomed to {zoom} — pick “Ireland” to zoom back out. "
    st.caption(
        f"Each of the 31 local authorities shaded into fifths by {phrase}. These are "
        "executive (Chief Executive) responsibilities, not the elected councillors'. "
        f"{zoom_note}Click a council to open its dossier, or pick a card below. "
        "Boundaries: Tailte Éireann / OSi (2026). Lightest fill = no published figure."
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


def _render_derelict_levy_ranking() -> None:
    """Cross-council derelict-levy ENFORCEMENT league — which councils levy the charge
    but collect little of it, and which levy nothing at all despite holding derelict
    sites on their register. The per-council figure is on each dossier; this is the
    national comparison. Reads v_la_derelict_sites_levy via data-access (no logic here).

    Levying and collecting are the Chief Executive's EXECUTIVE functions, and the
    obligation is reasserted by DHLGH Circulars PL 05/2022 & PL 09/2021 (annual return).
    collection_rate_pct is arrears-aware — shown as a direction, not a clean fraction."""
    res = fetch_derelict_levy_ranking_result()
    if not res.ok or res.data.empty:
        return
    df = res.data

    # Worst collectors: levied a real sum (>€10k) but collected under a quarter of it.
    levied = df[df["amount_levied_eur"].fillna(0) > 10_000].copy()
    levied = levied[levied["collection_rate_pct"].fillna(0) < 25]
    levied = levied.sort_values("collection_rate_pct", na_position="first").head(8)

    # Councils holding derelict sites on the register but levying €0 — no enforcement.
    nil = df[df["levied_nothing"] & (df["sites_on_register"].fillna(0) > 0)]
    nil_names = nil.sort_values("sites_on_register", ascending=False)["local_authority"].tolist()

    if levied.empty and not nil_names:
        return

    st.html(
        '<h2 style="font-size:1.15rem;margin:1.6rem 0 0.2rem;">Derelict-site levy enforcement</h2>'
        '<p style="color:#5b6b73;font-size:0.9rem;margin:0 0 0.9rem;max-width:52rem;">'
        "The Derelict Sites Act 1990 lets a council charge a 7% annual levy on a registered "
        "derelict site — an <strong>executive</strong> function, reasserted by DHLGH Circular "
        "PL 05/2022. These councils levied the charge but collected little of it. Click a "
        "council for its dossier.</p>"
    )

    bars: list[str] = []
    for r in levied.itertuples(index=False):
        la = str(r.local_authority)
        pct_v = 0.0 if pd.isna(r.collection_rate_pct) else float(r.collection_rate_pct)
        outstanding = eur(r.cumulative_outstanding_eur)
        width = max(2.0, min(pct_v, 100.0))
        bars.append(
            f'<a href="?la={quote(la)}" target="_self" '
            f'style="display:grid;grid-template-columns:9rem 1fr auto;gap:0.7rem;align-items:center;'
            f'padding:0.35rem 0;text-decoration:none;color:inherit;border-bottom:1px solid #ece7dc;">'
            f'<span style="font-weight:600;">{_h(la)}</span>'
            f'<span style="background:#f0eadd;border-radius:5px;height:0.7rem;position:relative;">'
            f'<span style="position:absolute;left:0;top:0;bottom:0;width:{width:.0f}%;'
            f'background:#a5431c;border-radius:5px;"></span></span>'
            f'<span style="font-variant-numeric:tabular-nums;color:#5b6b73;font-size:0.85rem;">'
            f'{pct_v:.0f}% collected · {_h(outstanding)} outstanding</span></a>'
        )
    if bars:
        st.html('<div style="max-width:52rem;">' + "".join(bars) + "</div>")

    if nil_names:
        chips = " ".join(
            f'<a href="?la={quote(n)}" target="_self" style="text-decoration:none;">'
            f'<span style="display:inline-block;background:#fbeecb;border:1px solid #e6c87a;'
            f'color:#7a5a00;border-radius:999px;padding:0.15rem 0.7rem;margin:0.15rem 0.2rem;'
            f'font-size:0.82rem;font-weight:600;">{_h(str(n))}</span></a>'
            for n in nil_names
        )
        st.html(
            f'<p style="color:#5b6b73;font-size:0.9rem;margin:1rem 0 0.3rem;max-width:52rem;">'
            f"<strong>{len(nil_names)} councils</strong> hold derelict sites on their register "
            f"but levied €0 in 2024 — no charge raised at all:</p>"
            f'<div style="max-width:52rem;">{chips}</div>'
            f'<p style="color:#8a97a0;font-size:0.78rem;margin:0.7rem 0 0;max-width:52rem;">'
            "Source: DHLGH Derelict Sites annual return, 2024 (gov.ie, CC-BY). Collection rate "
            "is arrears-aware (prior-year receipts can lift it) — a direction, not a clean "
            "within-year ratio. A low rate is context for scrutiny, not proof of failure.</p>"
        )


def _render_index() -> None:
    hero_banner(
        kicker="LOCAL GOVERNMENT",
        title="Who runs your county",
        dek="Most power in your county is held by an appointed official — the Chief Executive — "
        "not by the councillors you elect. Pick a council to see who runs it and how it performs.",
    )
    _render_national_summary()
    _render_derelict_levy_ranking()

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

    # Choropleth moved ABOVE the card grid (2026-06-22, user request — it previously
    # sat below the grid). The map is a static <img>, so it renders fine inline here.
    _render_choropleth()

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
        _metric(
            _pct(c.get("commercial_rates_pct")),
            "Commercial rates collected",
            _bench(c.get("commercial_rates_pct"), c.get("nat_commercial_rates_pct"), "{:.0f}%", True),
        ),
        _metric(
            _pct(c.get("rent_annuities_pct")),
            "Rent &amp; annuities collected",
            _bench(c.get("rent_annuities_pct"), c.get("nat_rent_annuities_pct"), "{:.0f}%", True),
        ),
        _metric(
            _pct(c.get("housing_loans_pct")),
            "Housing loans collected",
            _bench(c.get("housing_loans_pct"), c.get("nat_housing_loans_pct"), "{:.0f}%", True),
        ),
    ]
    return _stat_card("Money collected", rows, "NOAC Performance Indicator Report 2024")


def _card_housing(name: str) -> str:
    h = fetch_housing_performance_result(name)
    if not h.ok or h.data.empty:
        return ""
    r = h.data.iloc[0]
    rows = [
        _metric(
            _pct(r.get("vacancy_pct"), 1),
            "Council homes lying vacant",
            _bench(r.get("vacancy_pct"), r.get("nat_vacancy_pct"), "{:.1f}%", False),
        ),
        _metric(
            f"{_num1(r.get('reletting_weeks'))} wks",
            "Time to re-let an empty home",
            _bench(r.get("reletting_weeks"), r.get("nat_reletting_weeks"), "{:.0f} wks", False),
        ),
        _metric(
            _eur_full(r.get("maintenance_eur_per_dwelling")),
            "Upkeep spend per home",
            _bench(r.get("maintenance_eur_per_dwelling"), r.get("nat_maintenance_eur_per_dwelling"), "€{:,.0f}", None),
        ),
        _metric(
            _pct(r.get("retrofit_pct_of_stock"), 1),
            "Stock retrofitted (2024)",
            _bench(r.get("retrofit_pct_of_stock"), r.get("nat_retrofit_pct_of_stock"), "{:.1f}%", True),
        ),
        _metric(
            _pct(r.get("longterm_homeless_pct")),
            "Homeless adults long-term",
            _bench(r.get("longterm_homeless_pct"), r.get("nat_longterm_homeless_pct"), "{:.0f}%", False),
        ),
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


def _card_audit(name: str) -> str:
    """The LGAS statutory audit report — the independent auditor's own words on the council's
    AFS. VERBATIM only: the opinion sentence and the emphasis-of-matter flag come straight from
    the report (no derived good/bad score). The clearest external-accountability signal there is."""
    res = fetch_lgas_audit_result(name)
    if not res.ok or res.data.empty:
        return ""
    latest = res.data.iloc[0]
    yr = str(int(latest.get("year")))  # plain year — never thousands-separated ("2024" not "2,024")
    n_years = len(res.data)
    eom = bool(latest.get("has_emphasis_of_matter"))
    # emphasis-of-matter = the auditor is drawing readers' attention to something in the accounts;
    # state it factually, never as a verdict (no-inference rule)
    flag = (
        '<div class="lg-badge">⚑ Latest report carries an “Emphasis of Matter”</div>'
        if eom
        else ""
    )
    opinion = str(latest.get("audit_opinion_text") or "").strip()
    snippet = (opinion[:280].rsplit(" ", 1)[0] + "…") if len(opinion) > 280 else opinion
    quote = (
        f'<div class="lg-audit-quote">“{_h(snippet)}”</div>'
        if snippet
        else ""
    )
    rows = [
        _metric(str(yr), "Most recent audited year"),
        _metric(str(n_years), f"Audit reports published (2012–{yr})"),
    ]
    url = str(latest.get("report_page_url") or "")
    return _stat_card(
        "Independent audit (LGAS)",
        rows,
        f"Local Government Audit Service statutory audit report, {yr}. The auditor examines the "
        "accounts the Chief Executive administers; councillors sign none of it.",
        extra=flag + quote,
        src_url=url,
    )


def _card_planning(name: str) -> str:
    ov = fetch_planning_overturn_result(name)
    if not ov.ok or ov.data.empty:
        return ""
    o = ov.data.iloc[0]
    rows = [
        _metric(
            _pct(o.get("overturn_rate_pct"), 1),
            "Decisions overturned by An Bord Pleanála",
            _bench(o.get("overturn_rate_pct"), o.get("national_overturn_rate_pct"), "{:.1f}%", False),
        ),
        _metric(_int(o.get("n_appeals")), "Appeals decided (2016 on)"),
    ]
    return _stat_card("Planning decisions", rows, "An Bord Pleanála appeal outcomes")


def _card_council_money(name: str) -> str:
    m = fetch_council_money_result(name)
    if not m.ok or m.data.empty:
        return ""
    r = m.data.iloc[0]
    rows = []
    ordered = r.get("ordered_safe_eur")
    if ordered is not None and not pd.isna(ordered) and float(ordered) > 0:
        rows.append(_metric(_eur(ordered), "Ordered — purchase orders over €20k"))
    paid = r.get("paid_safe_eur")
    if paid is not None and not pd.isna(paid) and float(paid) > 0:
        rows.append(_metric(_eur(paid), "Paid — actual payments over €20k"))
    rows.append(_metric(_int(r.get("n_suppliers")), "Suppliers paid"))
    yr = f"{_int(r.get('min_year'))}–{_int(r.get('max_year'))}"
    # Drill-down CTA: land on the council's spending dossier (suppliers, then the individual
    # published line items) on the Council Spending page. Open on the tier this council actually
    # publishes — most publish purchase ORDERS (COMMITTED), so default there unless it has
    # actual payments (SPENT). Cross-page <a href> (not a soft rerun): different page, different
    # query-param namespace (?paid_publisher= vs this page's ?la=).
    tier = "SPENT" if (paid is not None and not pd.isna(paid) and float(paid) > 0) else "COMMITTED"
    cta = (
        f'<a class="lg-card-cta" href="{_h(council_spending_url(name, tier))}" target="_self" '
        f'aria-label="See {_h(name)} suppliers and the individual published line items">'
        "See its suppliers &amp; every line item →</a>"
    )
    return _stat_card(
        "Council money (executive-signed)",
        rows,
        f"Council purchase-order / payment disclosures, {yr}. Councillors sign none of this.",
        extra=cta,
    )


# ── NOAC scorecard cards (v_la_noac_scorecard) ───────────────────────────────
# Seven 2024 indicators grouped into two single-theme cards. Per-metric source deep-link
# goes to the exact NOAC report page (#page=); card source links the report landing page.
_NOAC_PDF = "https://cdn.noac.ie/wp-content/uploads/2025/09/NOAC-Local-Authority-Performance-Indicator-Report-2024.pdf"
_NOAC_REPORT = "https://www.noac.ie/noac_publications/report-77-noac-performance-indicator-report-2024/"
# metric key -> (value col, national-median col, label, value+benchmark fmt, NOAC PDF page)
_SCORECARD = {
    "revenue_balance": ("revenue_balance_pct", "nat_revenue_balance_pct", "Revenue balance", "{:.1f}%", 185),
    "overhead": ("mgmt_overhead_pct", "nat_mgmt_overhead_pct", "Management overhead", "{:.1f}%", 190),
    "insurance": (
        "insurance_claims_per_capita_eur",
        "nat_insurance_claims_per_capita_eur",
        "Insurance claims / person",
        "€{:.2f}",
        189,
    ),
    "sickness": ("sickness_absence_pct", "nat_sickness_absence_pct", "Sick-leave days lost", "{:.1f}%", 170),
    "roads": ("roads_poor_pct", "nat_roads_poor_pct", "Roads in poor condition", "{:.1f}%", 63),
    "fire": ("fire_within_10min_pct", "nat_fire_within_10min_pct", "Fires reached in 10 min", "{:.0f}%", 134),
    "litter": ("litter_problem_pct", "nat_litter_problem_pct", "Area with a litter problem", "{:.0f}%", 99),
}
_FIRE_NA_NOTE = "fire service provided regionally"


def _spark(series) -> str:
    """A tiny neutral trend line (NOAC 2022-2024) for one metric. No good/bad colour — it
    only shows the shape of the change; missing years are skipped. Returns '' if <2 points."""
    pts = [(yr, v) for yr, v in (series or []) if v is not None and not pd.isna(v)]
    if len(pts) < 2:
        return ""
    years = [yr for yr, _ in (series or [])]
    y0, yn = min(years), max(years)
    span = (yn - y0) or 1
    vals = [v for _, v in pts]
    lo, hi = min(vals), max(vals)
    rng = (hi - lo) or 1
    w, h, pad = 52, 14, 2

    def xy(yr, v):
        return ((yr - y0) / span * (w - 2 * pad) + pad, h - pad - (v - lo) / rng * (h - 2 * pad))

    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in (xy(yr, v) for yr, v in pts))
    lx, ly = xy(*pts[-1])
    # st.html strips inline <svg>, so embed as a base64 data-URI <img> (same technique as the
    # index choropleth). Muted stroke (no good/bad colour); the dot marks the latest year.
    svg = (
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{w}" height="{h}" viewBox="0 0 {w} {h}">'
        f'<polyline points="{poly}" fill="none" stroke="#9a8f80" stroke-width="1.3" '
        f'stroke-linecap="round" stroke-linejoin="round"/>'
        f'<circle cx="{lx:.1f}" cy="{ly:.1f}" r="1.8" fill="#9a8f80"/></svg>'
    )
    b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    return (
        f'<img class="lg-spark" src="data:image/svg+xml;base64,{b64}" width="{w}" height="{h}" '
        f'alt="{y0}–{yn} trend" title="{y0}–{yn} trend"/>'
    )


def _scorecard_metric(r, key: str, series=None) -> str:
    col, nat_col, label, fmt, page = _SCORECARD[key]
    v = r.get(col)
    value = "n/a" if v is None or pd.isna(v) else fmt.format(float(v))
    if key == "fire" and (v is None or pd.isna(v)):
        bench = f'<span class="lg-na-note">{_FIRE_NA_NOTE}</span>'
    else:
        bench = _bench(v, r.get(nat_col), fmt) + _spark(series)
    return _metric(value, label, bench, doc_url=f"{_NOAC_PDF}#page={page}", doc_page=page)


def _scorecard_card(name: str, title: str, keys: list[str]) -> str:
    res = fetch_noac_scorecard_result(name)
    if not res.ok or res.data.empty:
        return ""
    r = res.data.iloc[0]
    # one history fetch -> per-metric (year, value) series for the spark trend
    hist: dict[str, list] = {}
    hres = fetch_noac_scorecard_history_result(name)
    if hres.ok and not hres.data.empty:
        hd = hres.data.sort_values("year")
        for k in keys:
            col = _SCORECARD[k][0]
            if col in hd.columns:
                hist[col] = list(zip(hd["year"].astype(int), hd[col], strict=False))
    rows = [_scorecard_metric(r, k, hist.get(_SCORECARD[k][0])) for k in keys]
    return _stat_card(title, rows, "NOAC Performance Indicator Report 2024 (2022–24 trend)", src_url=_NOAC_REPORT)


def _card_how_run(name: str) -> str:
    return _scorecard_card(name, "How the council is run", ["revenue_balance", "overhead", "insurance", "sickness"])


def _card_services(name: str) -> str:
    return _scorecard_card(name, "Services to residents", ["roads", "fire", "litter"])


def _render_performance(name: str) -> None:
    # Lead group: the indicators a resident is most likely to want first (homes, services,
    # rates collected, planning). The governance / audit / spending-detail / derelict cards are
    # demoted behind one expander so first paint isn't an eight-card wall — the same declutter
    # treatment the Corporate page got. Every card is still one click away; nothing is dropped.
    lead = [c for c in (_card_housing(name), _card_services(name), _card_money_collected(name),
                        _card_planning(name)) if c]
    more = [c for c in (_card_council_money(name), _card_audit(name), _card_derelict(name),
                        _card_how_run(name)) if c]
    if not lead:  # a council with only the secondary indicators still leads with something
        lead, more = more, []

    evidence_heading(f"How {name} performs")
    if not lead and not more:
        empty_state("No indicators yet", "No published performance indicators are mapped for this council.")
        return
    st.html(
        '<p class="con-section-note">Indicators published by bodies including the <strong>National '
        "Oversight &amp; Audit Commission (NOAC)</strong>, An Bord Pleanála and the Department of "
        "Housing — each beside the <strong>national benchmark</strong> (median across the 31 "
        "councils). These are <strong>executive</strong> responsibilities — the Chief Executive's "
        "administration, not the elected councillors. ▲/▼ shows where the council sits relative to "
        "the benchmark; no judgement is implied.</p>"
    )
    st.html(f'<div class="lg-perf-grid">{"".join(lead)}</div>')
    if more:
        with st.expander(f"More indicators — governance, audit, spending & derelict sites ({len(more)})"):
            st.html(f'<div class="lg-perf-grid">{"".join(more)}</div>')
    _render_all_indicators(name)


def _render_all_indicators(name: str) -> None:
    """Single reference drill-down: EVERY published NOAC indicator for the council, as
    published. Secondary (behind one expander) so the headline cards stay the primary view;
    a dense table is the right tool here, not more cards."""
    res = fetch_noac_indicators_result(name)
    if not res.ok or res.data.empty:
        return
    df = res.data.rename(
        columns={
            "family": "Service",
            "series_label": "Indicator",
            "raw_value": "Value",
            "deep_link": "Source",
        }
    )
    with st.expander(f"All {len(df)} published NOAC indicators for {name} (2024)"):
        st.caption(
            "Everything NOAC publishes for this council, exactly as published — the cards above are "
            "the curated headline subset. Source links open the relevant NOAC report page."
        )
        st.dataframe(
            df[["Service", "Indicator", "Value", "Source"]],
            hide_index=True,
            width="stretch",
            column_config={
                "Service": st.column_config.TextColumn("Service", width="small"),
                "Indicator": st.column_config.TextColumn("Indicator", width="large"),
                "Value": st.column_config.TextColumn("Value", width="small"),
                "Source": st.column_config.LinkColumn("Source", display_text="NOAC ↗", width="small"),
            },
        )


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


@dt_page
def local_government_page() -> None:
    selected = st.query_params.get("la")
    if selected:
        _render_dossier(selected)
    else:
        _render_index()
