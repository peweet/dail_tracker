"""Siting Check — what planning issues does a site trigger?

A citizen enters a point (map-click / paste coordinates) and gets the planning issues that
site triggers, each with the governing Development-Plan standard quoted verbatim, the
specialist to engage, and An Coimisiún Pleanála precedent. It surfaces rules + triggers +
precedent — NEVER a grant/refuse verdict or a design prescription (the §23.4 liability line).

Logic lives in dail_tracker_core.siting (engine); this page is a thin renderer. Data access
via data_access.siting_data (no raw reads, no joins here).

CSS namespace: sc-*.
"""

from __future__ import annotations

import sys
from html import escape as _h
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # utility/ on path

from data_access.siting_data import evaluate_site  # noqa: E402
from shared_css import inject_css  # noqa: E402
from ui.components import hide_sidebar  # noqa: E402

# mitigation-class presentation: severity order F > D > P
_CLASS_META = {
    "F": ("#b3261e", "Often the dealbreaker", "🔴"),
    "D": ("#9a6700", "Usually mitigable with the right specialist", "🟡"),
    "P": ("#1a7f37", "Procedural — commission the report", "🟢"),
}
_DEV_TYPES = {
    "one_off_house": "One-off house",
    "extension": "Extension",
    "multi_unit": "Multiple houses / apartments",
    "commercial": "Commercial",
}


def _headline_class(classes: frozenset[str]) -> str:
    for c in ("F", "D", "P"):
        if c in classes:
            return c
    return "P"


def _css() -> None:
    st.html(
        """
        <style>
        .sc-hero h1 { margin:0 0 .15rem 0; font-size:1.8rem; }
        .sc-dek { color:#555; margin:0 0 1rem 0; max-width:46rem; }
        .sc-disc { background:#fff8e1; border:1px solid #f0d98c; border-radius:8px;
                   padding:.6rem .8rem; font-size:.86rem; color:#5b4b00; margin:.4rem 0 1rem 0; }
        .sc-meta { background:#ffffff; border:1px solid #e6e6e6; border-radius:10px;
                   padding:.7rem .9rem; margin:.3rem 0 1rem 0; }
        .sc-card { background:#ffffff; border:1px solid #e6e6e6; border-left-width:5px;
                   border-radius:10px; padding:.75rem .95rem; margin:.55rem 0; }
        .sc-card h3 { margin:.05rem 0 .35rem 0; font-size:1.05rem; }
        .sc-flag { color:#333; margin:.1rem 0 .5rem 0; }
        .sc-tag { display:inline-block; font-size:.72rem; font-weight:600; padding:.1rem .45rem;
                  border-radius:20px; background:#f1f1f1; color:#444; margin-right:.3rem; }
        .sc-engage { font-size:.82rem; color:#555; }
        .sc-note { font-size:.8rem; color:#777; margin-top:.35rem; }
        .sc-prec { font-size:.82rem; color:#444; background:#f7f7f9; border-radius:6px;
                   padding:.35rem .55rem; margin-top:.4rem; }
        </style>
        """
    )


def _render_card(issue) -> None:
    cls = _headline_class(issue.mitigation_classes)
    colour, label, dot = _CLASS_META[cls]
    engage = ", ".join(issue.engage) if issue.engage else ""
    parts = [
        f'<div class="sc-card" style="border-left-color:{colour}">',
        f"<h3>{dot} {_h(issue.title)}</h3>",
        f'<span class="sc-tag" style="background:{colour}1a;color:{colour}">{_h(label)}</span>',
    ]
    if issue.flag:
        parts.append(f'<p class="sc-flag">{_h(issue.flag)}</p>')
    if engage:
        parts.append(f'<p class="sc-engage"><b>Engage:</b> {_h(engage)}</p>')
    if issue.mitigates:
        parts.append(f'<p class="sc-note"><b>Mitigation:</b> {_h(issue.mitigates)}</p>')
    if issue.risk_note:
        parts.append(f'<p class="sc-note">{_h(issue.risk_note)}</p>')
    # real (non-stub) precedents
    for p in issue.precedents:
        if p.get("curate"):
            continue
        ref, outcome, note = p.get("ref", ""), p.get("outcome", ""), p.get("note", "")
        parts.append(
            f'<div class="sc-prec"><b>Precedent</b> — {_h(ref)} '
            f'({_h(str(outcome).replace("_", " "))}): {_h(note)}</div>'
        )
    if issue.extra.get("flood_link"):
        link = issue.extra["flood_link"]
        parts.append(f'<p class="sc-note">↗ <a href="{_h(link)}" target="_blank">'
                     f"View statutory flood maps for this site (floodinfo.ie)</a></p>")
    parts.append("</div>")
    st.html("".join(parts))

    rule = issue.rule
    if rule and (rule.dm_standards or rule.checklist):
        with st.expander("What the Development Plan says (verbatim)"):
            if rule.plan_name:
                st.caption(f"{rule.council_name} — {rule.plan_name}")
            for d in rule.dm_standards:
                st.markdown(f"**DM Standard {d.number}: {d.title}**")
                st.write(d.text[:1200] + ("…" if len(d.text) > 1200 else ""))
            for c in rule.checklist:
                st.markdown(f"**Required: {c.document}** — *trigger:* {c.trigger}")


def siting_check_page() -> None:
    hide_sidebar()
    inject_css()
    _css()

    st.html(
        '<div class="sc-hero"><h1>Siting Check</h1>'
        '<p class="sc-dek">Enter a site location to see the planning issues it is likely to '
        "trigger — what each one means, the specialist you would engage, what the Development "
        "Plan says, and how An Coimisiún Pleanála has ruled on similar cases.</p></div>"
    )
    st.html(
        '<div class="sc-disc"><b>Planning-risk triage, not professional planning advice.</b> '
        "Planning decisions are discretionary. This tool surfaces the rules your site triggers and "
        "the level of risk; it never tells you a decision outcome or how to design.</div>"
    )

    c1, c2, c3 = st.columns([1, 1, 1.2])
    with c1:
        lat = st.number_input("Latitude", value=53.3500, format="%.5f", key="sc_lat")
    with c2:
        lon = st.number_input("Longitude", value=-6.2600, format="%.5f", key="sc_lon")
    with c3:
        dev = st.selectbox("What do you want to build?", list(_DEV_TYPES),
                           format_func=lambda k: _DEV_TYPES[k], key="sc_dev")
    go = st.button("Check this site", type="primary")

    if not go:
        st.caption("Tip: paste coordinates from Google Maps (right-click → the first two numbers).")
        return

    if not (51.0 <= lat <= 56.0 and -11.0 <= lon <= -5.0):
        st.error("That point is outside the island of Ireland. Check the latitude/longitude.")
        return

    with st.spinner("Evaluating the site against the designation layers and the rulebook…"):
        res = evaluate_site(float(lon), float(lat), dev)

    st.map({"lat": [lat], "lon": [lon]}, zoom=12, size=60)

    council = res.council.council_name or res.council.authority or "Unknown council"
    bnd = " (near a council boundary — verify the authority)" if res.council.on_boundary else ""
    from data_access.siting_data import site_terrain
    t = site_terrain(float(lon), float(lat))
    elev = f"{t.elevation_m} m" if t.ok else "n/a"
    exp = " · elevated/exposed" if (t.ok and t.exposed) else ""
    st.html(
        f'<div class="sc-meta"><b>Governing authority:</b> {_h(council)}{_h(bnd)}<br>'
        f"<b>Terrain:</b> elevation {_h(elev)}{_h(exp)} "
        f'<span class="sc-note">(Copernicus DEM, ±30 m — a coarse signal, not a survey)</span></div>'
    )

    fired = sorted(res.fired, key=lambda i: "FDP".index(_headline_class(i.mitigation_classes)))
    if fired:
        st.subheader(f"{len(fired)} issue(s) your site triggers")
        for issue in fired:
            _render_card(issue)
    else:
        st.success("No mapped designation issues fired for this point from the layers loaded.")

    if res.missing_layers:
        pretty = ", ".join(m.replace("_", " ") for m in res.missing_layers)
        st.info(f"Not yet assessed here (data layer pending): {pretty}. "
                "Absence of a flag is not confirmation the issue does not apply.")

    st.caption(res.disclaimer)
