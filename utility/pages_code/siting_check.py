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

import re
import sys
from html import escape as _h
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))  # utility/ on path

from dail_tracker_core.siting.brief import cascade_text, road_sightline_line  # noqa: E402
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

# --- Google-Maps coordinate extraction ----------------------------------------------------
# A pasted Maps URL carries the point in several places. Preference order:
#   1. !3d<lat>!4d<lon>  — the dropped pin / Street-View target (most precise)
#   2. @<lat>,<lon>      — the map centre / where the pegman ("little man") is standing
#   3. ?q= / ll= / query= / center= / sll=  — query-parameter forms
# A bare "53.349, -6.260" paste (right-click → copy coordinates) is also accepted.
# No network: short share links (maps.app.goo.gl, goo.gl/maps) carry NO coordinates and
# must be opened first so the address-bar URL gains the @lat,lon segment.
_LL = r"(-?\d{1,3}\.\d+)"
_RE_PIN = re.compile(rf"!3d{_LL}!4d{_LL}")
_RE_AT = re.compile(rf"@{_LL},{_LL}")
_RE_Q = re.compile(rf"[?&](?:q|ll|query|center|sll)=\s*{_LL},\s*{_LL}")
_RE_BARE = re.compile(rf"^{_LL},\s*{_LL}$")


def parse_latlon_from_maps(text: str) -> tuple[float, float] | None:
    """Pull (lat, lon) out of a pasted Google-Maps URL or a bare 'lat, lon' string.

    Returns None when no coordinate can be found (e.g. an unresolved short share link).
    """
    text = (text or "").strip()
    if not text:
        return None
    for rx in (_RE_PIN, _RE_AT, _RE_Q, _RE_BARE):
        m = rx.search(text)
        if m:
            return float(m.group(1)), float(m.group(2))
    return None


def _is_short_link(text: str) -> bool:
    return any(h in (text or "") for h in ("maps.app.goo.gl", "goo.gl/maps"))


def _apply_maps_url() -> None:
    """on_change handler: parse the pasted link and pre-fill the lat/lon widgets.

    Runs before the widgets are instantiated this rerun, so writing their session_state
    keys is safe. Stashes a status tuple for the body to render.
    """
    raw = st.session_state.get("sc_url", "")
    parsed = parse_latlon_from_maps(raw)
    if not parsed and _is_short_link(raw):
        # Short share link: no coords in the text. Follow the redirect on the backend (cached
        # network call) to the full destination, then parse the @lat,lon / !3d!4d out of that.
        from data_access.siting_data import resolve_maps_short_link

        resolved = resolve_maps_short_link(raw.strip())
        parsed = parse_latlon_from_maps(resolved) if resolved else None
    if parsed:
        st.session_state["sc_lat"] = round(parsed[0], 5)
        st.session_state["sc_lon"] = round(parsed[1], 5)
        st.session_state["sc_url_msg"] = ("ok", parsed)
    elif _is_short_link(raw):
        st.session_state["sc_url_msg"] = ("short", None)  # resolution failed → old hint
    elif raw.strip():
        st.session_state["sc_url_msg"] = ("err", None)
    else:
        st.session_state.pop("sc_url_msg", None)


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
        .sc-excl { background:#fff0f0; border:1px solid #e6b3b3; border-left:6px solid #b3261e;
                   border-radius:10px; padding:.85rem 1rem; margin:.6rem 0 1rem 0; }
        .sc-excl h2 { margin:0 0 .35rem 0; font-size:1.15rem; color:#8a1c16; }
        .sc-excl .sc-site { font-weight:600; color:#333; }
        .sc-excl .sc-route { font-size:.85rem; color:#555; margin:.2rem 0 .1rem 0; }
        .sc-verify { background:#eef4fb; border:1px solid #c3d6ee; border-left:5px solid #2c6cb0; }
        </style>
        """
    )


def _render_exclusion_banner(exclusions) -> None:
    """Top-of-page hard-exclusion banner: the point is inside a statutory protected designation.

    States the FACT (the designation covers the point) and the narrow real route that could still
    permit development — never an absolute 'cannot build' verdict (it may be possible to mitigate).
    """
    rows = ['<div class="sc-excl"><h2>⛔ Excluded — statutory protected land</h2>'
            '<p class="sc-flag">This point lies inside a protected designation, where ordinary '
            "development is presumed against. This is a fact about the land, not the planning "
            "decision — and it may still be possible via the narrow statutory route below.</p>"]
    for e in exclusions:
        rows.append(f'<p class="sc-site">Inside {_h(e.site_name)} — {_h(e.designation)}</p>')
        if getattr(e, "mitigation", ""):
            rows.append(f'<p class="sc-route"><b>Possible route:</b> {_h(e.mitigation)}</p>')
    rows.append("</div>")
    st.html("".join(rows))


def _render_verify_card(issue) -> None:
    """A 'check this yourself' card for layers we can't read (e.g. the licensed OPW flood maps)."""
    parts = [
        '<div class="sc-card sc-verify">',
        f"<h3>🔎 {_h(issue.title)}</h3>",
        '<span class="sc-tag" style="background:#2c6cb01a;color:#2c6cb0">Check this yourself</span>',
    ]
    if issue.flag:
        parts.append(f'<p class="sc-flag">{_h(issue.flag)}</p>')
    if issue.extra.get("flood_link"):
        link = issue.extra["flood_link"]
        parts.append(f'<p class="sc-note">↗ <a href="{_h(link)}" target="_blank">'
                     f"Open the statutory flood maps for this exact point (floodinfo.ie)</a></p>")
    parts.append("</div>")
    st.html("".join(parts))


def _render_card(issue) -> None:
    # issues whose layer we can't read (deep_link_only, e.g. flood) are CHECKS the user must run
    # themselves, not confirmed findings — render them in a neutral "verify yourself" style so they
    # don't read as a confirmed hard constraint on every site.
    if issue.data_status == "deep_link_only":
        _render_verify_card(issue)
        return
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
    # road node: one key sightline number derived from the OSM road class/speed
    if issue.node_id == "road_sightlines":
        parts.append(f'<p class="sc-note"><b>Sightline:</b> {_h(road_sightline_line(issue.detail))}</p>')
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
                # numbered plans (e.g. Galway County) cite "DM Standard N"; concept-keyed
                # plans (number 0) carry the council's own section citation instead.
                if d.number:
                    st.markdown(f"**DM Standard {d.number}: {d.title}**")
                else:
                    st.markdown(f"**{d.title}**")
                    if getattr(d, "source_ref", ""):
                        st.caption(d.source_ref)
                st.write(d.text[:1200] + ("…" if len(d.text) > 1200 else ""))
            for c in rule.checklist:
                st.markdown(f"**Required: {c.document}** — *trigger:* {c.trigger}")

    # static if/then mitigation cascade (the "what happens if it passes/fails" tree)
    if issue.mitigation_path:
        with st.expander("Mitigation pathway (if / then)"):
            st.code(cascade_text(issue.mitigation_path), language=None)
            st.caption("Indicative process, not advice — outcomes are discretionary.")


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

    # Seed the coordinate widgets once. A pasted link overwrites these (via _apply_maps_url)
    # before the number_input widgets below are instantiated, so no default-vs-state warning.
    st.session_state.setdefault("sc_lat", 53.3500)
    st.session_state.setdefault("sc_lon", -6.2600)

    st.text_input(
        "Paste a Google Maps link",
        key="sc_url",
        on_change=_apply_maps_url,
        placeholder="paste any Maps link — full URL or a maps.app.goo.gl share link",
        help="Paste a Google Maps URL (the @lat,lon kind), a short share link "
             "(maps.app.goo.gl/…) which we resolve for you, or bare coordinates like "
             "53.34980, -6.26030. Tip: drag the orange figure onto your exact site first.",
    )
    msg = st.session_state.get("sc_url_msg")
    if msg and msg[0] == "ok":
        st.success(f"Located {msg[1][0]:.5f}, {msg[1][1]:.5f} — adjust below if needed.")
    elif msg and msg[0] == "short":
        st.warning("Couldn't resolve that short share link (network issue or it had no "
                   "coordinates). Open it in Google Maps and copy the full URL, or paste "
                   "coordinates like 53.34980, -6.26030.")
    elif msg and msg[0] == "err":
        st.warning("Couldn't find coordinates in that text. Paste a Google Maps URL or "
                   "coordinates like 53.34980, -6.26030.")

    c1, c2, c3 = st.columns([1, 1, 1.2])
    with c1:
        lat = st.number_input("Latitude", format="%.5f", key="sc_lat")
    with c2:
        lon = st.number_input("Longitude", format="%.5f", key="sc_lon")
    with c3:
        dev = st.selectbox("What do you want to build?", list(_DEV_TYPES),
                           format_func=lambda k: _DEV_TYPES[k], key="sc_dev")

    # scale inputs only matter for housing schemes / commercial — they drive the
    # scale-gated obligations (design statement, mobility, climate, EIA …)
    num_units = floor_area = None
    if dev in ("multi_unit", "commercial"):
        s1, s2 = st.columns(2)
        with s1:
            num_units = st.number_input("Number of units (if known)", min_value=0, value=0,
                                        step=1, key="sc_units") or None
        with s2:
            floor_area = st.number_input("Gross floor area m² (if known)", min_value=0, value=0,
                                         step=100, key="sc_fa") or None
    go = st.button("Check this site", type="primary")

    if not go:
        st.caption("Tip: in Google Maps, drag the orange 'little man' (bottom-right) onto your "
                   "exact site, then paste the address-bar link above — the point fills in for you.")
        return

    if not (51.0 <= lat <= 56.0 and -11.0 <= lon <= -5.0):
        st.error("That point is outside the island of Ireland. Check the latitude/longitude.")
        return

    with st.spinner("Evaluating the site against the designation layers and the rulebook…"):
        res = evaluate_site(float(lon), float(lat), dev,
                            num_units=int(num_units) if num_units else None,
                            floor_area_m2=float(floor_area) if floor_area else None)

    st.map({"lat": [lat], "lon": [lon]}, zoom=12, size=60)

    council = res.council.council_name or res.council.authority or "Unknown council"
    # be honest about HOW the council was resolved: zoning containment is authoritative;
    # nearest-application is a proxy that can snap across a boundary (on_boundary = the
    # nearest application is far away, i.e. low confidence — not "near an admin boundary").
    if getattr(res.council, "resolved_via", "") == "zoning":
        bnd = " (confirmed by the zoning map)"
    elif res.council.on_boundary:
        bnd = " (inferred from the nearest planning application, which is some distance away — verify the authority)"
    else:
        bnd = " (inferred from the nearest planning application)"
    from data_access.siting_data import site_terrain
    t = site_terrain(float(lon), float(lat))
    elev = f"{t.elevation_m} m" if t.ok else "n/a"
    exp = " · elevated/exposed" if (t.ok and t.exposed) else ""
    st.html(
        f'<div class="sc-meta"><b>Governing authority:</b> {_h(council)}{_h(bnd)}<br>'
        f"<b>Terrain:</b> elevation {_h(elev)}{_h(exp)} "
        f'<span class="sc-note">(Copernicus DEM, ±30 m — a coarse signal, not a survey)</span></div>'
    )

    # hard-exclusion mask first: if the point is inside a statutory protected designation, that is
    # the headline (presumption against development, with the narrow real route shown).
    if getattr(res, "excluded", False):
        _render_exclusion_banner(res.exclusions)

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
