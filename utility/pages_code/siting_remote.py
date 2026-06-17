"""Thin Cloud client for Siting Check — engine-free.

Renders the link box + spinner and delegates the actual evaluation to a LOCAL siting engine
exposed at SITING_API_URL (via a tunnel). Imports NOTHING from the geo engine / shapely, so it
loads on Streamlit Cloud where those are absent. The full local page is pages_code/siting_check.py.
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request

import streamlit as st

from data_access.maps_link import coords_from_anything

_CLASS = {
    "F": ("#b3261e", "Often the dealbreaker", "🔴"),
    "D": ("#9a6700", "Usually mitigable with the right specialist", "🟡"),
    "P": ("#1a7f37", "Procedural — commission the report", "🟢"),
}
_DEV = {
    "one_off_house": "One-off house",
    "extension": "Extension",
    "multi_unit": "Multiple houses / apartments",
    "commercial": "Commercial",
}


def _api_url() -> str:
    try:
        v = st.secrets.get("SITING_API_URL", "")  # type: ignore[attr-defined]
    except Exception:
        v = ""
    return (v or os.environ.get("SITING_API_URL", "")).strip()


def _call_api(base: str, lon: float, lat: float, dev: str) -> dict:
    q = urllib.parse.urlencode({"lon": lon, "lat": lat, "dev": dev})
    req = urllib.request.Request(
        f"{base.rstrip('/')}/evaluate?{q}", headers={"User-Agent": "siting-cloud"}
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def _order_key(issue: dict) -> int:
    return "FDP".index(issue.get("mitigation_class", "P") if issue.get("mitigation_class") in _CLASS else "P")


def siting_remote_page() -> None:
    st.title("Siting Check (experimental)")
    st.caption(
        "Planning-risk triage — what planning issues a site is likely to trigger. "
        "Not professional planning advice; never a grant/refuse outcome."
    )
    api = _api_url()
    if not api:
        st.warning(
            "The siting engine endpoint isn't configured for this deployment yet "
            "(set SITING_API_URL). It runs on a connected machine."
        )
        return

    txt = st.text_input(
        "Paste a Google Maps link or coordinates",
        placeholder="maps.app.goo.gl/… , a full Maps URL, or 53.27, -9.05",
    )
    dev = st.selectbox("What do you want to build?", list(_DEV), format_func=lambda k: _DEV[k])
    if not st.button("Check this site", type="primary"):
        st.caption("Tip: in Google Maps, share a pin or copy the address-bar URL, and paste it here.")
        return

    with st.spinner("Locating your site…"):
        coords = coords_from_anything(txt)
    if not coords:
        st.error("Couldn't read coordinates from that. Paste a Google Maps link or 'lat, lon'.")
        return
    lat, lon = coords
    if not (51.0 <= lat <= 56.0 and -11.0 <= lon <= -5.0):
        st.error("That point is outside the island of Ireland. Check the link.")
        return

    st.map({"lat": [lat], "lon": [lon]}, zoom=11, size=60)
    with st.spinner("Assessing the site against the designation layers and the rulebook…"):
        try:
            res = _call_api(api, lon, lat, dev)
        except Exception as e:  # noqa: BLE001
            st.error(f"Couldn't reach the siting engine ({type(e).__name__}). Is it running?")
            return

    council = res.get("council", {}).get("name") or "Unknown council"
    st.markdown(f"**Governing authority:** {council}")

    fired = sorted(res.get("fired", []), key=_order_key)
    if fired:
        st.subheader(f"{len(fired)} issue(s) your site triggers")
        for i in fired:
            colour, label, dot = _CLASS.get(i.get("mitigation_class", "P"), _CLASS["P"])
            with st.container(border=True):
                st.markdown(f"{dot} **{i.get('title','')}** — :gray[{label}]")
                if i.get("flag"):
                    st.write(i["flag"])
                if i.get("engage"):
                    st.caption("Engage: " + ", ".join(i["engage"]))
                if i.get("mitigates"):
                    st.caption("Mitigation: " + i["mitigates"])
    else:
        st.success("No mapped designation issues fired for this point from the loaded layers.")

    miss = res.get("missing_layers", [])
    if miss:
        st.info(
            "Not yet assessed here (data layer pending): "
            + ", ".join(m.replace("_", " ") for m in miss)
            + ". Absence of a flag is not confirmation the issue does not apply."
        )
    if res.get("disclaimer"):
        st.caption(res["disclaimer"])
