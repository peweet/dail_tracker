"""Engine-free Google-Maps link → (lat, lon) parsing + short-link resolution.

Pure stdlib (re + urllib) — NO siting engine, shapely or streamlit imports — so it loads on
Streamlit Cloud where the geo stack is absent. Used by the thin Cloud client
(pages_code/siting_remote.py). The local full page keeps its own copy in siting_check.py.
"""

from __future__ import annotations

import re
import urllib.request

# Coordinate sources in a pasted Maps URL, preference order: dropped pin (!3d!4d, most precise),
# then the map centre (@lat,lon), then query-param forms, then a bare "lat, lon" paste.
_LL = r"(-?\d{1,3}\.\d+)"
_RE_PIN = re.compile(rf"!3d{_LL}!4d{_LL}")
_RE_AT = re.compile(rf"@{_LL},{_LL}")
_RE_Q = re.compile(rf"[?&](?:q|ll|query|center|sll)=\s*{_LL},\s*{_LL}")
_RE_BARE = re.compile(rf"^{_LL},\s*{_LL}$")
_SHORT_HOSTS = ("maps.app.goo.gl", "goo.gl/maps")


def parse_latlon(text: str) -> tuple[float, float] | None:
    """(lat, lon) out of a pasted Maps URL or bare 'lat, lon' string, or None."""
    text = (text or "").strip()
    if not text:
        return None
    for rx in (_RE_PIN, _RE_AT, _RE_Q, _RE_BARE):
        m = rx.search(text)
        if m:
            return float(m.group(1)), float(m.group(2))
    return None


def is_short_link(text: str) -> bool:
    return any(h in (text or "") for h in _SHORT_HOSTS)


def resolve_short_link(url: str) -> str | None:
    """Follow a maps.app.goo.gl / goo.gl/maps redirect to its destination URL + page text."""
    try:
        req = urllib.request.Request(
            url.strip(), headers={"User-Agent": "Mozilla/5.0 (siting-check)"}
        )
        with urllib.request.urlopen(req, timeout=10) as resp:  # urlopen follows redirects
            final = resp.geturl() or ""
            try:
                body = resp.read(150_000).decode("utf-8", "ignore")
            except Exception:
                body = ""
        return f"{final}\n{body}"
    except Exception:
        return None


def coords_from_anything(text: str) -> tuple[float, float] | None:
    """parse_latlon, transparently resolving a short share link first when needed."""
    p = parse_latlon(text)
    if p:
        return p
    if is_short_link(text):
        resolved = resolve_short_link(text)
        if resolved:
            return parse_latlon(resolved)
    return None
