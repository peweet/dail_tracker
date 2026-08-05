"""Equal-area grid cartogram of the 31 local authorities.

WHY THIS EXISTS. A choropleth of Irish local authorities encodes its value in polygon
AREA, and for any per-capita measure area runs the wrong way: the urban authorities have
the smallest polygons and the highest rates. Measured on the IPAS accommodation map
(2026-08-01): the two authorities in the Comptroller & Auditor General's top "12+" band
are Galway City (20.6 applicants per 1,000 residents) and South Dublin (13.2), whose
outline paths are 129 and ~120 bytes of geometry against Galway County's 2,730 and Cork
County's 2,690. The map rendered its own headline finding at a few pixels while giving the
largest visual weight to mid-band rural counties.

A grid cartogram gives every authority one equal tile in roughly its geographic position.
Dublin's four authorities and the three city councils become as readable as Mayo. This is
the standard remedy for the same problem in UK/US election maps (ONS, FT, Guardian).

The layout is a DESIGN artifact, not data: tiles are schematic positions, not coordinates,
and the rendered figure says so. What the layout must never do is silently lose or double
an authority, which is what the tests in test/utility/test_cartogram.py pin.

Pure string-building — no Streamlit, no data access — so it is unit-testable directly.
"""

from __future__ import annotations

import base64
from html import escape as _esc

# (row, col) on a 6-wide grid, north to south, west to east. Hand-placed to read as Ireland
# at a glance: Donegal alone at the top, the border counties beneath it, the Dublin cluster
# on the east edge, Cork/Waterford/Wexford along the bottom. Keys are the outline names in
# data/_meta/local_authority_outlines.json — the same join key the choropleth uses, so a
# rename that breaks one breaks both loudly rather than silently mis-colouring one.
LAYOUT: dict[str, tuple[int, int]] = {
    "Donegal": (0, 1),
    "Sligo": (1, 0),
    "Leitrim": (1, 1),
    "Cavan": (1, 2),
    "Monaghan": (1, 3),
    "Mayo": (2, 0),
    "Roscommon": (2, 1),
    "Longford": (2, 2),
    "Westmeath": (2, 3),
    "Meath": (2, 4),
    "Louth": (2, 5),
    "Galway County": (3, 0),
    "Galway City": (3, 1),
    "Offaly": (3, 2),
    "Kildare": (3, 3),
    "Fingal": (3, 4),
    "Clare": (4, 0),
    "Limerick": (4, 1),
    "Laois": (4, 2),
    "Dublin City": (4, 3),
    "South Dublin": (4, 4),
    "Dun Laoghaire-Rathdown": (4, 5),
    "Kerry": (5, 0),
    "Tipperary": (5, 1),
    "Kilkenny": (5, 2),
    "Carlow": (5, 3),
    "Wicklow": (5, 4),
    "Cork County": (6, 0),
    "Cork City": (6, 1),
    "Waterford": (6, 2),
    "Wexford": (6, 3),
}

# Tile labels. Full names do not fit a tile, and truncation would render "Dun Laoghaire-R".
# Codes are the conventional Irish vehicle-registration/administrative abbreviations, with
# the city councils distinguished from their counties — the distinction the choropleth
# could not show at all.
ABBREV: dict[str, str] = {
    "Carlow": "CW",
    "Cavan": "CN",
    "Clare": "CE",
    "Cork City": "CKC",
    "Cork County": "CK",
    "Donegal": "DL",
    "Dublin City": "DUB",
    "Dun Laoghaire-Rathdown": "DLR",
    "Fingal": "FGL",
    "Galway City": "GYC",
    "Galway County": "GY",
    "Kerry": "KY",
    "Kildare": "KE",
    "Kilkenny": "KK",
    "Laois": "LS",
    "Leitrim": "LM",
    "Limerick": "LK",
    "Longford": "LD",
    "Louth": "LH",
    "Mayo": "MO",
    "Meath": "MH",
    "Monaghan": "MN",
    "Offaly": "OY",
    "Roscommon": "RN",
    "Sligo": "SO",
    "South Dublin": "SDU",
    "Tipperary": "TY",
    "Waterford": "WD",
    "Westmeath": "WH",
    "Wexford": "WX",
    "Wicklow": "WW",
}

_TILE = 62  # tile edge in user units
_GAP = 6  # gutter between tiles
_PAD = 4  # outer padding so focus rings are not clipped


def _luminance(hex_colour: str) -> float:
    """Relative luminance of a #rrggbb colour, for picking readable label ink."""
    h = hex_colour.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    try:
        r, g, b = (int(h[i : i + 2], 16) / 255 for i in (0, 2, 4))
    except ValueError:
        return 1.0

    def _lin(c: float) -> float:
        return c / 12.92 if c <= 0.04045 else ((c + 0.055) / 1.055) ** 2.4

    return 0.2126 * _lin(r) + 0.7152 * _lin(g) + 0.0722 * _lin(b)


def _ink(fill: str) -> str:
    """Label colour chosen per tile so text stays legible on every band. A single fixed ink
    would fail either the pale or the dark end of any sequential ramp."""
    return "#ffffff" if _luminance(fill) < 0.45 else "#16243a"


def cartogram_svg(
    fill_by_name: dict[str, str],
    *,
    value_by_name: dict[str, str] | None = None,
    nodata_fill: str = "#e4e7e9",
    title: str = "",
    alt: str = "",
) -> str:
    """One equal-area tile per local authority, positioned schematically.

    `fill_by_name` maps an authority to a CSS colour; anything absent draws in
    `nodata_fill`, which is a visible "we hold nothing here" state — never an implied zero.
    `value_by_name` supplies the second line on each tile (the actual figure, which the
    choropleth could not show at all) and the hover text.
    """
    values = value_by_name or {}
    rows = max(r for r, _ in LAYOUT.values()) + 1
    cols = max(c for _, c in LAYOUT.values()) + 1
    w = cols * _TILE + (cols - 1) * _GAP + 2 * _PAD
    h = rows * _TILE + (rows - 1) * _GAP + 2 * _PAD

    parts: list[str] = []
    for name, (row, col) in LAYOUT.items():
        x = _PAD + col * (_TILE + _GAP)
        y = _PAD + row * (_TILE + _GAP)
        fill = fill_by_name.get(name) or nodata_fill
        ink = _ink(fill)
        value = values.get(name, "")
        label = ABBREV.get(name, name[:3].upper())
        hover = f"{name}: {value}" if value else f"{name}: no data"
        parts.append(
            f"<g><title>{_esc(hover)}</title>"
            f'<rect x="{x}" y="{y}" width="{_TILE}" height="{_TILE}" rx="7" fill="{_esc(fill)}"/>'
            f'<text x="{x + _TILE / 2}" y="{y + (25 if value else 37)}" text-anchor="middle" '
            f'font-family="system-ui,sans-serif" font-size="16" font-weight="700" '
            f'fill="{ink}">{_esc(label)}</text>'
            + (
                f'<text x="{x + _TILE / 2}" y="{y + 45}" text-anchor="middle" '
                f'font-family="system-ui,sans-serif" font-size="15" fill="{ink}" '
                f'opacity="0.92">{_esc(value)}</text>'
                if value
                else ""
            )
            + "</g>"
        )

    heading = f"<title>{_esc(title or alt)}</title>" if (title or alt) else ""
    return (
        f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {w} {h}" '
        f'width="100%" style="max-width:{w}px;height:auto" role="img" '
        f'aria-label="{_esc(alt or title)}">{heading}{"".join(parts)}</svg>'
    )


def grid_size() -> tuple[int, int]:
    """Natural (width, height) of the tile grid in user units."""
    rows = max(r for r, _ in LAYOUT.values()) + 1
    cols = max(c for _, c in LAYOUT.values()) + 1
    return (cols * _TILE + (cols - 1) * _GAP + 2 * _PAD, rows * _TILE + (rows - 1) * _GAP + 2 * _PAD)


def cartogram_html(
    fill_by_name: dict[str, str],
    *,
    value_by_name: dict[str, str] | None = None,
    nodata_fill: str = "#e4e7e9",
    alt: str = "",
    scale: float = 1.0,
    map_name: str = "la-cartogram",
) -> str:
    """The cartogram as a FIXED-SIZE base64 <img> plus an imagemap of per-tile hover labels.

    Streamlit's ``st.html`` strips inline ``<svg>``, so an SVG returned by ``cartogram_svg``
    renders as nothing at all on a page — the same reason the outline choropleth and the
    figure mark in accommodation_spend.py are both data-URI images. Encoding it here keeps
    that one workaround in one place.

    An ``<img>`` also swallows the SVG's own ``<title>`` hovers, so each tile gets an
    ``<area>`` carrying the same text. That is how the outline choropleth already does
    interaction, and tiles are rectangles, so the coordinates are exact rather than the
    approximation a polygon outline needs. Coordinates are in the image's DISPLAYED pixel
    space, which is why the image is emitted at a fixed size and never ``width:100%``.
    """
    svg = cartogram_svg(fill_by_name, value_by_name=value_by_name, nodata_fill=nodata_fill, alt=alt)
    w, h = grid_size()
    px_w, px_h = round(w * scale), round(h * scale)
    b64 = base64.b64encode(svg.encode("utf-8")).decode("ascii")
    values = value_by_name or {}

    areas: list[str] = []
    for name, (row, col) in LAYOUT.items():
        x = (_PAD + col * (_TILE + _GAP)) * scale
        y = (_PAD + row * (_TILE + _GAP)) * scale
        value = values.get(name, "")
        hover = f"{name}: {value}" if value else f"{name}: no data"
        areas.append(
            f'<area shape="rect" coords="{round(x)},{round(y)},'
            f'{round(x + _TILE * scale)},{round(y + _TILE * scale)}" '
            f'title="{_esc(hover)}" alt="{_esc(hover)}">'
        )
    return (
        f'<img src="data:image/svg+xml;base64,{b64}" width="{px_w}" height="{px_h}" '
        f'alt="{_esc(alt)}" usemap="#{_esc(map_name)}" style="max-width:100%;height:auto"/>'
        f'<map name="{_esc(map_name)}">{"".join(areas)}</map>'
    )
