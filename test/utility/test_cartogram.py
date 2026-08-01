"""Guards on the local-authority grid cartogram (utility/ui/cartogram.py).

The cartogram replaced a choropleth because polygon area ran opposite to the value being
shown. What a tile layout can silently get wrong instead is coverage: drop an authority,
double one, or drift out of step with the outline names every other council view joins on.
Each test below is one of those failures.
"""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT / "utility"))

from ui.cartogram import ABBREV, LAYOUT, cartogram_html, cartogram_svg  # noqa: E402

OUTLINES = _ROOT / "data" / "_meta" / "local_authority_outlines.json"


def _outline_names() -> set[str]:
    return set(json.loads(OUTLINES.read_text(encoding="utf-8"))["local_authorities"])


def test_every_authority_has_exactly_one_tile():
    assert len(LAYOUT) == 31
    cells = Counter(LAYOUT.values())
    overlapping = {cell: n for cell, n in cells.items() if n > 1}
    assert not overlapping, f"two authorities share a grid cell: {overlapping}"


@pytest.mark.skipif(not OUTLINES.exists(), reason="outline geometry not present")
def test_layout_keys_match_the_outline_join_key():
    """The cartogram joins on the same names as the choropleth. If these drift apart, one of
    the two silently renders an authority as no-data while the other colours it."""
    names = _outline_names()
    assert set(LAYOUT) == names, {
        "in layout, not outlines": sorted(set(LAYOUT) - names),
        "in outlines, not layout": sorted(names - set(LAYOUT)),
    }


def test_every_tile_has_a_label():
    missing = [n for n in LAYOUT if n not in ABBREV]
    assert not missing, f"authorities with no tile abbreviation: {missing}"
    dupes = {a: n for a, n in Counter(ABBREV[n] for n in LAYOUT).items() if n > 1}
    assert not dupes, f"abbreviations used for more than one authority: {dupes}"


def test_cities_are_distinguishable_from_their_counties():
    """The whole point of the change: Cork City and Galway City were invisible slivers on the
    outline map. They must be separate, separately-labelled tiles here."""
    for city, county in (("Cork City", "Cork County"), ("Galway City", "Galway County")):
        assert LAYOUT[city] != LAYOUT[county]
        assert ABBREV[city] != ABBREV[county]


def test_svg_renders_every_authority_with_hover_text():
    svg = cartogram_svg({n: "#3d719c" for n in LAYOUT}, value_by_name={n: "1.0" for n in LAYOUT})
    assert svg.count("<rect") == 31
    assert svg.count("<title>") == 31  # one hover label per tile, no page-level title passed
    assert 'role="img"' in svg
    for name in ("Galway City", "Dun Laoghaire-Rathdown", "Cork County"):
        assert name in svg


def test_missing_data_draws_a_visible_nodata_tile_not_a_gap():
    """An authority we hold nothing for must still occupy its tile — an absent tile would read
    as 'not part of the country', and a zero-coloured one as 'we measured zero'."""
    svg = cartogram_svg({"Mayo": "#3d719c"}, nodata_fill="#e4e7e9")
    assert svg.count("<rect") == 31
    assert svg.count("#e4e7e9") == 30
    assert "Donegal: no data" in svg


def test_html_form_is_an_image_because_st_html_strips_inline_svg():
    """Streamlit's st.html removes inline <svg>, so a page that embedded the SVG directly
    would render NOTHING and nobody would see an error. The page must use the <img> form."""
    html = cartogram_html({n: "#3d719c" for n in LAYOUT}, value_by_name={"Mayo": "12.0"})
    assert html.startswith('<img src="data:image/svg+xml;base64,')
    assert "<svg" not in html  # the SVG is inside the data URI, never inline
    assert 'usemap="#la-cartogram"' in html


def test_html_form_carries_one_hover_area_per_authority():
    """An <img> swallows the SVG's own <title> hovers, so each tile needs an <area>."""
    html = cartogram_html({}, value_by_name={"Mayo": "12.0"})
    assert html.count("<area ") == 31
    assert 'title="Mayo: 12.0"' in html
    assert 'title="Donegal: no data"' in html


def test_hover_areas_do_not_overlap():
    """Overlapping rectangles would hand a tile's hover to its neighbour."""
    import re

    html = cartogram_html({})
    boxes = [tuple(map(int, m.split(","))) for m in re.findall(r'coords="([\d,]+)"', html)]
    assert len(boxes) == 31
    for i, (ax1, ay1, ax2, ay2) in enumerate(boxes):
        for bx1, by1, bx2, by2 in boxes[i + 1 :]:
            assert ax2 <= bx1 or bx2 <= ax1 or ay2 <= by1 or by2 <= ay1, "hover areas overlap"


def test_label_ink_flips_on_dark_and_pale_tiles():
    """One fixed ink colour is unreadable at one end of any sequential ramp."""
    dark = cartogram_svg({n: "#12303f" for n in LAYOUT})
    pale = cartogram_svg({n: "#eef3f6" for n in LAYOUT})
    assert '#ffffff' in dark
    assert '#16243a' in pale
