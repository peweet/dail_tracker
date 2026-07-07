"""Google-Maps coordinate extraction for the Siting Check page.

Locks the rule that fixed the "pasted link bears no relation to what was selected" bug:
when a short share-link resolves to "<final-url>\\n<page-body>", the redirected URL is
authoritative for the shared point. A global !3d!4d search across the 150 KB page body grabs
an unrelated nearby pin, so the URL must be parsed in full BEFORE the body is ever consulted.
"""

from __future__ import annotations

import sys
import warnings
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for path in (str(PROJECT_ROOT), str(PROJECT_ROOT / "utility")):
    if path not in sys.path:
        sys.path.insert(0, path)

with warnings.catch_warnings():
    warnings.filterwarnings("ignore", message="No runtime found")
    from utility.pages_code.siting_check import coords_from_resolved, parse_latlon_from_maps


def test_pin_preferred_over_viewport_in_a_single_url():
    # /maps/place/<name>/@<viewport>/data=...!3d<pin-lat>!4d<pin-lon> — the pin is the place.
    url = "https://www.google.com/maps/place/X/@53.3400,-6.2500,17z/data=!3d53.3412!4d-6.2531"
    assert parse_latlon_from_maps(url) == (53.3412, -6.2531)


def test_bare_coordinates_paste():
    assert parse_latlon_from_maps("53.34980, -6.26030") == (53.3498, -6.2603)


def test_resolved_url_wins_over_body_pins():
    # Final URL carries only the @viewport (centred on the shared point); the body is full of
    # OTHER places' !3d!4d pins. The URL's coordinate must win — not the first body pin.
    final_url = "https://www.google.com/maps/place/Shared+Pin/@53.3500,-6.2600,17z/data=!4m2"
    body = (
        "<html>...!3d40.0000!4d-3.0000... a nearby cafe ..."
        "@10.0000,20.0000 some viewport ...!3d12.3456!4d-7.6543 unrelated POI...</html>"
    )
    resolved = f"{final_url}\n{body}"
    assert coords_from_resolved(resolved) == (53.3500, -6.2600)


def test_body_fallback_only_when_url_has_no_coords():
    # If the redirect target genuinely carries no coordinate, the body is the only signal left.
    final_url = "https://consent.google.com/m?continue=somethingwithoutcoords"
    body = "...!3d53.2707!4d-9.0568..."
    resolved = f"{final_url}\n{body}"
    assert coords_from_resolved(resolved) == (53.2707, -9.0568)


def test_no_coordinates_anywhere_returns_none():
    assert coords_from_resolved("https://maps.app.goo.gl/abc\n<html>no coords here</html>") is None
    assert parse_latlon_from_maps("just some text") is None
