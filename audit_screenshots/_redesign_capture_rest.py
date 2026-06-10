"""Re-capture only the shots the first sweep missed (server died after #15).
Glossary desktop + tablet pass + mobile pass. Same wait logic."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8534"
DESK = {"width": 1440, "height": 1600}
TAB = {"width": 768, "height": 1280}
MOB = {"width": 390, "height": 844}

DESK_REST = [("16_glossary", "glossary")]
TABLET_ROUTES = [
    ("procurement", "rankings-procurement"),
    ("statutory_instruments", "rankings-statutory-instruments"),
    ("legislation", "rankings-legislation"),
    ("committees", "rankings-committees"),
]
MOBILE_ROUTES = [
    ("member_overview", "member-overview"),
    ("payments", "rankings-payments"),
    ("procurement", "rankings-procurement"),
    ("statutory_instruments", "rankings-statutory-instruments"),
    ("corporate", "rankings-corporate"),
    ("lobbying", "rankings-lobbying"),
]


def _wait(pg, ms=3200):
    try:
        pg.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    try:
        pg.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=12000,
        )
    except Exception:
        pass
    pg.wait_for_timeout(ms)


def _shoot(pg, view, name, route):
    try:
        pg.goto(f"{BASE}/{route}", wait_until="domcontentloaded")
        _wait(pg)
        pg.screenshot(path=str(OUT / f"_rd_{view}_{name}.png"))
        print("saved", view, name)
    except Exception as e:
        print(f"  FAIL {view} {name}: {e}")


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context(viewport=DESK, device_scale_factor=1)
    pg = ctx.new_page()
    for name, route in DESK_REST:
        _shoot(pg, "desk", name, route)
    pg.set_viewport_size(TAB)
    for name, route in TABLET_ROUTES:
        _shoot(pg, "tab", name, route)
    pg.set_viewport_size(MOB)
    for name, route in MOBILE_ROUTES:
        _shoot(pg, "mob", name, route)
    b.close()
print("DONE")
