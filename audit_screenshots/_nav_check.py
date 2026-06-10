"""Quick check of the grouped top-nav after the IA edit. Captures the nav
band on member-overview at desktop + mobile, and clicks the nav to reveal any
group dropdown so we can see whether grouping renders acceptably."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8534"


def _wait(pg, ms=3500):
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


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    # Desktop — full nav band
    ctx = b.new_context(viewport={"width": 1440, "height": 900}, device_scale_factor=2)
    pg = ctx.new_page()
    pg.goto(f"{BASE}/member-overview", wait_until="domcontentloaded")
    _wait(pg)
    pg.screenshot(path=str(OUT / "_nav_desk.png"), clip={"x": 0, "y": 0, "width": 1440, "height": 220})
    print("saved _nav_desk")

    # Mobile — how the grouped nav collapses
    pg.set_viewport_size({"width": 390, "height": 844})
    pg.goto(f"{BASE}/member-overview", wait_until="domcontentloaded")
    _wait(pg)
    pg.screenshot(path=str(OUT / "_nav_mob.png"), clip={"x": 0, "y": 0, "width": 390, "height": 300})
    print("saved _nav_mob")
    b.close()
print("DONE")
