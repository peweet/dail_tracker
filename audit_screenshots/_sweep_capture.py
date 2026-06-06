"""Full-app UI sweep — landing state of every route (desktop tall) + mobile for key pages."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8534"
DESK = {"width": 1440, "height": 1300}
MOB = {"width": 390, "height": 844}

ROUTES = [
    ("01_member_overview", "member-overview"),
    ("02_attendance", "rankings-attendance"),
    ("03_votes", "rankings-votes"),
    ("04_interests", "rankings-interests"),
    ("05_payments", "rankings-payments"),
    ("06_lobbying", "rankings-lobbying"),
    ("07_legislation", "rankings-legislation"),
    ("08_statutory_instruments", "rankings-statutory-instruments"),
    ("09_appointments", "rankings-appointments"),
    ("10_corporate", "rankings-corporate"),
    ("11_procurement", "rankings-procurement"),
    ("12_committees", "rankings-committees"),
    ("13_judiciary", "rankings-judiciary"),
    ("14_glossary", "glossary"),
]
MOBILE_ROUTES = [
    ("m1_member_overview", "member-overview"),
    ("m2_payments", "rankings-payments"),
    ("m3_lobbying", "rankings-lobbying"),
    ("m4_interests", "rankings-interests"),
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


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context(viewport=DESK, device_scale_factor=1)
    pg = ctx.new_page()
    for name, route in ROUTES:
        try:
            pg.goto(f"{BASE}/{route}", wait_until="domcontentloaded")
            _wait(pg)
            pg.screenshot(path=str(OUT / f"_sweep_{name}.png"))
            print("saved", name)
        except Exception as e:
            print(f"  FAIL {name}: {e}")

    pg.set_viewport_size(MOB)
    for name, route in MOBILE_ROUTES:
        try:
            pg.goto(f"{BASE}/{route}", wait_until="domcontentloaded")
            _wait(pg)
            pg.screenshot(path=str(OUT / f"_sweep_{name}.png"))
            print("saved", name)
        except Exception as e:
            print(f"  FAIL {name}: {e}")
    b.close()
print("DONE")
