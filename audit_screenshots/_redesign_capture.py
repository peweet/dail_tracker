"""Phase-0 redesign sweep — landing state of EVERY visible route at desktop +
mobile, plus a tablet pass on the stat-strip-heavy pages. Reuses the same
networkidle + status-widget wait as _sweep_capture.py.

Output: audit_screenshots/_rd_<view>_<name>.png
Run against a FRESH Streamlit on :8534 (hot-reload serves stale CSS)."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8534"
DESK = {"width": 1440, "height": 1600}
TAB = {"width": 768, "height": 1280}
MOB = {"width": 390, "height": 844}

# All 16 visible routes (Home is hidden; it renders member-overview).
ROUTES = [
    ("01_member_overview", "member-overview"),
    ("02_attendance", "rankings-attendance"),
    ("03_votes", "rankings-votes"),
    ("04_interests", "rankings-interests"),
    ("05_payments", "rankings-payments"),
    ("06_election_spending", "rankings-election-spending"),
    ("07_lobbying", "rankings-lobbying"),
    ("08_legislation", "rankings-legislation"),
    ("09_statutory_instruments", "rankings-statutory-instruments"),
    ("10_appointments", "rankings-appointments"),
    ("11_corporate", "rankings-corporate"),
    ("12_procurement", "rankings-procurement"),
    ("13_public_payments", "rankings-public-payments"),
    ("14_committees", "rankings-committees"),
    ("15_judiciary", "rankings-judiciary"),
    ("16_glossary", "glossary"),
]
# Mobile (390) for the pages where cramp/IA pain is worst, tablet (768) for the
# multi-stat-strip pages the audit flagged.
MOBILE_ROUTES = [
    ("member_overview", "member-overview"),
    ("payments", "rankings-payments"),
    ("procurement", "rankings-procurement"),
    ("statutory_instruments", "rankings-statutory-instruments"),
    ("corporate", "rankings-corporate"),
    ("lobbying", "rankings-lobbying"),
]
TABLET_ROUTES = [
    ("procurement", "rankings-procurement"),
    ("statutory_instruments", "rankings-statutory-instruments"),
    ("legislation", "rankings-legislation"),
    ("committees", "rankings-committees"),
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
    for name, route in ROUTES:
        _shoot(pg, "desk", name, route)

    pg.set_viewport_size(TAB)
    for name, route in TABLET_ROUTES:
        _shoot(pg, "tab", name, route)

    pg.set_viewport_size(MOB)
    for name, route in MOBILE_ROUTES:
        _shoot(pg, "mob", name, route)
    b.close()
print("DONE")
