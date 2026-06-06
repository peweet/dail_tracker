"""Procurement UI-audit capture — viewport shots (full_page is broken on this
Streamlit/Windows combo, see memory feedback_streamlit_playwright). Captures the
above-the-fold furniture, each tab, a supplier drill-down and a mobile view.
Saves to audit_screenshots/_pa_*.png.
"""

from __future__ import annotations

import urllib.parse
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8533"
DESKTOP = {"width": 1440, "height": 900}
MOBILE = {"width": 390, "height": 844}


def _settle(page, ms: int = 3500) -> None:
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    try:
        page.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=12000,
        )
    except Exception:
        pass
    page.wait_for_timeout(ms)


def _shoot(page, name: str) -> None:
    p = OUT / f"{name}.png"
    page.screenshot(path=str(p), full_page=False)
    print(f"  saved {p.name} ({p.stat().st_size // 1024} KB)")


def _dismiss_toast(page) -> None:
    # The rerun toast can overlay the top-right; press Escape to clear.
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


def capture(pw):
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
    page = ctx.new_page()

    print("[1] landing top (hero + caveat + stats + glossary + year pills)")
    page.goto(f"{BASE}/rankings-procurement", wait_until="domcontentloaded")
    _settle(page, 5000)
    _dismiss_toast(page)
    _shoot(page, "_pa_1_top")

    print("[2] landing scrolled (first ranking cards)")
    page.mouse.wheel(0, 760)
    page.wait_for_timeout(1200)
    _shoot(page, "_pa_2_cards")

    for idx, label in enumerate(["Contracting authorities", "Categories", "Lobbying overlap"], start=3):
        print(f"[{idx}] tab — {label}")
        try:
            page.mouse.wheel(0, -2000)
            page.wait_for_timeout(400)
            page.get_by_role("tab", name=label).click()
            _settle(page, 2500)
            page.mouse.wheel(0, 520)
            page.wait_for_timeout(900)
            _shoot(page, f"_pa_{idx}_{label.split()[0].lower()}")
        except Exception as e:
            print(f"   skipped {label}: {e}")

    print("[6] supplier drill-down profile")
    # Grab the first supplier's href from the live DOM so we hit a real norm.
    try:
        href = page.eval_on_selector(
            "a.dt-card-link", "el => el.getAttribute('href')"
        )
    except Exception:
        href = None
    if not href:
        href = "?supplier=" + urllib.parse.quote("hse")
    page.goto(f"{BASE}/rankings-procurement{href}", wait_until="domcontentloaded")
    _settle(page, 4000)
    _dismiss_toast(page)
    _shoot(page, "_pa_6_supplier_top")
    page.mouse.wheel(0, 700)
    page.wait_for_timeout(1000)
    _shoot(page, "_pa_7_supplier_awards")

    print("[8] mobile landing")
    mctx = browser.new_context(viewport=MOBILE, device_scale_factor=2)
    mp = mctx.new_page()
    mp.goto(f"{BASE}/rankings-procurement", wait_until="domcontentloaded")
    _settle(mp, 5000)
    _dismiss_toast(mp)
    _shoot(mp, "_pa_8_mobile_top")
    mp.mouse.wheel(0, 900)
    mp.wait_for_timeout(1200)
    _shoot(mp, "_pa_9_mobile_cards")

    browser.close()


if __name__ == "__main__":
    with sync_playwright() as p:
        capture(p)
    print("DONE")
