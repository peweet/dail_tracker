"""Procurement civic-UI audit capture (current 4-tab structure). Viewport shots only
(full_page broken on this Windows/Streamlit combo). Saves audit_screenshots/_pc_*.png.
"""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8533/rankings-procurement"
DESKTOP = {"width": 1440, "height": 900}
MOBILE = {"width": 390, "height": 844}


def _settle(page, ms=4000):
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(ms)
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


def _shoot(page, name):
    p = OUT / f"{name}.png"
    page.screenshot(path=str(p), full_page=False)
    print(f"  saved {p.name} ({p.stat().st_size // 1024} KB)")


def capture(pw):
    b = pw.chromium.launch(headless=True)
    pg = b.new_context(viewport=DESKTOP, device_scale_factor=1).new_page()

    print("[1] top: hero + search + caveat + glossary")
    pg.goto(BASE, wait_until="domcontentloaded")
    _settle(pg, 6000)
    _shoot(pg, "_pc_1_top")

    print("[2] who-wins cards (scrolled)")
    pg.mouse.wheel(0, 820)
    pg.wait_for_timeout(1200)
    _shoot(pg, "_pc_2_winners")

    for idx, name in enumerate(["Who actually gets paid?", "Open right now", "Patterns"], start=3):
        print(f"[{idx}] tab — {name}")
        try:
            pg.mouse.wheel(0, -3000)
            pg.wait_for_timeout(400)
            pg.get_by_role("tab", name=name).click()
            _settle(pg, 3000)
            _shoot(pg, f"_pc_{idx}a_{idx}_top")
            pg.mouse.wheel(0, 760)
            pg.wait_for_timeout(1000)
            _shoot(pg, f"_pc_{idx}b_scrolled")
        except Exception as e:
            print(f"   skipped {name}: {e}")

    print("[6] mobile top")
    mp = b.new_context(viewport=MOBILE, device_scale_factor=2).new_page()
    mp.goto(BASE, wait_until="domcontentloaded")
    _settle(mp, 6000)
    _shoot(mp, "_pc_6_mobile_top")
    mp.mouse.wheel(0, 900)
    mp.wait_for_timeout(1200)
    _shoot(mp, "_pc_7_mobile_cards")

    b.close()


if __name__ == "__main__":
    with sync_playwright() as p:
        capture(p)
    print("DONE")
