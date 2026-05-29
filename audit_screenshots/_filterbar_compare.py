"""Side-by-side: legislation_v2 (filter bar, no sidebar) vs original (sidebar)."""
from __future__ import annotations
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8599"
OUT = Path(__file__).resolve().parent
V2_DESK = OUT / "_cmp_v2_desktop.png"
ORIG_DESK = OUT / "_cmp_orig_desktop.png"
V2_MOBILE = OUT / "_cmp_v2_mobile.png"


def _settle(page):
    time.sleep(3)
    try:
        ar = page.get_by_role("button", name="Always rerun")
        if ar.is_visible(timeout=1200):
            ar.click(); time.sleep(2.0)
    except Exception:
        pass
    try:
        cb = page.locator('div[role="dialog"] button[aria-label="Close"]').first
        if cb.is_visible(timeout=1200):
            cb.click(); time.sleep(0.4)
    except Exception:
        pass
    page.mouse.move(0, 0)
    time.sleep(1.0)


def _probe(page):
    return page.evaluate(
        """() => {
            const sb = document.querySelector('[data-testid=\\"sidebar\\"]') ||
                       document.querySelector('[data-testid=\\"stSidebar\\"]');
            const vis = sb ? (getComputedStyle(sb).display !== 'none' && sb.getBoundingClientRect().width > 5) : false;
            const bi = document.querySelector('.site-banner-inner');
            return {
                sidebarVisible: vis,
                sidebarWidth: sb ? Math.round(sb.getBoundingClientRect().width) : 0,
                filterbarMarker: document.querySelectorAll('.dt-filterbar-marker').length,
                filterRule: document.querySelectorAll('.dt-filterbar-rule').length,
                fieldLabels: [...document.querySelectorAll('.dt-field-label')].map(e => e.textContent),
                bannerPadLeft: bi ? getComputedStyle(bi).paddingLeft : null,
            };
        }"""
    )


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        # v2 desktop
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        pg = ctx.new_page()
        pg.goto(f"{BASE}/rankings-legislation-v2", wait_until="networkidle", timeout=30000)
        _settle(pg)
        print("V2 desktop probe:", _probe(pg))
        pg.screenshot(path=str(V2_DESK), clip={"x": 0, "y": 0, "width": 1440, "height": 720})
        print(f"saved {V2_DESK}")
        ctx.close()

        # original desktop (placeholder)
        ctx2 = browser.new_context(viewport={"width": 1440, "height": 900})
        pg2 = ctx2.new_page()
        pg2.goto(f"{BASE}/rankings-legislation", wait_until="networkidle", timeout=30000)
        _settle(pg2)
        print("ORIG desktop probe:", _probe(pg2))
        pg2.screenshot(path=str(ORIG_DESK), clip={"x": 0, "y": 0, "width": 1440, "height": 720})
        print(f"saved {ORIG_DESK}")
        ctx2.close()

        # v2 mobile
        ctx3 = browser.new_context(viewport={"width": 390, "height": 844})
        pg3 = ctx3.new_page()
        pg3.goto(f"{BASE}/rankings-legislation-v2", wait_until="networkidle", timeout=30000)
        _settle(pg3)
        print("V2 mobile probe:", _probe(pg3))
        pg3.screenshot(path=str(V2_MOBILE), clip={"x": 0, "y": 0, "width": 390, "height": 860})
        print(f"saved {V2_MOBILE}")
        ctx3.close()

        browser.close()


if __name__ == "__main__":
    main()
