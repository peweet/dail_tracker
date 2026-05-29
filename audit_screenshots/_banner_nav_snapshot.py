"""Single-shot capture of the top banner + nav strip on /member-overview.

Reuses the already-running Streamlit at localhost:8501. Captures a viewport
screenshot at desktop width with the camera anchored at the top of the page
so the banner + custom nav strip are framed.
"""

from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "_debug_banner_nav.png"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()
        page.goto(f"{BASE}/member-overview", wait_until="networkidle", timeout=30000)
        # Streamlit re-runs after first paint; give the nav strip + JS killer time to settle
        time.sleep(4)
        # The "Page not found" modal renders before the inject_css() JS killer fires.
        # Force-close it if present so the banner is unobscured.
        try:
            close_btn = page.locator('div[role="dialog"] button[aria-label="Close"]').first
            if close_btn.is_visible(timeout=1500):
                close_btn.click()
                time.sleep(0.5)
        except Exception:
            pass
        # Park the mouse far away so no nav link picks up :hover.
        page.mouse.move(0, 0)
        time.sleep(0.3)
        # Crop to top 320px — banner + topnav strip both visible
        page.screenshot(path=str(OUT), full_page=False, clip={"x": 0, "y": 0, "width": 1440, "height": 320})
        print(f"saved {OUT} ({OUT.stat().st_size // 1024} KB)")
        browser.close()


if __name__ == "__main__":
    main()
