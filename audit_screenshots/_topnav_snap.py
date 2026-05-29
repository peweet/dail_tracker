"""Diagnose whether the page still renders .site-topnav (old custom HTML)."""
from __future__ import annotations
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "_topnav_current.png"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        page.goto(f"{BASE}/member-overview", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3)
        try:
            ar = page.get_by_role("button", name="Always rerun")
            if ar.is_visible(timeout=1000):
                ar.click()
                time.sleep(2.0)
        except Exception:
            pass
        page.mouse.move(0, 0)
        time.sleep(0.5)

        # Hunt for any stale .site-topnav nodes
        survey = page.evaluate(
            """() => ({
                siteTopnav: document.querySelectorAll('.site-topnav').length,
                siteTopnavLinks: document.querySelectorAll('.site-topnav-link').length,
                siteBanner: document.querySelectorAll('.site-banner').length,
                nativeNav: document.querySelectorAll('[data-testid="stTopNavLink"]').length,
            })"""
        )
        print("DOM survey:", survey)

        page.screenshot(path=str(OUT), clip={"x": 0, "y": 0, "width": 1440, "height": 280})
        print(f"saved {OUT}")
        browser.close()


if __name__ == "__main__":
    main()
