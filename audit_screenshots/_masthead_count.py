"""Count masthead/nav DOM nodes to confirm whether the banner+nav is duplicated."""
from __future__ import annotations
import time
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_context(viewport={"width": 1440, "height": 900}).new_page()
        page.goto(f"{BASE}/rankings-legislation", wait_until="domcontentloaded", timeout=30000)
        time.sleep(3.5)
        try:
            ar = page.get_by_role("button", name="Always rerun")
            if ar.is_visible(timeout=1500):
                ar.click(); time.sleep(2.0)
        except Exception:
            pass
        page.mouse.move(0, 0); time.sleep(0.5)
        counts = page.evaluate(
            """() => ({
                siteBanner: document.querySelectorAll('.site-banner').length,
                stHeader: document.querySelectorAll('header[data-testid="stHeader"]').length,
                topNavContainers: document.querySelectorAll('[data-testid="stTopNav"]').length,
                topNavLinks: document.querySelectorAll('[data-testid="stTopNavLink"]').length,
                deployVisible: !!document.querySelector('[data-testid="stAppDeployButton"]'),
                styleTags: document.querySelectorAll('style').length,
                bannerTexts: [...document.querySelectorAll('.site-banner-title')].map(e => e.textContent),
            })"""
        )
        print("MASTHEAD DOM COUNTS")
        for k, v in counts.items():
            print(f"  {k}: {v}")
        browser.close()


if __name__ == "__main__":
    main()
