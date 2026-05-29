"""Verify sidebar→filter-bar rollout: migrated pages have no sidebar, an
unmigrated page still does, and no page throws an exception."""
from __future__ import annotations
import time
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8599"
ROUTES = {
    "legislation (migrated)": "rankings-legislation",
    "statutory-instruments (migrated)": "rankings-statutory-instruments",
    "votes (unmigrated)": "rankings-votes",
}


def _settle(page):
    time.sleep(3)
    try:
        ar = page.get_by_role("button", name="Always rerun")
        if ar.is_visible(timeout=1200):
            ar.click(); time.sleep(2.0)
    except Exception:
        pass
    page.mouse.move(0, 0)
    time.sleep(0.8)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for label, slug in ROUTES.items():
            ctx = browser.new_context(viewport={"width": 1440, "height": 900})
            page = ctx.new_page()
            page.goto(f"{BASE}/{slug}", wait_until="networkidle", timeout=30000)
            _settle(page)
            res = page.evaluate(
                """() => {
                    const sb = document.querySelector('[data-testid="stSidebar"]');
                    const vis = sb ? (getComputedStyle(sb).display !== 'none' && sb.getBoundingClientRect().width > 5) : false;
                    return {
                        sidebarVisible: vis,
                        filterbar: document.querySelectorAll('.dt-filterbar-marker').length,
                        exception: document.querySelectorAll('[data-testid="stException"]').length,
                        h1: (document.querySelector('h1')||{}).innerText || null,
                    };
                }"""
            )
            print(f"{label:36s} -> sidebar={res['sidebarVisible']!s:5s} "
                  f"filterbar={res['filterbar']} exception={res['exception']} h1={res['h1']!r}")
            ctx.close()
        browser.close()


if __name__ == "__main__":
    main()
