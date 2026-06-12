"""Measure when .site-banner appears after a cold full-page load."""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        for path in ("/rankings-votes", "/rankings-lobbying", "/member-overview"):
            ctx = browser.new_context(viewport={"width": 1440, "height": 900})
            page = ctx.new_page()
            t0 = time.monotonic()
            page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
            t_dom = time.monotonic() - t0
            try:
                page.wait_for_selector(".site-banner", timeout=30000)
                t_banner = time.monotonic() - t0
            except Exception:
                t_banner = -1.0
            try:
                page.wait_for_selector('[data-testid="stTopNavLink"]', timeout=30000)
                t_nav = time.monotonic() - t0
            except Exception:
                t_nav = -1.0
            try:
                page.wait_for_selector('[data-testid="stMainBlockContainer"] h1', timeout=30000)
                t_h1 = time.monotonic() - t0
            except Exception:
                t_h1 = -1.0
            print(f"{path}: dom={t_dom:.1f}s banner={t_banner:.1f}s nav={t_nav:.1f}s h1={t_h1:.1f}s")
            ctx.close()
        browser.close()


if __name__ == "__main__":
    main()
