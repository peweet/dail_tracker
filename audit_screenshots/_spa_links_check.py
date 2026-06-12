"""Verify in-page ?param tile links soft-rerun instead of full-reloading.

Sets a sentinel on the window object before clicking; a full browser
navigation wipes it, a websocket soft rerun keeps it. Exercises:
 1. SI index card  (a.si-card-link, href="?si=…")  -> detail view
 2. Election 2024 tab chip (a.e24-tab, href="?view=…")
 3. Corporate year spark-bar (href="?spark=…") + active-filter clear chip

Run with the dev server already running at localhost:8631.
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"


def check_click(page, label: str, selector: str, expect_param: str) -> None:
    page.evaluate("() => { window.__dt_reload_sentinel = true; }")
    loc = page.locator(selector).first
    loc.scroll_into_view_if_needed(timeout=10000)
    loc.click(timeout=10000)
    time.sleep(6)
    survived = page.evaluate("() => window.__dt_reload_sentinel === true")
    url = page.url
    ok_param = expect_param in url
    verdict = "SOFT (no reload)" if survived else "FULL RELOAD"
    print(f"[{label}] click {selector!r}")
    print(f"    url after: {url}")
    print(f"    param {expect_param!r} in url: {ok_param} | {verdict}")
    if not survived or not ok_param:
        print("    >>> FAIL")
    else:
        print("    >>> PASS")


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()

        # ── 1. Statutory Instruments: card -> detail ──
        page.goto(
            f"{BASE}/rankings-statutory-instruments",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        time.sleep(12)
        n_iframe = page.evaluate(
            """() => document.querySelectorAll('iframe[title*="dt_spa_links"]').length"""
        )
        print(f"spa_links component iframes mounted: {n_iframe}")
        check_click(page, "SI card", "a.si-card-link", "si=")
        has_detail = page.locator("text=Back to SI Index").count() > 0
        print(f"    SI detail rendered (back button present): {has_detail}")

        # ── 2. Election 2024: tab chip ──
        page.goto(
            f"{BASE}/rankings-election-spending",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        time.sleep(12)
        check_click(page, "E24 tab", "a.e24-tab:not(.active)", "view=")

        # ── 3. Corporate: spark-bar year filter, then clear chip ──
        page.goto(f"{BASE}/rankings-corporate", wait_until="domcontentloaded", timeout=60000)
        time.sleep(12)
        check_click(page, "Corp spark", 'a[href^="?spark="]', "")
        # the ?spark param is drained server-side; the active-filter bar appears
        n_chips = page.locator("a.corp-active-chip").count()
        print(f"    active-filter chips after spark click: {n_chips}")
        if n_chips:
            check_click(page, "Corp clear chip", "a.corp-active-chip", "")
            time.sleep(2)
            n_after = page.locator("a.corp-active-chip").count()
            print(f"    chips after clearing: {n_after}")

        browser.close()


if __name__ == "__main__":
    main()
