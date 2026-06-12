"""Does navigating via the top nav preserve scroll position (landing mid-page)?

Scroll deep on one page, click a top-nav link, and measure where the next
page renders: scrollTop of the main scroll container + whether the page hero
(h1) is visible in the viewport.
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"


def scroll_state(page) -> dict:
    return page.evaluate(
        """() => {
            const cands = [
                document.querySelector('[data-testid="stMain"]'),
                document.querySelector('section.main'),
                document.scrollingElement,
            ].filter(Boolean);
            const tops = cands.map(el => ({tag: el.tagName + '.' + (el.getAttribute('data-testid')||''), top: el.scrollTop}));
            const h1 = document.querySelector('[data-testid="stMainBlockContainer"] h1');
            const r = h1 ? h1.getBoundingClientRect() : null;
            return {tops, h1Visible: r ? (r.top >= 0 && r.top < window.innerHeight) : null,
                    h1Top: r ? r.top : null, winY: window.scrollY};
        }"""
    )


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        page.goto(f"{BASE}/rankings-votes", wait_until="domcontentloaded", timeout=60000)
        time.sleep(9)
        page.evaluate(
            """() => {
                for (const el of [document.querySelector('[data-testid="stMain"]'),
                                  document.scrollingElement]) {
                    if (el) el.scrollTop = 2500;
                }
                window.scrollTo(0, 2500);
            }"""
        )
        time.sleep(1)
        print("on votes after deep scroll:", scroll_state(page))

        # list what the top nav actually exposes, then click another page link
        labels = page.evaluate(
            """() => [...document.querySelectorAll('[data-testid="stTopNavLink"], header a, header button')]
                .map(e => (e.textContent || '').trim()).filter(Boolean)"""
        )
        print("nav labels:", labels)
        target = page.get_by_test_id("stTopNavLink").filter(has_text="Attendance").first
        target.click(timeout=10000)
        time.sleep(6)
        print("after nav -> interests:", scroll_state(page))
        print("url:", page.url)

        browser.close()


if __name__ == "__main__":
    main()
