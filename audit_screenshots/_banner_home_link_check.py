"""Verify the masthead brand title is a working home link.

Checks, against a freshly started server:
 1. the <a class="site-banner-title" href="/"> survives st.html sanitization
 2. no other element overlays it (elementFromPoint at its centre)
 3. clicking it from a non-home page navigates back to /

Run with the dev server already running at localhost:8631.
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()

        page.goto(f"{BASE}/rankings-procurement", wait_until="domcontentloaded", timeout=60000)
        time.sleep(10)

        info = page.evaluate(
            """() => {
                const links = document.querySelectorAll('a.site-banner-title');
                const banners = document.querySelectorAll('.site-banner');
                const a = links[0];
                if (!a) return {found: false, bannerCount: banners.length,
                                 bannerHTML: banners[0] ? banners[0].outerHTML.slice(0, 500) : '(none)'};
                const r = a.getBoundingClientRect();
                const cx = r.left + r.width / 2, cy = r.top + r.height / 2;
                const hit = document.elementFromPoint(cx, cy);
                return {
                    found: true,
                    linkCount: links.length,
                    bannerCount: banners.length,
                    href: a.getAttribute('href'),
                    rect: {x: r.left, y: r.top, w: r.width, h: r.height},
                    hitIsLink: hit === a || a.contains(hit),
                    hitDesc: hit ? hit.tagName + '.' + hit.className + ' testid=' + (hit.getAttribute('data-testid') || '') : '(none)',
                    pointerEvents: getComputedStyle(a).pointerEvents,
                    cursor: getComputedStyle(a).cursor,
                };
            }"""
        )
        print("DOM check:", info)

        if info.get("found"):
            url_before = page.url
            page.locator("a.site-banner-title").first.click(timeout=5000)
            time.sleep(4)
            print(f"URL before click: {url_before}")
            print(f"URL after click:  {page.url}")
            print("NAVIGATED HOME" if page.url.rstrip("/") == BASE else "DID NOT NAVIGATE")

        browser.close()


if __name__ == "__main__":
    main()
