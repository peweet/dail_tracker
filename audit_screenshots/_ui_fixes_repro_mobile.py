"""Mobile-viewport repro: banner/nav visibility + search clear on a phone."""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"


def state(page) -> dict:
    return page.evaluate(
        """() => {
            const out = {};
            const b = document.querySelector('.site-banner');
            out.banner = b ? {y: b.getBoundingClientRect().top, h: b.getBoundingClientRect().height,
                              vis: getComputedStyle(b).visibility} : null;
            const h = document.querySelector('header[data-testid="stHeader"]');
            out.header = h ? {y: h.getBoundingClientRect().top, h: h.getBoundingClientRect().height} : null;
            const nav = [...document.querySelectorAll('[data-testid="stTopNavLink"]')];
            out.navLinks = nav.length;
            out.navVisible = nav.filter(a => a.getBoundingClientRect().width > 0).length;
            // any overflow/More menu button in the header?
            const btns = h ? [...h.querySelectorAll('button')] : [];
            out.headerButtons = btns.map(x => ({
                label: (x.getAttribute('aria-label') || x.textContent || '').trim().slice(0, 30),
                visible: x.getBoundingClientRect().width > 0 && getComputedStyle(x).display !== 'none',
            }));
            const h1 = document.querySelector('[data-testid="stMainBlockContainer"] h1');
            out.h1 = h1 ? {y: h1.getBoundingClientRect().top, text: h1.textContent.slice(0, 30)} : null;
            // masthead bottom edge (header bottom)
            if (h) out.mastheadBottom = h.getBoundingClientRect().bottom;
            out.scrollY = window.scrollY;
            return out;
        }"""
    )


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 390, "height": 844}, is_mobile=True, has_touch=True)
        page = ctx.new_page()

        page.goto(f"{BASE}/member-overview", wait_until="domcontentloaded", timeout=60000)
        time.sleep(8)
        print("MOBILE initial:", state(page))
        page.screenshot(path="audit_screenshots/_mobile_top.png")

        page.reload(wait_until="domcontentloaded")
        time.sleep(6)
        print("MOBILE after reload:", state(page))

        # scroll down then trigger a rerun via pill click — does the masthead stay?
        page.evaluate("() => { const m = document.querySelector('[data-testid=\"stMain\"]'); if (m) m.scrollTo(0, 800); window.scrollTo(0, 800); }")
        time.sleep(1)
        print("MOBILE scrolled:", state(page))
        page.screenshot(path="audit_screenshots/_mobile_scrolled.png")

        browser.close()


if __name__ == "__main__":
    main()
