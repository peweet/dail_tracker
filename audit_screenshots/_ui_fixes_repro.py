"""Reproduce two reported issues against a fresh dev server.

1. "missed the main header each time it reloads" — is .site-banner present
   after initial load, after browser reload, and after a widget interaction?
   Also: does the page's first heading hide beneath the fixed masthead?
2. "search filter does nothing when text deleted" — on /member-overview
   browse, type into the Find-a-TD search box, commit, read the result
   count; then clear the text and read the count again.

Run with the dev server already running at localhost:8631.
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"


def banner_state(page) -> dict:
    return page.evaluate(
        """() => {
            const b = document.querySelector('.site-banner');
            const h = document.querySelector('header[data-testid="stHeader"]');
            const h1 = document.querySelector('.main h1, [data-testid="stMainBlockContainer"] h1');
            const out = {banner: !!b, header: !!h};
            if (b) { const r = b.getBoundingClientRect(); out.bannerRect = {y: r.top, h: r.height, vis: getComputedStyle(b).visibility, disp: getComputedStyle(b).display}; }
            if (h) { const r = h.getBoundingClientRect(); out.headerRect = {y: r.top, h: r.height}; }
            if (h1) { const r = h1.getBoundingClientRect(); out.h1Rect = {y: r.top, text: h1.textContent.slice(0, 40)}; }
            out.scrollY = window.scrollY;
            return out;
        }"""
    )


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        # ── 1. header presence across loads ──
        page.goto(f"{BASE}/member-overview", wait_until="domcontentloaded", timeout=60000)
        time.sleep(8)
        print("A initial load:", banner_state(page))

        page.reload(wait_until="domcontentloaded")
        time.sleep(2)
        print("B 2s after reload:", banner_state(page))
        time.sleep(6)
        print("C 8s after reload:", banner_state(page))

        # widget interaction → rerun
        try:
            pills = page.locator('[data-testid="stPills"] button')
            if pills.count() > 0:
                pills.nth(1).click()
                time.sleep(3)
                print("D after pill click:", banner_state(page))
        except Exception as e:  # noqa: BLE001
            print("D pill click failed:", e)

        # ── 2. search filter clear behaviour ──
        def result_count() -> str:
            try:
                return page.evaluate(
                    """() => {
                        const els = [...document.querySelectorAll('h2, h3, .dt-evidence-heading, [class*="evidence"]')];
                        const m = els.map(e => e.textContent).find(t => /TDs?$/.test((t||'').trim()));
                        return m || '(none)';
                    }"""
                )
            except Exception as e:  # noqa: BLE001
                return f"(err {e})"

        page.goto(f"{BASE}/member-overview", wait_until="domcontentloaded")
        time.sleep(8)
        print("E baseline count:", result_count())

        box = page.locator('input[aria-label="Find a TD"]').first
        try:
            box.wait_for(timeout=5000)
        except Exception:
            # fall back: first text input on page
            box = page.locator('[data-testid="stTextInput"] input').first
        box.click()
        box.fill("healy")
        box.press("Enter")
        time.sleep(3)
        print("F after typing 'healy':", result_count(), "| box value:", box.input_value())

        # highlight-all and delete, then commit with Enter
        box.click()
        box.press("Control+a")
        box.press("Delete")
        box.press("Enter")
        time.sleep(3)
        print("G after clear+Enter:", result_count(), "| box value:", box.input_value())

        # clear WITHOUT Enter — click elsewhere (blur)
        box.click()
        box.fill("healy")
        box.press("Enter")
        time.sleep(2)
        box.click()
        box.press("Control+a")
        box.press("Delete")
        page.locator("body").click(position={"x": 700, "y": 400})
        time.sleep(3)
        print("H after clear+blur:", result_count(), "| box value:", box.input_value())

        page.screenshot(path="audit_screenshots/_ui_fixes_repro.png", full_page=False)
        browser.close()


if __name__ == "__main__":
    main()
