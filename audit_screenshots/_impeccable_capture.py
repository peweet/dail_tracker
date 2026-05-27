"""Impeccable audit — uniform desktop + mobile capture for all 11 pages.

One screenshot at 1440x900 and one at 390x844 per page. Used to score the
impeccable 5-dimension scorecard (A11y, Perf, Theming, Responsive, AP)
against what the rendered UI actually looks like, not just the code.

Run with the Streamlit app already up on http://localhost:8501.
"""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "_impeccable"
OUT.mkdir(parents=True, exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
MOBILE = {"width": 390, "height": 844}

PAGES = [
    ("01_member_overview", "/member-overview"),
    ("02_attendance",      "/rankings-attendance"),
    ("03_votes",           "/rankings-votes"),
    ("04_interests",       "/rankings-interests"),
    ("05_payments",        "/rankings-payments"),
    ("06_lobbying",        "/rankings-lobbying"),
    ("07_lobbying_poc",    "/rankings-lobbying-poc"),
    ("08_legislation",     "/rankings-legislation"),
    ("09_statutory_instr", "/rankings-statutory-instruments"),
    ("10_committees",      "/rankings-committees"),
    ("11_glossary",        "/glossary"),
]

PAGE_LOAD_WAIT = 7.0      # seconds — heavy SQL views
RERUN_WAIT = 2.0


def dismiss_modal(page: Page) -> None:
    try:
        modal = page.locator('div[role="dialog"]').filter(has_text="Page not found")
        if modal.count() > 0:
            btn = modal.locator('button[aria-label="Close"]')
            if btn.count() > 0:
                btn.first.click(force=True)
                time.sleep(0.8)
    except Exception:
        pass


def capture(page: Page, slug: str, path: str, viewport: dict, suffix: str) -> None:
    page.set_viewport_size(viewport)
    print(f"  > {slug} @ {viewport['width']}x{viewport['height']}  {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    time.sleep(PAGE_LOAD_WAIT)
    dismiss_modal(page)
    time.sleep(RERUN_WAIT)
    out = OUT / f"{slug}_{suffix}.png"
    page.screenshot(path=str(out), full_page=True)
    kb = out.stat().st_size // 1024
    print(f"     -> {out.name} ({kb} KB)")


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        try:
            for slug, path in PAGES:
                ctx = browser.new_context(viewport=DESKTOP)
                page = ctx.new_page()
                try:
                    capture(page, slug, path, DESKTOP, "desktop")
                    capture(page, slug, path, MOBILE, "mobile")
                except Exception as e:
                    print(f"     !! {slug} failed: {e}")
                finally:
                    ctx.close()
        finally:
            browser.close()
    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
