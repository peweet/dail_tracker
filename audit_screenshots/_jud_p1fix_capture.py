"""Capture the two P1 fixes: waiting-times card rendering + Legal Diary search."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
URL = "http://localhost:8533/rankings-judiciary"


def _wait(pg, ms=2500):
    try:
        pg.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    try:
        pg.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=12000,
        )
    except Exception:
        pass
    pg.wait_for_timeout(ms)


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width": 1440, "height": 940}, device_scale_factor=1).new_page()
    pg.goto(URL, wait_until="domcontentloaded")
    _wait(pg, 3500)

    # 1. Courts -> waiting times cards
    pg.get_by_role("tab", name="The Courts", exact=False).first.click()
    _wait(pg, 2200)
    pg.get_by_text("Waiting times", exact=False).first.scroll_into_view_if_needed()
    pg.wait_for_timeout(800)
    pg.screenshot(path=str(OUT / "_jp1_01_waiting.png"))
    print("saved _jp1_01_waiting.png")

    # 2. Legal Diary -> search for a company
    pg.get_by_role("tab", name="Legal Diary", exact=False).first.click()
    _wait(pg, 2200)
    pg.get_by_text("Every listed matter", exact=False).first.scroll_into_view_if_needed()
    pg.wait_for_timeout(600)
    box = pg.get_by_placeholder("A company, a State body", exact=False).first
    box.click()
    box.fill("Pepper")
    box.press("Enter")
    _wait(pg, 2200)
    pg.get_by_text("Every listed matter", exact=False).first.scroll_into_view_if_needed()
    pg.wait_for_timeout(800)
    pg.screenshot(path=str(OUT / "_jp1_02_search.png"))
    print("saved _jp1_02_search.png")
    b.close()
print("DONE")
