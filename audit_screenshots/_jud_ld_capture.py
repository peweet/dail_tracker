"""Legal Diary plaintiff-rework capture — scroll the plaintiff section into view."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
URL = "http://localhost:8533/rankings-judiciary"


def _wait(page, ms=2500):
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    try:
        page.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=12000,
        )
    except Exception:
        pass
    page.wait_for_timeout(ms)


def _shoot(pg, name):
    pg.screenshot(path=str(OUT / name))
    print("saved", name)


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width": 1440, "height": 940}, device_scale_factor=1).new_page()
    pg.goto(URL, wait_until="domcontentloaded")
    _wait(pg, 3500)
    pg.get_by_role("tab", name="Legal Diary", exact=False).first.click()
    _wait(pg, 2500)

    # 1. "Who's bringing these cases" section
    try:
        pg.get_by_text("Who's bringing these cases", exact=False).first.scroll_into_view_if_needed()
        pg.wait_for_timeout(700)
        _shoot(pg, "_jud_ld_01_plaintiffs.png")
    except Exception as e:
        print("plaintiffs scroll skip:", e)

    # 2. open the first case expander, scroll it in, show reworked rows
    try:
        pg.get_by_text("Every listed matter", exact=False).first.scroll_into_view_if_needed()
        pg.wait_for_timeout(500)
        exp = pg.locator('[data-testid="stExpander"] summary').first
        exp.click()
        _wait(pg, 1200)
        exp.scroll_into_view_if_needed()
        pg.wait_for_timeout(700)
        _shoot(pg, "_jud_ld_02_rows.png")
    except Exception as e:
        print("expander skip:", e)
    b.close()
print("DONE")
