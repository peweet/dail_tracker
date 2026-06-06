"""Legal Diary capture — confirm every court appears in the case listing."""

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


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width": 1440, "height": 940}, device_scale_factor=1).new_page()
    pg.goto(URL, wait_until="domcontentloaded")
    _wait(pg, 3500)
    pg.get_by_role("tab", name="Legal Diary", exact=False).first.click()
    _wait(pg, 2500)
    # scroll to the case listing so the court sub-headers are visible
    pg.get_by_text("Every listed matter", exact=False).first.scroll_into_view_if_needed()
    pg.wait_for_timeout(800)
    pg.screenshot(path=str(OUT / "_jud_ld_03_courts.png"))
    print("saved _jud_ld_03_courts.png")
    # full page to verify all court headers present in the DOM
    body = pg.content()
    for court in ["Supreme Court", "Court of Appeal", "High Court", "Central Criminal Court"]:
        print(f"  has '{court}':", court in body)
    b.close()
print("DONE")
