from __future__ import annotations
from pathlib import Path
from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8533"
URL = f"{BASE}/rankings-judiciary"


def _wait(page, ms=2500):
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    page.wait_for_timeout(ms)


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width": 1440, "height": 900}).new_page()

    # bench, district court (many judges, all same lower salary band) scrolled
    pg.goto(URL, wait_until="domcontentloaded")
    _wait(pg, 4000)
    pg.evaluate("window.scrollTo(0, 1200)")
    pg.wait_for_timeout(800)
    pg.screenshot(path=str(OUT / "_jud_07_bench_scroll.png"))
    print("saved 07")

    # appointments tab scrolled to ladder + vacancy lifecycle
    pg.get_by_role("tab", name="Appointments", exact=False).first.click()
    _wait(pg, 2500)
    pg.evaluate("window.scrollTo(0, 850)")
    pg.wait_for_timeout(800)
    pg.screenshot(path=str(OUT / "_jud_08_appt_scroll.png"))
    print("saved 08")
    pg.evaluate("window.scrollTo(0, 1500)")
    pg.wait_for_timeout(800)
    pg.screenshot(path=str(OUT / "_jud_09_appt_scroll2.png"))
    print("saved 09")
    b.close()
print("DONE")
