"""Judiciary full-audit capture — every tab + scroll states, profile (spine + no-spine), mobile."""

from __future__ import annotations

from urllib.parse import quote
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8533"
URL = f"{BASE}/rankings-judiciary"
DESK = {"width": 1440, "height": 940}
MOB = {"width": 390, "height": 844}


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


def _shoot(pg, name, full=False):
    pg.screenshot(path=str(OUT / f"_jaudit_{name}.png"), full_page=full)
    print("saved", name)


def _tab(pg, label):
    pg.get_by_role("tab", name=label, exact=False).first.click()
    _wait(pg, 2200)


def _to(pg, text):
    try:
        pg.get_by_text(text, exact=False).first.scroll_into_view_if_needed()
        pg.wait_for_timeout(700)
        return True
    except Exception as e:
        print(f"  scroll '{text}' skip: {e}")
        return False


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport=DESK, device_scale_factor=1).new_page()

    pg.goto(URL, wait_until="domcontentloaded")
    _wait(pg, 3800)
    _shoot(pg, "01_bench_top")

    _tab(pg, "Appointments")
    _to(pg, "How each appointment was recorded")
    _shoot(pg, "02_appt_authority")
    _to(pg, "Vacancy lifecycle")
    _shoot(pg, "03_appt_vacancy")

    _tab(pg, "The Courts")
    _shoot(pg, "04_courts_clearance")
    _to(pg, "Break a court down by area")
    _shoot(pg, "05_courts_area")
    _to(pg, "Clearance over time")
    _shoot(pg, "06_courts_trend")
    _to(pg, "Waiting times")
    _shoot(pg, "07_courts_waiting")
    _to(pg, "Where the courts sit")
    _shoot(pg, "08_courts_map")

    _tab(pg, "Legal Diary")
    _shoot(pg, "09_ld_top")
    _to(pg, "Who's bringing these cases")
    _shoot(pg, "10_ld_plaintiffs")
    _to(pg, "Every listed matter")
    _shoot(pg, "11_ld_cases_courts")

    # profiles
    pg.goto(f"{URL}?judge={quote('aileen donnelly')}", wait_until="domcontentloaded")
    _wait(pg, 3000)
    _shoot(pg, "12_profile_spine")
    pg.goto(f"{URL}?judge={quote('donal donnell')}", wait_until="domcontentloaded")
    _wait(pg, 3000)
    _shoot(pg, "13_profile_nospine")

    # mobile
    pg.set_viewport_size(MOB)
    pg.goto(URL, wait_until="domcontentloaded")
    _wait(pg, 3500)
    _shoot(pg, "14_mobile_bench")
    _tab(pg, "Legal Diary")
    _to(pg, "Who's bringing these cases")
    _shoot(pg, "15_mobile_ld")

    b.close()
print("DONE")
