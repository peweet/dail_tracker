"""Verify the 2026-06-11 persona-driven judiciary uplift on a FRESH server (:8631):
profile 'Before the court' diary section, cross-day 'Who is suing' league,
court-grouped waiting times, and status chips on case rows."""

from __future__ import annotations

import urllib.parse
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8631/rankings-judiciary"


def _wait(pg, ms=2500):
    try:
        pg.wait_for_load_state("networkidle", timeout=20000)
    except Exception:
        pass
    try:
        pg.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=15000,
        )
    except Exception:
        pass
    pg.wait_for_timeout(ms)


with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    pg = b.new_context(viewport={"width": 1440, "height": 1000}, device_scale_factor=1).new_page()

    # 1. judge profile with diary listings — "Before the court"
    pg.goto(f"{BASE}?judge={urllib.parse.quote('mark heslin')}", wait_until="domcontentloaded")
    _wait(pg, 4000)
    found = pg.get_by_text("Before the court", exact=False).count()
    print(f"profile 'Before the court' present: {found > 0}")
    if found:
        pg.get_by_text("Before the court", exact=False).first.scroll_into_view_if_needed()
        pg.wait_for_timeout(700)
    pg.screenshot(path=str(OUT / "_jpv_01_profile_diary.png"))
    print("saved _jpv_01_profile_diary.png")

    # 2. Legal Diary -> cross-day plaintiff league
    pg.goto(BASE, wait_until="domcontentloaded")
    _wait(pg, 4000)
    pg.get_by_role("tab", name="Legal Diary", exact=False).first.click()
    _wait(pg, 2500)
    league = pg.get_by_text("Who is suing — across all captured days", exact=False)
    print(f"league section present: {league.count() > 0}")
    if league.count():
        league.first.scroll_into_view_if_needed()
        pg.wait_for_timeout(700)
    pg.screenshot(path=str(OUT / "_jpv_02_league.png"))
    print("saved _jpv_02_league.png")

    # 2b. league court filter
    try:
        pg.get_by_role("button", name="High Court", exact=True).last.click()
        _wait(pg, 1800)
        league.first.scroll_into_view_if_needed()
        pg.wait_for_timeout(600)
        pg.screenshot(path=str(OUT / "_jpv_03_league_hc.png"))
        print("saved _jpv_03_league_hc.png")
    except Exception as e:  # noqa: BLE001
        print(f"league pill click skipped: {e}")

    # 3. case rows with status chips — search auto-expands sittings
    pg.get_by_text("Every listed matter", exact=False).first.scroll_into_view_if_needed()
    box = pg.get_by_placeholder("A company, a State body", exact=False).first
    box.click()
    box.fill("Pepper")
    box.press("Enter")
    _wait(pg, 2200)
    pg.get_by_text("Every listed matter", exact=False).first.scroll_into_view_if_needed()
    pg.wait_for_timeout(700)
    pg.screenshot(path=str(OUT / "_jpv_04_status_chips.png"))
    print("saved _jpv_04_status_chips.png")

    # 4. The Courts -> grouped waiting times
    pg.goto(BASE, wait_until="domcontentloaded")
    _wait(pg, 4000)
    pg.get_by_role("tab", name="The Courts", exact=False).first.click()
    _wait(pg, 2500)
    wt = pg.get_by_text("Waiting times — what to expect", exact=False)
    print(f"grouped waiting present: {wt.count() > 0}")
    if wt.count():
        wt.first.scroll_into_view_if_needed()
        pg.wait_for_timeout(700)
    pg.screenshot(path=str(OUT / "_jpv_05_waiting_grouped.png"))
    print("saved _jpv_05_waiting_grouped.png")
    ccc = pg.get_by_text("Central Criminal Court", exact=False)
    if ccc.count():
        ccc.first.scroll_into_view_if_needed()
        pg.wait_for_timeout(700)
        pg.screenshot(path=str(OUT / "_jpv_06_waiting_ccc.png"))
        print("saved _jpv_06_waiting_ccc.png")

    b.close()
print("DONE")
