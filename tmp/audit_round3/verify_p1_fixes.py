"""Verify the P0/P1/P2/P3 fixes visually."""
from __future__ import annotations
import time
import urllib.parse
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8511"
OUT = Path(__file__).resolve().parent
WAIT = 5
MARY_LOU_CODE = "Mary-Lou-McDonald.D.2011-03-09"
MICHEAL_CODE = "Micheál-Martin.D.1989-06-29"

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()

    def shot(name: str, full: bool = False) -> None:
        page.screenshot(path=str(OUT / f"{name}.png"), full_page=full)
        print(f"  -> {name}.png")

    def goto(path: str) -> None:
        print(f"\n  > {path}")
        page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
        time.sleep(WAIT)

    # P0 + P1-B: payments landing (apostrophe + name format + "(unmapped)" + modal)
    goto("/rankings-payments")
    shot("v4_50_payments_landing")

    # P1-A + P1-D: committees register (jargon callout) + committee detail (empty df)
    goto("/rankings-committees")
    shot("v4_70_committees_register")

    # P1-B: legislation bill detail (<p> tag stripping)
    goto("/rankings-legislation")
    time.sleep(2)
    # Click first bill link
    bill_links = page.locator('a[href*="bill="]')
    if bill_links.count() > 0:
        bill_links.first.click()
        time.sleep(WAIT)
        shot("v4_62_legislation_bill_detail")

    # P1-A + P3-2: interests landing (jargon + no emoji pills)
    goto("/rankings-interests")
    shot("v4_40_interests_landing")

    # P1-F + Polish-1: minister profile (no em-dash strip; quieter profile-nav)
    encoded = urllib.parse.quote(MICHEAL_CODE, safe="")
    goto(f"/member-overview?member={encoded}")
    shot("v4_14_minister_profile_after_fix")

    # Regular TD profile — should show profile-nav quieter
    encoded = urllib.parse.quote(MARY_LOU_CODE, safe="")
    goto(f"/member-overview?member={encoded}")
    shot("v4_04_profile_after_polish")

    ctx.close()
    browser.close()
print("done")
