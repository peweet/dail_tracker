"""Verify the Page-not-found modal is gone after moving default=True."""
from __future__ import annotations
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8511"
OUT = Path(__file__).resolve().parent
WAIT = 5

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()

    # Fresh load — verify no modal
    page.goto(f"{BASE}/rankings-attendance", wait_until="domcontentloaded")
    time.sleep(WAIT)
    page.screenshot(path=str(OUT / "v3_20_att_landing_after_modal_fix.png"), full_page=False)
    print("captured: v3_20_att_landing_after_modal_fix.png")

    # Legacy redirect — should be clean callout, no modal
    page.goto(f"{BASE}/rankings-attendance?att_td=Mary Lou McDonald", wait_until="domcontentloaded")
    time.sleep(WAIT)
    page.screenshot(path=str(OUT / "v3_21_att_legacy_redirect_after_modal_fix.png"), full_page=False)
    print("captured: v3_21_att_legacy_redirect_after_modal_fix.png")

    # Root URL should now route to member-overview (the new default)
    page.goto(f"{BASE}/", wait_until="domcontentloaded")
    time.sleep(WAIT)
    page.screenshot(path=str(OUT / "v3_root_now_member_overview.png"), full_page=False)
    print("captured: v3_root_now_member_overview.png")

    ctx.close()
    browser.close()
print("done")
