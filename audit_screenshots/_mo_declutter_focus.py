"""Focused captures: payments chart default year + debates speech cards (open/closed details)."""

from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent / "_mo_declutter"
BASE = "http://localhost:8534"
CODE = "Michael-Healy-Rae.D.2011-03-09"

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context(viewport={"width": 1440, "height": 1300})
    page = ctx.new_page()
    page.set_default_timeout(30000)
    page.goto(f"{BASE}/member-overview?member={quote(CODE, safe='')}", wait_until="domcontentloaded")
    time.sleep(14)

    # Payments: scroll the year pills into view
    page.locator("#mo-section-payments").scroll_into_view_if_needed()
    time.sleep(1.2)
    page.screenshot(path=str(OUT / "focus_payments_top.png"))

    # Debates: first speech card
    card = page.locator(".mo-speech-card").first
    if card.count():
        card.scroll_into_view_if_needed()
        time.sleep(1.0)
        page.screenshot(path=str(OUT / "focus_debates_cards.png"))
        # open the first details
        summ = page.locator(".mo-speech-card details > summary").first
        if summ.count():
            summ.click()
            time.sleep(0.6)
            page.screenshot(path=str(OUT / "focus_debates_open.png"))
    else:
        print("no speech card found")

    ctx.close()
    b.close()
print("DONE")
