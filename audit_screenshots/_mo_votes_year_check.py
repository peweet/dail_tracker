"""Verify the Votes-by-year chart now renders on the member profile."""

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
    page.locator("#mo-section-votes").scroll_into_view_if_needed()
    time.sleep(1.5)
    page.screenshot(path=str(OUT / "focus_votes_by_year.png"))
    body = page.locator('[data-testid="stApp"]').inner_text()
    print("'Votes by year' present:", "Votes by year" in body)
    print("'Coming soon' present:", "Coming soon" in body)
    ctx.close()
    b.close()
print("DONE")
