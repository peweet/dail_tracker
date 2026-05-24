"""Verify minister stat-strip fix — direct load, no prior navigation."""
from __future__ import annotations
import time
import urllib.parse
from pathlib import Path
from playwright.sync_api import sync_playwright

BASE = "http://localhost:8511"
OUT = Path(__file__).resolve().parent

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(viewport={"width": 1440, "height": 900})
    page = ctx.new_page()

    # Direct load Micheál Martin — fresh context, no chain nav
    encoded = urllib.parse.quote("Micheál-Martin.D.1989-06-29", safe="")
    page.goto(f"{BASE}/member-overview?member={encoded}", wait_until="domcontentloaded")
    time.sleep(5)
    page.screenshot(path=str(OUT / "v5_14_minister_stat_strip_fix.png"), full_page=False)
    print("  -> v5_14_minister_stat_strip_fix.png")

    ctx.close()
    browser.close()
print("done")
