"""Dossier-only capture (server flaked after 2 shots on the first run)."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8534"

with sync_playwright() as p:
    b = p.chromium.launch(headless=True)
    ctx = b.new_context(viewport={"width": 1440, "height": 1600}, device_scale_factor=1)
    pg = ctx.new_page()
    pg.goto(f"{BASE}/company?supplier=DELOITTE%20LLP", wait_until="domcontentloaded")
    try:
        pg.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    pg.wait_for_timeout(4000)
    pg.screenshot(path=str(OUT / "rv_company_dossier.png"))
    print("saved rv_company_dossier")
    b.close()
print("DONE")
