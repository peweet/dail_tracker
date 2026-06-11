"""Verify chart removal: payments (no Vega), attendance month grid, votes-by-year rows."""

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
    time.sleep(15)

    page.locator("#mo-section-payments").scroll_into_view_if_needed()
    time.sleep(1.2)
    page.screenshot(path=str(OUT / "nc_payments.png"))

    page.locator(".att-cal-strip").first.scroll_into_view_if_needed()
    time.sleep(1.2)
    page.screenshot(path=str(OUT / "nc_attendance.png"))

    page.locator(".vote-year-legend").first.scroll_into_view_if_needed()
    time.sleep(1.2)
    page.screenshot(path=str(OUT / "nc_votes.png"))

    n_vega = page.locator('[data-testid="stVegaLiteChart"], .vega-embed').count()
    n_plotly = page.locator(".js-plotly-plot").count()
    print(f"vega charts on page: {n_vega} | plotly charts: {n_plotly}")
    ctx.close()
    b.close()
print("DONE")
