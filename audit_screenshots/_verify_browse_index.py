"""Verify the new lobbying politician + policy-area index views."""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "verify_browse_index"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
COLD = 12000
WARM = 4000


def shot(page: Page, name: str, *, full_page: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    print(f"  -> {path.name} ({path.stat().st_size // 1024} KB)")


def goto(page: Page, path: str, *, cold: bool = False) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    time.sleep((COLD if cold else WARM) / 1000)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()

        # Politician index — deep link
        goto(page, "/rankings-lobbying?lob_polindex=1", cold=True)
        shot(page, "01_pol_index_above_fold")
        page.evaluate("window.scrollTo(0, 600)")
        time.sleep(0.4)
        shot(page, "02_pol_index_cards")

        # Policy-area index — deep link
        goto(page, "/rankings-lobbying?lob_areaindex=1")
        shot(page, "03_area_index_above_fold")
        page.evaluate("window.scrollTo(0, 600)")
        time.sleep(0.4)
        shot(page, "04_area_index_cards")

        ctx.close()
        browser.close()
    print(f"\nDONE. Shots in {OUT}")


if __name__ == "__main__":
    main()
