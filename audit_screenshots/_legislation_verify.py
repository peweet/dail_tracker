"""Focused verification capture for the 2026-05-26 legislation fixes.

Targets the surfaces that changed:
  - V01 desktop index (hero kicker, no PIPELINE TODO callout, sponsor
    em-dash drop on enacted)
  - V02 mobile index (pipeline strip stacks vertically — was clipping
    "525 Enacted")
  - V03 bill detail above-fold (long-title spacing, kicker)
  - V04 bill detail documents (sentence-case labels)
  - V05 bill detail SI section (EU badge uses signal-eu class)
  - V06 mobile bill detail (stat strip 2x2 grid — was clipping Method)

Output: audit_screenshots/verify_legislation/V*.png.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-legislation"
OUT = Path(__file__).resolve().parent / "verify_legislation"
OUT.mkdir(parents=True, exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
MOBILE  = {"width": 390,  "height": 844}


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)

        # --- Desktop ---
        ctx = browser.new_context(viewport=DESKTOP)
        page = ctx.new_page()
        page.goto(f"{BASE}{ROUTE}", wait_until="domcontentloaded")
        time.sleep(6)
        page.screenshot(path=str(OUT / "V01_index_desktop.png"), full_page=False)
        print("  -> V01_index_desktop.png (hero kicker + no PIPELINE TODO)")
        # Switch to Enacted phase to verify sponsor em-dash drop
        try:
            page.get_by_role("button", name=re.compile(r"^Enacted")).first.click()
            time.sleep(2.5)
            page.mouse.wheel(0, 700); time.sleep(0.6)
            page.screenshot(path=str(OUT / "V02_enacted_no_emdash.png"), full_page=False)
            print("  -> V02_enacted_no_emdash.png (sponsor `—` dropped)")
        except Exception as e:
            print(f"     enacted phase failed: {e}")

        # Bill detail — first bill on index
        page.goto(f"{BASE}{ROUTE}", wait_until="domcontentloaded")
        time.sleep(5)
        bill_links = page.locator('a[href*="bill="]')
        if bill_links.count() > 0:
            try:
                bill_links.first.click()
                time.sleep(3)
                page.screenshot(path=str(OUT / "V03_bill_detail_above_fold.png"), full_page=False)
                print("  -> V03_bill_detail_above_fold.png (no PIPELINE TODO)")
                page.mouse.wheel(0, 1400); time.sleep(0.6)
                page.screenshot(path=str(OUT / "V04_documents_sentence_case.png"), full_page=False)
                print("  -> V04_documents_sentence_case.png (sentence-case labels)")
            except Exception as e:
                print(f"     bill detail click failed: {e}")
        ctx.close()

        # --- Mobile ---
        ctx = browser.new_context(viewport=MOBILE)
        page = ctx.new_page()
        page.goto(f"{BASE}{ROUTE}", wait_until="domcontentloaded")
        time.sleep(6)
        page.screenshot(path=str(OUT / "V05_mobile_index_pipeline_stacks.png"))
        print("  -> V05_mobile_index_pipeline_stacks.png (no clip)")

        # Click into a bill on mobile to check stat strip wrap
        bill_links = page.locator('a[href*="bill="]')
        if bill_links.count() > 0:
            try:
                bill_links.first.click()
                time.sleep(3)
                page.screenshot(path=str(OUT / "V06_mobile_bill_detail_strip.png"))
                print("  -> V06_mobile_bill_detail_strip.png (stat strip 2x2)")
            except Exception as e:
                print(f"     mobile bill detail click failed: {e}")
        ctx.close()
        browser.close()

    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
