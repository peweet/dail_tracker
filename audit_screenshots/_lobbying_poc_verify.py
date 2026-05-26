"""Focused verification capture for the 2026-05-26 Lobbying-PoC fixes.

Targets the surfaces that changed:
  - V01 landing: hero date format ("Sep 2015 to May 2026" not "2015-09"),
    clickable gateway + topic tiles
  - V02 org detail Ibec: friendly period in hero ("Sep 2015 to Sep 2025"),
    pluralised pills, clean switcher (no inline style)
  - V03 RD index: "1 firm" singular pluralisation
  - V04 DPO individual: firms + clients tables render (data correct),
    pluralised pills
  - V05 area detail Health: "2,213 politicians" with comma

Output: audit_screenshots/verify_lobbying_poc/V*.png.
"""
from __future__ import annotations

import time
import urllib.parse
from pathlib import Path

from playwright.sync_api import sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-lobbying-poc"
OUT = Path(__file__).resolve().parent / "verify_lobbying_poc"
OUT.mkdir(parents=True, exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
MOBILE  = {"width": 390,  "height": 844}


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP)
        page = ctx.new_page()

        # V01: landing
        page.goto(f"{BASE}{ROUTE}", wait_until="domcontentloaded")
        time.sleep(7)
        page.screenshot(path=str(OUT / "V01_landing_desktop.png"), full_page=False)
        print("  -> V01_landing_desktop.png (date format + clickable tiles)")

        # V02: org detail Ibec
        page.goto(f"{BASE}{ROUTE}?lp3_org={urllib.parse.quote('Ibec')}", wait_until="domcontentloaded")
        time.sleep(6)
        page.screenshot(path=str(OUT / "V02_org_detail_ibec.png"), full_page=False)
        print("  -> V02_org_detail_ibec.png (friendly period + plurals)")

        # V03: RD index
        page.goto(f"{BASE}{ROUTE}?lp3_rd=1", wait_until="domcontentloaded")
        time.sleep(6)
        page.mouse.wheel(0, 600); time.sleep(0.6)
        page.screenshot(path=str(OUT / "V03_rd_index_plurals.png"), full_page=False)
        print("  -> V03_rd_index_plurals.png (1 firm singular)")

        # V04: DPO individual Lorraine Higgins
        page.goto(f"{BASE}{ROUTE}?lp3_dpo={urllib.parse.quote('Lorraine Higgins')}", wait_until="domcontentloaded")
        time.sleep(6)
        page.screenshot(path=str(OUT / "V04_dpo_above_fold.png"), full_page=False)
        print("  -> V04_dpo_above_fold.png")
        page.mouse.wheel(0, 700); time.sleep(0.6)
        page.screenshot(path=str(OUT / "V05_dpo_firms_clients.png"), full_page=False)
        print("  -> V05_dpo_firms_clients.png (datasette tables not empty)")

        # V06: area detail Health
        page.goto(f"{BASE}{ROUTE}?lp3_area=Health", wait_until="domcontentloaded")
        time.sleep(6)
        page.screenshot(path=str(OUT / "V06_area_health.png"), full_page=False)
        print("  -> V06_area_health.png (2,213 with comma)")

        ctx.close()
        browser.close()

    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
