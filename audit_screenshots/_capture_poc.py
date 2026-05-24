"""Capture every PoC view for review."""

from __future__ import annotations

from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent / "poc"
OUT.mkdir(exist_ok=True)
BASE = "http://localhost:8501"
DESKTOP = {"width": 1440, "height": 900}
MOBILE = {"width": 390, "height": 844}


def _settle(page, ms: int = 4000) -> None:
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    try:
        page.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=12000,
        )
    except Exception:
        pass
    page.wait_for_timeout(ms)


def _shoot(page, name: str) -> None:
    out = OUT / f"{name}.png"
    page.screenshot(path=str(out), full_page=True)
    print(f"  saved {out.name} ({out.stat().st_size // 1024} KB)")


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP)
    page = ctx.new_page()

    print("[1] landing desktop")
    page.goto(f"{BASE}/rankings-lobbying-poc", wait_until="domcontentloaded")
    _settle(page, 5000)
    _shoot(page, "01_landing_desktop")

    print("[2] organisation index")
    page.goto(f"{BASE}/rankings-lobbying-poc?lp3_orgindex=1", wait_until="domcontentloaded")
    _settle(page, 4500)
    _shoot(page, "03_org_index")

    print("[3] organisation Stage 2 — Ibec")
    page.goto(f"{BASE}/rankings-lobbying-poc?lp3_org={quote('Ibec')}", wait_until="domcontentloaded")
    _settle(page, 4500)
    _shoot(page, "04_org_detail_ibec")

    print("[4] revolving-door index")
    page.goto(f"{BASE}/rankings-lobbying-poc?lp3_rd=1", wait_until="domcontentloaded")
    _settle(page, 4500)
    _shoot(page, "05_rd_index")

    print("[5] RD individual (first card)")
    try:
        link = page.locator('a[aria-label^="View revolving-door profile for"]').first
        href = link.get_attribute("href")
        if href:
            target = href if href.startswith("http") else f"{BASE}/rankings-lobbying-poc{href}"
            page.goto(target, wait_until="domcontentloaded")
            _settle(page, 4500)
            _shoot(page, "06_rd_individual")
    except Exception as e:
        print(f"  skipped RD individual: {e}")

    print("[6] topic — Climate")
    page.goto(f"{BASE}/rankings-lobbying-poc?lp3_topic={quote('Climate')}", wait_until="domcontentloaded")
    _settle(page, 5000)
    _shoot(page, "07_topic_climate")

    print("[7] area — Housing")
    page.goto(f"{BASE}/rankings-lobbying-poc?lp3_area={quote('Housing')}", wait_until="domcontentloaded")
    _settle(page, 4500)
    _shoot(page, "08_area_housing")

    print("[8] area × politician Stage 3")
    try:
        link = page.locator('a[aria-label^="View every return targeting"]').first
        href = link.get_attribute("href")
        if href:
            target = href if href.startswith("http") else f"{BASE}/rankings-lobbying-poc{href}"
            page.goto(target, wait_until="domcontentloaded")
            _settle(page, 4500)
            _shoot(page, "09_area_politician_results")
    except Exception as e:
        print(f"  skipped area×politician: {e}")

    print("[9] empty-state org (invalid name)")
    page.goto(
        f"{BASE}/rankings-lobbying-poc?lp3_org={quote('THIS_ORG_DOES_NOT_EXIST_XYZ')}",
        wait_until="domcontentloaded",
    )
    _settle(page, 3500)
    _shoot(page, "12_empty_state_org")

    print("[10] landing mobile")
    page.set_viewport_size(MOBILE)
    page.goto(f"{BASE}/rankings-lobbying-poc", wait_until="domcontentloaded")
    _settle(page, 4000)
    _shoot(page, "10_landing_mobile")

    browser.close()

print("DONE")
