"""Judiciary-page audit screenshot capture (four tabs + profile drilldown)."""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8533"
URL = f"{BASE}/rankings-judiciary"

DESKTOP = {"width": 1440, "height": 900}
MOBILE = {"width": 390, "height": 844}


def _wait(page, settle_ms: int = 2500) -> None:
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
    page.wait_for_timeout(settle_ms)


def _shoot(page, name: str, full_page: bool = True) -> None:
    p = OUT / f"_jud_{name}.png"
    page.screenshot(path=str(p), full_page=full_page)
    print(f"  saved {p.name}  ({p.stat().st_size // 1024} KB)")


def _click_tab(page, label: str) -> bool:
    try:
        tab = page.get_by_role("tab", name=label, exact=False).first
        tab.click()
        _wait(page, 2200)
        return True
    except Exception as e:
        print(f"   couldn't click tab {label!r}: {e}")
        return False


def capture(playwright):
    browser = playwright.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
    page = ctx.new_page()

    print("[1] bench (default tab) + hero")
    page.goto(URL, wait_until="domcontentloaded")
    _wait(page, 4000)
    _shoot(page, "01_bench")

    print("[2] appointments & government tab")
    if _click_tab(page, "Appointments"):
        _shoot(page, "02_appointments")

    print("[3] the courts tab")
    if _click_tab(page, "The Courts"):
        _shoot(page, "03_courts")

    print("[4] legal diary tab")
    if _click_tab(page, "Legal Diary"):
        _shoot(page, "04_legal_diary")

    print("[5] judge profile drilldown — first bench card")
    page.goto(URL, wait_until="domcontentloaded")
    _wait(page, 3500)
    try:
        link = page.locator('a[aria-label^="View the appointment history"]').first
        href = link.get_attribute("href")
        if href:
            target = href if href.startswith("http") else f"{URL}{href}"
            page.goto(target, wait_until="domcontentloaded")
            _wait(page, 3500)
            _shoot(page, "05_profile")
        else:
            print("   no judge link href")
    except Exception as e:
        print(f"   skipped profile: {e}")

    print("[6] mobile — bench")
    page.set_viewport_size(MOBILE)
    page.goto(URL, wait_until="domcontentloaded")
    _wait(page, 4000)
    _shoot(page, "06_mobile_bench")

    browser.close()


if __name__ == "__main__":
    with sync_playwright() as p:
        capture(p)
    print("DONE")
