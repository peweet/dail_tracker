"""Capture the NEW "Open right now" tab national-eTenders sections (live tenders +
expiring contracts) for the civic-ui-review pass. Viewport shots only (full_page is
broken on this Streamlit/Windows combo — see memory feedback_streamlit_playwright).
Saves to audit_screenshots/_pon_*.png.
"""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8533"
DESKTOP = {"width": 1440, "height": 900}
MOBILE = {"width": 390, "height": 844}


def _settle(page, ms: int = 3500) -> None:
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
    p = OUT / f"{name}.png"
    page.screenshot(path=str(p), full_page=False)
    print(f"  saved {p.name} ({p.stat().st_size // 1024} KB)")


def _dismiss_toast(page) -> None:
    try:
        page.keyboard.press("Escape")
    except Exception:
        pass


def capture(pw):
    browser = pw.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
    page = ctx.new_page()

    print("[open] procurement page")
    page.goto(f"{BASE}/rankings-procurement", wait_until="domcontentloaded")
    _settle(page, 5000)
    _dismiss_toast(page)

    print("[1] click 'Open right now' tab")
    page.get_by_role("tab", name="Open right now").click()
    _settle(page, 2500)
    _shoot(page, "_pon_1_open_tenders_top")

    print("[2] scroll into the national live-tender cards")
    page.mouse.wheel(0, 620)
    page.wait_for_timeout(1000)
    _shoot(page, "_pon_2_open_tenders_cards")

    print("[3] scroll to the register divider + TED section")
    page.mouse.wheel(0, 900)
    page.wait_for_timeout(1000)
    _shoot(page, "_pon_3_register_divider")

    print("[4] switch segment -> Contract terms ending")
    page.mouse.wheel(0, -2200)
    page.wait_for_timeout(500)
    try:
        page.get_by_text("Contract terms ending", exact=True).click()
    except Exception as e:
        print(f"   segment click fallback: {e}")
        page.get_by_role("button", name="Contract terms ending").click()
    _settle(page, 2500)
    _shoot(page, "_pon_4_expiring_top")

    print("[5] scroll into the national expiring cards")
    page.mouse.wheel(0, 640)
    page.wait_for_timeout(1000)
    _shoot(page, "_pon_5_expiring_cards")

    print("[6] mobile — open tenders")
    mctx = browser.new_context(viewport=MOBILE, device_scale_factor=2)
    mp = mctx.new_page()
    mp.goto(f"{BASE}/rankings-procurement", wait_until="domcontentloaded")
    _settle(mp, 5000)
    _dismiss_toast(mp)
    try:
        mp.get_by_role("tab", name="Open right now").click()
        _settle(mp, 2200)
    except Exception as e:
        print(f"   mobile tab skip: {e}")
    mp.mouse.wheel(0, 520)
    mp.wait_for_timeout(1000)
    _shoot(mp, "_pon_6_mobile_open")

    browser.close()


if __name__ == "__main__":
    with sync_playwright() as p:
        capture(p)
    print("DONE")
