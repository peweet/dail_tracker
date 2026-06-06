"""Procurement-page render confirmation capture.

Shoots the landing state + each of the four tabs (Suppliers / Contracting
authorities / Categories / Lobbying overlap) so we can eyeball that the LIVE
page renders real gold data. Saves PNGs to audit_screenshots/_proc_*.png.
"""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8533"
DESKTOP = {"width": 1440, "height": 900}


def _wait_for_render(page, settle_ms: int = 3000) -> None:
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


def _shoot(page, name: str) -> None:
    p = OUT / f"{name}.png"
    page.screenshot(path=str(p), full_page=True)
    print(f"  saved {p.name}  ({p.stat().st_size // 1024} KB)")


def capture(playwright):
    browser = playwright.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
    page = ctx.new_page()

    print("[1/5] landing (Suppliers tab default)")
    page.goto(f"{BASE}/rankings-procurement", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=4500)
    _shoot(page, "_proc_1_landing")

    # Tabs are st.tabs — labelled buttons. Click each by visible text.
    for idx, label in enumerate(
        ["Contracting authorities", "Categories", "Lobbying overlap"], start=2
    ):
        print(f"[{idx}/5] tab — {label}")
        try:
            page.get_by_role("tab", name=label).click()
            _wait_for_render(page, settle_ms=2500)
            _shoot(page, f"_proc_{idx}_{label.split()[0].lower()}")
        except Exception as e:
            print(f"   skipped {label}: {e}")

    # Capture the headline badges + caveat panel region explicitly (top of page).
    print("[5/5] hero + caveat (viewport top)")
    page.get_by_role("tab", name="Suppliers").click()
    _wait_for_render(page, settle_ms=2000)
    page.screenshot(path=str(OUT / "_proc_5_hero.png"), full_page=False)
    print("  saved _proc_5_hero.png")

    browser.close()


if __name__ == "__main__":
    with sync_playwright() as p:
        capture(p)
    print("DONE")
