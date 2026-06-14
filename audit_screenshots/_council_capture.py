"""Council Spending UI-audit capture — viewport shots (full_page is broken on this
Streamlit/Windows combo, see memory feedback_streamlit_playwright). Captures the index
above-the-fold, the province bands, a per-council dossier drill-down (incl. AFS), and a
mobile view. Saves to audit_screenshots/_cs_*.png.
"""

from __future__ import annotations

from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8533"
SLUG = "/rankings-council-spending"
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

    print("[1] index top (hero + lede)")
    page.goto(f"{BASE}{SLUG}", wait_until="domcontentloaded")
    _settle(page, 6000)
    _dismiss_toast(page)
    _shoot(page, "_cs_1_top")

    print("[2] index scrolled — province bands + cards")
    page.mouse.wheel(0, 720)
    page.wait_for_timeout(1200)
    _shoot(page, "_cs_2_bands")

    print("[3] index scrolled further — later provinces")
    page.mouse.wheel(0, 900)
    page.wait_for_timeout(1000)
    _shoot(page, "_cs_3_bands2")

    print("[4] per-council dossier (ordered-only council, e.g. Donegal)")
    # Pull a real council card href from the live DOM.
    href = None
    try:
        href = page.eval_on_selector("a.dt-card-link", "el => el.getAttribute('href')")
    except Exception:
        pass
    if not href:
        href = "?paid_publisher=Donegal&paid_tier=COMMITTED"
    page.goto(f"{BASE}{SLUG}{href}", wait_until="domcontentloaded")
    _settle(page, 4500)
    _dismiss_toast(page)
    _shoot(page, "_cs_4_dossier_top")
    page.mouse.wheel(0, 760)
    page.wait_for_timeout(1000)
    _shoot(page, "_cs_5_dossier_afs")

    print("[6] paid council dossier (Meath — actual payments + AFS)")
    page.goto(f"{BASE}{SLUG}?paid_publisher=Meath&paid_tier=SPENT", wait_until="domcontentloaded")
    _settle(page, 4500)
    _dismiss_toast(page)
    _shoot(page, "_cs_6_meath_top")
    page.mouse.wheel(0, 820)
    page.wait_for_timeout(1000)
    _shoot(page, "_cs_7_meath_afs")

    print("[8] mobile index")
    mctx = browser.new_context(viewport=MOBILE, device_scale_factor=2)
    mp = mctx.new_page()
    mp.goto(f"{BASE}{SLUG}", wait_until="domcontentloaded")
    _settle(mp, 6000)
    _dismiss_toast(mp)
    _shoot(mp, "_cs_8_mobile_top")
    mp.mouse.wheel(0, 760)
    mp.wait_for_timeout(1200)
    _shoot(mp, "_cs_9_mobile_bands")

    browser.close()


if __name__ == "__main__":
    with sync_playwright() as p:
        capture(p)
    print("DONE")
