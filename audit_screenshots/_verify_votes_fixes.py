"""Visual verification of the Votes fixes applied 2026-05-26.

Captures key shots so we can eyeball:
- Mode A division card: stage pill in header, Oireachtas link demoted
  to footer (quieter), plain-English margin ("won by N").
- Mode A caption: filter breadcrumb shows outcome word when active.
- TD picker: single-sentence card, no redundant CTA button, quieter
  yes/no chip.
- Mode B redirect: shared member_moved_callout with TD name.
- Mode C empty state: no "for ID:" prefix.
"""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "verify_votes"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
MOBILE  = {"width": 390,  "height": 844}
COLD = 12000
WARM = 4000


def shot(page: Page, name: str, *, full_page: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    kb = path.stat().st_size // 1024
    print(f"  -> {path.name} ({kb} KB)")


def goto(page: Page, path: str, *, cold: bool = False) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    time.sleep((COLD if cold else WARM) / 1000)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()

        # Mode A landing — should show stage pill on cards, plain-English
        # margin, footer Oireachtas link.
        goto(page, "/rankings-votes", cold=True)
        page.evaluate("window.scrollTo(0, 200)")
        time.sleep(0.4)
        shot(page, "01_modeA_cards_with_stage_pill")

        # TDs view — picker landing, single-sentence cards.
        goto(page, "/rankings-votes?")
        # Set view to TDs via session state isn't possible from here; click the
        # sidebar segmented control "TDs" instead.
        try:
            tds_btn = page.locator('[data-testid="stSidebar"] [data-testid="stSegmentedControl"] button:has-text("TDs")').first
            if tds_btn.count() > 0:
                tds_btn.click()
                time.sleep(WARM / 1000)
        except Exception as e:
            print(f"     could not click TDs toggle: {e}")
        shot(page, "02_tds_picker_landing", full_page=True)

        # Mode B redirect — visit a member URL, expect shared callout.
        goto(page, "/rankings-votes?member=NaoiseOCearuil")
        shot(page, "03_modeB_redirect_with_name")

        # Mode C empty state — bogus vote id.
        goto(page, "/rankings-votes?vote=NONEXISTENT_VOTE_ID")
        shot(page, "04_modeC_not_found_civic_copy")

        # Mobile Mode A — verify the demoted Oireachtas link doesn't
        # compete with the internal navigation on small viewports.
        page.set_viewport_size(MOBILE)
        goto(page, "/rankings-votes")
        page.evaluate("window.scrollTo(0, 350)")
        time.sleep(0.4)
        shot(page, "05_mobile_modeA_cards")

        ctx.close()
        browser.close()
    print(f"\nDONE. Verification shots in {OUT}")


if __name__ == "__main__":
    main()
