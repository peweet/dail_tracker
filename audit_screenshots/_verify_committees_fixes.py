"""Visual verification of the Committees fixes applied 2026-05-26.

Captures key shots to confirm:
- P1-1: committee detail callout now shows real citizen sentence,
  not "More data coming soon".
- P1-2: search placeholder explicit about Enter-to-apply.
- P1-3: roster Party column wider — "Independent Ireland" fits.
- P1-4: composition Y-axis no longer truncates "Independent Irel...".
- P1-6: committee titles no longer carry "(Dáil Éireann)" suffix.
- P2-1: stat-strip label is "Current memberships" not "Active".
- P2-6: roster header is "Member" not "TD".
"""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "verify_committees"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
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

        # P1-6 + P1-2 + P2-1: register landing — titles without chamber
        # suffix, search placeholder explicit, stat strip renamed.
        goto(page, "/rankings-committees", cold=True)
        shot(page, "01_register_landing_no_chamber_suffix")
        page.evaluate("window.scrollTo(0, 250)")
        time.sleep(0.4)
        shot(page, "02_register_filters_and_stats")

        # Click the first committee card to enter detail view — needs to
        # find a committee link.
        try:
            link = page.locator('a[href*="committee="]').first
            if link.count() > 0:
                link.click()
                time.sleep(WARM / 1000)
            else:
                print("     no committee links found; trying ?committee= deep link")
                goto(page, "/rankings-committees?committee=Committee%20of%20Public%20Accounts")
        except Exception as e:
            print(f"     committee click failed: {e}")
            goto(page, "/rankings-committees?committee=Committee%20of%20Public%20Accounts")

        # P1-1 + P1-4 + P2-6: committee detail — citizen callout, composition
        # chart labels not truncated, roster header "Member".
        shot(page, "03_committee_detail_above_fold", full_page=False)
        page.evaluate("window.scrollTo(0, 500)")
        time.sleep(0.4)
        shot(page, "04_committee_detail_composition_chart")
        page.evaluate("window.scrollTo(0, 1100)")
        time.sleep(0.4)
        shot(page, "05_committee_detail_roster_with_party_widths")

        ctx.close()
        browser.close()
    print(f"\nDONE. Verification shots in {OUT}")


if __name__ == "__main__":
    main()
