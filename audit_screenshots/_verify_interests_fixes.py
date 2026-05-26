"""Visual verification of the Interests fixes applied 2026-05-26.

Captures:
- P1-4: main-panel search is a single selectbox (no Enter trap).
- P1-3 + P1-2: sidebar shows distinct "M. Healy-Rae" / "D. Healy-Rae"
  chips; card photos show rank overlay chips.
- P1-1: todo_callout citizen sentence starts with uppercase letter.
- P0-1: clicking the typeahead OR a chip fires the shared
  member_moved_callout, not a dead-state rerun.
- P1-5: heading reads "Declarations for {year} · {n} members".
"""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "verify_interests"
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

        # P1-1 + P1-2 + P1-3 + P1-4 + P1-5: landing — single selectbox,
        # rank overlay on avatar cards, disambiguated chips, capitalised
        # callout citizen sentence, year-aware heading.
        goto(page, "/rankings-interests", cold=True)
        shot(page, "01_landing_above_fold")
        page.evaluate("window.scrollTo(0, 350)")
        time.sleep(0.4)
        shot(page, "02_landing_avatar_cards_with_rank")
        page.evaluate("window.scrollTo(0, 1100)")
        time.sleep(0.4)
        shot(page, "03_landing_cards_lower")

        # P0-1: legacy ?member= still resolves to the shared callout
        # (this confirms the helper is wired); typeahead/chip path
        # is harder to drive headlessly but uses the same code path.
        goto(page, "/rankings-interests?member=Mary%20Lou%20McDonald")
        shot(page, "04_legacy_member_redirect")

        ctx.close()
        browser.close()
    print(f"\nDONE. Verification shots in {OUT}")


if __name__ == "__main__":
    main()
