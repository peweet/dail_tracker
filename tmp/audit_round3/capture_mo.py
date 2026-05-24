"""Re-run only the MEMBER-OVERVIEW captures with the ACTUAL valid
unique_member_code (registry format: <Name>.D.<DateElected>), since
name_join_key() returns a different value that the views don't recognise.

This is itself a critical audit finding — the cross-page contract from
Phases 3–8 is broken because every dimension page hrefs via
name_join_key(name), which doesn't match the registered code.
"""
from __future__ import annotations

import sys
import time
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))

from playwright.sync_api import Page, sync_playwright  # noqa: E402

BASE = "http://localhost:8511"
OUT = Path(__file__).resolve().parent

# Real codes (from v_member_registry diagnostic):
MARY_LOU_CODE = "Mary-Lou-McDonald.D.2011-03-09"      # plain TD
MICHEAL_CODE = "Micheál-Martin.D.1989-06-29"          # Taoiseach (minister)
PEARSE_CODE = "Pearse-Doherty.S.2007-07-23"           # opposition finance

PAGE_LOAD_WAIT = 5000
RERUN_WAIT = 2500


def shot(page: Page, name: str, *, full_page: bool = True) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    print(f"  -> {path.name}")


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def goto(page: Page, path: str) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(PAGE_LOAD_WAIT)


def click_button(page: Page, text: str) -> bool:
    btn = page.get_by_role("button", name=text)
    if btn.count() > 0:
        btn.first.click()
        wait(RERUN_WAIT)
        return True
    print(f"    [missed] button '{text}' not found")
    return False


def click_summary(page: Page, label: str) -> bool:
    """Click an st.expander <summary> by visible text."""
    summary = page.locator(f'details summary:has-text("{label}")')
    if summary.count() > 0:
        summary.first.click()
        wait(RERUN_WAIT)
        return True
    print(f"    [missed] expander summary '{label}' not found")
    return False


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        # ── Profile — default load (Interests open per round-2 default) ──
        encoded = urllib.parse.quote(MARY_LOU_CODE, safe="")
        goto(page, f"/member-overview?member={encoded}")
        shot(page, "04b_mo_profile_default_interests_open")

        # Top-of-page focused view (above-fold)
        shot(page, "04c_mo_profile_above_fold", full_page=False)

        # ── Chevron-click wart on Payments (currently gated) ──
        if click_summary(page, "Payments"):
            shot(page, "05b_mo_profile_chevron_wart_payments", full_page=False)

        # ── Open all sections (journalist mode) ──
        if click_button(page, "Open all sections"):
            shot(page, "06b_mo_profile_open_all_above_fold", full_page=False)
            shot(page, "09b_mo_profile_open_all_full")

        # ── Scroll positions within open-all ──
        page.mouse.wheel(0, 2500)
        wait(600)
        shot(page, "07b_mo_profile_open_all_scroll_mid", full_page=False)

        page.mouse.wheel(0, 4000)
        wait(600)
        shot(page, "08b_mo_profile_open_all_scroll_bottom", full_page=False)

        # ── Switch to minister (Micheál Martin) — should show SI sub-section ──
        encoded = urllib.parse.quote(MICHEAL_CODE, safe="")
        goto(page, f"/member-overview?member={encoded}")
        shot(page, "14b_mo_profile_minister_default")

        if click_button(page, "Open all sections"):
            # Scroll to legislation expander
            page.mouse.wheel(0, 4500)
            wait(800)
            shot(page, "15b_mo_profile_minister_legislation", full_page=False)
            # Full minister page
            page.mouse.wheel(0, -10000)
            wait(400)
            shot(page, "16b_mo_profile_minister_full")

        ctx.close()
        browser.close()
    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
