"""Comprehensive Interests page audit capture.

Walks /rankings-interests exhaustively for an impeccable audit. Post-Phase 3
the per-TD profile branch was lifted out to /member-overview, so the page
itself is now browse-only:
  - Landing: hero + Find-a-TD typeahead + year pills + member fallback cards
  - Chamber toggle Dáil <-> Seanad
  - Year switch (year pills filter the member list)
  - Notable-member sidebar chips (sets selected_td then reruns)
  - Find-a-TD typeahead (main panel) — type + select from dropdown
  - Empty-state probes (no-match name filter, distant year if any)
  - Legacy ?member=<name> redirect (Phase 3 + round-3 shared helper)

Output: audit_screenshots/_interests/IN-X-name.png.

Streamlit + Playwright gotchas (see [[feedback_streamlit_playwright]]):
- Wrong slug silently renders default page with a "Page not found" modal.
- Streamlit's websocket never goes idle; use explicit waits.
- Multi-page chain navigation can re-trigger the modal -- use a fresh
  `browser.new_context()` per legacy-URL capture.
- ASCII-only print() for Windows CP1252 console safety.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-interests"
OUT = Path(__file__).resolve().parent / "_interests"
OUT.mkdir(parents=True, exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
TABLET  = {"width": 820,  "height": 1180}
MOBILE  = {"width": 390,  "height": 844}

PAGE_LOAD_WAIT  = 5500
RERUN_WAIT      = 2400


def shot(page: Page, name: str, *, full_page: bool = True) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    kb = path.stat().st_size // 1024
    print(f"  -> {path.name} ({kb} KB)")


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def goto(page: Page, path: str = ROUTE) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(PAGE_LOAD_WAIT)


def settle(page: Page, ms: int = RERUN_WAIT) -> None:
    wait(ms)


def click_button_re(page: Page, pattern: str) -> bool:
    btn = page.get_by_role("button", name=re.compile(pattern))
    if btn.count() > 0:
        try:
            btn.first.click()
            settle(page)
            return True
        except Exception as e:
            print(f"     click_button_re('{pattern}') failed: {e}")
    return False


# --------------------------------------------------------------------------
# Phase A -- Landing (default Dail, current year)
# --------------------------------------------------------------------------

def phase_landing(page: Page) -> None:
    print("\n== PHASE A -- landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_landing_full_desktop")
    shot(page, "A02_landing_above_fold", full_page=False)
    page.mouse.wheel(0, 900); settle(page, 600)
    shot(page, "A03_landing_year_pills_typeahead", full_page=False)
    page.mouse.wheel(0, 1200); settle(page, 600)
    shot(page, "A04_landing_member_cards", full_page=False)
    page.mouse.wheel(0, 4000); settle(page, 600)
    shot(page, "A05_landing_bottom_provenance", full_page=False)

    # Tablet
    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "A06_landing_tablet")

    # Mobile
    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "A07_landing_mobile")
    page.mouse.wheel(0, 900); settle(page, 500)
    shot(page, "A08_landing_mobile_cards", full_page=False)
    page.mouse.wheel(0, 1400); settle(page, 500)
    shot(page, "A09_landing_mobile_bottom", full_page=False)


# --------------------------------------------------------------------------
# Phase B -- Chamber toggle (Dail -> Seanad)
# --------------------------------------------------------------------------

def phase_chamber(page: Page) -> None:
    print("\n== PHASE B -- chamber toggle ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    seanad_btn = page.get_by_role("button", name=re.compile(r"^Seanad$"))
    if seanad_btn.count() > 0:
        try:
            seanad_btn.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "B01_seanad_landing")
            page.mouse.wheel(0, 1200); settle(page, 600)
            shot(page, "B02_seanad_cards", full_page=False)
        except Exception as e:
            print(f"     Seanad pill click failed: {e}")
    else:
        print("     Seanad pill not found")

    # Switch back to Dail
    dail_btn = page.get_by_role("button", name=re.compile(r"^D[áa]il$"))
    if dail_btn.count() > 0:
        try:
            dail_btn.first.click()
            settle(page, RERUN_WAIT)
        except Exception:
            pass


# --------------------------------------------------------------------------
# Phase C -- Year switch (click a non-default year pill)
# --------------------------------------------------------------------------

def phase_year(page: Page) -> None:
    print("\n== PHASE C -- year switch ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 600); settle(page, 400)

    # Year pills render as buttons whose name is the 4-digit year
    year_buttons = page.get_by_role("button", name=re.compile(r"^20\d{2}$"))
    n = year_buttons.count()
    print(f"     found {n} year pill buttons")
    if n >= 2:
        try:
            # Click the second year pill (not the default)
            year_buttons.nth(1).click()
            settle(page, RERUN_WAIT)
            shot(page, "C01_year_switched")
            page.mouse.wheel(0, 1200); settle(page, 600)
            shot(page, "C02_year_switched_cards", full_page=False)
        except Exception as e:
            print(f"     year pill click failed: {e}")

    if n >= 5:
        try:
            # Older year toward the end of the list
            year_buttons.nth(n - 2).click()
            settle(page, RERUN_WAIT)
            shot(page, "C03_year_oldish")
            page.mouse.wheel(0, 1200); settle(page, 600)
            shot(page, "C04_year_oldish_cards", full_page=False)
        except Exception as e:
            print(f"     older year click failed: {e}")


# --------------------------------------------------------------------------
# Phase D -- Notable chips (sidebar) -- writes selected_td, reruns
# --------------------------------------------------------------------------

def phase_notable_chip(page: Page) -> None:
    print("\n== PHASE D -- notable chip ==")
    page.set_viewport_size(DESKTOP)
    goto(page)

    # The chips are sidebar buttons named with the surname (last word of name).
    # Take a baseline screenshot of the sidebar.
    shot(page, "D01_sidebar_notable_chips", full_page=False)

    # Pick a likely surname -- "McDonald", "Martin", etc. The chip text is the
    # surname. We will click the first chip we can find.
    candidates = ["McDonald", "Martin", "Bacik", "Harris", "Doherty"]
    clicked = False
    for surname in candidates:
        btn = page.get_by_role("button", name=re.compile(rf"^{surname}$"))
        if btn.count() > 0:
            try:
                btn.first.click()
                settle(page, RERUN_WAIT)
                shot(page, f"D02_notable_chip_clicked_{surname}")
                clicked = True
                print(f"     clicked notable chip: {surname}")
                break
            except Exception as e:
                print(f"     notable chip click failed for {surname}: {e}")
    if not clicked:
        print("     no notable chip matched any candidate surname")


# --------------------------------------------------------------------------
# Phase E -- Find-a-TD typeahead (main panel)
# --------------------------------------------------------------------------

def phase_typeahead(page: Page) -> None:
    print("\n== PHASE E -- typeahead ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 500); settle(page, 400)

    # main_member_jump renders: kicker line + text_input + selectbox in two cols.
    # The text_input is the first input[type="text"] on the page (after the
    # sidebar chamber). The selectbox is a baseweb select.
    inputs = page.locator('input[type="text"]')
    nt = inputs.count()
    print(f"     found {nt} text inputs on page")
    if nt >= 1:
        try:
            # First text input is the typeahead search filter.
            inputs.first.fill("Mary Lou")
            settle(page, 1500)
            shot(page, "E01_typeahead_with_query", full_page=False)
            # Now open the selectbox (second column) and pick the first non-default option
            select = page.locator('[data-baseweb="select"]').first
            if select.count() > 0:
                select.click()
                settle(page, 800)
                opts = page.locator('[role="option"]')
                if opts.count() > 1:
                    # index 0 is "-- select --", so pick 1
                    opts.nth(1).click()
                    settle(page, RERUN_WAIT)
                    shot(page, "E02_typeahead_after_pick")
                else:
                    shot(page, "E02_typeahead_no_options", full_page=False)
        except Exception as e:
            print(f"     typeahead flow failed: {e}")


# --------------------------------------------------------------------------
# Phase F -- No-match empty state (typeahead filter with bogus query)
# --------------------------------------------------------------------------

def phase_no_match(page: Page) -> None:
    print("\n== PHASE F -- no-match empty state ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 500); settle(page, 400)

    inputs = page.locator('input[type="text"]')
    if inputs.count() >= 1:
        try:
            inputs.first.fill("zzzzz-nonexistent-name")
            settle(page, 1500)
            shot(page, "F01_typeahead_no_match", full_page=False)
            # Try to open the selectbox to confirm there are no real options
            select = page.locator('[data-baseweb="select"]').first
            if select.count() > 0:
                select.click()
                settle(page, 800)
                shot(page, "F02_typeahead_no_match_dropdown", full_page=False)
        except Exception as e:
            print(f"     no-match flow failed: {e}")


# --------------------------------------------------------------------------
# Phase G -- Pagination next page
# --------------------------------------------------------------------------

def phase_paginate(page: Page) -> None:
    print("\n== PHASE G -- pagination ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 6000); settle(page, 800)
    shot(page, "G01_paginator_at_bottom", full_page=False)
    if click_button_re(page, r"^2$") or click_button_re(page, r"Next"):
        shot(page, "G02_page_2")
    else:
        print("     no paginator buttons matched")


# --------------------------------------------------------------------------
# Phase H -- Legacy ?member= redirect (fresh context)
# --------------------------------------------------------------------------

def phase_legacy_redirect(playwright_root) -> None:
    print("\n== PHASE H -- legacy ?member= redirect ==")
    browser = playwright_root.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP)
    page = ctx.new_page()
    try:
        import urllib.parse
        encoded = urllib.parse.quote("Mary Lou McDonald", safe="")
        page.goto(f"{BASE}{ROUTE}?member={encoded}", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        page.screenshot(path=str(OUT / "H01_legacy_member_redirect.png"))
        print(f"  -> H01_legacy_member_redirect.png")
    finally:
        ctx.close()
        browser.close()


# --------------------------------------------------------------------------
# Main
# --------------------------------------------------------------------------

def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP)
        page = ctx.new_page()
        try:
            for label, fn in [
                ("A landing",   phase_landing),
                ("B chamber",   phase_chamber),
                ("C year",      phase_year),
                ("D notable",   phase_notable_chip),
                ("E typeahead", phase_typeahead),
                ("F no-match",  phase_no_match),
                ("G paginate",  phase_paginate),
            ]:
                print(f"\n## {label}")
                try:
                    fn(page)
                except Exception as e:
                    print(f"  !! phase '{label}' failed: {e}")
        finally:
            ctx.close()
            browser.close()

        try:
            phase_legacy_redirect(p)
        except Exception as e:
            print(f"  !! phase H failed: {e}")

    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
