"""Comprehensive Committees page audit capture.

Walks /rankings-committees exhaustively for an impeccable audit. The
page has 2 active stages (the third — _STAGE_TD — was lifted out in
Phase 8) plus a Find-a-TD typeahead and legacy `?member=` redirect:
  - Stage 1 (register): chamber pills + filter command bar + paginated
    committee list
  - Stage 2 (committee detail): identity strip + composition chart +
    roster dataframe
  - Find-a-TD inline confirmation (cmd_r typeahead)
  - Legacy ?member= redirect (Phase 8 + round-3 shared helper)

Output: audit_screenshots/_committees/CC-X-name.png.

Streamlit + Playwright gotchas (see [[feedback_streamlit_playwright]]):
- Wrong slug silently renders default page with a "Page not found" modal.
- Streamlit's websocket never goes idle; use explicit waits.
- Multi-page chain navigation can re-trigger the modal — use a fresh
  `browser.new_context()` per legacy-URL capture.
- ASCII-only print() for Windows CP1252 console safety.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-committees"
OUT = Path(__file__).resolve().parent / "_committees"
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


# ─────────────────────────────────────────────────────────────────────────
# Phase A — Register landing (default, no filters)
# ─────────────────────────────────────────────────────────────────────────

def phase_landing(page: Page) -> None:
    print("\n== PHASE A — Register landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_register_full_desktop")
    shot(page, "A02_register_above_fold", full_page=False)
    page.mouse.wheel(0, 1400); settle(page, 600)
    shot(page, "A03_register_mid", full_page=False)
    page.mouse.wheel(0, 1400); settle(page, 600)
    shot(page, "A04_register_cards", full_page=False)
    page.mouse.wheel(0, 5000); settle(page, 600)
    shot(page, "A05_register_bottom", full_page=False)

    # Tablet
    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "A06_register_tablet")

    # Mobile
    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "A07_register_mobile")
    page.mouse.wheel(0, 1200); settle(page, 500)
    shot(page, "A08_register_mobile_cards", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase B — Chamber toggle (Dáil -> Seanad)
# ─────────────────────────────────────────────────────────────────────────

def phase_chamber(page: Page) -> None:
    print("\n== PHASE B — chamber toggle ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    # The chamber pills are rendered via st.pills; each option is a button
    seanad_btn = page.get_by_role("button", name=re.compile(r"^Seanad$"))
    if seanad_btn.count() > 0:
        try:
            seanad_btn.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "B01_seanad_register")
            page.mouse.wheel(0, 1400); settle(page, 600)
            shot(page, "B02_seanad_cards", full_page=False)
        except Exception as e:
            print(f"     Seanad pill click failed: {e}")
    else:
        print("     Seanad pill not found")

    # Switch back to Dáil for the next phases
    dail_btn = page.get_by_role("button", name=re.compile(r"^D[áa]il$"))
    if dail_btn.count() > 0:
        try:
            dail_btn.first.click()
            settle(page, RERUN_WAIT)
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────────────────
# Phase C — Filter command bar (search, type, status)
# ─────────────────────────────────────────────────────────────────────────

def phase_filters(page: Page) -> None:
    print("\n== PHASE C — filters ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 400); settle(page, 400)  # bring filter bar into view

    # Committee-name search (the FIRST text input — the typeahead is also a
    # text input but is separate)
    search = page.locator('input[type="text"]').first
    if search.count() > 0:
        try:
            search.fill("Finance")
            settle(page, RERUN_WAIT)
            shot(page, "C01_search_finance")
            search.fill("")
            settle(page, RERUN_WAIT)
        except Exception as e:
            print(f"     search fill failed: {e}")

    # Type dropdown (committees type)
    type_box = page.locator('[data-baseweb="select"]').first
    if type_box.count() > 0:
        try:
            type_box.click()
            settle(page, 800)
            opts = page.locator('[role="option"]')
            # pick the 2nd option (skip "All types" at index 0)
            if opts.count() > 1:
                opts.nth(1).click()
                settle(page)
                shot(page, "C02_type_filter_selected")
        except Exception as e:
            print(f"     type filter failed: {e}")

    # Reset filters (refresh page)
    goto(page)

    # Status segmented control — click "Active"
    active_btn = page.get_by_role("button", name=re.compile(r"^Active$"))
    if active_btn.count() > 0:
        try:
            active_btn.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "C03_status_active")
        except Exception as e:
            print(f"     Active status failed: {e}")

    ended_btn = page.get_by_role("button", name=re.compile(r"^Ended$"))
    if ended_btn.count() > 0:
        try:
            ended_btn.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "C04_status_ended")
        except Exception as e:
            print(f"     Ended status failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase D — Pagination (click next-page button if rendered)
# ─────────────────────────────────────────────────────────────────────────

def phase_paginate(page: Page) -> None:
    print("\n== PHASE D — pagination ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 6000); settle(page, 800)
    shot(page, "D01_paginator_at_bottom", full_page=False)
    if click_button_re(page, r"^2$") or click_button_re(page, r"Next"):
        shot(page, "D02_page_2")
    else:
        print("     no paginator buttons matched")


# ─────────────────────────────────────────────────────────────────────────
# Phase E — Committee detail
# ─────────────────────────────────────────────────────────────────────────

def phase_detail(page: Page) -> None:
    print("\n== PHASE E — committee detail ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    # First committee card has a clickable link with href containing ?committee=
    comm_links = page.locator('a[href*="committee="]')
    if comm_links.count() > 0:
        try:
            comm_links.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "E01_committee_detail_full")
            shot(page, "E02_committee_detail_above_fold", full_page=False)
            page.mouse.wheel(0, 1000); settle(page, 600)
            shot(page, "E03_committee_detail_composition_roster", full_page=False)
            page.mouse.wheel(0, 2000); settle(page, 600)
            shot(page, "E04_committee_detail_bottom", full_page=False)
        except Exception as e:
            print(f"     committee detail click failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase F — Find-a-TD typeahead inline confirmation
# ─────────────────────────────────────────────────────────────────────────

def phase_typeahead(page: Page) -> None:
    print("\n== PHASE F — Find-a-TD typeahead ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 400); settle(page, 400)

    # The Find-a-TD typeahead is the SECOND text input on the page (after
    # the committee-name search). It uses find_a_td_search which is a
    # custom widget; the input element is reachable by index.
    inputs = page.locator('input[type="text"]')
    if inputs.count() >= 2:
        try:
            inputs.nth(1).fill("Mary Lou McDonald")
            settle(page, 1500)
            shot(page, "F01_typeahead_with_query", full_page=False)
            # The dropdown shows suggestions; click the first match
            opts = page.locator('[role="option"], li[role="option"]')
            if opts.count() == 0:
                # try a different selector — the typeahead may render as a
                # listbox under the input
                opts = page.locator('[data-baseweb="popover"] li')
            if opts.count() > 0:
                opts.first.click()
                settle(page, RERUN_WAIT)
                shot(page, "F02_typeahead_after_pick", full_page=False)
            else:
                print("     no typeahead suggestions visible")
                # Capture anyway to see the state
                shot(page, "F02_typeahead_no_suggestions", full_page=False)
        except Exception as e:
            print(f"     typeahead flow failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase G — Legacy ?member= redirect (fresh context)
# ─────────────────────────────────────────────────────────────────────────

def phase_legacy_redirect(playwright_root) -> None:
    print("\n== PHASE G — legacy ?member= redirect ==")
    browser = playwright_root.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP)
    page = ctx.new_page()
    try:
        import urllib.parse
        encoded = urllib.parse.quote("Mary Lou McDonald", safe="")
        page.goto(f"{BASE}{ROUTE}?member={encoded}", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        page.screenshot(path=str(OUT / "G01_legacy_member_redirect.png"))
        print(f"  -> G01_legacy_member_redirect.png")
    finally:
        ctx.close()
        browser.close()


# ─────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP)
        page = ctx.new_page()
        try:
            for label, fn in [
                ("A landing",   phase_landing),
                ("B chamber",   phase_chamber),
                ("C filters",   phase_filters),
                ("D paginate",  phase_paginate),
                ("E detail",    phase_detail),
                ("F typeahead", phase_typeahead),
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
            print(f"  !! phase G failed: {e}")

    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
