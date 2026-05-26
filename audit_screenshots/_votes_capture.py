"""Comprehensive Votes page audit capture.

Walks /rankings-votes exhaustively for an impeccable audit. The page has
the richest IA in the app — 3 modes plus a sidebar that mutates by view:
  - Mode A (Dáil view): divisions index with year/outcome/party filters
  - Mode C: division evidence (click a vote card)
  - TDs view + TD picker (curated topical-vote cards, cross-page jumps)
  - Mode B legacy redirect: ?member=<id> bookmarks now redirect to MO

Output: audit_screenshots/_votes/VV-X-name.png (two-digit per phase).

Streamlit + Playwright gotchas (see [[feedback_streamlit_playwright]]):
- Wrong slug silently renders default page with a "Page not found" modal.
- Streamlit's websocket never goes idle; use explicit waits.
- Multi-page chain navigation can re-trigger the modal on percent-encoded
  URLs — open a fresh `browser.new_context()` per legacy-URL capture.
- ASCII-only print() for Windows CP1252 console safety.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-votes"
OUT = Path(__file__).resolve().parent / "_votes"
OUT.mkdir(parents=True, exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
TABLET  = {"width": 820,  "height": 1180}
MOBILE  = {"width": 390,  "height": 844}

PAGE_LOAD_WAIT  = 5500
RERUN_WAIT      = 2400
SETTLE_TINY     = 400


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


def select_in_box(page: Page, label_selector: str, option_text: str) -> bool:
    """Open a Streamlit selectbox by index and pick an option by visible text.

    The selectboxes on this page are in the sidebar. `label_selector` is
    a 0-based index into the rendered selectboxes on the page (the first
    visible one is the Outcome filter on Mode A).
    """
    boxes = page.locator('[data-baseweb="select"]')
    if boxes.count() == 0:
        return False
    try:
        idx = int(label_selector) if str(label_selector).isdigit() else 0
        boxes.nth(idx).click()
        settle(page, 800)
        # Streamlit renders dropdown options into a portal at body level.
        opts = page.locator('[role="option"]')
        for i in range(opts.count()):
            if option_text in opts.nth(i).inner_text():
                opts.nth(i).click()
                settle(page)
                return True
    except Exception as e:
        print(f"     select_in_box(idx={label_selector}, '{option_text}') failed: {e}")
    return False


# ─────────────────────────────────────────────────────────────────────────
# Phase A — Mode A landing (Dáil view, no filters) across viewports
# ─────────────────────────────────────────────────────────────────────────

def phase_landing(page: Page) -> None:
    print("\n== PHASE A — Mode A landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_modeA_full_desktop")
    shot(page, "A02_modeA_above_fold_desktop", full_page=False)
    page.mouse.wheel(0, 1400); settle(page, 600)
    shot(page, "A03_modeA_mid_desktop", full_page=False)
    page.mouse.wheel(0, 1400); settle(page, 600)
    shot(page, "A04_modeA_cards_desktop", full_page=False)
    page.mouse.wheel(0, 4000); settle(page, 600)
    shot(page, "A05_modeA_bottom_desktop", full_page=False)

    # Tablet
    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "A06_modeA_tablet")

    # Mobile
    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "A07_modeA_mobile")
    page.mouse.wheel(0, 1500); settle(page, 500)
    shot(page, "A08_modeA_mobile_cards", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase B — Sidebar filters (outcome, party, year)
# ─────────────────────────────────────────────────────────────────────────

def phase_filters(page: Page) -> None:
    print("\n== PHASE B — sidebar filters ==")
    page.set_viewport_size(DESKTOP)
    goto(page)

    # B01: outcome = Carried
    if select_in_box(page, "0", "Carried"):
        shot(page, "B01_outcome_carried")

    # B02: outcome = Lost
    if select_in_box(page, "0", "Lost"):
        shot(page, "B02_outcome_lost")

    # B03: reset outcome and try party filter
    if select_in_box(page, "0", "All"):
        settle(page, 800)

    # Party is the second selectbox in the sidebar when view=Dáil
    boxes = page.locator('[data-baseweb="select"]')
    if boxes.count() >= 2:
        try:
            boxes.nth(1).click()
            settle(page, 800)
            opts = page.locator('[role="option"]')
            # Pick the 2nd option (skip "All parties" at index 0)
            if opts.count() > 1:
                opts.nth(1).click()
                settle(page)
                shot(page, "B03_party_filter_one_selected")
        except Exception as e:
            print(f"     party filter failed: {e}")

    # B04: year pill click (year_selector lives in main panel)
    goto(page)
    year_buttons = page.locator('button[aria-pressed="false"]:has-text("20")')
    if year_buttons.count() > 0:
        try:
            # Click an older year that's not the default
            year_buttons.first.click()
            settle(page)
            shot(page, "B04_year_switched", full_page=False)
        except Exception as e:
            print(f"     year pill failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase C — Show-all button
# ─────────────────────────────────────────────────────────────────────────

def phase_show_all(page: Page) -> None:
    print("\n== PHASE C — show all divisions ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 5000); settle(page, 800)
    if click_button_re(page, r"^Show all"):
        shot(page, "C01_show_all_expanded")
        page.mouse.wheel(0, 5000); settle(page, 600)
        shot(page, "C02_show_all_mid", full_page=False)
        page.mouse.wheel(0, 10000); settle(page, 600)
        shot(page, "C03_show_all_bottom", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase D — Mode C (division evidence) — click a card
# ─────────────────────────────────────────────────────────────────────────

def phase_mode_c(page: Page) -> None:
    print("\n== PHASE D — Mode C division evidence ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    # Click the first card link (href includes ?vote=)
    vote_links = page.locator('a[href*="vote="]')
    if vote_links.count() > 0:
        try:
            vote_links.first.click()
            settle(page)
            shot(page, "D01_modeC_division_detail_full")
            shot(page, "D02_modeC_above_fold", full_page=False)
            page.mouse.wheel(0, 1200); settle(page, 600)
            shot(page, "D03_modeC_mid", full_page=False)
            page.mouse.wheel(0, 2000); settle(page, 600)
            shot(page, "D04_modeC_member_votes", full_page=False)
            page.mouse.wheel(0, 4000); settle(page, 600)
            shot(page, "D05_modeC_bottom", full_page=False)
        except Exception as e:
            print(f"     mode C click failed: {e}")
    else:
        print("     no vote cards found")


# ─────────────────────────────────────────────────────────────────────────
# Phase E — TDs view & TD picker
# ─────────────────────────────────────────────────────────────────────────

def phase_td_picker(page: Page) -> None:
    print("\n== PHASE E — TDs view + picker ==")
    page.set_viewport_size(DESKTOP)
    goto(page)

    # The view-toggle is a segmented_control in the sidebar with options
    # "Dáil" / "TDs".  Click the TDs button.
    tds_btn = page.locator('label:has-text("TDs"), button:has-text("TDs")').first
    if tds_btn.count() == 0:
        # segmented_control renders as a label-wrapped radio; try a different
        # selector
        tds_btn = page.get_by_role("radio", name="TDs")
    try:
        tds_btn.first.click()
        settle(page, 1500)
        shot(page, "E01_tds_view_picker_landing")
        # Scroll through the picker
        page.mouse.wheel(0, 1500); settle(page, 600)
        shot(page, "E02_tds_view_picker_mid", full_page=False)
        page.mouse.wheel(0, 1500); settle(page, 600)
        shot(page, "E03_tds_view_picker_bottom", full_page=False)
    except Exception as e:
        print(f"     TDs view toggle failed: {e}")

    # Mobile version of the TD picker — important for citizen flow
    page.set_viewport_size(MOBILE)
    goto(page)
    try:
        # Streamlit collapses the sidebar on mobile; the toggle has to be
        # opened. Skip the toggle, just capture the main view in TDs mode.
        # Easiest: pre-set session state via URL... but votes.py doesn't have
        # one for view. Just capture the Mode A mobile (default).
        shot(page, "E04_tds_picker_mobile_note", full_page=False)
    except Exception as e:
        print(f"     mobile TD picker capture skipped: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase F — Mode B legacy redirect
# ─────────────────────────────────────────────────────────────────────────

def phase_mode_b_redirect(playwright_root) -> None:
    """Fresh browser context to avoid multi-page chain modal artifact."""
    print("\n== PHASE F — Mode B legacy redirect ==")
    browser = playwright_root.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP)
    page = ctx.new_page()
    try:
        # Use Mary Lou's real registry code (Phase 1 cross-page contract)
        import urllib.parse
        encoded = urllib.parse.quote("Mary-Lou-McDonald.D.2011-03-09", safe="")
        page.goto(f"{BASE}{ROUTE}?member={encoded}", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        page.screenshot(path=str(OUT / "F01_modeB_redirect.png"))
        print(f"  -> F01_modeB_redirect.png")
    finally:
        ctx.close()
        browser.close()


# ─────────────────────────────────────────────────────────────────────────
# Phase G — Mode C from an invalid vote_id (edge state)
# ─────────────────────────────────────────────────────────────────────────

def phase_invalid_vote(playwright_root) -> None:
    print("\n== PHASE G — Mode C invalid vote_id ==")
    browser = playwright_root.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP)
    page = ctx.new_page()
    try:
        page.goto(f"{BASE}{ROUTE}?vote=nonexistent-vote-id", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        page.screenshot(path=str(OUT / "G01_modeC_invalid_id.png"))
        print(f"  -> G01_modeC_invalid_id.png")
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
                ("A landing",          phase_landing),
                ("B filters",          phase_filters),
                ("C show all",         phase_show_all),
                ("D mode C detail",    phase_mode_c),
                ("E TDs picker",       phase_td_picker),
            ]:
                print(f"\n## {label}")
                try:
                    fn(page)
                except Exception as e:
                    print(f"  !! phase '{label}' failed: {e}")
        finally:
            ctx.close()
            browser.close()

        # Fresh-context phases (avoid multi-nav modal artifact)
        try:
            phase_mode_b_redirect(p)
        except Exception as e:
            print(f"  !! phase F failed: {e}")
        try:
            phase_invalid_vote(p)
        except Exception as e:
            print(f"  !! phase G failed: {e}")

    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
