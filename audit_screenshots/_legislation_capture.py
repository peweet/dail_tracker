"""Comprehensive Legislation page audit capture.

Walks /rankings-legislation exhaustively for an impeccable audit. The
page has two top-level states:
  - Index: hero + pipeline strip (Dáil/Seanad/Enacted) + Government-Bills
    todo callout + phase selector (All / Dáil / Seanad / Enacted) +
    bill cards (paginated 10/page) + export + provenance
  - Bill detail (?bill=X): identity strip + stat strip + two-column
    (timeline | debates) + Documents + SIs under this Act + provenance

Plus the sidebar filter command bar (date range, status dropdown,
title search), the pre-2014 Act synthetic-ID flow (`act_<year>_<slug>`),
and the SI year/operation pills nested inside bill detail.

Output: audit_screenshots/_legislation/LG-X-name.png.

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
ROUTE = "/rankings-legislation"
OUT = Path(__file__).resolve().parent / "_legislation"
OUT.mkdir(parents=True, exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
TABLET  = {"width": 820,  "height": 1180}
MOBILE  = {"width": 390,  "height": 844}

PAGE_LOAD_WAIT  = 6000
RERUN_WAIT      = 2500


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
# Phase A -- Index landing (default, no filters)
# --------------------------------------------------------------------------

def phase_landing(page: Page) -> None:
    print("\n== PHASE A -- Index landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_index_full_desktop")
    shot(page, "A02_index_above_fold", full_page=False)
    page.mouse.wheel(0, 700); settle(page, 600)
    shot(page, "A03_index_pipeline_strip", full_page=False)
    page.mouse.wheel(0, 800); settle(page, 600)
    shot(page, "A04_index_cards", full_page=False)
    page.mouse.wheel(0, 4000); settle(page, 600)
    shot(page, "A05_index_bottom_provenance", full_page=False)

    # Tablet
    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "A06_index_tablet")

    # Mobile
    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "A07_index_mobile")
    page.mouse.wheel(0, 1000); settle(page, 500)
    shot(page, "A08_index_mobile_cards", full_page=False)
    page.mouse.wheel(0, 2000); settle(page, 500)
    shot(page, "A09_index_mobile_bottom", full_page=False)


# --------------------------------------------------------------------------
# Phase B -- Phase selector (Dáil, Seanad, Enacted)
# --------------------------------------------------------------------------

def phase_selector(page: Page) -> None:
    print("\n== PHASE B -- phase selector ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 800); settle(page, 600)

    # The segmented control buttons have labels like "Dáil Stages (N)"
    for label_pattern, fname in [
        (r"D[áa]il Stages", "B01_phase_dail"),
        (r"Seanad Stages",   "B02_phase_seanad"),
        (r"Enacted",         "B03_phase_enacted"),
    ]:
        btn = page.get_by_role("button", name=re.compile(label_pattern))
        if btn.count() > 0:
            try:
                btn.first.click()
                settle(page, RERUN_WAIT)
                shot(page, fname)
                page.mouse.wheel(0, 800); settle(page, 600)
                shot(page, f"{fname}_cards", full_page=False)
                page.mouse.wheel(0, -800); settle(page, 400)
            except Exception as e:
                print(f"     phase '{label_pattern}' click failed: {e}")
        else:
            print(f"     phase '{label_pattern}' button not found")

    # Reset to All
    all_btn = page.get_by_role("button", name=re.compile(r"^All "))
    if all_btn.count() > 0:
        try:
            all_btn.first.click()
            settle(page, RERUN_WAIT)
        except Exception:
            pass


# --------------------------------------------------------------------------
# Phase C -- Sidebar filters (status dropdown, title search)
# --------------------------------------------------------------------------

def phase_filters(page: Page) -> None:
    print("\n== PHASE C -- filters ==")
    page.set_viewport_size(DESKTOP)
    goto(page)

    # Status dropdown — sidebar selectbox
    status_box = page.locator('[data-baseweb="select"]').first
    if status_box.count() > 0:
        try:
            status_box.click()
            settle(page, 800)
            opts = page.locator('[role="option"]')
            n = opts.count()
            print(f"     {n} status options")
            if n > 1:
                # pick option #2 (skip "All" at index 0)
                opts.nth(1).click()
                settle(page, RERUN_WAIT)
                shot(page, "C01_status_filter_selected")
        except Exception as e:
            print(f"     status filter failed: {e}")

    # Reset by reloading
    goto(page)

    # Title search — text_input in sidebar
    title_search = page.locator('input[type="text"]').first
    if title_search.count() > 0:
        try:
            title_search.fill("Housing")
            settle(page, RERUN_WAIT)
            # Press Enter to apply
            title_search.press("Enter")
            settle(page, RERUN_WAIT)
            shot(page, "C02_title_search_housing")
            page.mouse.wheel(0, 800); settle(page, 500)
            shot(page, "C03_title_search_housing_cards", full_page=False)
        except Exception as e:
            print(f"     title search failed: {e}")


# --------------------------------------------------------------------------
# Phase D -- Pagination
# --------------------------------------------------------------------------

def phase_paginate(page: Page) -> None:
    print("\n== PHASE D -- pagination ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 5000); settle(page, 800)
    shot(page, "D01_paginator_at_bottom", full_page=False)
    if click_button_re(page, r"^2$") or click_button_re(page, r"Next"):
        shot(page, "D02_page_2")
        page.mouse.wheel(0, 800); settle(page, 600)
        shot(page, "D03_page_2_cards", full_page=False)
    else:
        print("     no paginator buttons matched")


# --------------------------------------------------------------------------
# Phase E -- Bill detail (click first card)
# --------------------------------------------------------------------------

def phase_detail(page: Page) -> None:
    print("\n== PHASE E -- bill detail ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    # Cards are <a href="?bill=...">
    bill_links = page.locator('a[href*="bill="]')
    n = bill_links.count()
    print(f"     {n} bill links visible")
    if n > 0:
        try:
            bill_links.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "E01_bill_detail_full")
            shot(page, "E02_bill_detail_above_fold", full_page=False)
            page.mouse.wheel(0, 800); settle(page, 600)
            shot(page, "E03_bill_detail_timeline_debates", full_page=False)
            page.mouse.wheel(0, 800); settle(page, 600)
            shot(page, "E04_bill_detail_documents", full_page=False)
            page.mouse.wheel(0, 1000); settle(page, 600)
            shot(page, "E05_bill_detail_sis", full_page=False)
            page.mouse.wheel(0, 2500); settle(page, 600)
            shot(page, "E06_bill_detail_bottom", full_page=False)
        except Exception as e:
            print(f"     bill detail flow failed: {e}")


# --------------------------------------------------------------------------
# Phase F -- SI year/operation pills (within bill detail)
# --------------------------------------------------------------------------

def phase_si_filters(page: Page) -> None:
    print("\n== PHASE F -- SI year/operation pills ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    bill_links = page.locator('a[href*="bill="]')
    if bill_links.count() == 0:
        print("     no bill links visible")
        return
    try:
        bill_links.first.click()
        settle(page, RERUN_WAIT)
        # Scroll to SI section
        page.mouse.wheel(0, 2500); settle(page, 600)
        shot(page, "F01_si_section_default", full_page=False)
        # Try to click a year pill that isn't "All years"
        year_pills = page.get_by_role("button", name=re.compile(r"^20\d{2}$"))
        if year_pills.count() > 0:
            try:
                year_pills.first.click()
                settle(page, RERUN_WAIT)
                shot(page, "F02_si_year_pill_selected", full_page=False)
            except Exception as e:
                print(f"     SI year pill failed: {e}")
        else:
            print("     no SI year pills visible — bill may have no SIs")
    except Exception as e:
        print(f"     SI section flow failed: {e}")


# --------------------------------------------------------------------------
# Phase G -- Back button + URL clearing
# --------------------------------------------------------------------------

def phase_back(page: Page) -> None:
    print("\n== PHASE G -- back button ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    bill_links = page.locator('a[href*="bill="]')
    if bill_links.count() == 0:
        return
    try:
        bill_links.first.click()
        settle(page, RERUN_WAIT)
        # Back button — back_button() in components.py
        back_btn = page.get_by_role("button", name=re.compile(r"Back.*Legislation"))
        if back_btn.count() > 0:
            try:
                back_btn.first.click()
                settle(page, RERUN_WAIT)
                shot(page, "G01_back_to_index", full_page=False)
            except Exception as e:
                print(f"     back button click failed: {e}")
    except Exception as e:
        print(f"     back flow failed: {e}")


# --------------------------------------------------------------------------
# Phase H -- Empty-state: bogus title search
# --------------------------------------------------------------------------

def phase_empty(page: Page) -> None:
    print("\n== PHASE H -- empty state ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    title_search = page.locator('input[type="text"]').first
    if title_search.count() > 0:
        try:
            title_search.fill("zzzzz-nonexistent-bill-title")
            settle(page, 1500)
            title_search.press("Enter")
            settle(page, RERUN_WAIT)
            shot(page, "H01_empty_state_no_match", full_page=False)
        except Exception as e:
            print(f"     empty-state flow failed: {e}")


# --------------------------------------------------------------------------
# Phase I -- Mobile bill detail (fresh context)
# --------------------------------------------------------------------------

def phase_mobile_detail(playwright_root) -> None:
    print("\n== PHASE I -- mobile bill detail ==")
    browser = playwright_root.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=MOBILE)
    page = ctx.new_page()
    try:
        page.goto(f"{BASE}{ROUTE}", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        bill_links = page.locator('a[href*="bill="]')
        if bill_links.count() > 0:
            try:
                bill_links.first.click()
                wait(RERUN_WAIT)
                page.screenshot(path=str(OUT / "I01_mobile_bill_detail.png"), full_page=True)
                print("  -> I01_mobile_bill_detail.png")
                page.mouse.wheel(0, 1200); wait(600)
                page.screenshot(path=str(OUT / "I02_mobile_bill_detail_mid.png"))
                print("  -> I02_mobile_bill_detail_mid.png")
            except Exception as e:
                print(f"     mobile detail click failed: {e}")
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
                ("B selector",  phase_selector),
                ("C filters",   phase_filters),
                ("D paginate",  phase_paginate),
                ("E detail",    phase_detail),
                ("F si pills",  phase_si_filters),
                ("G back",      phase_back),
                ("H empty",     phase_empty),
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
            phase_mobile_detail(p)
        except Exception as e:
            print(f"  !! phase I failed: {e}")

    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
