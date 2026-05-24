"""Comprehensive SI page audit capture.

Walks /rankings-statutory-instruments exhaustively for an impeccable audit.

Output: tmp/audit_si/SS-XX-name.png (two-digit ordering per phase).

Notes on Streamlit + Playwright (learnt the hard way):
- Multi-page route is /rankings-statutory-instruments, NOT /statutory-instruments.
  Wrong slug silently renders the default page (Attendance) with a
  "Page not found" modal overlay — every screenshot looks like a different page.
- get_by_role(name=...) accepts str or re.Pattern, NOT a callable. Use
  re.compile(r"^Show these") for fuzzy text match on accessible names.
- Streamlit's websocket never goes idle; use explicit waits.
- The SI search box is the page's only text input; other text inputs in
  the DOM (search-a-member combobox in the multipage navigation, etc.) are
  not present on this page, so input[type=text] is safe here. Even so we
  scope by placeholder for clarity.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-statutory-instruments"
OUT = Path(__file__).resolve().parent

DESKTOP = {"width": 1440, "height": 900}
TABLET  = {"width": 820,  "height": 1180}
MOBILE  = {"width": 390,  "height": 844}

PAGE_LOAD_WAIT  = 5500   # initial domcontentloaded + first render
RERUN_WAIT      = 2400   # after a click / filter change
SETTLE_TINY     = 400

SEARCH_PLACEHOLDER = "Search SI titles"


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


def search_input(page: Page):
    """The SI title search box, scoped to its placeholder so we never grab a
    different text input."""
    return page.locator(f'input[placeholder*="{SEARCH_PLACEHOLDER}"]').first


def click_button_re(page: Page, pattern: str) -> bool:
    """Click the first <button> whose accessible name matches `pattern` (regex)."""
    btn = page.get_by_role("button", name=re.compile(pattern))
    if btn.count() > 0:
        try:
            btn.first.click()
            settle(page)
            return True
        except Exception as e:
            print(f"     click_button_re('{pattern}') failed: {e}")
            return False
    return False


def click_tab(page: Page, label_prefix: str) -> bool:
    tabs = page.locator('[role="tab"]')
    for i in range(tabs.count()):
        text = tabs.nth(i).inner_text().strip()
        if text.startswith(label_prefix):
            try:
                tabs.nth(i).click()
                settle(page, 1500)
                return True
            except Exception as e:
                print(f"     click_tab('{label_prefix}') failed: {e}")
                return False
    return False


def first_count_pill(page: Page):
    """First pill whose label carries the ' · N' count badge — i.e. any non-All
    facet option. Returns a Locator (may be empty)."""
    return page.locator('button:has-text(" · ")').first


# ─────────────────────────────────────────────────────────────────────────
# Phase A — Landing across three viewports
# ─────────────────────────────────────────────────────────────────────────

def phase_landing(page: Page) -> None:
    print("\n== PHASE A — landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_landing_full_desktop")
    shot(page, "A02_landing_above_fold_desktop", full_page=False)
    page.mouse.wheel(0, 1400); settle(page, 600)
    shot(page, "A03_landing_mid_desktop", full_page=False)
    page.mouse.wheel(0, 1400); settle(page, 600)
    shot(page, "A04_landing_cards_desktop", full_page=False)
    page.mouse.wheel(0, 4000); settle(page, 600)
    shot(page, "A05_landing_bottom_desktop", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase B — EU scrutiny callout
# ─────────────────────────────────────────────────────────────────────────

def phase_eu_callout(page: Page) -> None:
    print("\n== PHASE B — EU scrutiny callout ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "B01_callout_visible", full_page=False)

    if click_button_re(page, r"^Show these \d"):
        shot(page, "B02_callout_after_show_clicked")
        page.mouse.wheel(0, 1600); settle(page, 500)
        shot(page, "B03_callout_filter_applied_cards", full_page=False)
    else:
        print("  !! Show-these button not found")

    if click_button_re(page, r"^Clear all$"):
        shot(page, "B04_after_clear_all", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase C — Search box live
# ─────────────────────────────────────────────────────────────────────────

def phase_search(page: Page) -> None:
    print("\n== PHASE C — search ==")
    page.set_viewport_size(DESKTOP)
    goto(page)

    s = search_input(page)
    if s.count() == 0:
        print("  !! SI search input not found")
        return

    for query, name in [
        ("fisheries",  "C01_search_fisheries"),
        ("sanctions",  "C02_search_sanctions"),
        ("covid",      "C03_search_covid"),
        ("zzzzqqq",    "C04_search_zero_results"),
    ]:
        s.fill(query)
        settle(page, RERUN_WAIT)
        shot(page, name)
        page.mouse.wheel(0, 1500); settle(page, 500)
        shot(page, f"{name}_cards", full_page=False)
        page.mouse.wheel(0, -10000); settle(page, 400)

    s.fill("")
    settle(page, RERUN_WAIT)


# ─────────────────────────────────────────────────────────────────────────
# Phase D — Facet tabs
# ─────────────────────────────────────────────────────────────────────────

def phase_facets(page: Page) -> None:
    print("\n== PHASE D — facet tabs ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 700); settle(page, 600)

    if click_tab(page, "Department"):
        shot(page, "D01_tab_department_pills", full_page=False)
        cand = first_count_pill(page)
        if cand.count() > 0:
            try:
                txt = cand.inner_text()
                cand.click()
                settle(page)
                shot(page, "D02_tab_department_one_selected", full_page=False)
                print(f"     selected dept pill: {txt[:50]}")
            except Exception as e:
                print(f"     dept pill click failed: {e}")
        click_button_re(page, r"^Clear all$")

    if click_tab(page, "What it does"):
        shot(page, "D03_tab_operation_pills", full_page=False)
        cand = first_count_pill(page)
        if cand.count() > 0:
            try:
                cand.click(); settle(page)
                shot(page, "D04_tab_operation_one_selected", full_page=False)
            except Exception as e:
                print(f"     op pill click failed: {e}")
        click_button_re(page, r"^Clear all$")

    if click_tab(page, "Policy area"):
        shot(page, "D05_tab_policy_pills", full_page=False)
        cand = first_count_pill(page)
        if cand.count() > 0:
            try:
                cand.click(); settle(page)
                shot(page, "D06_tab_policy_one_selected", full_page=False)
            except Exception as e:
                print(f"     policy pill click failed: {e}")
        click_button_re(page, r"^Clear all$")

    if click_tab(page, "Minister"):
        shot(page, "D07_tab_minister_selectbox", full_page=False)
        sb = page.locator('[data-baseweb="select"]').first
        if sb.count() > 0:
            try:
                sb.click(); settle(page, 800)
                shot(page, "D08_tab_minister_dropdown_open", full_page=False)
                opts = page.locator('[role="option"]')
                if opts.count() > 1:
                    opts.nth(1).click(); settle(page)
                    shot(page, "D09_tab_minister_one_selected", full_page=False)
            except Exception as e:
                print(f"     minister select failed: {e}")
        click_button_re(page, r"^Clear all$")

    if click_tab(page, "EU scrutiny") or click_tab(page, "⚠ EU"):
        shot(page, "D10_tab_eu_scrutiny")


# ─────────────────────────────────────────────────────────────────────────
# Phase E — Active filter bar combinations
# ─────────────────────────────────────────────────────────────────────────

def phase_active_filter_bar(page: Page) -> None:
    print("\n== PHASE E — active filter bar ==")
    page.set_viewport_size(DESKTOP)

    goto(page)
    page.mouse.wheel(0, 600); settle(page, 400)
    shot(page, "E01_single_year_filter", full_page=False)

    eu = page.locator('label:has-text("EU-derived only")').first
    if eu.count() > 0:
        try:
            eu.click(); settle(page)
        except Exception as e:
            print(f"     EU toggle click failed: {e}")
    s = search_input(page)
    if s.count() > 0:
        s.fill("regulations"); settle(page)
    if click_tab(page, "Department"):
        cand = first_count_pill(page)
        if cand.count() > 0:
            try:
                cand.click(); settle(page)
            except Exception as e:
                print(f"     dept multi-filter pill failed: {e}")

    page.mouse.wheel(0, -800); settle(page, 400)
    shot(page, "E02_multi_filter_bar", full_page=False)

    if click_button_re(page, r"^Clear all$"):
        settle(page, 600)
        shot(page, "E03_after_clear_all", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase F — Pagination
# ─────────────────────────────────────────────────────────────────────────

def phase_pagination(page: Page) -> None:
    print("\n== PHASE F — pagination ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 12000); settle(page, 700)
    shot(page, "F01_pagination_footer", full_page=False)

    # Try Next-style navigation. Pagination controls may use chevrons or
    # text — try several variants.
    for pat in (r"Next", r"→", r">", r"^›$"):
        if click_button_re(page, pat):
            page.mouse.wheel(0, 12000); settle(page, 700)
            shot(page, "F02_pagination_page2", full_page=False)
            break

    for pat in (r"Last", r"»", r"^›\s*›$"):
        if click_button_re(page, pat):
            page.mouse.wheel(0, 12000); settle(page, 700)
            shot(page, "F03_pagination_last_page", full_page=False)
            break


# ─────────────────────────────────────────────────────────────────────────
# Phase G — SI detail variants
# ─────────────────────────────────────────────────────────────────────────

def open_first_detail(page: Page) -> str | None:
    """Click the first 'View detail →' button. Returns the resulting ?si= id
    or None."""
    btn = page.get_by_role("button", name=re.compile(r"View detail"))
    if btn.count() == 0:
        return None
    try:
        btn.first.click()
        settle(page, RERUN_WAIT + 600)
    except Exception as e:
        print(f"     view-detail click failed: {e}")
        return None
    url = page.url
    return url.split("si=")[-1].split("&")[0] if "si=" in url else None


def phase_si_detail(page: Page) -> None:
    print("\n== PHASE G — SI detail variants ==")
    page.set_viewport_size(DESKTOP)

    # G1: First card on default landing
    goto(page)
    page.mouse.wheel(0, 1500); settle(page, 500)
    sid = open_first_detail(page)
    if sid:
        print(f"     opened SI {sid}")
        shot(page, "G01_detail_default_full")
        shot(page, "G02_detail_default_above_fold", full_page=False)
        page.mouse.wheel(0, 1200); settle(page, 500)
        shot(page, "G03_detail_default_mid", full_page=False)
        page.mouse.wheel(0, 1500); settle(page, 500)
        shot(page, "G04_detail_default_bottom", full_page=False)

    # G2: EU-derived only, signed since Dec 2025 (callout scope)
    goto(page)
    if click_button_re(page, r"^Show these \d"):
        page.mouse.wheel(0, 1800); settle(page, 500)
        sid = open_first_detail(page)
        if sid:
            shot(page, "G05_detail_eu_minister_full")
            shot(page, "G06_detail_eu_minister_above_fold", full_page=False)

    # G3: Deep-link round-trip (cold load from URL)
    if sid:
        goto(page, f"{ROUTE}?si={sid}")
        shot(page, "G07_detail_deeplink_cold_load")
        # Back button
        if click_button_re(page, r"Back to SI Index"):
            shot(page, "G08_after_back_button", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase H — Empty / not-found
# ─────────────────────────────────────────────────────────────────────────

def phase_empty_states(page: Page) -> None:
    print("\n== PHASE H — empty / not-found ==")
    page.set_viewport_size(DESKTOP)

    goto(page)
    s = search_input(page)
    if s.count() > 0:
        s.fill("xx_no_match_xx"); settle(page, RERUN_WAIT)
        page.mouse.wheel(0, 1500); settle(page, 500)
        shot(page, "H01_no_results_filter", full_page=False)

    goto(page, f"{ROUTE}?si=BOGUS_NONEXISTENT")
    shot(page, "H02_detail_not_found")


# ─────────────────────────────────────────────────────────────────────────
# Phase I — Tablet + mobile
# ─────────────────────────────────────────────────────────────────────────

def phase_responsive(page: Page) -> None:
    print("\n== PHASE I — responsive ==")

    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "I01_tablet_landing_full")
    shot(page, "I02_tablet_above_fold", full_page=False)
    page.mouse.wheel(0, 1500); settle(page, 500)
    shot(page, "I03_tablet_facets", full_page=False)
    page.mouse.wheel(0, 1500); settle(page, 500)
    shot(page, "I04_tablet_cards", full_page=False)

    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "I05_mobile_landing_full")
    shot(page, "I06_mobile_above_fold", full_page=False)
    page.mouse.wheel(0, 1200); settle(page, 500)
    shot(page, "I07_mobile_callout", full_page=False)
    page.mouse.wheel(0, 1200); settle(page, 500)
    shot(page, "I08_mobile_facet_tabs", full_page=False)
    page.mouse.wheel(0, 1200); settle(page, 500)
    shot(page, "I09_mobile_cards", full_page=False)
    page.mouse.wheel(0, 5000); settle(page, 500)
    shot(page, "I10_mobile_pagination", full_page=False)

    sid = open_first_detail(page)
    if sid:
        shot(page, "I11_mobile_detail_full")
        shot(page, "I12_mobile_detail_above_fold", full_page=False)

    page.set_viewport_size(DESKTOP)


# ─────────────────────────────────────────────────────────────────────────
# Phase J — Keyboard focus
# ─────────────────────────────────────────────────────────────────────────

def phase_focus_ring(page: Page) -> None:
    print("\n== PHASE J — keyboard focus ==")
    page.set_viewport_size(DESKTOP)
    goto(page)

    s = search_input(page)
    if s.count() > 0:
        try:
            s.click(timeout=8000); settle(page, 400)
            shot(page, "J01_focus_search", full_page=False)
            for _ in range(8):
                page.keyboard.press("Tab"); wait(250)
            shot(page, "J02_focus_after_8_tabs", full_page=False)
        except Exception as e:
            print(f"     focus capture failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase K — Year-pill edge cases
# ─────────────────────────────────────────────────────────────────────────

def phase_year_pills(page: Page) -> None:
    print("\n== PHASE K — year pills ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 600); settle(page, 400)
    shot(page, "K01_year_default_three", full_page=False)


# ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()

        phases = [
            ("LANDING",      phase_landing),
            ("EU CALLOUT",   phase_eu_callout),
            ("SEARCH",       phase_search),
            ("FACETS",       phase_facets),
            ("ACTIVE BAR",   phase_active_filter_bar),
            ("PAGINATION",   phase_pagination),
            ("SI DETAIL",    phase_si_detail),
            ("EMPTY",        phase_empty_states),
            ("RESPONSIVE",   phase_responsive),
            ("FOCUS",        phase_focus_ring),
            ("YEAR PILLS",   phase_year_pills),
        ]
        for label, fn in phases:
            print(f"\n========== {label} ==========")
            try:
                fn(page)
            except Exception as e:
                print(f"  !! phase {label} crashed: {e}")

        ctx.close()
        browser.close()
    print(f"\nDONE. Screenshots in {OUT}")


if __name__ == "__main__":
    main()
