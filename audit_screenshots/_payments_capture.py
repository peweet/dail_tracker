"""Comprehensive payments page audit capture.

Walks /rankings-payments exhaustively for an impeccable audit.

Output: audit_screenshots/payments/<phase><nn>_name.png

Page model:
- Primary view = segmented control selecting "Rankings" (since-2020) OR a year (2020–2025)
- "Rankings" view: 3 st.metric cards + two-column 1–10 / 11–20 leaderboard cards
- Year view: pay-totals-strip header + 1–10 / 11–20 cards with TAA-band pills + payment-count
- Sidebar = member-name search + notable chips → both redirect to /member-overview
- Card click → redirect to /member-overview (post-Phase-6)

Fixes vs _attendance_capture.py:
- INITIAL_LOAD_WAIT bumped to 10000 ms — Streamlit cold start was leaving
  pages blank in 6 s.
- Subsequent navigations use a shorter wait (3500 ms) because the app is
  warm.
- For mobile scrolling we use page.evaluate() with window.scrollTo so
  the inner DOM moves, not the parent wheel-event proxy.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-payments"
OUT = Path(__file__).resolve().parent / "payments"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
TABLET  = {"width": 820,  "height": 1180}
MOBILE  = {"width": 390,  "height": 844}

INITIAL_LOAD_WAIT = 10000   # cold-start hydrate
WARM_LOAD_WAIT    = 3500    # second goto onwards
RERUN_WAIT        = 2500
SETTLE_TINY       = 400

_cold = True  # toggled to False after the first goto


def shot(page: Page, name: str, *, full_page: bool = True) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    kb = path.stat().st_size // 1024
    print(f"  -> {path.name} ({kb} KB)")


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def goto(page: Page, path: str = ROUTE) -> None:
    global _cold
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(INITIAL_LOAD_WAIT if _cold else WARM_LOAD_WAIT)
    _cold = False


def settle(page: Page, ms: int = RERUN_WAIT) -> None:
    wait(ms)


def scroll_to(page: Page, y: int) -> None:
    """Scroll the page's main document — works for Streamlit's main pane."""
    page.evaluate(f"window.scrollTo({{top: {y}, behavior: 'instant'}})")
    wait(SETTLE_TINY)


def scroll_by(page: Page, dy: int) -> None:
    page.evaluate(f"window.scrollBy({{top: {dy}, behavior: 'instant'}})")
    wait(SETTLE_TINY)


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
    return False


def click_segmented(page: Page, label: str) -> bool:
    """Click an option in a stSegmentedControl."""
    btn = page.locator(f'[data-testid="stSegmentedControl"] button:has-text("{label}")').first
    if btn.count() == 0:
        btn = page.get_by_role("button", name=re.compile(rf"^{re.escape(label)}$"))
    if btn.count() > 0:
        try:
            btn.first.click()
            settle(page)
            return True
        except Exception as e:
            print(f"     segmented click ({label}) failed: {e}")
            return False
    return False


# ─────────────────────────────────────────────────────────────────────────
# Phase A — Landing (default segmented view)
# ─────────────────────────────────────────────────────────────────────────

def phase_landing(page: Page) -> None:
    print("\n== PHASE A — landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_landing_full_desktop")
    shot(page, "A02_landing_above_fold_desktop", full_page=False)
    scroll_by(page, 700); settle(page, 500)
    shot(page, "A03_landing_totals_and_cards", full_page=False)
    scroll_by(page, 900); settle(page, 500)
    shot(page, "A04_landing_cards_lower", full_page=False)
    scroll_by(page, 1500); settle(page, 500)
    shot(page, "A05_landing_export_and_provenance", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase B — Segmented control: Rankings + each year
# ─────────────────────────────────────────────────────────────────────────

def phase_segmented_views(page: Page) -> None:
    print("\n== PHASE B — segmented views ==")
    page.set_viewport_size(DESKTOP)
    goto(page)

    if click_segmented(page, "Rankings"):
        scroll_to(page, 0); shot(page, "B01_view_rankings_above_fold", full_page=False)
        scroll_by(page, 700); settle(page, 500)
        shot(page, "B02_view_rankings_cards", full_page=False)
        scroll_by(page, 1500); settle(page, 500)
        shot(page, "B03_view_rankings_provenance", full_page=False)

    # Year views
    for yr in ("2024", "2023", "2020"):
        if click_segmented(page, yr):
            scroll_to(page, 0)
            shot(page, f"B04_view_{yr}_above_fold", full_page=False)
            scroll_by(page, 700); settle(page, 400)
            shot(page, f"B05_view_{yr}_cards", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase C — Sidebar interaction
# ─────────────────────────────────────────────────────────────────────────

def phase_sidebar(page: Page) -> None:
    print("\n== PHASE C — sidebar ==")
    page.set_viewport_size(DESKTOP)
    goto(page)

    shot(page, "C01_sidebar_default", full_page=False)

    # Name search
    search = page.locator('[data-testid="stSidebar"] input[type="text"]').first
    if search.count() > 0:
        try:
            search.fill("McDonald"); settle(page, RERUN_WAIT)
            shot(page, "C02_sidebar_member_search_filtered", full_page=False)
        except Exception as e:
            print(f"     sidebar search failed: {e}")

    # Notable chip → redirect
    goto(page)
    chip = page.locator('[data-testid="stSidebar"] button').filter(
        has_text=re.compile(r"^(Martin|Harris|McDonald|Doherty|Healy-Rae|Cairns|Lowry)$")
    ).first
    if chip.count() > 0:
        try:
            chip.click(); settle(page, RERUN_WAIT)
            shot(page, "C03_after_notable_chip_redirect")
        except Exception as e:
            print(f"     chip click failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase D — Card-click redirect (cross-page link via clickable_card_link)
# ─────────────────────────────────────────────────────────────────────────

def phase_card_click(page: Page) -> None:
    print("\n== PHASE D — card click redirect ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    # Pick a year view that uses clickable_card_link wrappers
    click_segmented(page, "2024")
    scroll_by(page, 700); settle(page, 400)
    link = page.locator('a:has(.dt-name-card), a[href*="member-overview"]').first
    if link.count() > 0:
        href = link.get_attribute("href")
        print(f"     opening {href}")
        try:
            link.click(); settle(page, INITIAL_LOAD_WAIT)
            shot(page, "D01_card_click_to_member_overview")
            shot(page, "D02_member_overview_above_fold", full_page=False)
        except Exception as e:
            print(f"     card link click failed: {e}")
    else:
        print("     no card links found")

    # Legacy ?member= redirect
    goto(page, f"{ROUTE}?member=Catherine%20Connolly")
    shot(page, "D03_legacy_member_param_redirect", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase E — Empty / not-found
# ─────────────────────────────────────────────────────────────────────────

def phase_empty(page: Page) -> None:
    print("\n== PHASE E — empty / edge ==")
    page.set_viewport_size(DESKTOP)
    goto(page, f"{ROUTE}?member=NONEXISTENT_PERSON")
    shot(page, "E01_bogus_member_redirect", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase F — Provenance expander
# ─────────────────────────────────────────────────────────────────────────

def phase_provenance(page: Page) -> None:
    print("\n== PHASE F — provenance ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    scroll_by(page, 6000); settle(page, 500)
    exp = page.locator('summary, button').filter(
        has_text=re.compile(r"About.*data|provenance|source", re.IGNORECASE)
    ).first
    if exp.count() > 0:
        try:
            exp.click(); settle(page, 800)
            shot(page, "F01_provenance_open", full_page=False)
            scroll_by(page, 600); settle(page, 300)
            shot(page, "F02_provenance_taa_bands", full_page=False)
            scroll_by(page, 800); settle(page, 300)
            shot(page, "F03_provenance_pdf_links", full_page=False)
        except Exception as e:
            print(f"     provenance click failed: {e}")
    else:
        print("     provenance expander not found")


# ─────────────────────────────────────────────────────────────────────────
# Phase G — Tablet + mobile
# ─────────────────────────────────────────────────────────────────────────

def phase_responsive(page: Page) -> None:
    print("\n== PHASE G — responsive ==")

    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "G01_tablet_landing_full")
    shot(page, "G02_tablet_above_fold", full_page=False)
    scroll_by(page, 1000); settle(page, 400)
    shot(page, "G03_tablet_cards", full_page=False)
    scroll_by(page, 1500); settle(page, 400)
    shot(page, "G04_tablet_export_and_footer", full_page=False)

    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "G05_mobile_landing_full")
    shot(page, "G06_mobile_above_fold", full_page=False)
    scroll_by(page, 500); settle(page, 400)
    shot(page, "G07_mobile_after_500", full_page=False)
    scroll_by(page, 700); settle(page, 400)
    shot(page, "G08_mobile_after_1200", full_page=False)
    scroll_by(page, 900); settle(page, 400)
    shot(page, "G09_mobile_card_list", full_page=False)
    scroll_by(page, 1500); settle(page, 400)
    shot(page, "G10_mobile_export_provenance", full_page=False)

    # Mobile in Rankings view
    if click_segmented(page, "Rankings"):
        scroll_to(page, 0)
        shot(page, "G11_mobile_rankings_above_fold", full_page=False)
        scroll_by(page, 700); settle(page, 400)
        shot(page, "G12_mobile_rankings_metrics_and_cards", full_page=False)

    page.set_viewport_size(DESKTOP)


# ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()

        phases = [
            ("LANDING",     phase_landing),
            ("SEGMENTED",   phase_segmented_views),
            ("SIDEBAR",     phase_sidebar),
            ("CARD CLICK",  phase_card_click),
            ("EMPTY",       phase_empty),
            ("PROVENANCE",  phase_provenance),
            ("RESPONSIVE",  phase_responsive),
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
