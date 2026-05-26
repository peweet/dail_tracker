"""Comprehensive attendance page audit capture.

Walks /rankings-attendance exhaustively for an impeccable audit.

Output: tmp/audit_attendance/<phase><nn>_name.png — two-digit ordering per phase.

Page model (post-Phase-6):
- Primary view = good-cop / bad-cop hall (top-15 vs bottom-15) keyed by year pills
- Sidebar = member-name search + notable chips → both redirect to /member-overview
- Card click → redirect to /member-overview (no in-page profile branch)
- Missing-members expander + provenance expander at the foot

Phases:
  A landing across viewports
  B year-pill interaction (default / older year / current YTD)
  C sidebar interaction (notable chip click → redirect)
  D card-click redirect (member moved callout)
  E missing-members expander
  F provenance expander
  G empty state (bogus year via query param if supported, else hover legacy URLs)
  H tablet + mobile responsive
  I keyboard focus traversal

Streamlit + Playwright gotchas (from feedback_streamlit_playwright):
- Route is /rankings-attendance (NOT /attendance). Wrong slug silently renders
  the default page and a "Page not found" modal floats above every screenshot.
- get_by_role(name=...) accepts str or re.Pattern, not callables.
- Streamlit's websocket never goes idle — use explicit waits.
- full_page=True is sometimes truncated on Streamlit. Take both full + above-fold.
"""
from __future__ import annotations

import re
import sys
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-attendance"
OUT = Path(__file__).resolve().parent / "attendance"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
TABLET  = {"width": 820,  "height": 1180}
MOBILE  = {"width": 390,  "height": 844}

PAGE_LOAD_WAIT = 6000
RERUN_WAIT     = 2500
SETTLE_TINY    = 400


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


def open_sidebar(page: Page) -> None:
    """Ensure the sidebar is open. The collapse arrow is the first button in
    the document on Streamlit; the page CSS hides toolbar but keeps sidebar
    toggle."""
    btn = page.locator('[data-testid="stSidebarCollapseButton"] button').first
    if btn.count() > 0:
        try:
            btn.click(timeout=2000)
            settle(page, SETTLE_TINY)
        except Exception:
            pass


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


def click_year_pill(page: Page, year: str) -> bool:
    """Year pills are rendered by st.pills inside a stPills group. Each option
    is a button-like element. Click the first whose accessible name matches
    the year string exactly."""
    pill = page.locator(f'[data-testid="stPills"] button:has-text("{year}")').first
    if pill.count() == 0:
        # fallback: any button whose label is the year
        pill = page.get_by_role("button", name=re.compile(rf"^{year}$"))
    if pill.count() > 0:
        try:
            pill.first.click()
            settle(page)
            return True
        except Exception as e:
            print(f"     year-pill click ({year}) failed: {e}")
            return False
    return False


# ─────────────────────────────────────────────────────────────────────────
# Phase A — Landing
# ─────────────────────────────────────────────────────────────────────────

def phase_landing(page: Page) -> None:
    print("\n== PHASE A — landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_landing_full_desktop")
    shot(page, "A02_landing_above_fold_desktop", full_page=False)
    page.mouse.wheel(0, 900); settle(page, 500)
    shot(page, "A03_landing_hall_cards_top", full_page=False)
    page.mouse.wheel(0, 900); settle(page, 500)
    shot(page, "A04_landing_hall_cards_bottom", full_page=False)
    page.mouse.wheel(0, 2400); settle(page, 500)
    shot(page, "A05_landing_export_and_missing", full_page=False)
    page.mouse.wheel(0, 1600); settle(page, 500)
    shot(page, "A06_landing_provenance_area", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase B — Year pill interaction
# ─────────────────────────────────────────────────────────────────────────

def phase_year_pills(page: Page) -> None:
    print("\n== PHASE B — year pills ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 250); settle(page, 400)
    shot(page, "B01_year_pills_default", full_page=False)

    # Click 2024 — should be a completed year showing the full top/bottom split
    if click_year_pill(page, "2024"):
        page.mouse.wheel(0, 600); settle(page, 500)
        shot(page, "B02_year_2024_hall_split")
        page.mouse.wheel(0, -600); settle(page, 300)

    # Click 2026 — should be the in-progress year
    if click_year_pill(page, "2026"):
        page.mouse.wheel(0, 600); settle(page, 500)
        shot(page, "B03_year_2026_in_progress")
        page.mouse.wheel(0, -600); settle(page, 300)

    # Click 2020 — earliest year; might be missing or low-data
    if click_year_pill(page, "2020"):
        page.mouse.wheel(0, 600); settle(page, 500)
        shot(page, "B04_year_2020_earliest", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase C — Sidebar interaction
# ─────────────────────────────────────────────────────────────────────────

def phase_sidebar(page: Page) -> None:
    print("\n== PHASE C — sidebar ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    open_sidebar(page)
    shot(page, "C01_sidebar_default", full_page=False)

    # Type into the member search
    search = page.locator('[data-testid="stSidebar"] input[type="text"]').first
    if search.count() > 0:
        try:
            search.fill("McDonald"); settle(page, RERUN_WAIT)
            shot(page, "C02_sidebar_member_search_filtered", full_page=False)
        except Exception as e:
            print(f"     sidebar search fill failed: {e}")

    # Reset and click a notable chip
    goto(page)
    open_sidebar(page)
    # Notable chips are buttons with key prefix chip_att_<name>
    chip = page.locator('[data-testid="stSidebar"] button').filter(has_text=re.compile(r"^(Martin|Harris|McDonald|Bacik|Murphy|Tóibín)$")).first
    if chip.count() > 0:
        try:
            chip.click(); settle(page, RERUN_WAIT)
            shot(page, "C03_after_notable_chip_redirect")
        except Exception as e:
            print(f"     chip click failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase D — Card-click redirect
# ─────────────────────────────────────────────────────────────────────────

def phase_card_click(page: Page) -> None:
    print("\n== PHASE D — card click redirect ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    # Cards are anchor-wrapped att-hall-card-good / bad
    card_link = page.locator('a:has(.att-hall-card-good), a:has(.att-hall-card-bad)').first
    if card_link.count() > 0:
        try:
            href = card_link.get_attribute("href") or "(unknown)"
            print(f"     clicking first card → {href}")
            card_link.click()
            settle(page, PAGE_LOAD_WAIT)
            shot(page, "D01_card_click_lands_on_member_overview")
            shot(page, "D02_member_overview_above_fold", full_page=False)
        except Exception as e:
            print(f"     card-link click failed: {e}")
    else:
        print("     no card links found — falling back to deep link")

    # Direct legacy URL — ?att_td=<name> should trigger member_moved_callout
    goto(page, f"{ROUTE}?att_td=Catherine%20Connolly")
    shot(page, "D03_legacy_att_td_param_redirect", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase E — Missing members expander
# ─────────────────────────────────────────────────────────────────────────

def phase_missing_members(page: Page) -> None:
    print("\n== PHASE E — missing members ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 5000); settle(page, 600)
    # Expander label starts with "⚠"
    exp = page.locator('summary, button').filter(has_text=re.compile(r"TDs do not appear")).first
    if exp.count() > 0:
        try:
            exp.click(); settle(page, 800)
            shot(page, "E01_missing_members_open")
            page.mouse.wheel(0, 1500); settle(page, 400)
            shot(page, "E02_missing_members_no_record_group", full_page=False)
        except Exception as e:
            print(f"     missing-members expander click failed: {e}")
    else:
        print("     missing-members expander not found")


# ─────────────────────────────────────────────────────────────────────────
# Phase F — Provenance expander
# ─────────────────────────────────────────────────────────────────────────

def phase_provenance(page: Page) -> None:
    print("\n== PHASE F — provenance ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    page.mouse.wheel(0, 9000); settle(page, 600)
    exp = page.locator('summary, button').filter(has_text=re.compile(r"About.*data|provenance|source", re.IGNORECASE)).first
    if exp.count() > 0:
        try:
            exp.click(); settle(page, 800)
            shot(page, "F01_provenance_open", full_page=False)
            page.mouse.wheel(0, 800); settle(page, 400)
            shot(page, "F02_provenance_pdf_links", full_page=False)
        except Exception as e:
            print(f"     provenance click failed: {e}")
    else:
        print("     provenance expander not found")


# ─────────────────────────────────────────────────────────────────────────
# Phase G — Empty / edge states
# ─────────────────────────────────────────────────────────────────────────

def phase_empty_states(page: Page) -> None:
    print("\n== PHASE G — empty / edge ==")
    page.set_viewport_size(DESKTOP)
    # Bogus member redirect
    goto(page, f"{ROUTE}?att_td=NONEXISTENT_PERSON")
    shot(page, "G01_bogus_member_redirect", full_page=False)
    # Bogus legacy ?member=
    goto(page, f"{ROUTE}?member=NONEXISTENT_PERSON")
    shot(page, "G02_bogus_member_legacy_param", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase H — Responsive
# ─────────────────────────────────────────────────────────────────────────

def phase_responsive(page: Page) -> None:
    print("\n== PHASE H — responsive ==")

    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "H01_tablet_landing_full")
    shot(page, "H02_tablet_above_fold", full_page=False)
    page.mouse.wheel(0, 1200); settle(page, 500)
    shot(page, "H03_tablet_hall_cards", full_page=False)
    page.mouse.wheel(0, 2000); settle(page, 500)
    shot(page, "H04_tablet_missing_and_footer", full_page=False)

    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "H05_mobile_landing_full")
    shot(page, "H06_mobile_above_fold", full_page=False)
    page.mouse.wheel(0, 700); settle(page, 500)
    shot(page, "H07_mobile_year_pills", full_page=False)
    page.mouse.wheel(0, 900); settle(page, 500)
    shot(page, "H08_mobile_hall_good", full_page=False)
    page.mouse.wheel(0, 1500); settle(page, 500)
    shot(page, "H09_mobile_hall_bad", full_page=False)
    page.mouse.wheel(0, 2000); settle(page, 500)
    shot(page, "H10_mobile_missing_or_footer", full_page=False)

    page.set_viewport_size(DESKTOP)


# ─────────────────────────────────────────────────────────────────────────
# Phase I — Keyboard focus
# ─────────────────────────────────────────────────────────────────────────

def phase_focus(page: Page) -> None:
    print("\n== PHASE I — keyboard focus ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    # Press Tab a few times and capture focus ring states
    page.locator("body").click(position={"x": 100, "y": 100})
    settle(page, 300)
    for _ in range(6):
        page.keyboard.press("Tab"); wait(220)
    shot(page, "I01_focus_after_6_tabs", full_page=False)
    for _ in range(8):
        page.keyboard.press("Tab"); wait(220)
    shot(page, "I02_focus_after_14_tabs", full_page=False)


# ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()

        phases = [
            ("LANDING",        phase_landing),
            ("YEAR PILLS",     phase_year_pills),
            ("SIDEBAR",        phase_sidebar),
            ("CARD CLICK",     phase_card_click),
            ("MISSING MEMBERS",phase_missing_members),
            ("PROVENANCE",     phase_provenance),
            ("EMPTY",          phase_empty_states),
            ("RESPONSIVE",     phase_responsive),
            ("FOCUS",          phase_focus),
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
