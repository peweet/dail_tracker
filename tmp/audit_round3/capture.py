"""Round-3 visual audit screenshot capture.

Drives /member-overview + rankings pages (lobbying excluded — owned by
another agent) through every drill-down state requested:
  - landing, party filter, text search, scroll, pagination
  - profile load with Interests default-open, expander chevron click,
    "Open all sections", inner year/status filter changes
  - cross-page card clicks (redirect to /member-overview)
  - legacy ?member= / ?att_td= redirect callouts
  - rankings drill-downs: votes Mode A→C, legislation bill detail,
    committees committee detail + Find-a-TD typeahead, SI index→detail

Streamlit interaction notes:
  - rerun cycle is 1–2s; wait_for_load_state("networkidle") is unreliable
    (websocket polling never goes idle). Use explicit waits.
  - Expanders use <summary> elements; clicking the summary toggles them.
  - Pills/buttons render as native <button>; text selectors work reliably.

Output: PNGs in tmp/audit_round3/, prefixed with 2-digit ordering.
"""
from __future__ import annotations

import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))
from ui.entity_links import name_join_key  # noqa: E402

from playwright.sync_api import Page, sync_playwright  # noqa: E402

BASE = "http://localhost:8511"
OUT = Path(__file__).resolve().parent

# Targets
MARY_LOU = "Mary Lou McDonald"  # plain TD baseline
MICHEAL = "Micheál Martin"      # Taoiseach (minister → has SI sub-section)
MARY_LOU_CODE = name_join_key(MARY_LOU)
MICHEAL_CODE = name_join_key(MICHEAL)

# Streamlit render budgets
PAGE_LOAD_WAIT = 4500  # ms — initial load + cache warm
RERUN_WAIT = 2200      # ms — after click / typing / filter change
HOVER_WAIT = 600


def shot(page: Page, name: str, *, full_page: bool = True) -> None:
    """Save a screenshot. Default full_page=True so we capture below-fold."""
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    print(f"  → {path.name}")


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def goto(page: Page, path: str) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(PAGE_LOAD_WAIT)


def click_text(page: Page, text: str, *, nth: int = 0) -> bool:
    """Click an element matching exact text; returns success."""
    locator = page.get_by_text(text, exact=True)
    if locator.count() > nth:
        locator.nth(nth).click()
        wait(RERUN_WAIT)
        return True
    return False


def click_button(page: Page, text: str) -> bool:
    """Click a <button> by its accessible name."""
    btn = page.get_by_role("button", name=text)
    if btn.count() > 0:
        btn.first.click()
        wait(RERUN_WAIT)
        return True
    return False


def open_expander(page: Page, label: str) -> bool:
    """Open an st.expander by its label."""
    summary = page.locator(f'details summary:has-text("{label}")')
    if summary.count() > 0:
        summary.first.click()
        wait(RERUN_WAIT)
        return True
    return False


# ─────────────────────────────────────────────────────────────────────────
# 1. MEMBER-OVERVIEW
# ─────────────────────────────────────────────────────────────────────────

def capture_member_overview(page: Page) -> None:
    # 01: Browse landing (no member selected)
    goto(page, "/member-overview")
    shot(page, "01_mo_browse_landing")

    # 02: Browse — text search applied
    search = page.locator('input[type="text"]').first
    if search.count() > 0:
        search.fill("Holly")
        wait(RERUN_WAIT)
        shot(page, "02_mo_browse_text_search")
        search.fill("")
        wait(RERUN_WAIT)

    # 03: Browse — scroll to pagination (below-fold)
    page.mouse.wheel(0, 4000)
    wait(800)
    shot(page, "03_mo_browse_scroll_paginator", full_page=False)

    # 04: Profile — default load (Interests auto-open per round-2 distill)
    goto(page, f"/member-overview?member={MARY_LOU_CODE}")
    shot(page, "04_mo_profile_default_interests_open")

    # 05: Profile — chevron-click wart
    #   Click chevron on Payments while gated. Should show "Section
    #   collapsed" caption (the documented round-2 P0 #3 UX wart).
    if open_expander(page, "Payments"):
        shot(page, "05_mo_profile_chevron_wart_payments")

    # 06: Profile — Open all sections
    if click_button(page, "Open all sections"):
        shot(page, "06_mo_profile_open_all_top", full_page=False)

    # 07: Profile — Open all, scroll midway
    page.mouse.wheel(0, 2500)
    wait(600)
    shot(page, "07_mo_profile_open_all_scroll_mid", full_page=False)

    # 08: Profile — Open all, scroll to bottom
    page.mouse.wheel(0, 6000)
    wait(600)
    shot(page, "08_mo_profile_open_all_scroll_bottom", full_page=False)

    # 09: Profile — Open all, full-page (single long PNG of the whole thing)
    shot(page, "09_mo_profile_open_all_full")

    # 10: Profile — Interests year pill changed (back to top, click older yr)
    page.mouse.wheel(0, -10000)
    wait(600)
    # Year pills on Interests body — try previous year (e.g. 2023)
    if click_text(page, "2023"):
        shot(page, "10_mo_profile_interests_year_2023", full_page=False)

    # 11: Profile — Interests diff toggle off
    diff = page.get_by_text("Show changes since", exact=False)
    if diff.count() > 0:
        # st.toggle renders as a <label> with a switch — click the label
        try:
            diff.first.click()
            wait(RERUN_WAIT)
            shot(page, "11_mo_profile_interests_diff_off", full_page=False)
        except Exception as e:
            print(f"    diff toggle click failed: {e}")

    # 12: Profile — Committees status filter switched to Active
    page.mouse.wheel(0, 8000)
    wait(600)
    if click_button(page, "Active"):
        shot(page, "12_mo_profile_committees_status_active", full_page=False)

    # 13: Profile — Payments year switched (scroll back to Payments)
    page.mouse.wheel(0, -3000)
    wait(600)
    # Click year that's not currently selected (find a year != current)
    # Year pills are <button> elements with year text
    for yr_attempt in ("2022", "2021", "2020"):
        if click_button(page, yr_attempt):
            shot(page, f"13_mo_profile_payments_year_{yr_attempt}", full_page=False)
            break

    # 14: Profile — minister (Micheál Martin) — should show SI sub-section
    goto(page, f"/member-overview?member={MICHEAL_CODE}")
    if click_button(page, "Open all sections"):
        # Scroll to legislation expander to capture SI sub-section
        page.mouse.wheel(0, 5000)
        wait(800)
        shot(page, "14_mo_profile_minister_legislation_si", full_page=False)
        shot(page, "15_mo_profile_minister_full", full_page=True)


# ─────────────────────────────────────────────────────────────────────────
# 2. RANKINGS PAGES (lobbying excluded — owned by another agent)
# ─────────────────────────────────────────────────────────────────────────

def capture_attendance(page: Page) -> None:
    goto(page, "/rankings-attendance")
    shot(page, "20_att_landing")

    # Legacy redirect callout
    goto(page, f"/rankings-attendance?att_td={MARY_LOU}")
    shot(page, "21_att_legacy_redirect", full_page=False)


def capture_votes(page: Page) -> None:
    # Mode A — divisions index (default landing)
    goto(page, "/rankings-votes")
    shot(page, "30_votes_mode_a_landing")

    # Mode A — scroll down to see division cards
    page.mouse.wheel(0, 1500)
    wait(600)
    shot(page, "31_votes_mode_a_scroll", full_page=False)

    # Mode C — click a division card to see division evidence
    #   Vote cards are clickable links. Try clicking the first vote.
    vote_links = page.locator('a[href*="vote="]')
    if vote_links.count() > 0:
        vote_links.first.click()
        wait(RERUN_WAIT)
        shot(page, "32_votes_mode_c_division_detail")

    # TD picker — sidebar toggle from Dáil → TDs view
    goto(page, "/rankings-votes")
    # The view toggle is in the sidebar. Try clicking "TDs"
    if click_button(page, "TDs"):
        shot(page, "33_votes_td_picker", full_page=False)

    # Mode B redirect callout (legacy ?member= bookmark)
    goto(page, f"/rankings-votes?member={MARY_LOU_CODE}")
    shot(page, "34_votes_mode_b_redirect", full_page=False)


def capture_interests(page: Page) -> None:
    goto(page, "/rankings-interests")
    shot(page, "40_int_landing")

    # Scroll to see leaderboard
    page.mouse.wheel(0, 1500)
    wait(600)
    shot(page, "41_int_leaderboard_scroll", full_page=False)

    # Legacy redirect
    goto(page, f"/rankings-interests?member={MARY_LOU}")
    shot(page, "42_int_legacy_redirect", full_page=False)


def capture_payments(page: Page) -> None:
    goto(page, "/rankings-payments")
    shot(page, "50_pay_landing")

    page.mouse.wheel(0, 1500)
    wait(600)
    shot(page, "51_pay_scroll", full_page=False)

    # Legacy redirect (sidebar selectbox or URL)
    goto(page, f"/rankings-payments?member={MARY_LOU}")
    shot(page, "52_pay_legacy_redirect", full_page=False)


def capture_legislation(page: Page) -> None:
    goto(page, "/rankings-legislation")
    shot(page, "60_leg_landing")

    page.mouse.wheel(0, 1500)
    wait(600)
    shot(page, "61_leg_scroll", full_page=False)

    # Click first bill card → detail view
    bill_links = page.locator('a[href*="bill="]')
    if bill_links.count() > 0:
        bill_links.first.click()
        wait(RERUN_WAIT)
        shot(page, "62_leg_bill_detail")


def capture_committees(page: Page) -> None:
    goto(page, "/rankings-committees")
    shot(page, "70_comm_register_landing")

    # Click first committee card → committee detail
    comm_links = page.locator('a[href*="committee="]')
    if comm_links.count() > 0:
        comm_links.first.click()
        wait(RERUN_WAIT)
        shot(page, "71_comm_committee_detail")

    # Find a TD typeahead — should render redirect callout
    goto(page, "/rankings-committees")
    td_search = page.locator('input[type="text"]').last
    if td_search.count() > 0:
        td_search.fill("Mary Lou McDonald")
        wait(RERUN_WAIT)
        # The typeahead may need an Enter or a click on the dropdown option
        page.keyboard.press("Enter")
        wait(RERUN_WAIT)
        shot(page, "72_comm_td_typeahead_redirect", full_page=False)

    # Legacy redirect
    goto(page, f"/rankings-committees?member={MARY_LOU}")
    shot(page, "73_comm_legacy_redirect", full_page=False)


def capture_si(page: Page) -> None:
    goto(page, "/rankings-statutory-instruments")
    shot(page, "80_si_index_top")

    page.mouse.wheel(0, 1500)
    wait(600)
    shot(page, "81_si_index_facets", full_page=False)

    page.mouse.wheel(0, 1500)
    wait(600)
    shot(page, "82_si_index_cards", full_page=False)

    # Click first SI card → detail view (cards set ?si= URL param)
    si_buttons = page.get_by_role("button").filter(has_text="Open SI")
    if si_buttons.count() == 0:
        # Alternative: any clickable SI card
        si_buttons = page.locator('button:has-text("View")')
    if si_buttons.count() > 0:
        si_buttons.first.click()
        wait(RERUN_WAIT)
        shot(page, "83_si_detail")


# ─────────────────────────────────────────────────────────────────────────

def main() -> None:
    print(f"Mary Lou code: {MARY_LOU_CODE}")
    print(f"Micheál code: {MICHEAL_CODE}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        sections = [
            ("MEMBER-OVERVIEW", capture_member_overview),
            ("ATTENDANCE",      capture_attendance),
            ("VOTES",           capture_votes),
            ("INTERESTS",       capture_interests),
            ("PAYMENTS",        capture_payments),
            ("LEGISLATION",     capture_legislation),
            ("COMMITTEES",      capture_committees),
            ("STATUTORY INSTR", capture_si),
        ]
        for label, fn in sections:
            print(f"\n== {label} ==")
            try:
                fn(page)
            except Exception as e:
                print(f"  !! section failed: {e}")

        ctx.close()
        browser.close()
    print(f"\nDone. Output: {OUT}")


if __name__ == "__main__":
    main()
