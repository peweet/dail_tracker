"""Comprehensive Member Overview audit capture.

Walks /member-overview exhaustively for an impeccable audit. The page
has two top-level states:
  - Stage 1: browse-all TDs (174 cards, no ?member= param)
  - Stage 2: full profile (?member=<unique_member_code>) — hero +
    stat strip + section-nav chip row + Open-all toggle + 7
    expanders (Interests → Lobbying → Payments → Attendance → Votes
    → Legislation → Committees) + anchored sub-sections

Plus the sidebar Find-a-TD typeahead, mobile views at 390x844, the
minister stat-strip fallback ("Different rules apply"), section
anchors (#interests, #payments, …) via member_profile_url(section=),
legacy URL redirects (?att_td=, ?lob_pol=, ?member=<bogus>), and the
debate sub-section year/topic pills nested inside Votes.

Output: audit_screenshots/_member_overview/MO-X-name.png.

Streamlit + Playwright gotchas (see [[feedback_streamlit_playwright]]):
- Wrong slug silently renders default page with a "Page not found" modal.
- Multi-page chain navigation can re-trigger the modal -- use a fresh
  browser.new_context() per legacy-URL capture.
- The apostrophe in Darragh O'Brien's code (Darragh-O'Brien.D.2007-06-14)
  tests URL-encoding round-tripping; quote the path component.
- ASCII-only print() for Windows CP1252 console safety.
- expanders open via aria-expanded toggle (the <details> render path).
"""
from __future__ import annotations

import re
import time
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/member-overview"
OUT = Path(__file__).resolve().parent / "_member_overview"
OUT.mkdir(parents=True, exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
TABLET  = {"width": 820,  "height": 1180}
MOBILE  = {"width": 390,  "height": 844}

PAGE_LOAD_WAIT  = 7000   # member-overview joins ~10 SQL views; needs extra
RERUN_WAIT      = 3000   # expander toggles trigger reruns

# Real unique_member_codes pulled from data/silver/parquet/flattened_members.parquet
MEMBER_OPPOSITION = "Mary-Lou-McDonald.D.2011-03-09"   # rich data path
MEMBER_MINISTER   = "Darragh-O'Brien.D.2007-06-14"      # minister fallback; apostrophe-in-URL edge
MEMBER_INDEP      = "Catherine-Connolly.D.2016-10-03"   # independent — different party-pill colour
MEMBER_BOGUS      = "Not-A-Real-Member.D.1900-01-01"    # empty/not-found path

SECTIONS = ["interests", "lobbying", "payments", "attendance", "votes", "legislation", "committees"]


def shot(page: Page, name: str, *, full_page: bool = True) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    kb = path.stat().st_size // 1024
    print(f"  -> {path.name} ({kb} KB)")


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def member_url(code: str, section: str | None = None) -> str:
    """Build a /member-overview URL that survives apostrophes (O'Brien)."""
    q = f"?member={quote(code, safe='')}"
    anchor = f"#mo-section-{section}" if section else ""
    return f"{ROUTE}{q}{anchor}"


def dismiss_page_not_found(page: Page) -> bool:
    """Streamlit fires a 'Page not found' modal on /member-overview cold
    loads (a real P0 finding for the audit). For capture purposes we
    need to dismiss it so subsequent clicks reach the underlying page.
    Returns True if a modal was found and closed."""
    try:
        modal = page.locator('div[role="dialog"]').filter(has_text="Page not found")
        if modal.count() > 0:
            # The close <button> is the X icon on the modal header
            close_btn = modal.locator('button[aria-label="Close"]')
            if close_btn.count() > 0:
                close_btn.first.click(force=True)
                wait(800)
                return True
    except Exception as e:
        print(f"     dismiss_page_not_found failed: {e}")
    return False


def goto(page: Page, path: str = ROUTE) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(PAGE_LOAD_WAIT)
    dismiss_page_not_found(page)


def settle(page: Page, ms: int = RERUN_WAIT) -> None:
    wait(ms)


def open_expander(page: Page, label_pattern: str) -> bool:
    """Streamlit st.expander renders as a <summary> with the label.
    Click toggles aria-expanded."""
    summary = page.locator("summary").filter(has_text=re.compile(label_pattern, re.IGNORECASE))
    if summary.count() == 0:
        print(f"     expander '{label_pattern}' not found")
        return False
    try:
        target = summary.first
        if target.get_attribute("aria-expanded") == "true":
            return True  # already open
        target.click()
        settle(page, RERUN_WAIT)
        return True
    except Exception as e:
        print(f"     expander click failed for '{label_pattern}': {e}")
        return False


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
# Phase A -- Stage 1 landing (browse 174 TDs)
# --------------------------------------------------------------------------

def phase_landing(page: Page) -> None:
    print("\n== PHASE A -- Stage 1 browse landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_landing_full_desktop")
    shot(page, "A02_landing_above_fold", full_page=False)
    page.mouse.wheel(0, 700); settle(page, 600)
    shot(page, "A03_landing_cards", full_page=False)
    page.mouse.wheel(0, 3000); settle(page, 600)
    shot(page, "A04_landing_mid_cards", full_page=False)
    page.mouse.wheel(0, 4000); settle(page, 600)
    shot(page, "A05_landing_bottom", full_page=False)

    # Tablet
    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "A06_landing_tablet")

    # Mobile
    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "A07_landing_mobile")
    page.mouse.wheel(0, 1000); settle(page, 500)
    shot(page, "A08_landing_mobile_cards", full_page=False)


# --------------------------------------------------------------------------
# Phase B -- Stage 2 hero (Mary Lou McDonald, closed expanders)
# --------------------------------------------------------------------------

def phase_profile_hero(page: Page) -> None:
    print("\n== PHASE B -- Stage 2 hero (closed expanders) ==")
    page.set_viewport_size(DESKTOP)
    goto(page, member_url(MEMBER_OPPOSITION))
    shot(page, "B01_profile_full_closed")
    shot(page, "B02_profile_above_fold", full_page=False)
    page.mouse.wheel(0, 400); settle(page, 600)
    shot(page, "B03_profile_stat_strip", full_page=False)
    page.mouse.wheel(0, 400); settle(page, 600)
    shot(page, "B04_profile_section_nav_chips", full_page=False)
    page.mouse.wheel(0, 400); settle(page, 600)
    shot(page, "B05_profile_expanders_closed", full_page=False)


# --------------------------------------------------------------------------
# Phase C -- Open each of the 7 expanders one at a time
# --------------------------------------------------------------------------

def phase_each_expander(page: Page) -> None:
    print("\n== PHASE C -- expander deep-dive (one per visit) ==")
    page.set_viewport_size(DESKTOP)

    # Two-letter codes for the IA order
    labels = [
        ("interests",   "Interests"),
        ("lobbying",    "Lobbying"),
        ("payments",    "Payments"),
        ("attendance",  "Attendance"),
        ("votes",       "Votes"),
        ("legislation", "Legislation"),
        ("committees",  "Committees"),
    ]
    for idx, (sid, label) in enumerate(labels, start=1):
        print(f"\n  -- C{idx}: {label} ({sid}) --")
        goto(page, member_url(MEMBER_OPPOSITION, sid))
        # The anchor jump may or may not auto-open; click the summary explicitly
        if not open_expander(page, rf"^\s*{re.escape(label)}\b"):
            continue
        # Scroll to the opened section so it's in frame
        anchor = page.locator(f'div[id="mo-section-{sid}"]')
        if anchor.count() > 0:
            try:
                anchor.first.scroll_into_view_if_needed()
                settle(page, 800)
            except Exception:
                pass
        shot(page, f"C{idx:02d}_{sid}_expanded_full")
        shot(page, f"C{idx:02d}_{sid}_expanded_above_fold", full_page=False)


# --------------------------------------------------------------------------
# Phase D -- Open-all toggle (long-page mobile worst-case)
# --------------------------------------------------------------------------

def phase_open_all(page: Page) -> None:
    print("\n== PHASE D -- Open-all toggle ==")
    page.set_viewport_size(DESKTOP)
    goto(page, member_url(MEMBER_OPPOSITION))
    if not click_button_re(page, r"Open all sections"):
        print("     'Open all sections' button not found")
        return
    settle(page, RERUN_WAIT * 2)  # 7 sections worth of SQL
    shot(page, "D01_open_all_full_desktop")
    page.mouse.wheel(0, 1200); settle(page, 600)
    shot(page, "D02_open_all_mid", full_page=False)

    # Mobile open-all — worst-case scroll length
    page.set_viewport_size(MOBILE)
    goto(page, member_url(MEMBER_OPPOSITION))
    if click_button_re(page, r"Open all sections"):
        settle(page, RERUN_WAIT * 2)
        shot(page, "D03_open_all_full_mobile")


# --------------------------------------------------------------------------
# Phase E -- Minister stat-strip fallback (Darragh O'Brien)
# --------------------------------------------------------------------------

def phase_minister_fallback(page: Page) -> None:
    print("\n== PHASE E -- minister stat-strip fallback ==")
    page.set_viewport_size(DESKTOP)
    goto(page, member_url(MEMBER_MINISTER))
    shot(page, "E01_minister_full")
    shot(page, "E02_minister_above_fold", full_page=False)
    # The "Different rules apply" callout replaces the 3-stat strip
    page.mouse.wheel(0, 400); settle(page, 600)
    shot(page, "E03_minister_callout_zoom", full_page=False)
    # Mobile minister
    page.set_viewport_size(MOBILE)
    goto(page, member_url(MEMBER_MINISTER))
    shot(page, "E04_minister_mobile")


# --------------------------------------------------------------------------
# Phase F -- Independent TD (different party-pill colour palette)
# --------------------------------------------------------------------------

def phase_independent(page: Page) -> None:
    print("\n== PHASE F -- independent TD ==")
    page.set_viewport_size(DESKTOP)
    goto(page, member_url(MEMBER_INDEP))
    shot(page, "F01_independent_full")
    shot(page, "F02_independent_above_fold", full_page=False)


# --------------------------------------------------------------------------
# Phase G -- Section-anchor cross-page jump (#interests, etc.)
# --------------------------------------------------------------------------

def phase_anchor_jumps(page: Page) -> None:
    print("\n== PHASE G -- section anchor jumps ==")
    page.set_viewport_size(DESKTOP)
    # Just verify the deep-link works for the three most-cited sections
    for sid in ["interests", "payments", "votes"]:
        goto(page, member_url(MEMBER_OPPOSITION, sid))
        anchor = page.locator(f'div[id="mo-section-{sid}"]')
        if anchor.count() > 0:
            try:
                anchor.first.scroll_into_view_if_needed()
                settle(page, 800)
            except Exception:
                pass
        shot(page, f"G_anchor_{sid}", full_page=False)


# --------------------------------------------------------------------------
# Phase H -- Empty / not-found
# --------------------------------------------------------------------------

def phase_not_found(page: Page) -> None:
    print("\n== PHASE H -- not-found path ==")
    page.set_viewport_size(DESKTOP)
    goto(page, member_url(MEMBER_BOGUS))
    shot(page, "H01_not_found_bogus_code")


# --------------------------------------------------------------------------
# Phase I -- Sidebar Find-a-TD
# --------------------------------------------------------------------------

def phase_sidebar(page: Page) -> None:
    print("\n== PHASE I -- sidebar Find-a-TD ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "I01_sidebar_default", full_page=False)
    # Sidebar selectbox: type a name and pick from suggestions
    sb_input = page.locator('[data-testid="stSidebar"] input').first
    if sb_input.count() > 0:
        try:
            sb_input.fill("Mary Lou")
            settle(page, 1200)
            shot(page, "I02_sidebar_typeahead_typed", full_page=False)
            # Listbox option
            opts = page.locator('[role="option"]')
            if opts.count() > 0:
                opts.first.click()
                settle(page, RERUN_WAIT)
                shot(page, "I03_sidebar_typeahead_selected")
        except Exception as e:
            print(f"     sidebar typeahead failed: {e}")


# --------------------------------------------------------------------------
# Phase J -- Legacy redirect URLs (cross-page contract)
# --------------------------------------------------------------------------

def phase_legacy_redirects(playwright_root) -> None:
    """Each legacy URL needs a fresh browser context — Streamlit's
    page-router can fire its own "Page not found" modal during a
    chained-nav capture session. See [[feedback_streamlit_playwright]]."""
    print("\n== PHASE J -- legacy redirect URLs (fresh contexts) ==")
    legacy_urls = [
        ("J01_legacy_att_td",     f"/rankings-attendance?att_td={quote('Mary Lou McDonald')}"),
        ("J02_legacy_payments",   f"/rankings-payments?member={quote('Mary Lou McDonald')}"),
        ("J03_legacy_interests",  f"/rankings-interests?member={quote('Mary Lou McDonald')}"),
        ("J04_legacy_committees", f"/rankings-committees?member={quote('Mary Lou McDonald')}"),
        ("J05_legacy_lob_pol",    f"/rankings-lobbying?lob_pol={quote('Mary Lou McDonald')}"),
    ]
    browser = playwright_root.chromium.launch(headless=True)
    try:
        for name, path in legacy_urls:
            ctx = browser.new_context(viewport=DESKTOP)
            p = ctx.new_page()
            try:
                p.goto(f"{BASE}{path}", wait_until="domcontentloaded")
                wait(PAGE_LOAD_WAIT)
                p.screenshot(path=str(OUT / f"{name}.png"), full_page=True)
                print(f"  -> {name}.png")
            except Exception as e:
                print(f"     {name} failed: {e}")
            finally:
                ctx.close()
    finally:
        browser.close()


# --------------------------------------------------------------------------
# Phase K -- Apostrophe URL round-trip (Darragh O'Brien minister)
# --------------------------------------------------------------------------

def phase_apostrophe(page: Page) -> None:
    print("\n== PHASE K -- apostrophe in URL ==")
    page.set_viewport_size(DESKTOP)
    goto(page, member_url(MEMBER_MINISTER))
    # Just verify the page renders identity (not the not-found path)
    shot(page, "K01_apostrophe_renders", full_page=False)


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
                ("A landing",         phase_landing),
                ("B profile hero",    phase_profile_hero),
                ("C each expander",   phase_each_expander),
                ("D open all",        phase_open_all),
                ("E minister",        phase_minister_fallback),
                ("F independent",     phase_independent),
                ("G anchor jumps",    phase_anchor_jumps),
                ("H not found",       phase_not_found),
                ("I sidebar",         phase_sidebar),
                ("K apostrophe",      phase_apostrophe),
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
            phase_legacy_redirects(p)
        except Exception as e:
            print(f"  !! phase J failed: {e}")

    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
