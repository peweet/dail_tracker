"""Comprehensive Lobbying-PoC page audit capture.

Walks /rankings-lobbying-poc (lobbying_3.py) exhaustively. The PoC is a
calmer reimagining of the lobbying register IA with a quieter hero
treatment, three gateway tiles, ranked card lists, datasette-style
tables for tabular detail, and a unified return-card helper.

State matrix:
  - Landing: hero + gateways (Politician / Org / Area) + topic rail
    + 2 ranked-card lists (most-lobbied politicians, most active orgs)
    + revolving door + recent returns + provenance
  - Org index (?lp3_orgindex=1): toggle state-funded + 2 filters + cards
  - Org detail (?lp3_org=X): hero + switcher + politicians + clients
    table + returns cards + attached refs + official sources
  - Area detail (?lp3_area=X): hero + switcher + politicians + returns
  - Area x Politician Stage 3 (?lp3_area=X&lp3_result_pol=Y): returns
  - Topic detail (?lp3_topic=X): hero + caveat + returns
  - Revolving Door index (?lp3_rd=1): chamber filter + DPO cards
  - DPO individual (?lp3_dpo=X): firms + clients + politicians + returns
  - Sidebar search + jump-to-selectbox
  - 404 / not-found states (bad org name)

Output: audit_screenshots/_lobbying_poc/LP-X-name.png.

Streamlit + Playwright gotchas (see [[feedback_streamlit_playwright]]):
- Wrong slug silently renders default page with a "Page not found" modal.
- Multi-page chain navigation can re-trigger the modal -- use a fresh
  `browser.new_context()` per legacy-URL capture.
- ASCII-only print() for Windows CP1252 console safety.
"""
from __future__ import annotations

import re
import time
import urllib.parse
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-lobbying-poc"
OUT = Path(__file__).resolve().parent / "_lobbying_poc"
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


def fresh_goto(p_root, viewport: dict, path: str = ROUTE):
    """Return (browser, ctx, page) freshly opened. Caller must close."""
    browser = p_root.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=viewport)
    page = ctx.new_page()
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(PAGE_LOAD_WAIT)
    return browser, ctx, page


# --------------------------------------------------------------------------
# Phase A -- Landing (default)
# --------------------------------------------------------------------------

def phase_landing(page: Page) -> None:
    print("\n== PHASE A -- Landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_landing_full_desktop")
    shot(page, "A02_landing_above_fold", full_page=False)
    page.mouse.wheel(0, 800); settle(page, 600)
    shot(page, "A03_landing_gateways_topics", full_page=False)
    page.mouse.wheel(0, 900); settle(page, 600)
    shot(page, "A04_landing_ranked_lists", full_page=False)
    page.mouse.wheel(0, 1200); settle(page, 600)
    shot(page, "A05_landing_revolving_door", full_page=False)
    page.mouse.wheel(0, 1400); settle(page, 600)
    shot(page, "A06_landing_recent_returns", full_page=False)

    # Tablet
    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "A07_landing_tablet")

    # Mobile
    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "A08_landing_mobile")
    page.mouse.wheel(0, 900); settle(page, 500)
    shot(page, "A09_landing_mobile_gateways", full_page=False)
    page.mouse.wheel(0, 1500); settle(page, 500)
    shot(page, "A10_landing_mobile_lists", full_page=False)


# --------------------------------------------------------------------------
# Phase B -- Sidebar search + jump-to
# --------------------------------------------------------------------------

def phase_sidebar_search(page: Page) -> None:
    print("\n== PHASE B -- sidebar search ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    # Sidebar search input is the first text input (sidebar comes first in DOM)
    inputs = page.locator('input[type="text"]')
    n = inputs.count()
    print(f"     found {n} text inputs")
    if n >= 1:
        try:
            inputs.first.fill("Ibec")
            settle(page, 1500)
            shot(page, "B01_sidebar_search_ibec", full_page=False)
            # Now expand the selectbox to see if Ibec appears
            select = page.locator('[data-baseweb="select"]').first
            if select.count() > 0:
                select.click()
                settle(page, 800)
                shot(page, "B02_sidebar_jump_dropdown", full_page=False)
                # close dropdown
                page.keyboard.press("Escape")
                settle(page, 400)
        except Exception as e:
            print(f"     sidebar search failed: {e}")


# --------------------------------------------------------------------------
# Phase C -- Org index (Browse orgs button)
# --------------------------------------------------------------------------

def phase_org_index(page: Page) -> None:
    print("\n== PHASE C -- Org index ==")
    page.set_viewport_size(DESKTOP)
    goto(page, f"{ROUTE}?lp3_orgindex=1")
    shot(page, "C01_org_index_full")
    shot(page, "C02_org_index_above_fold", full_page=False)
    page.mouse.wheel(0, 800); settle(page, 600)
    shot(page, "C03_org_index_filters_cards", full_page=False)

    # Toggle "Include state-funded public bodies"
    toggle = page.get_by_role("checkbox").first
    if toggle.count() > 0:
        try:
            toggle.click()
            settle(page, RERUN_WAIT)
            shot(page, "C04_org_index_with_state_funded", full_page=False)
        except Exception as e:
            print(f"     state-funded toggle failed: {e}")


# --------------------------------------------------------------------------
# Phase D -- Org detail (click an org card from index)
# --------------------------------------------------------------------------

def phase_org_detail(page: Page) -> None:
    print("\n== PHASE D -- Org detail ==")
    page.set_viewport_size(DESKTOP)
    # Land on org index, then click first org card
    goto(page, f"{ROUTE}?lp3_orgindex=1")
    page.mouse.wheel(0, 600); settle(page, 500)
    org_links = page.locator('a[href*="lp3_org="]')
    n = org_links.count()
    print(f"     {n} org links visible")
    if n > 0:
        try:
            org_links.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "D01_org_detail_full")
            shot(page, "D02_org_detail_above_fold", full_page=False)
            page.mouse.wheel(0, 600); settle(page, 500)
            shot(page, "D03_org_detail_politicians", full_page=False)
            page.mouse.wheel(0, 1000); settle(page, 500)
            shot(page, "D04_org_detail_returns", full_page=False)
            page.mouse.wheel(0, 1500); settle(page, 500)
            shot(page, "D05_org_detail_sources_provenance", full_page=False)
        except Exception as e:
            print(f"     org detail click failed: {e}")


# --------------------------------------------------------------------------
# Phase E -- Area detail (Browse policy areas button)
# --------------------------------------------------------------------------

def phase_area_detail(page: Page) -> None:
    print("\n== PHASE E -- Area detail ==")
    page.set_viewport_size(DESKTOP)
    # Direct URL load with a known area
    area = urllib.parse.quote("Health")
    goto(page, f"{ROUTE}?lp3_area={area}")
    shot(page, "E01_area_detail_full")
    shot(page, "E02_area_detail_above_fold", full_page=False)
    page.mouse.wheel(0, 600); settle(page, 500)
    shot(page, "E03_area_detail_politicians", full_page=False)
    page.mouse.wheel(0, 1000); settle(page, 500)
    shot(page, "E04_area_detail_returns", full_page=False)


# --------------------------------------------------------------------------
# Phase F -- Area x Politician (Stage 3)
# --------------------------------------------------------------------------

def phase_stage3(page: Page) -> None:
    print("\n== PHASE F -- Area x Politician Stage 3 ==")
    page.set_viewport_size(DESKTOP)
    area = urllib.parse.quote("Health")
    goto(page, f"{ROUTE}?lp3_area={area}")
    page.mouse.wheel(0, 600); settle(page, 500)
    pol_links = page.locator('a[href*="lp3_result_pol="]')
    n = pol_links.count()
    print(f"     {n} politician links on area page")
    if n > 0:
        try:
            pol_links.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "F01_stage3_full")
            shot(page, "F02_stage3_above_fold", full_page=False)
            page.mouse.wheel(0, 600); settle(page, 500)
            shot(page, "F03_stage3_returns", full_page=False)
        except Exception as e:
            print(f"     stage3 click failed: {e}")


# --------------------------------------------------------------------------
# Phase G -- Topic detail
# --------------------------------------------------------------------------

def phase_topic(page: Page) -> None:
    print("\n== PHASE G -- Topic detail ==")
    page.set_viewport_size(DESKTOP)
    topic = urllib.parse.quote("Housing crisis")
    goto(page, f"{ROUTE}?lp3_topic={topic}")
    shot(page, "G01_topic_full")
    shot(page, "G02_topic_above_fold", full_page=False)
    page.mouse.wheel(0, 600); settle(page, 500)
    shot(page, "G03_topic_caveat_returns", full_page=False)


# --------------------------------------------------------------------------
# Phase H -- Revolving Door index
# --------------------------------------------------------------------------

def phase_rd_index(page: Page) -> None:
    print("\n== PHASE H -- RD index ==")
    page.set_viewport_size(DESKTOP)
    goto(page, f"{ROUTE}?lp3_rd=1")
    shot(page, "H01_rd_index_full")
    shot(page, "H02_rd_index_above_fold", full_page=False)
    page.mouse.wheel(0, 700); settle(page, 500)
    shot(page, "H03_rd_index_cards", full_page=False)

    # Try chamber filter selectbox
    select = page.locator('[data-baseweb="select"]').first
    if select.count() > 0:
        try:
            select.click()
            settle(page, 800)
            opts = page.locator('[role="option"]')
            n = opts.count()
            print(f"     {n} chamber filter options")
            if n > 1:
                opts.nth(1).click()  # pick Dáil bucket
                settle(page, RERUN_WAIT)
                shot(page, "H04_rd_filter_applied", full_page=False)
        except Exception as e:
            print(f"     RD filter failed: {e}")


# --------------------------------------------------------------------------
# Phase I -- DPO individual
# --------------------------------------------------------------------------

def phase_dpo_individual(page: Page) -> None:
    print("\n== PHASE I -- DPO individual ==")
    page.set_viewport_size(DESKTOP)
    goto(page, f"{ROUTE}?lp3_rd=1")
    page.mouse.wheel(0, 700); settle(page, 500)
    dpo_links = page.locator('a[href*="lp3_dpo="]')
    n = dpo_links.count()
    print(f"     {n} DPO links on RD index")
    if n > 0:
        try:
            dpo_links.first.click()
            settle(page, RERUN_WAIT)
            shot(page, "I01_dpo_full")
            shot(page, "I02_dpo_above_fold", full_page=False)
            page.mouse.wheel(0, 700); settle(page, 500)
            shot(page, "I03_dpo_firms_clients", full_page=False)
            page.mouse.wheel(0, 800); settle(page, 500)
            shot(page, "I04_dpo_politicians", full_page=False)
            page.mouse.wheel(0, 1000); settle(page, 500)
            shot(page, "I05_dpo_returns", full_page=False)
        except Exception as e:
            print(f"     DPO click failed: {e}")


# --------------------------------------------------------------------------
# Phase J -- Mobile org detail (fresh context)
# --------------------------------------------------------------------------

def phase_mobile_org(p_root) -> None:
    print("\n== PHASE J -- Mobile org detail ==")
    browser = p_root.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=MOBILE)
    page = ctx.new_page()
    try:
        page.goto(f"{BASE}{ROUTE}?lp3_orgindex=1", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        org_links = page.locator('a[href*="lp3_org="]')
        if org_links.count() > 0:
            try:
                org_links.first.click()
                wait(RERUN_WAIT)
                page.screenshot(path=str(OUT / "J01_mobile_org_detail.png"), full_page=True)
                print("  -> J01_mobile_org_detail.png")
                page.mouse.wheel(0, 1200); wait(600)
                page.screenshot(path=str(OUT / "J02_mobile_org_detail_mid.png"))
                print("  -> J02_mobile_org_detail_mid.png")
            except Exception as e:
                print(f"     mobile org click failed: {e}")
    finally:
        ctx.close()
        browser.close()


# --------------------------------------------------------------------------
# Phase K -- Empty / not-found states (fresh context to avoid bleed)
# --------------------------------------------------------------------------

def phase_empty_states(p_root) -> None:
    print("\n== PHASE K -- empty / not-found ==")
    browser = p_root.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP)
    page = ctx.new_page()
    try:
        # Bogus org name -> "Organisation not found"
        bogus_org = urllib.parse.quote("zzz-nonexistent-org")
        page.goto(f"{BASE}{ROUTE}?lp3_org={bogus_org}", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        page.screenshot(path=str(OUT / "K01_empty_org_not_found.png"))
        print("  -> K01_empty_org_not_found.png")

        # Bogus area
        bogus_area = urllib.parse.quote("zzz-fake-policy-area")
        page.goto(f"{BASE}{ROUTE}?lp3_area={bogus_area}", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        page.screenshot(path=str(OUT / "K02_empty_area_not_found.png"))
        print("  -> K02_empty_area_not_found.png")

        # Unknown topic
        bogus_topic = urllib.parse.quote("zzz-not-a-curated-topic")
        page.goto(f"{BASE}{ROUTE}?lp3_topic={bogus_topic}", wait_until="domcontentloaded")
        wait(PAGE_LOAD_WAIT)
        page.screenshot(path=str(OUT / "K03_empty_topic_not_found.png"))
        print("  -> K03_empty_topic_not_found.png")
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
                ("A landing",     phase_landing),
                ("B sidebar",     phase_sidebar_search),
                ("C org index",   phase_org_index),
                ("D org detail",  phase_org_detail),
                ("E area detail", phase_area_detail),
                ("F stage3",      phase_stage3),
                ("G topic",       phase_topic),
                ("H rd index",    phase_rd_index),
                ("I dpo",         phase_dpo_individual),
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
            phase_mobile_org(p)
        except Exception as e:
            print(f"  !! phase J failed: {e}")

        try:
            phase_empty_states(p)
        except Exception as e:
            print(f"  !! phase K failed: {e}")

    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
