"""Comprehensive lobbying page audit capture.

Walks /rankings-lobbying exhaustively — three-path gateway, Stage 2 views
(politician redirect, org profile, area, area×politician results), revolving
door (Stage 2a index + Stage 2b individual), org index, topic search.

Output: audit_screenshots/lobbying/<phase><nn>_name.png

Phases:
  A landing (gateway)
  B sidebar interaction
  C three-path gateway clicks (politician redirect, org, area)
  D revolving door Stage 2a/2b
  E org index
  F topic search (curated keyword scan)
  G provenance + empty/edge states
  H tablet + mobile responsive
"""
from __future__ import annotations

import re
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
ROUTE = "/rankings-lobbying"
OUT = Path(__file__).resolve().parent / "lobbying"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
TABLET  = {"width": 820,  "height": 1180}
MOBILE  = {"width": 390,  "height": 844}

INITIAL_LOAD_WAIT = 10000
WARM_LOAD_WAIT    = 3500
RERUN_WAIT        = 2500
SETTLE_TINY       = 400

_cold = True


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


# ─────────────────────────────────────────────────────────────────────────
# Phase A — Landing
# ─────────────────────────────────────────────────────────────────────────

def phase_landing(page: Page) -> None:
    print("\n== PHASE A — landing ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "A01_landing_full_desktop")
    shot(page, "A02_landing_above_fold_desktop", full_page=False)
    scroll_by(page, 800); settle(page, 500)
    shot(page, "A03_landing_gateway_cards", full_page=False)
    scroll_by(page, 900); settle(page, 500)
    shot(page, "A04_landing_leaderboards", full_page=False)
    scroll_by(page, 1500); settle(page, 500)
    shot(page, "A05_landing_revolving_door_callout", full_page=False)
    scroll_by(page, 1500); settle(page, 500)
    shot(page, "A06_landing_recent_returns", full_page=False)
    scroll_by(page, 2500); settle(page, 500)
    shot(page, "A07_landing_provenance_area", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase B — Sidebar
# ─────────────────────────────────────────────────────────────────────────

def phase_sidebar(page: Page) -> None:
    print("\n== PHASE B — sidebar ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    shot(page, "B01_sidebar_default", full_page=False)

    search = page.locator('[data-testid="stSidebar"] input[type="text"]').first
    if search.count() > 0:
        try:
            search.fill("Ibec"); settle(page, RERUN_WAIT)
            shot(page, "B02_sidebar_search_ibec", full_page=False)
            search.fill("McDonald"); settle(page, RERUN_WAIT)
            shot(page, "B03_sidebar_search_mcdonald", full_page=False)
            search.fill(""); settle(page, RERUN_WAIT)
        except Exception as e:
            print(f"     sidebar search failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase C — Three-path gateway clicks
# ─────────────────────────────────────────────────────────────────────────

def phase_gateway(page: Page) -> None:
    print("\n== PHASE C — gateway ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    scroll_by(page, 500); settle(page, 400)

    # The path cards are likely buttons or anchor wrappers. Try by text first.
    for label, name in (
        ("politician", "C01_after_click_politicians_path"),
        ("organisation", "C02_after_click_orgs_path"),
        ("policy area", "C03_after_click_policy_path"),
    ):
        goto(page)
        scroll_by(page, 500); settle(page, 400)
        btn = page.get_by_role("button", name=re.compile(label, re.IGNORECASE)).first
        if btn.count() > 0:
            try:
                btn.click(); settle(page, RERUN_WAIT)
                shot(page, name)
            except Exception as e:
                print(f"     {label} click failed: {e}")

    # Org index: click a card or use ?lob_org=
    goto(page, f"{ROUTE}?lob_org=Ibec")
    shot(page, "C04_org_profile_ibec")
    shot(page, "C05_org_profile_ibec_above_fold", full_page=False)

    # Area Stage 2 (policy area filtered)
    goto(page, f"{ROUTE}?lob_area=Health")
    shot(page, "C06_area_health")

    # Politician redirect (legacy ?lob_pol=)
    goto(page, f"{ROUTE}?lob_pol=Mary%20Lou%20McDonald")
    shot(page, "C07_legacy_pol_redirect", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase D — Revolving Door
# ─────────────────────────────────────────────────────────────────────────

def phase_revolving_door(page: Page) -> None:
    print("\n== PHASE D — revolving door ==")
    page.set_viewport_size(DESKTOP)
    goto(page, f"{ROUTE}?lob_rd=1")
    shot(page, "D01_rd_index_full")
    shot(page, "D02_rd_index_above_fold", full_page=False)
    scroll_by(page, 1500); settle(page, 400)
    shot(page, "D03_rd_index_cards", full_page=False)

    # Click first DPO card if possible
    link = page.locator('a[href*="lob_dpo="]').first
    if link.count() > 0:
        href = link.get_attribute("href")
        print(f"     opening DPO {href}")
        try:
            link.click(); settle(page, WARM_LOAD_WAIT)
            shot(page, "D04_dpo_individual_full")
            shot(page, "D05_dpo_individual_above_fold", full_page=False)
            scroll_by(page, 800); settle(page, 400)
            shot(page, "D06_dpo_firms_clients", full_page=False)
            scroll_by(page, 1200); settle(page, 400)
            shot(page, "D07_dpo_returns", full_page=False)
        except Exception as e:
            print(f"     DPO click failed: {e}")


# ─────────────────────────────────────────────────────────────────────────
# Phase E — Org Index
# ─────────────────────────────────────────────────────────────────────────

def phase_org_index(page: Page) -> None:
    print("\n== PHASE E — org index ==")
    page.set_viewport_size(DESKTOP)
    goto(page, f"{ROUTE}?lob_orgindex=1")
    shot(page, "E01_org_index_full")
    shot(page, "E02_org_index_above_fold", full_page=False)
    scroll_by(page, 1200); settle(page, 400)
    shot(page, "E03_org_index_cards", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase F — Topic search
# ─────────────────────────────────────────────────────────────────────────

def phase_topic(page: Page) -> None:
    print("\n== PHASE F — topic ==")
    page.set_viewport_size(DESKTOP)
    goto(page, f"{ROUTE}?lob_topic=Housing%20crisis")
    shot(page, "F01_topic_housing_full")
    shot(page, "F02_topic_housing_above_fold", full_page=False)
    scroll_by(page, 1200); settle(page, 400)
    shot(page, "F03_topic_housing_results", full_page=False)

    goto(page, f"{ROUTE}?lob_topic=Immigration%20%26%20asylum")
    shot(page, "F04_topic_immigration_above_fold", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase G — Provenance + edges
# ─────────────────────────────────────────────────────────────────────────

def phase_edges(page: Page) -> None:
    print("\n== PHASE G — provenance + edges ==")
    page.set_viewport_size(DESKTOP)
    goto(page)
    scroll_by(page, 10000); settle(page, 500)
    exp = page.locator('summary, button').filter(
        has_text=re.compile(r"About.*data|provenance|source", re.IGNORECASE)
    ).first
    if exp.count() > 0:
        try:
            exp.click(); settle(page, 800)
            shot(page, "G01_provenance_open", full_page=False)
        except Exception as e:
            print(f"     provenance click failed: {e}")

    # Bogus org
    goto(page, f"{ROUTE}?lob_org=NONEXISTENT_ORG")
    shot(page, "G02_bogus_org_empty_state", full_page=False)

    # Bogus DPO
    goto(page, f"{ROUTE}?lob_dpo=NONEXISTENT_PERSON")
    shot(page, "G03_bogus_dpo_empty_state", full_page=False)


# ─────────────────────────────────────────────────────────────────────────
# Phase H — Responsive
# ─────────────────────────────────────────────────────────────────────────

def phase_responsive(page: Page) -> None:
    print("\n== PHASE H — responsive ==")

    page.set_viewport_size(TABLET)
    goto(page)
    shot(page, "H01_tablet_landing_full")
    shot(page, "H02_tablet_above_fold", full_page=False)
    scroll_by(page, 1000); settle(page, 400)
    shot(page, "H03_tablet_gateway", full_page=False)
    scroll_by(page, 1500); settle(page, 400)
    shot(page, "H04_tablet_revolving_door", full_page=False)

    page.set_viewport_size(MOBILE)
    goto(page)
    shot(page, "H05_mobile_landing_full")
    shot(page, "H06_mobile_above_fold", full_page=False)
    scroll_by(page, 700); settle(page, 400)
    shot(page, "H07_mobile_gateway", full_page=False)
    scroll_by(page, 1000); settle(page, 400)
    shot(page, "H08_mobile_leaderboards", full_page=False)
    scroll_by(page, 1500); settle(page, 400)
    shot(page, "H09_mobile_revolving_door", full_page=False)

    # Mobile org profile (deep link)
    goto(page, f"{ROUTE}?lob_org=Ibec")
    shot(page, "H10_mobile_org_profile_full")
    shot(page, "H11_mobile_org_profile_above_fold", full_page=False)

    page.set_viewport_size(DESKTOP)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()
        phases = [
            ("LANDING",         phase_landing),
            ("SIDEBAR",         phase_sidebar),
            ("GATEWAY",         phase_gateway),
            ("REVOLVING DOOR",  phase_revolving_door),
            ("ORG INDEX",       phase_org_index),
            ("TOPIC",           phase_topic),
            ("EDGES",           phase_edges),
            ("RESPONSIVE",      phase_responsive),
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
