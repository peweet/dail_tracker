"""Cross-page sidebar audit capture.

Walks every page in the app and captures sidebar-focused screenshots so
we can compare layouts side-by-side. Each page gets:
- Desktop sidebar (1440x900, sidebar expanded)
- Mobile sidebar (390x844, sidebar drawer open)
- Sidebar collapsed state (desktop)

Output: audit_screenshots/sidebar/<phase>_<page>_<state>.png

Pages walked (route slug per app.py):
  /member-overview
  /rankings-attendance
  /rankings-votes
  /rankings-interests
  /rankings-payments
  /rankings-lobbying
  /rankings-lobbying-poc
  /rankings-legislation
  /rankings-statutory-instruments
  /rankings-committees
  /glossary

Notes:
- Streamlit auto-collapses the sidebar on viewports <= ~960px; the mobile
  shots intentionally capture both states (drawer closed and drawer open).
- The `data-testid=stSidebarUserContent` selector is the canonical
  inner container for the sidebar body content.
"""
from __future__ import annotations

import re
import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "sidebar"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
MOBILE  = {"width": 390,  "height": 844}

INITIAL_LOAD_WAIT = 12000
WARM_LOAD_WAIT    = 4500
SETTLE_TINY       = 400

_cold = True

# (route, short_id, optional follow-on action description)
PAGES: list[tuple[str, str]] = [
    ("/member-overview",                "01_member_overview"),
    ("/rankings-attendance",            "02_attendance"),
    ("/rankings-votes",                 "03_votes_dail"),
    ("/rankings-interests",             "04_interests"),
    ("/rankings-payments",              "05_payments"),
    ("/rankings-lobbying",              "06_lobbying"),
    ("/rankings-lobbying-poc",          "07_lobbying_poc"),
    ("/rankings-legislation",           "08_legislation"),
    ("/rankings-statutory-instruments", "09_si"),
    ("/rankings-committees",            "10_committees"),
    ("/glossary",                       "11_glossary"),
]


def shot(page: Page, name: str, *, full_page: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    kb = path.stat().st_size // 1024
    print(f"  -> {path.name} ({kb} KB)")


def wait(ms: int) -> None:
    time.sleep(ms / 1000)


def goto(page: Page, path: str) -> None:
    global _cold
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    wait(INITIAL_LOAD_WAIT if _cold else WARM_LOAD_WAIT)
    _cold = False


def settle(page: Page, ms: int = 1500) -> None:
    wait(ms)


def ensure_sidebar_open(page: Page) -> bool:
    """Open the sidebar if it's collapsed. Returns True if it opened."""
    btn = page.locator('[data-testid="stSidebarCollapseButton"] button').first
    if btn.count() == 0:
        return False
    try:
        if btn.is_visible():
            btn.click()
            settle(page, 500)
            return True
    except Exception:
        return False
    return False


# ─────────────────────────────────────────────────────────────────────────
# Phase A — Desktop sidebar (expanded)
# ─────────────────────────────────────────────────────────────────────────

def phase_desktop(page: Page) -> None:
    print("\n== PHASE A — desktop sidebars ==")
    page.set_viewport_size(DESKTOP)
    for route, short_id in PAGES:
        goto(page, route)
        shot(page, f"A_{short_id}_desktop")


# ─────────────────────────────────────────────────────────────────────────
# Phase B — Mobile sidebar (drawer closed AND drawer open per page)
# ─────────────────────────────────────────────────────────────────────────

def phase_mobile(page: Page) -> None:
    print("\n== PHASE B — mobile sidebars ==")
    page.set_viewport_size(MOBILE)
    for route, short_id in PAGES:
        goto(page, route)
        # Drawer closed (default mobile state)
        shot(page, f"B_{short_id}_mobile_closed")
        # Try to open the drawer
        if ensure_sidebar_open(page):
            shot(page, f"B_{short_id}_mobile_open")
    page.set_viewport_size(DESKTOP)


# ─────────────────────────────────────────────────────────────────────────
# Phase C — Stateful interactions on a representative subset
# ─────────────────────────────────────────────────────────────────────────

def phase_stateful(page: Page) -> None:
    """Capture key state transitions of the sidebar widgets:
    - sidebar_member_filter with text typed (verifies the Enter-trap finding)
    - votes view toggle Dáil → TDs (sidebar reflows)
    - lobbying notable-targets expander opened
    """
    print("\n== PHASE C — stateful interactions ==")
    page.set_viewport_size(DESKTOP)

    # Votes — view toggle Dáil → TDs
    goto(page, "/rankings-votes")
    try:
        tds = page.locator(
            '[data-testid="stSidebar"] [data-testid="stSegmentedControl"] button:has-text("TDs")'
        ).first
        if tds.count() > 0:
            tds.click()
            settle(page, 1500)
            shot(page, "C_votes_tds_view")
    except Exception as e:
        print(f"     votes view toggle failed: {e}")

    # Attendance — type into sidebar search to expose Enter-trap behaviour
    goto(page, "/rankings-attendance")
    try:
        ti = page.locator('[data-testid="stSidebar"] input[type="text"]').first
        if ti.count() > 0:
            ti.fill("McDonald")
            settle(page, 1500)
            shot(page, "C_attendance_search_typed")
    except Exception as e:
        print(f"     attendance sidebar search failed: {e}")

    # Lobbying — open the Notable Targets expander
    goto(page, "/rankings-lobbying")
    try:
        exp = page.locator(
            '[data-testid="stSidebar"] details summary, [data-testid="stSidebar"] button'
        ).filter(has_text=re.compile("Notable targets", re.IGNORECASE)).first
        if exp.count() > 0:
            exp.click()
            settle(page, 1500)
            shot(page, "C_lobbying_notable_open")
    except Exception as e:
        print(f"     lobbying notable expander failed: {e}")

    # Committees — what happens in the sidebar when the data is missing?
    # (We can't easily simulate this without breaking the data layer; skip.)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()
        phases = [
            ("DESKTOP",  phase_desktop),
            ("MOBILE",   phase_mobile),
            ("STATEFUL", phase_stateful),
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
