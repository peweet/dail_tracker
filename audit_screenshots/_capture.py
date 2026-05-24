"""Lobbying-page audit screenshot capture.

Walks every reachable state of the Lobbying page (and the embedded
render_member_lobbying inside member-overview) and saves PNGs to
audit_screenshots/. Designed for the impeccable audit pass.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import sync_playwright

OUT = Path(__file__).parent
BASE = "http://localhost:8501"

# Desktop viewport (typical journalist laptop).
DESKTOP = {"width": 1440, "height": 900}
MOBILE = {"width": 390, "height": 844}  # iPhone 14 portrait


def _wait_for_render(page, settle_ms: int = 2500) -> None:
    """Streamlit is async; wait for the spinner to stop and a key element to mount.

    We anchor on the global skip-nav landmark and then a hard wait for any
    deferred re-renders triggered by query_param sync.
    """
    try:
        page.wait_for_load_state("networkidle", timeout=15000)
    except Exception:
        pass
    # Streamlit's "Running" spinner — give it time to clear.
    try:
        page.wait_for_function(
            """() => !document.querySelector('[data-testid="stStatusWidget"] [aria-label*="Running"]')""",
            timeout=12000,
        )
    except Exception:
        pass
    page.wait_for_timeout(settle_ms)


def _shoot(page, name: str, full_page: bool = True) -> Path:
    p = OUT / f"{name}.png"
    page.screenshot(path=str(p), full_page=full_page)
    print(f"  saved {p.name}  ({p.stat().st_size // 1024} KB)")
    return p


def capture(playwright):
    browser = playwright.chromium.launch(headless=True)
    ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
    page = ctx.new_page()

    # 1. Landing page
    print("[1/11] landing")
    page.goto(f"{BASE}/rankings-lobbying", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=4000)
    _shoot(page, "01_landing")

    # 2. Landing — date-range expander open
    print("[2/11] landing + date-range expander open")
    try:
        page.get_by_text("Filter by date range", exact=False).first.click()
        _wait_for_render(page, settle_ms=1200)
        _shoot(page, "02_landing_date_open")
    except Exception as e:
        print(f"   skipped date expander: {e}")

    # 3. Organisation index
    print("[3/11] org index")
    page.goto(f"{BASE}/rankings-lobbying?lob_orgindex=1", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=3500)
    _shoot(page, "03_org_index")

    # 4. Org detail — pick a real org name from the index page
    print("[4/11] org detail — Ibec")
    page.goto(f"{BASE}/rankings-lobbying?lob_org={quote('Ibec')}", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=3500)
    _shoot(page, "04_org_detail_ibec")

    # 5. Revolving Door index
    print("[5/11] revolving-door index")
    page.goto(f"{BASE}/rankings-lobbying?lob_rd=1", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=3500)
    _shoot(page, "05_rd_index")

    # 6. Revolving Door — individual. Pick the top-ranked card by reading its name.
    print("[6/11] revolving-door individual (top of list)")
    try:
        # Look for the first "View revolving door profile for" link
        link = page.locator('a[aria-label^="View revolving door profile for"]').first
        href = link.get_attribute("href")
        if href:
            target_url = href if href.startswith("http") else f"{BASE}/rankings-lobbying{href}"
            page.goto(target_url, wait_until="domcontentloaded")
            _wait_for_render(page, settle_ms=3500)
            _shoot(page, "06_rd_individual")
        else:
            print("   couldn't find DPO link — skipping")
    except Exception as e:
        print(f"   skipped RD individual: {e}")

    # 7. Topic Stage 2 — Climate
    print("[7/11] topic Stage 2 — Climate")
    page.goto(f"{BASE}/rankings-lobbying?lob_topic={quote('Climate')}", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=4000)
    _shoot(page, "07_topic_climate")

    # 8. Area Stage 2 — go back to landing, find a top policy area from the URL
    print("[8/11] area Stage 2 — Housing")
    # Common areas: "Housing, Planning and Local Government", "Health", "Justice and Equality"
    for area_name in ("Housing", "Health", "Justice and Equality"):
        page.goto(
            f"{BASE}/rankings-lobbying?lob_area={quote(area_name)}",
            wait_until="domcontentloaded",
        )
        _wait_for_render(page, settle_ms=3500)
        # If the page didn't 'No returns', we found one
        content = page.content()
        if "No lobbying returns on record" not in content:
            _shoot(page, f"08_area_{area_name.lower().split()[0]}")
            captured_area = area_name
            break
    else:
        # Take whatever came last as evidence of the empty state path
        _shoot(page, "08_area_empty")
        captured_area = None

    # 9. Area x politician (Stage 3)
    if captured_area:
        print(f"[9/11] area x politician — first card on {captured_area}")
        try:
            link = page.locator('a[aria-label^="View every return targeting"]').first
            href = link.get_attribute("href")
            if href:
                target_url = href if href.startswith("http") else f"{BASE}/rankings-lobbying{href}"
                page.goto(target_url, wait_until="domcontentloaded")
                _wait_for_render(page, settle_ms=3500)
                _shoot(page, "09_area_politician_results")
            else:
                print("   couldn't find area-politician link")
        except Exception as e:
            print(f"   skipped area x politician: {e}")
    else:
        print("[9/11] skipped (no non-empty area)")

    # 10. Mobile breakpoint — landing
    print("[10/11] mobile landing")
    page.set_viewport_size(MOBILE)
    page.goto(f"{BASE}/rankings-lobbying", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=4000)
    _shoot(page, "10_mobile_landing")

    # Mobile — org index
    print("[10b/11] mobile org index")
    page.goto(f"{BASE}/rankings-lobbying?lob_orgindex=1", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=3500)
    _shoot(page, "10b_mobile_org_index")

    # 11. Member-overview embedded lobbying — try a known TD
    print("[11/11] embedded — member-overview lobbying section")
    page.set_viewport_size(DESKTOP)
    # Visit /member-overview without a code → landing list; then we need a code
    page.goto(f"{BASE}/member-overview", wait_until="domcontentloaded")
    _wait_for_render(page, settle_ms=3500)
    _shoot(page, "11a_member_overview_landing")
    # Try clicking the first profile link if present
    try:
        link = page.locator('a[href*="member-overview?member="]').first
        href = link.get_attribute("href")
        if href:
            # Force the lobbying section
            sep = "&" if "?" in href else "?"
            target = f"{href}{sep}section=lobbying"
            if not target.startswith("http"):
                target = f"{BASE}{target}" if target.startswith("/") else f"{BASE}/{target}"
            page.goto(target, wait_until="domcontentloaded")
            _wait_for_render(page, settle_ms=4000)
            _shoot(page, "11b_member_overview_with_lobbying")
    except Exception as e:
        print(f"   skipped member-overview embed: {e}")

    # 12. Bonus: empty state for an org that doesn't exist
    print("[bonus] empty-state org")
    page.goto(
        f"{BASE}/rankings-lobbying?lob_org={quote('THIS_ORG_DOES_NOT_EXIST_XYZ')}",
        wait_until="domcontentloaded",
    )
    _wait_for_render(page, settle_ms=3000)
    _shoot(page, "12_empty_state_org")

    browser.close()


if __name__ == "__main__":
    with sync_playwright() as p:
        capture(p)
    print("DONE")
