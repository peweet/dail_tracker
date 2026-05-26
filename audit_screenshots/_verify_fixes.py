"""Quick visual verification of the fixes applied 2026-05-26.

Captures one above-fold screenshot of each touched page so we can
eyeball that:
- Attendance default year is now a completed year (P1-1); leaderboard
  counts are within the official sitting-day total (P0).
- Lobbying hero badge shows a real date range, not "Data: None → None"
  (P0-1).
- Lobbying org profile (Ibec) no longer renders "nan" badges (P1-1).
- Payments Rankings cards now show names + TAA bands + total badges
  (P0-1 + P0-2 — schema-aligned + reading from registered view).
- Legacy ?lob_pol= redirect renders a stable "moved" callout, not a
  blank flash (P1-2).
- Cross-page "Per-td" → "Per-TD" casing fix.

Output: audit_screenshots/verify_fixes/*.png
"""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "verify_fixes"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
COLD = 10000
WARM = 3500


def shot(page: Page, name: str) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=False)
    kb = path.stat().st_size // 1024
    print(f"  -> {path.name} ({kb} KB)")


def goto(page: Page, path: str, *, cold: bool = False) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    time.sleep((COLD if cold else WARM) / 1000)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()

        # P0/P1 attendance — default year is now most-recent COMPLETED;
        # leaderboard counts are within official sitting-day total.
        goto(page, "/rankings-attendance", cold=True)
        shot(page, "01_attendance_landing")

        # P0 lobbying — hero badge should show real date range.
        goto(page, "/rankings-lobbying")
        shot(page, "02_lobbying_landing")

        # P1 lobbying — org Stage 2 should no longer say "nan".
        goto(page, "/rankings-lobbying?lob_org=Ibec")
        shot(page, "03_lobbying_ibec_no_nan")

        # P1 lobbying — legacy ?lob_pol= redirect should now render
        # the shared member_moved_callout (stable, with Per-TD casing).
        goto(page, "/rankings-lobbying?lob_pol=Mary%20Lou%20McDonald")
        shot(page, "04_lobbying_legacy_pol_redirect")

        # P0 payments — Rankings view should now show names + TAA bands.
        goto(page, "/rankings-payments")
        time.sleep(1)
        # Click "Rankings" segment via header label
        rb = page.locator('[data-testid="stSegmentedControl"] button:has-text("Rankings")').first
        if rb.count() > 0:
            try:
                rb.click()
                time.sleep(WARM / 1000)
            except Exception:
                pass
        shot(page, "05_payments_rankings_with_names")

        # Cross-page Per-td → Per-TD casing fix — visiting a moved-member
        # link on attendance should now say "Per-TD attendance" exactly.
        goto(page, "/rankings-attendance?att_td=Catherine%20Connolly")
        shot(page, "06_attendance_per_td_casing")

        # Cross-page Per-td fix on payments too.
        goto(page, "/rankings-payments?member=Catherine%20Connolly")
        shot(page, "07_payments_per_td_casing")

        ctx.close()
        browser.close()
    print(f"\nDONE. Verification shots in {OUT}")


if __name__ == "__main__":
    main()
