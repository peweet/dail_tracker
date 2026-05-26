"""Visual verification of the sidebar fixes applied 2026-05-26.

Captures sidebar state across the pages we touched:
- Attendance / Payments / Votes / Attendance Overview — verify single
  selectbox replaces the old text_input + selectbox Enter-trap.
- Committees — verify it now has the expected header + caption (no
  st.error path; we can't easily simulate the empty branch).
- Lobbying — verify the Notable Targets chips no longer wrap mid-word.
- All pages — verify the new <h2 class="page-title"> rendering (looks
  identical visually, screen-readers can navigate by heading).
"""
from __future__ import annotations

import time
from pathlib import Path

from playwright.sync_api import Page, sync_playwright

BASE = "http://localhost:8501"
OUT = Path(__file__).resolve().parent / "verify_sidebar"
OUT.mkdir(exist_ok=True)

DESKTOP = {"width": 1440, "height": 900}
COLD = 12000
WARM = 4500


def shot(page: Page, name: str, *, full_page: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    print(f"  -> {path.name} ({path.stat().st_size // 1024} KB)")


def goto(page: Page, path: str, *, cold: bool = False) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    time.sleep((COLD if cold else WARM) / 1000)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport=DESKTOP, device_scale_factor=1)
        page = ctx.new_page()

        # Attendance — single selectbox (no text_input + selectbox pair)
        goto(page, "/rankings-attendance", cold=True)
        shot(page, "01_attendance_sidebar")

        # Payments — sidebar_page_header now present
        goto(page, "/rankings-payments")
        shot(page, "02_payments_sidebar_with_header")

        # Lobbying — notable chips no longer wrap mid-word
        goto(page, "/rankings-lobbying")
        try:
            exp = page.locator(
                '[data-testid="stSidebar"] details summary, [data-testid="stSidebar"] button'
            ).filter(has_text="Notable targets").first
            if exp.count() > 0:
                exp.click()
                time.sleep(WARM / 1000)
        except Exception as e:
            print(f"     notable-targets click failed: {e}")
        shot(page, "03_lobbying_notable_no_wrap")

        # Committees — clean sidebar
        goto(page, "/rankings-committees")
        shot(page, "04_committees_sidebar")

        # Attendance Overview — divider gone
        goto(page, "/rankings-attendance-overview")
        shot(page, "05_attendance_overview_sidebar")

        ctx.close()
        browser.close()
    print(f"\nDONE. Shots in {OUT}")


if __name__ == "__main__":
    main()
