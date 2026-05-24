"""Targeted re-capture to verify the round-3 audit P0 fixes.

Captures four legacy-redirect callouts (one per refactored dimension page)
plus a fresh /member-overview to check the header-clip fix. Files use a
`_v2` suffix so we can diff against the originals.
"""
from __future__ import annotations

import sys
import time
import urllib.parse
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "utility"))

from playwright.sync_api import Page, sync_playwright  # noqa: E402

BASE = "http://localhost:8511"
OUT = Path(__file__).resolve().parent
MARY_LOU = "Mary Lou McDonald"

PAGE_LOAD_WAIT = 5000


def shot(page: Page, name: str, *, full_page: bool = False) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full_page)
    print(f"  -> {path.name}")


def goto(page: Page, path: str) -> None:
    print(f"\n  > {path}")
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded")
    time.sleep(PAGE_LOAD_WAIT / 1000)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()

        # ── Header clip — fresh load of /member-overview ──
        goto(page, "/member-overview")
        shot(page, "v2_01_header_clip_check")

        # ── Profile load with REAL code (no name_join_key needed) ──
        code = urllib.parse.quote("Mary-Lou-McDonald.D.2011-03-09", safe="")
        goto(page, f"/member-overview?member={code}")
        shot(page, "v2_04_profile_default")

        # ── Legacy redirects: should now show ONLY the callout, no page body ──
        goto(page, f"/rankings-attendance?att_td={MARY_LOU}")
        shot(page, "v2_21_att_legacy_redirect")

        goto(page, f"/rankings-interests?member={MARY_LOU}")
        shot(page, "v2_42_int_legacy_redirect")

        goto(page, f"/rankings-payments?member={MARY_LOU}")
        shot(page, "v2_52_pay_legacy_redirect")

        goto(page, f"/rankings-committees?member={MARY_LOU}")
        shot(page, "v2_73_comm_legacy_redirect")

        # ── Verify card-click target works ──
        # Open /rankings-attendance landing, then capture the link target on a
        # rendered card to confirm cross-page contract is repaired.
        goto(page, "/rankings-attendance")
        shot(page, "v2_20_att_landing_after_fix")

        ctx.close()
        browser.close()
    print(f"\nDone. {OUT}")


if __name__ == "__main__":
    main()
