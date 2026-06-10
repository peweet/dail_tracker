"""Capture the member-overview profile section-by-section for the declutter pass."""

from __future__ import annotations

import time
from pathlib import Path
from urllib.parse import quote

from playwright.sync_api import sync_playwright

OUT = Path(__file__).resolve().parent / "_mo_declutter"
OUT.mkdir(parents=True, exist_ok=True)
BASE = "http://localhost:8534"
CODE = "Michael-Healy-Rae.D.2011-03-09"

SECTIONS = [
    "interests",
    "lobbying",
    "payments",
    "attendance",
    "votes",
    "debates",
    "questions",
    "legislation",
    "committees",
]


def main() -> None:
    with sync_playwright() as p:
        b = p.chromium.launch(headless=True)
        ctx = b.new_context(viewport={"width": 1440, "height": 1300})
        page = ctx.new_page()
        page.set_default_timeout(30000)
        page.goto(
            f"{BASE}/member-overview?member={quote(CODE, safe='')}",
            wait_until="domcontentloaded",
        )
        time.sleep(14)

        # Hero / top of page
        page.screenshot(path=str(OUT / "00_hero.png"))

        for i, sid in enumerate(SECTIONS, start=1):
            anchor = page.locator(f"#mo-section-{sid}")
            if anchor.count() == 0:
                print(f"missing anchor: {sid}")
                continue
            anchor.scroll_into_view_if_needed()
            time.sleep(1.2)
            page.screenshot(path=str(OUT / f"{i:02d}_{sid}.png"))
            print("saved", sid)

        # extra: payments scrolled further down (chart + records)
        anchor = page.locator("#mo-section-payments")
        if anchor.count():
            anchor.scroll_into_view_if_needed()
            page.mouse.wheel(0, 1100)
            time.sleep(1.0)
            page.screenshot(path=str(OUT / "03b_payments_lower.png"))
            page.mouse.wheel(0, 1100)
            time.sleep(1.0)
            page.screenshot(path=str(OUT / "03c_payments_records.png"))

        ctx.close()
        b.close()
    print("DONE")


if __name__ == "__main__":
    main()
