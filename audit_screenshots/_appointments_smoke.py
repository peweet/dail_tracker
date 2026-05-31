"""Smoke-test the Public Appointments page: navigate, screenshot, dump
visible-text checks. Throwaway. Run after `streamlit run utility/app.py
--server.port=8513`."""
from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

URL = "http://localhost:8590/rankings-appointments"
SHOT_DIR = Path(__file__).resolve().parent / "_smoke_out"
SHOT_DIR.mkdir(exist_ok=True)


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        # Wait for Streamlit's main content container.
        try:
            page.wait_for_selector("[data-testid='stMain']", timeout=30000)
        except Exception:
            pass
        # Now wait specifically for either an h1 or our featured-panel heading
        # to confirm the script-run actually painted content.
        try:
            page.wait_for_selector("h1:has-text('Public Appointments'), .pa-featured-h", timeout=30000)
        except Exception:
            pass
        # Dismiss any one-shot rerun toast.
        try:
            page.locator("button:has-text('Stop')").click(timeout=1000)
        except Exception:
            pass
        page.wait_for_timeout(1500)  # let late re-renders settle

        # Capture body text + screenshot for visual review.
        body_text = page.locator("body").inner_text()
        (SHOT_DIR / "body.txt").write_text(body_text, encoding="utf-8")
        page.screenshot(path=str(SHOT_DIR / "01_landing.png"), full_page=False)

        # Markers to verify.
        markers = {
            "hero title": "Public Appointments",
            "kicker": "Public office",
            "constitutional caption": "first official language",
            "featured kicker": "Special advisers",
            "featured heading": "Who advises which minister",
            "search box": "Search the full record",
            "feed marker": "match the current filters",
            "iris credit": "Iris",
        }
        results = {label: (needle in body_text) for label, needle in markers.items()}

        # Check for any visible exception text from Streamlit.
        errors_visible = (
            "Traceback" in body_text
            or "uncaught" in body_text.lower()
            or "Something went wrong" in body_text
        )

        # Scroll to the feed by targeting the first card link, then snap.
        try:
            page.locator(".pa-card-link").first.scroll_into_view_if_needed(timeout=4000)
        except Exception:
            page.evaluate("document.querySelector('[data-testid=\"stMain\"]').scrollTo(0, 1300)")
        page.wait_for_timeout(800)
        page.screenshot(path=str(SHOT_DIR / "01b_feed.png"), full_page=False)

        # Try navigating to the SpAd-filtered view (?spark=2024) to exercise that path.
        page.goto(URL + "?spark=2024", wait_until="domcontentloaded", timeout=30000)
        try:
            page.wait_for_selector(".pa-active-chip", timeout=20000)
        except Exception:
            pass
        page.wait_for_timeout(1500)
        page.screenshot(path=str(SHOT_DIR / "02_year_2024.png"), full_page=False)
        body2 = page.locator("body").inner_text()
        (SHOT_DIR / "body2.txt").write_text(body2, encoding="utf-8")
        results["year filter applied"] = "2024" in body2 and "Filtered by" in body2

        # Scroll to capture the filtered feed.
        try:
            page.locator(".pa-active-chip").first.scroll_into_view_if_needed(timeout=4000)
        except Exception:
            page.evaluate("document.querySelector('[data-testid=\"stMain\"]').scrollTo(0, 1100)")
        page.wait_for_timeout(800)
        page.screenshot(path=str(SHOT_DIR / "02b_feed_2024.png"), full_page=False)

        browser.close()

    print("=== marker checks ===")
    for label, ok in results.items():
        print(f"   {'OK ' if ok else 'MISS'}  {label}")
    print(f"   {'NO error visible' if not errors_visible else 'ERROR text visible (see body.txt)'}")
    print(f"\nbody.txt + 01_landing.png + 02_year_2024.png saved under {SHOT_DIR}")
    return 0 if all(results.values()) and not errors_visible else 1


if __name__ == "__main__":
    sys.exit(main())
