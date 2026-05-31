"""Smoke-test the Corporate page: navigate, screenshot, verify key markers.
Throwaway after first pass; per reference_audit_toolkit kept as the page audit
artifact for the corporate slug."""
from __future__ import annotations
import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

URL = "http://localhost:8596/rankings-corporate"
SHOT_DIR = Path(__file__).resolve().parent / "_smoke_out"
SHOT_DIR.mkdir(exist_ok=True)


def main() -> int:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1280, "height": 900})

        page.goto(URL, wait_until="domcontentloaded", timeout=30000)
        # Wait for the Streamlit main container then for h1/featured-panel to paint.
        try:
            page.wait_for_selector("[data-testid='stMain']", timeout=30000)
            page.wait_for_selector("h1:has-text('Corporate'), .corp-featured-h", timeout=30000)
        except Exception:
            pass
        try:
            page.locator("button:has-text('Stop')").click(timeout=1000)
        except Exception:
            pass
        page.wait_for_timeout(1500)

        body = page.locator("body").inner_text()
        (SHOT_DIR / "corp_body.txt").write_text(body, encoding="utf-8")
        page.screenshot(path=str(SHOT_DIR / "corp_01_landing.png"), full_page=False)

        markers = {
            "hero kicker": "CORPORATE DISTRESS",
            "hero title": "Corporate",
            "featured kicker": "RECEIVER-APPOINTERS",
            "featured heading": "Who's calling in Irish loans",
            "honest coverage subhead": "name a known loan-book buyer",
            "Cerberus listed": "Cerberus",
            "AIB listed": "AIB",
            "glossary SPV": "SPV",
            "glossary ICAV": "ICAV",
            "glossary SCARP": "SCARP",
            "privacy line": "personal insolvency",
            "sparkline label": "Receivership notices by year",
            "search input present": True,  # placeholder isn't in DOM; check via selector
            "type tabs": "Receiverships",
            "feed marker": "match the current filters",
        }
        results = {k: (v in body) if isinstance(v, str) else True for k, v in markers.items()}
        # Search input check
        try:
            si = page.locator("input[aria-label='Search by company name']").count()
            results["search input present"] = si > 0
        except Exception:
            results["search input present"] = False

        # Capture scrolled feed view + a Cerberus-filtered view.
        try:
            page.locator(".corp-card-link").first.scroll_into_view_if_needed(timeout=4000)
        except Exception:
            pass
        page.wait_for_timeout(600)
        page.screenshot(path=str(SHOT_DIR / "corp_02_feed.png"), full_page=False)

        # Filter by fund via the URL handler.
        page.goto(URL + "?fund=Cerberus", wait_until="domcontentloaded", timeout=20000)
        try:
            page.wait_for_selector(".corp-active-chip", timeout=15000)
        except Exception:
            pass
        page.wait_for_timeout(1200)
        page.screenshot(path=str(SHOT_DIR / "corp_03_cerberus.png"), full_page=False)
        body3 = page.locator("body").inner_text()
        (SHOT_DIR / "corp_body3.txt").write_text(body3, encoding="utf-8")
        results["Cerberus filter chip"] = "Fund: Cerberus" in body3 or "FILTERED BY" in body3.upper()

        # Check for any visible Python traceback / error.
        errors_visible = "Traceback" in body or "Traceback" in body3 or "Something went wrong" in body

        browser.close()

    print("=== marker checks ===")
    for k, ok in results.items():
        print(f"   {'OK ' if ok else 'MISS'}  {k}")
    print(f"   {'NO error visible' if not errors_visible else 'ERROR text visible'}")
    print(f"\nsaved under {SHOT_DIR}")
    return 0 if all(results.values()) and not errors_visible else 1


if __name__ == "__main__":
    sys.exit(main())
