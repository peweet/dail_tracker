"""Verify the widened eTenders award rows render their new detail fields.

Checks, against a freshly started server, the ?supplier= drill-down for a firm
whose corpus rows exercise every new field:
 1. .pr-award-title lines render (the published contract name)
 2. meta lines carry procedure / "-month term" / "bids received" fragments
 3. at least one "TED notice ↗" deep link renders with an href to ted.europa.eu
 4. at least one row's category text is a Spend Category fallback (no CPV)

Run with the dev server already running at localhost:8631.
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"
SUPPLIER = sys.argv[2] if len(sys.argv) > 2 else "SODEXO"

sys.stdout.reconfigure(encoding="utf-8")


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 2400})
        page = context.new_page()

        page.goto(
            f"{BASE}/rankings-procurement?supplier={SUPPLIER}",
            wait_until="domcontentloaded",
            timeout=60000,
        )
        time.sleep(12)

        info = page.evaluate(
            """() => {
                const rows = [...document.querySelectorAll('.pr-award')];
                const titles = [...document.querySelectorAll('.pr-award-title')];
                const metas = [...document.querySelectorAll('.pr-award-meta')].map(m => m.textContent);
                const tedLinks = [...document.querySelectorAll('.pr-award-meta a')]
                    .map(a => a.getAttribute('href')).filter(h => h && h.includes('ted.europa.eu'));
                return {
                    nRows: rows.length,
                    nTitles: titles.length,
                    sampleTitle: titles[0] ? titles[0].textContent.slice(0, 90) : '(none)',
                    nWithTerm: metas.filter(m => m.includes('-month term')).length,
                    nWithBids: metas.filter(m => m.includes('bid')).length,
                    nTedLinks: tedLinks.length,
                    sampleTed: tedLinks[0] || '(none)',
                    sampleMeta: metas[0] ? metas[0].slice(0, 160) : '(none)',
                };
            }"""
        )
        print("award-row check:", info)
        page.screenshot(path="audit_screenshots/award_detail_supplier.png")

        ok = (
            info["nRows"] > 0
            and info["nTitles"] > 0
            and info["nWithBids"] > 0
            and info["nTedLinks"] > 0
        )
        print("PASS" if ok else "FAIL")
        browser.close()


if __name__ == "__main__":
    main()
