"""Verify the authority (?authority=) and category (?cpv=) drill-down award rows
render the new detail fields (title line + procedure/term/bids/TED meta).

Run with the dev server already running at localhost:8631.
"""

from __future__ import annotations

import sys
import time
from urllib.parse import quote

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"

sys.stdout.reconfigure(encoding="utf-8")

TARGETS = [
    ("authority", "Dublin City Council", "award_detail_authority.png"),
    ("cpv", "72000000", "award_detail_cpv.png"),  # IT services
]


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 2400})
        page = context.new_page()
        all_ok = True

        for param, value, shot in TARGETS:
            page.goto(
                f"{BASE}/rankings-procurement?{param}={quote(value)}",
                wait_until="domcontentloaded",
                timeout=60000,
            )
            # poll instead of a fixed sleep — the bigger drill-downs (1k+ rows) can take
            # well past 12s on a cold cache before the award rows replace the skeleton
            for _ in range(12):
                time.sleep(5)
                if page.evaluate("() => document.querySelectorAll('.pr-award-title').length") > 0:
                    break
            info = page.evaluate(
                """() => {
                    const titles = [...document.querySelectorAll('.pr-award-title')];
                    const metas = [...document.querySelectorAll('.pr-award-meta')].map(m => m.textContent);
                    const tedLinks = [...document.querySelectorAll('.pr-award-meta a')]
                        .map(a => a.getAttribute('href')).filter(h => h && h.includes('ted.europa.eu'));
                    return {
                        nTitles: titles.length,
                        sampleTitle: titles[0] ? titles[0].textContent.slice(0, 90) : '(none)',
                        nWithBids: metas.filter(m => m.includes('bid')).length,
                        nTedLinks: tedLinks.length,
                        sampleMeta: metas[0] ? metas[0].slice(0, 160) : '(none)',
                    };
                }"""
            )
            ok = info["nTitles"] > 0 and info["nWithBids"] > 0
            all_ok = all_ok and ok
            print(f"{param}={value}: {'PASS' if ok else 'FAIL'} {info}")
            page.screenshot(path=f"audit_screenshots/{shot}")

        print("ALL PASS" if all_ok else "SOME FAILED")
        browser.close()


if __name__ == "__main__":
    main()
