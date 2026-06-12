"""Probe every page's search box: type → commit → clear → does the page reset?

For each page we record a result indicator (first count-ish heading or the
visible card count), then type a term, press Enter, then select-all + delete +
Enter, and compare indicators. FAIL = indicator does not return to baseline or
box text reappears.
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"

TARGETS = [
    ("/member-overview", "Find a TD", "healy"),
    ("/rankings-appointments", "Search the full record", "judge"),
    ("/rankings-corporate", "Search by company name", "bank"),
    ("/rankings-judiciary", "Search by party or judge", "kelly"),
    ("/rankings-statutory-instruments", "Search title", "fisheries"),
    ("/rankings-legislation", "Search title", "housing"),
    ("/glossary", "Search the glossary", "lobby"),
]


def indicator(page) -> str:
    return page.evaluate(
        """() => {
            const main = document.querySelector('[data-testid="stMainBlockContainer"]');
            if (!main) return '(no main)';
            const txt = main.innerText || '';
            // grab all "N something" count-looking fragments as a fingerprint
            const counts = (txt.match(/[\\d,]+\\s+(TDs?|records?|notices?|appointments?|results?|instruments?|bills?|terms?|judges?|matches?|organisations?|returns?)/gi) || []).slice(0, 5);
            return counts.join(' | ') || txt.slice(0, 120).replace(/\\n/g, ' ');
        }"""
    )


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        for path, label, term in TARGETS:
            try:
                page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
                time.sleep(8)
                box = page.locator(f'input[aria-label="{label}"]').first
                box.wait_for(timeout=8000)
                base = indicator(page)
                box.click()
                box.fill(term)
                box.press("Enter")
                time.sleep(3)
                typed = indicator(page)
                box.click()
                box.press("Control+a")
                box.press("Delete")
                box.press("Enter")
                time.sleep(3)
                cleared = indicator(page)
                val = box.input_value()
                ok = (cleared == base) and (val == "")
                print(f"{'PASS' if ok else 'FAIL'} {path} [{label}]")
                if not ok:
                    print(f"    base    : {base}")
                    print(f"    typed   : {typed}")
                    print(f"    cleared : {cleared}")
                    print(f"    box now : {val!r}")
            except Exception as e:  # noqa: BLE001
                print(f"SKIP {path} [{label}]: {type(e).__name__}: {e}")
        browser.close()


if __name__ == "__main__":
    main()
