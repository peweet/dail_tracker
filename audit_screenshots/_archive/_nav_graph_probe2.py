"""Follow-up probe: characterise the Company back-links (D) and properly
drill the Public Payments supplier detail view (B)."""

from __future__ import annotations

import sys
import time
from urllib.parse import quote

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"


def harvest(page):
    return page.evaluate(
        """() => [...document.querySelectorAll('a')].map(a => ({
            href: a.getAttribute('href') || '',
            text: (a.textContent || '').trim().slice(0, 70)
        }))"""
    )


def goto(page, path, settle=10):
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
    time.sleep(settle)


def main() -> None:
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 4200})
        page = ctx.new_page()

        # ---- D: characterise Company back-links ----
        goto(page, "/company?supplier=" + quote("DELOITTE LLP"), settle=12)
        anchors = harvest(page)
        print("\n===== D: /company?supplier=DELOITTE LLP back-links =====")
        for a in anchors:
            if "rankings-procurement" in a["href"] or "rankings-public-payments" in a["href"]:
                print(f"  href={a['href']!r}\n     text={a['text']!r}")
        print(f"  (total anchors on page: {len(anchors)})")

        # ---- B: Public Payments supplier drill via URL ----
        goto(page, "/rankings-public-payments", settle=12)
        anchors = harvest(page)
        sup = [a for a in anchors if a["href"].startswith("?supplier=")]
        pub = [a for a in anchors if a["href"].startswith("?publisher=")]
        print("\n===== B: Public Payments landing anchors =====")
        print(f"  ?supplier= anchors: {len(sup)} (sample: {[a['href'] for a in sup[:3]]})")
        print(f"  ?publisher= anchors: {len(pub)} (sample: {[a['href'] for a in pub[:3]]})")
        company_top = [a for a in anchors if "/company?supplier=" in a["href"]]
        print(f"  /company anchors at landing: {len(company_top)}")

        # drill into first supplier by URL
        if sup:
            goto(page, "/rankings-public-payments" + sup[0]["href"], settle=12)
            danchors = harvest(page)
            dcompany = [a for a in danchors if "/company?supplier=" in a["href"]]
            print(f"\n  drilled {sup[0]['href']!r}")
            print(f"    url={page.url}")
            print(f"    /company anchors in supplier detail: {len(dcompany)}")
            print(f"    sample anchors in detail: "
                  f"{[(a['href'][:40], a['text'][:30]) for a in danchors[:12]]}")

        # also drill a publisher (the buyer side) to see if it links suppliers->company
        if pub:
            goto(page, "/rankings-public-payments" + pub[0]["href"], settle=12)
            panchors = harvest(page)
            pcompany = [a for a in panchors if "/company?supplier=" in a["href"]]
            print(f"\n  drilled publisher {pub[0]['href']!r}")
            print(f"    url={page.url}")
            print(f"    /company anchors in publisher detail: {len(pcompany)}")

        browser.close()


if __name__ == "__main__":
    main()
