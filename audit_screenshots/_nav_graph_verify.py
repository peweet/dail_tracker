"""Re-test the two implemented fixes against the live DOM.

  #1 Public Payments -> Company: drill into a supplier detail (by URL) and
     expect a contextual /company?supplier= anchor now present.
  #2 Member Overview bills: load a member with sponsored bills and expect
     /rankings-legislation?bill= anchors now present.

Run with a fresh server up:  python _nav_graph_verify.py http://127.0.0.1:8645
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"
# Seeded from the pre-fix run: this member's profile had a Legislation section
# present but 0 bill anchors. Post-fix it should expose bill= links.
MEMBER = "Aengus-%C3%93-Snodaigh.D.2002-06-06"


def harvest(page):
    return page.evaluate(
        """() => [...document.querySelectorAll('a')].map(a => ({
            href: a.getAttribute('href') || '',
            text: (a.textContent || '').trim().slice(0, 60)
        }))"""
    )


def goto(page, path, settle=11):
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
    time.sleep(settle)


def main() -> None:
    out = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 4200})
        page = ctx.new_page()

        # #1 — Public Payments supplier detail
        goto(page, "/rankings-public-payments", settle=12)
        sup = [a for a in harvest(page) if a["href"].startswith("?supplier=")]
        if sup:
            goto(page, "/rankings-public-payments" + sup[0]["href"], settle=12)
            comp = [a for a in harvest(page) if "/company?supplier=" in a["href"]]
            out.append((
                "#1 PublicPayments supplier -> Company",
                "PASS" if comp else "FAIL",
                f"drilled {sup[0]['href']!r}; /company anchors={len(comp)} "
                f"(sample={[a['href'] for a in comp[:2]]})",
            ))
        else:
            out.append(("#1 PublicPayments supplier -> Company", "SKIP", "no supplier anchors"))

        # #2 — Member Overview bills
        goto(page, "/member-overview?member=" + MEMBER, settle=13)
        anchors = harvest(page)
        bills = [a for a in anchors if "bill=" in a["href"]]
        has_leg = page.locator("text=Legislation sponsored").count() > 0
        out.append((
            "#2 MemberOverview bill links",
            "PASS" if bills else "FAIL",
            f"bill anchors={len(bills)}; legislation section present={has_leg} "
            f"(sample={[a['href'] for a in bills[:2]]})",
        ))

        browser.close()

    print("\n" + "=" * 70)
    print("NAV GRAPH FIX VERIFICATION")
    print("=" * 70)
    for name, verdict, detail in out:
        print(f"\n[{verdict}] {name}\n    {detail}")
    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()
