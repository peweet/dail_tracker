"""Empirically validate the navigation-graph audit findings before implementation.

Tests four claims from the static audit against the live rendered DOM:

  A. BASELINE (should PASS as a working edge): Procurement supplier card
     links to /company?supplier=... and clicking it actually navigates.
     Proves the spa_links/href fix pattern works -> our fixes are viable.

  B. FALSE DEAD-END #1: Public Payments renders suppliers but exposes NO
     anchor to /company. Expect zero /company hrefs even after drilling
     into a single supplier. Confirms the flagship missing edge.

  C. FALSE DEAD-END #2: Member Overview legislation section renders bill
     titles as plain text, not anchored to ?bill=. Expect zero bill hrefs.

  D. ONE-WAY SLIDE: the /company dossier exposes NO anchor back to
     /rankings-procurement or /rankings-public-payments. Confirms the
     non-closing loop on the seller side.

Run with a fresh server already up:  python _nav_graph_test.py http://localhost:8645
"""

from __future__ import annotations

import sys
import time

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8645"
SETTLE = 8


def harvest(page):
    """Return list of {href, text} for every anchor on the page."""
    return page.evaluate(
        """() => [...document.querySelectorAll('a')].map(a => ({
            href: a.getAttribute('href') || '',
            text: (a.textContent || '').trim().slice(0, 60)
        }))"""
    )


def goto(page, path: str, settle: int = SETTLE):
    page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
    time.sleep(settle)


def count_hrefs(anchors, needle: str) -> list:
    return [a for a in anchors if needle in a["href"]]


def main() -> None:
    results: list[tuple[str, str, str]] = []  # (test, verdict, detail)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 4200})
        page = ctx.new_page()

        # ───────────────────────── A. BASELINE: procurement -> company ─────
        goto(page, "/rankings-procurement", settle=12)
        anchors = harvest(page)
        company_links = count_hrefs(anchors, "/company?supplier=")
        seed_supplier = None
        if company_links:
            seed_supplier = company_links[0]["href"]
            # click it and confirm navigation lands on /company
            page.evaluate("() => { window.__nav_sentinel = true; }")
            try:
                loc = page.locator('a[href*="/company?supplier="]').first
                loc.scroll_into_view_if_needed(timeout=8000)
                loc.click(timeout=8000)
                time.sleep(SETTLE)
                landed = "/company" in page.url
                results.append((
                    "A. Procurement->Company (baseline edge)",
                    "PASS" if landed else "FAIL",
                    f"{len(company_links)} /company anchors on procurement; "
                    f"after click url={page.url}",
                ))
            except Exception as e:  # noqa: BLE001
                results.append(("A. Procurement->Company (baseline edge)", "ERROR", str(e)))
        else:
            results.append((
                "A. Procurement->Company (baseline edge)",
                "FAIL",
                "no /company?supplier= anchor found on procurement page",
            ))

        # ───────────────────────── D. company return edges (one-way slide) ─
        # (we are on a /company page now if A navigated; else load seed)
        if seed_supplier:
            if "/company" not in page.url:
                goto(page, "/" + seed_supplier.lstrip("/"), settle=10)
            anchors = harvest(page)
            back_proc = count_hrefs(anchors, "rankings-procurement")
            back_pay = count_hrefs(anchors, "rankings-public-payments")
            n_back = len(back_proc) + len(back_pay)
            results.append((
                "D. Company->Procurement/PublicPayments (return edge)",
                "CONFIRMED MISSING" if n_back == 0 else "EDGE EXISTS",
                f"procurement back-links={len(back_proc)}, "
                f"public-payments back-links={len(back_pay)} on {page.url}",
            ))
        else:
            results.append((
                "D. Company->Procurement/PublicPayments (return edge)",
                "SKIPPED",
                "no seed supplier to load /company",
            ))

        # ───────────────────────── B. public payments -> company ──────────
        goto(page, "/rankings-public-payments", settle=12)
        anchors = harvest(page)
        pp_company_top = count_hrefs(anchors, "/company?supplier=")
        # drill into one supplier via its in-page ?supplier= card, then re-check
        supplier_drill = [a for a in anchors if a["href"].startswith("?supplier=")]
        drill_company = []
        drill_url = None
        if supplier_drill:
            try:
                loc = page.locator('a[href^="?supplier="]').first
                loc.scroll_into_view_if_needed(timeout=8000)
                loc.click(timeout=8000)
                time.sleep(SETTLE)
                drill_url = page.url
                drill_company = count_hrefs(harvest(page), "/company?supplier=")
            except Exception as e:  # noqa: BLE001
                drill_url = f"(drill click error: {e})"
        n_pp = len(pp_company_top) + len(drill_company)
        results.append((
            "B. PublicPayments->Company (false dead-end #1)",
            "CONFIRMED MISSING" if n_pp == 0 else "EDGE EXISTS",
            f"/company anchors: top-level={len(pp_company_top)}, "
            f"after supplier drill={len(drill_company)} (drill_url={drill_url})",
        ))

        # ───────────────────────── C. member overview bills ───────────────
        # seed a member code from the browse index
        goto(page, "/member-overview", settle=12)
        anchors = harvest(page)
        member_links = [a for a in anchors if "member=" in a["href"]]
        seed_member = member_links[0]["href"] if member_links else None
        if seed_member:
            # normalise to absolute path on member-overview
            path = seed_member if seed_member.startswith("?") else "/" + seed_member.lstrip("/")
            if path.startswith("?"):
                path = "/member-overview" + path
            goto(page, path, settle=12)
            anchors = harvest(page)
            bill_links = count_hrefs(anchors, "bill=")
            # presence check: is there a Legislation section at all on this member?
            has_leg = page.locator("text=Legislation").count() > 0
            results.append((
                "C. MemberOverview bill links (false dead-end #2)",
                "CONFIRMED MISSING" if len(bill_links) == 0 else "EDGE EXISTS",
                f"bill anchors={len(bill_links)} on {page.url}; "
                f"legislation section present={has_leg}",
            ))
        else:
            results.append((
                "C. MemberOverview bill links (false dead-end #2)",
                "SKIPPED",
                "no member link found to seed a profile",
            ))

        browser.close()

    print("\n" + "=" * 78)
    print("NAVIGATION GRAPH EMPIRICAL TEST")
    print("=" * 78)
    for test, verdict, detail in results:
        print(f"\n[{verdict}] {test}")
        print(f"    {detail}")
    print("\n" + "=" * 78)


if __name__ == "__main__":
    main()
