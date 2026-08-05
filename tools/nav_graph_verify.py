#!/usr/bin/env python
"""Live-DOM verifier for the edges claimed in doc/NAVIGATION_GRAPH.md.

The dynamic half of the navigation-graph pair:
  * `tools/check_nav_graph.py` — STATIC ratchet (AST; in the fast suite). Flags an
    entity column rendered without an `entity_links` helper.
  * this file                  — DYNAMIC verifier. Drives each claimed edge on a
    real server and asserts present/absent.

Restored 2026-08-01 from `audit_screenshots/_nav_graph_edges.py`, which was
deleted during the screenshot-probe consolidation. That deletion was a mistake:
the file carried no imports, but NAVIGATION_GRAPH.md:150 names it "the single
live-DOM verifier", so nothing referenced it while everything depended on it.
It now lives in tools/ beside its static sibling instead of among the disposable
`audit_screenshots/_*` probes.

**Score edges by "does the entity travel?", not "is there an anchor?"** The
top-nav chrome (~16 links) is on every page, so counting `<a>` tags concludes
every page is well-connected — see NAVIGATION_GRAPH.md "The two edge classes".
`edges_to()` is that rule in code: a hit needs the target slug AND the entity
param.

Two state-indexing traps, both of which read as app regressions when they are
not (NAVIGATION_GRAPH.md §"Test method"):
  1. Some edges are CONDITIONAL on the seeded entity — a bill needs a resolvable
     sponsor; the payments→company CTA is awards-register gated
     (public_payments.py:523). Seeding one candidate blind mis-reports the edge
     as broken. Hence `probe_first` / candidate iteration.
  2. Some edges only render in a sub-state — member→bill lives behind
     `?section=legislation`, not the default overview.

    python tools/nav_graph_verify.py            # starts its own server
    python tools/nav_graph_verify.py --base http://127.0.0.1:8501
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import services.runtime_env  # noqa: E402, F401  isort:skip  (BLAS cap; first project import at every entry point)

import argparse  # noqa: E402
from urllib.parse import parse_qs, urlparse  # noqa: E402

from tools.ui_capture import DEFAULT_BASE, Server, settle  # noqa: E402


def harvest(page) -> list[str]:
    return page.evaluate("() => [...document.querySelectorAll('a')].map(a => a.getAttribute('href')||'')")


def visit(page, url: str, *, timeout: float) -> list[str]:
    """Navigate and return every href once the frame is stable.

    The original slept a fixed 8-11s per navigation. `settle()` polls Streamlit's
    status widget / skeleton count / DOM size instead, so this both returns sooner
    and stops harvesting a half-rendered page (an edge missing because the page
    had not finished painting is indistinguishable from a real regression).
    """
    page.goto(url, wait_until="domcontentloaded", timeout=60_000)
    settle(page, timeout=timeout)
    return harvest(page)


def edges_to(hrefs: list[str], slug: str, param: str, host: str) -> list[str]:
    """Contextual edges only: the anchor must reach `slug` AND carry `param`.

    This is the chrome-vs-contextual discriminator. A bare `/company` link is
    nav chrome and scores nothing; `/company?supplier=<x>` carries the entity.
    """
    hits = []
    for href in hrefs:
        if not href or href.startswith(("mailto:", "#")):
            continue
        if href.startswith("?"):
            target_slug, query = slug, parse_qs(href[1:])
        else:
            parsed = urlparse(href)
            if parsed.scheme in ("http", "https") and parsed.netloc != host:
                continue
            target_slug, query = parsed.path.lstrip("/").rstrip("/"), parse_qs(parsed.query)
        if target_slug == slug and param in query:
            hits.append(href)
    return hits


def _query_of(href: str) -> dict:
    return parse_qs(urlparse(href).query) or (parse_qs(href[1:]) if href.startswith("?") else {})


def first_param_val(hrefs: list[str], param: str) -> str | None:
    for href in hrefs:
        value = _query_of(href).get(param, [None])[0]
        if value:
            return value
    return None


def all_param_vals(hrefs: list[str], param: str, limit: int = 12) -> list[str]:
    """Ordered, de-duplicated candidate values — see trap 1 in the module docstring."""
    out: list[str] = []
    for href in hrefs:
        value = _query_of(href).get(param, [None])[0]
        if value and value not in out:
            out.append(value)
            if len(out) >= limit:
                break
    return out


def run_checks(page, base: str, host: str, timeout: float) -> tuple[list[tuple], dict]:
    """Returns (results, seeds). The seeds are NOT decoration: a mismatch caused
    by an ineligible seed looks identical to a real regression in the results
    table, and the doc's trap 1 says ineligible seeds are common. Print them."""
    results: list[tuple] = []

    def record(label, present, expect, sample):
        verdict = "OK" if expect is None else ("OK" if present == expect else "!! MISMATCH")
        expected = "info" if expect is None else ("present" if expect else "absent")
        results.append((label, "PRESENT" if present else "ABSENT", expected, verdict, sample or []))

    committee = first_param_val(visit(page, f"{base}/rankings-committees", timeout=timeout), "committee")
    si = first_param_val(visit(page, f"{base}/rankings-statutory-instruments", timeout=timeout), "si")
    # 40 not 12: most listed bills are older enacted ones with a NULL/unresolvable
    # sponsor, so a 12-bill pool resolved exactly ONE sponsor (2026-08-01) and the
    # forward #2 test had a single member to try — indistinguishable from a real
    # regression. The pool must be wide enough that trap 1 cannot masquerade as a
    # defect.
    bill_candidates = all_param_vals(visit(page, f"{base}/rankings-legislation", timeout=timeout), "bill", limit=40)
    # Suppliers seeded from PROCUREMENT, not payments: the payments→company CTA is
    # awards-register gated, and most paid suppliers are payments-only by design.
    supplier_candidates = all_param_vals(visit(page, f"{base}/rankings-procurement", timeout=timeout), "supplier")

    # #8 back-edge: legislation(bill) → member(sponsor). Collect SEVERAL sponsors —
    # the sponsor of one old bill may have no bills in their own recency window,
    # which the forward test below needs choices to survive.
    bill_used, first_hit, sponsors = None, None, []
    for candidate in bill_candidates:
        hits = edges_to(
            visit(page, f"{base}/rankings-legislation?bill={candidate}", timeout=timeout),
            "member-overview",
            "member",
            host,
        )
        if not hits:
            continue
        if bill_used is None:
            bill_used, first_hit = candidate, hits[:1]
        member = first_param_val(hits, "member")
        if member and member not in sponsors:
            sponsors.append(member)
        if len(sponsors) >= 6:
            break
    record(f"legislation(bill) -> member(sponsor) [#8]  (bill={bill_used})", bool(first_hit), True, first_hit)

    # #2 forward: member(?section=legislation) → legislation(bill). Trap 2 — the
    # default overview never renders bill links.
    #
    # Sponsors harvested from the bill list are a WEAK seed: the landing page
    # exposes ~20 bills, mostly older ones whose sponsor is unresolvable, which on
    # 2026-08-01 yielded exactly ONE sponsor — a former TD with no bills in her own
    # section. The edge read as a regression when direct testing showed Aengus
    # Ó Snodaigh with 22 bill links and Alan Kelly with 6. So fall back to CURRENT
    # members from the /member-overview landing, which is what the edge is actually
    # about; a one-candidate test cannot distinguish trap 1 from a real defect.
    landing_members = all_param_vals(visit(page, f"{base}/member-overview", timeout=timeout), "member", limit=8)
    forward_member, forward_hit = None, None
    for member in [*sponsors, *[m for m in landing_members if m not in sponsors]]:
        hits = edges_to(
            visit(page, f"{base}/member-overview?member={member}&section=legislation", timeout=timeout),
            "rankings-legislation",
            "bill",
            host,
        )
        if hits:
            forward_member, forward_hit = member, hits[:1]
            break
    record(
        f"member(section=legislation) -> legislation(bill) [#2]  (member={forward_member})",
        bool(forward_hit),
        True,
        forward_hit,
    )

    # #1: public-payments(supplier) → company, registry-gated.
    supplier_used, payment_hit = None, None
    for candidate in supplier_candidates:
        hits = edges_to(
            visit(page, f"{base}/rankings-public-payments?supplier={candidate}", timeout=timeout),
            "company",
            "supplier",
            host,
        )
        if hits:
            supplier_used, payment_hit = candidate, hits[:1]
            break
    record(
        f"public-payments(supplier) -> company [#1, gated]  (supplier={supplier_used})",
        bool(payment_hit),
        True,
        payment_hit,
    )

    # #4: committees(committee) → member. Fixed 2026-06-20 (roster LinkColumn).
    if committee:
        hits = edges_to(
            visit(page, f"{base}/rankings-committees?committee={committee}", timeout=timeout),
            "member-overview",
            "member",
            host,
        )
        record("committees(committee) -> member [#4]", bool(hits), True, hits[:1])

    # SI → signing minister: info-only. Not every SI renders an identifiable
    # minister (nested-anchor cards skip the link), so absence is not a defect.
    if si:
        hits = edges_to(
            visit(page, f"{base}/rankings-statutory-instruments?si={si}", timeout=timeout),
            "member-overview",
            "member",
            host,
        )
        record("statutory-instruments(si) -> member(minister) [info]", bool(hits), None, hits[:1])

    seeds = {
        "bill_candidates": len(bill_candidates),
        "bill_used": bill_used,
        "sponsors_found": sponsors,
        "supplier_candidates": len(supplier_candidates),
        "supplier_used": supplier_used,
        "si": si,
        "committee": committee,
    }
    return results, seeds


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default=DEFAULT_BASE, help=f"app URL (default {DEFAULT_BASE})")
    parser.add_argument("--attach", action="store_true", help="use a server already running at --base")
    parser.add_argument("--timeout", type=float, default=45.0, help="per-page settle timeout, seconds")
    args = parser.parse_args(argv)

    from playwright.sync_api import sync_playwright

    with Server(args.base, serve=not args.attach) as base, sync_playwright() as p:
        host = base.removeprefix("http://").removeprefix("https://")
        browser = p.chromium.launch(headless=True)
        # Tall viewport: contextual edges further down a long page must be in the
        # DOM to be harvested, and Streamlit does not virtualise these lists.
        page = browser.new_context(viewport={"width": 1440, "height": 4200}).new_page()
        results, seeds = run_checks(page, base, host, args.timeout)
        browser.close()

    print("\n" + "=" * 78)
    print("NAV GRAPH — claimed edges verified on their correct detail states")
    print("=" * 78)
    print(f"  seeds: {seeds['bill_candidates']} bill candidates, used {seeds['bill_used']!r}")
    print(f"         sponsors resolved: {seeds['sponsors_found'] or 'NONE'}")
    print(f"         {seeds['supplier_candidates']} supplier candidates, used {seeds['supplier_used']!r}")
    print(f"         si={seeds['si']!r} committee={seeds['committee']!r}\n")
    mismatches = 0
    for label, state, expected, verdict, sample in results:
        if verdict.startswith("!!"):
            mismatches += 1
        print(f"  [{state:7}] {verdict:11} exp={expected:8} {label}")
        if sample:
            print(f"            sample={sample}")
    print("-" * 78)
    print(f"  mismatches vs doc/NAVIGATION_GRAPH.md: {mismatches}")
    return 1 if mismatches else 0


if __name__ == "__main__":
    raise SystemExit(main())
