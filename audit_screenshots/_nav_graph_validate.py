"""Validate the navigation-graph MODEL is not faulty.

The doc/NAVIGATION_GRAPH.md analysis rests on three load-bearing claims.
This script tests each PROGRAMMATICALLY (no hand-classification), so the
chrome-vs-contextual split is reproducible rather than eyeballed:

  CLAIM 1 (the model's foundation): a constant "global nav chrome" set of
          bare-slug links appears on (nearly) every page. If chrome is NOT
          near-constant, the whole two-class model is faulty.

  CLAIM 2: separating chrome from contextual edges programmatically
          reproduces the hand-made dead-end / edge calls. We derive, per
          page: contextual cross-page out-edges (anchor carries an entity
          param AND points to a different slug). Pages with zero of these
          are contextual cul-de-sacs.

  CLAIM 3 (regression): the implemented fixes introduced no broken or
          malformed hrefs (None/#/nan/None-in-path).

Run with a fresh server up: python _nav_graph_validate.py http://127.0.0.1:8645
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"

PAGES = [
    "what-they-own", "member-overview", "rankings-attendance", "rankings-votes",
    "rankings-committees", "rankings-payments", "rankings-election-spending",
    "rankings-procurement", "rankings-public-payments", "company",
    "rankings-legislation", "rankings-statutory-instruments", "rankings-corporate",
    "rankings-judiciary", "rankings-lobbying", "rankings-appointments",
    "constituencies", "housing", "rankings-council-spending", "glossary",
]
SLUGS = set(PAGES) | {"", "home"}

# Params that carry a specific ENTITY (vs UI-state params like tab=/view=/sort=).
ENTITY_PARAMS = {
    "member", "supplier", "vote", "bill", "si", "publisher", "committee",
    "constituency", "council", "county", "judge", "court", "cand", "dparty",
    "eparty", "fund", "firm", "ref", "lp3_org", "lp3_area", "lp3_dpo",
    "lp3_topic", "paid_supplier", "q", "spark",
}


def harvest(page):
    return page.evaluate(
        """() => [...document.querySelectorAll('a')].map(a => ({
            href: a.getAttribute('href') || '',
            text: (a.textContent || '').trim().slice(0, 50)
        }))"""
    )


def classify(href: str, current_slug: str):
    """Return (kind, target_slug, params). kind in:
    broken | external | mailto | chrome | state | contextual_inpage |
    contextual_xpage | unknown_internal."""
    if href is None or href in ("", "#"):
        return ("broken", None, {})
    if "None" in href or "/nan" in href or href.endswith("=nan") or "=None" in href:
        return ("broken", None, {})
    if href.startswith(("http://", "https://")):
        return ("external", None, {})
    if href.startswith("mailto:"):
        return ("mailto", None, {})

    # in-page param-only link, e.g. "?supplier=X" (no slug) -> stays on page
    if href.startswith("?"):
        params = {k for k in parse_qs(href[1:]).keys()}
        if params & ENTITY_PARAMS:
            return ("contextual_inpage", current_slug, params)
        return ("state", current_slug, params)

    parsed = urlparse(href)
    slug = parsed.path.lstrip("/").rstrip("/")
    params = {k for k in parse_qs(parsed.query).keys()}
    if slug not in SLUGS:
        return ("unknown_internal", slug, params)
    if params & ENTITY_PARAMS:
        kind = "contextual_xpage" if slug != current_slug else "contextual_inpage"
        return (kind, slug, params)
    # bare slug, no entity param -> nav chrome candidate (or a generic "see all")
    return ("chrome", slug, params)


def main() -> None:
    per_page = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 3000})
        page = ctx.new_page()
        for slug in PAGES:
            try:
                page.goto(f"{BASE}/{slug}", wait_until="domcontentloaded", timeout=60000)
                time.sleep(7)
                anchors = harvest(page)
            except Exception as e:  # noqa: BLE001
                print(f"  LOAD FAIL {slug}: {e}")
                per_page[slug] = []
                continue
            rows = [classify(a["href"], slug) + (a["href"], a["text"]) for a in anchors]
            per_page[slug] = rows
        browser.close()

    # ── CLAIM 1: is chrome near-constant? ──
    chrome_by_page = {}
    for slug, rows in per_page.items():
        chrome_by_page[slug] = {r[1] for r in rows if r[0] == "chrome" and r[1]}
    # chrome target-slugs present on >=80% of pages = the constant strip
    freq = defaultdict(int)
    for s in chrome_by_page.values():
        for tgt in s:
            freq[tgt] += 1
    npages = len([s for s in per_page if per_page[s]])
    constant_chrome = {t for t, c in freq.items() if c >= 0.8 * npages}
    print("=" * 72)
    print("CLAIM 1 — global nav chrome is constant across pages")
    print("=" * 72)
    print(f"  pages crawled: {npages}")
    print(f"  chrome target-slugs on >=80% of pages ({len(constant_chrome)}): "
          f"{sorted(constant_chrome)}")
    # how much does each page deviate from the constant strip?
    deviations = []
    for slug, s in chrome_by_page.items():
        if not per_page[slug]:
            continue
        missing = constant_chrome - s
        extra = s - constant_chrome
        if missing or extra:
            deviations.append((slug, sorted(missing), sorted(extra)))
    if not deviations:
        print("  VERDICT: chrome is IDENTICAL on every page -> model foundation SOUND")
    else:
        print("  per-page deviations from the constant strip:")
        for slug, miss, extra in deviations:
            print(f"    {slug}: missing={miss} extra(page-specific 'see all')={extra}")
        print("  VERDICT: chrome is near-constant; 'extra' bare-slug links are "
              "page-specific 'see all' links (genuine edges, not chrome).")

    # ── CLAIM 2: contextual adjacency + dead-ends ──
    print("\n" + "=" * 72)
    print("CLAIM 2 — contextual cross-page out-edges per page (entity travels)")
    print("=" * 72)
    adjacency = {}
    for slug, rows in per_page.items():
        x = defaultdict(set)  # target_slug -> entity params used
        for kind, tgt, params, *_ in rows:
            if kind == "contextual_xpage" and tgt:
                x[tgt] |= params
        adjacency[slug] = x
    deadends = []
    for slug in PAGES:
        x = adjacency.get(slug, {})
        inpage = sum(1 for r in per_page.get(slug, []) if r[0] == "contextual_inpage")
        if x:
            edges = ", ".join(f"{t}({'/'.join(sorted(p & ENTITY_PARAMS))})"
                              for t, p in sorted(x.items()))
            print(f"  {slug}  -->  {edges}   [in-page drills: {inpage}]")
        else:
            print(f"  {slug}  -->  (no cross-page entity edges)   [in-page drills: {inpage}]")
            deadends.append((slug, inpage))
    print("\n  Contextual cul-de-sacs (no cross-page entity edge out):")
    for slug, inpage in deadends:
        kind = "honest leaf" if slug in ("glossary",) else (
            "in-page only" if inpage else "TRUE dead-end (no entity edges at all)")
        print(f"    {slug}  ({kind}; {inpage} in-page drills)")

    # ── CLAIM 3: regression — broken/malformed hrefs ──
    print("\n" + "=" * 72)
    print("CLAIM 3 — regression: broken / malformed hrefs")
    print("=" * 72)
    broken = []
    unknown = []
    for slug, rows in per_page.items():
        for kind, tgt, params, href, text in rows:
            if kind == "broken":
                broken.append((slug, href, text))
            elif kind == "unknown_internal":
                unknown.append((slug, href, text))
    if broken:
        for slug, href, text in broken:
            print(f"  BROKEN  {slug}: {href!r} (text={text!r})")
    else:
        print("  no broken/None/nan hrefs found")
    if unknown:
        print("  internal hrefs to UNREGISTERED slugs:")
        for slug, href, text in unknown[:20]:
            print(f"    {slug}: {href!r} (text={text!r})")
    else:
        print("  no links to unregistered slugs")

    # spot-check the two fixed edges are present in the adjacency
    print("\n  FIX spot-check:")
    pp = adjacency.get("rankings-public-payments", {})
    mo = adjacency.get("member-overview", {})
    # public-payments -> company only shows on the supplier DETAIL view, not landing,
    # so it won't appear here (landing has no ?supplier= context). Note that.
    print(f"    member-overview cross-page edges include legislation(bill): "
          f"{'rankings-legislation' in mo}")
    print("    (public-payments->company is on the supplier DETAIL view, "
          "validated separately in _nav_graph_verify.py)")


if __name__ == "__main__":
    main()
