"""Validate the nav-graph model — v2, fixing the two faults v1 exposed.

FAULT 1 (classifier): nav-chrome links render as ABSOLUTE same-origin URLs
  (http://host/slug). v1's `startswith("http")` mislabelled them external, so
  chrome was invisible. FIX: same-origin absolute -> internal; the true
  discriminator is ENTITY-PARAM presence, not absolute-vs-relative.

FAULT 2 (coverage): v1 crawled only landing states. Most contextual cross-page
  edges live on DETAIL states (a member profile, a supplier dossier, an SI/bill
  detail). FIX: for each page, also follow its first in-page entity drill to a
  detail state and harvest that too. Adjacency = union(landing, detail).

Run with a fresh server up: python _nav_graph_validate2.py http://127.0.0.1:8645
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from urllib.parse import urlparse, parse_qs

from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8645"
BASE_HOST = urlparse(BASE).netloc

PAGES = [
    "what-they-own", "member-overview", "rankings-attendance", "rankings-votes",
    "rankings-committees", "rankings-payments", "rankings-election-spending",
    "rankings-procurement", "rankings-public-payments", "company",
    "rankings-legislation", "rankings-statutory-instruments", "rankings-corporate",
    "rankings-judiciary", "rankings-lobbying", "rankings-appointments",
    "constituencies", "housing", "rankings-council-spending", "glossary",
]
SLUGS = set(PAGES) | {"", "home", "."}
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
    """(kind, target_slug, params). kind: broken|external|mailto|chrome|state|
    contextual_inpage|contextual_xpage|unknown_internal."""
    if href is None or href in ("", "#"):
        return ("broken", None, set())
    low = href.lower()
    if "/none" in low or "/nan" in low or low.endswith(("=nan", "=none")):
        return ("broken", None, set())
    if href.startswith("mailto:"):
        return ("mailto", None, set())

    if href.startswith("?"):  # relative in-page param link
        slug, params = current_slug, set(parse_qs(href[1:]))
    else:
        parsed = urlparse(href)
        if parsed.scheme in ("http", "https") and parsed.netloc != BASE_HOST:
            return ("external", None, set())          # truly external host
        # relative path OR same-origin absolute -> internal
        slug = parsed.path.lstrip("/").rstrip("/") or "home"
        params = set(parse_qs(parsed.query))

    if slug not in SLUGS:
        return ("unknown_internal", slug, params)
    if params & ENTITY_PARAMS:
        return (("contextual_xpage" if slug != current_slug else "contextual_inpage"),
                slug, params)
    return ("chrome", slug, params)


def crawl(page, url, current_slug, settle=7):
    page.goto(url, wait_until="domcontentloaded", timeout=60000)
    time.sleep(settle)
    return [classify(a["href"], current_slug) + (a["href"], a["text"])
            for a in harvest(page)]


def main() -> None:
    landing = {}
    detail = {}
    detail_url = {}
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 3000})
        page = ctx.new_page()
        for slug in PAGES:
            try:
                rows = crawl(page, f"{BASE}/{slug}", slug)
            except Exception as e:  # noqa: BLE001
                print(f"  LOAD FAIL {slug}: {e}")
                landing[slug] = []
                continue
            landing[slug] = rows
            # follow first in-page entity drill -> detail state
            drill = next((r for r in rows if r[0] == "contextual_inpage"
                          and r[3].startswith("?")), None)
            if drill:
                durl = f"{BASE}/{slug}{drill[3]}"
                try:
                    detail[slug] = crawl(page, durl, slug)
                    detail_url[slug] = drill[3]
                except Exception as e:  # noqa: BLE001
                    print(f"  detail FAIL {slug}: {e}")
        browser.close()

    allrows = {s: landing.get(s, []) + detail.get(s, []) for s in PAGES}
    npages = len([s for s in landing if landing[s]])

    # ── CLAIM 1: chrome constant? (now correctly counting same-origin absolutes) ──
    chrome_by_page = {s: {r[1] for r in landing.get(s, []) if r[0] == "chrome" and r[1]}
                      for s in PAGES}
    freq = defaultdict(int)
    for s in chrome_by_page.values():
        for t in s:
            freq[t] += 1
    constant = {t for t, c in freq.items() if c >= 0.8 * npages}
    print("=" * 72)
    print("CLAIM 1 — global nav chrome constant across pages (fixed classifier)")
    print("=" * 72)
    print(f"  pages crawled: {npages}")
    print(f"  constant chrome slugs on >=80% of pages ({len(constant)}): {sorted(constant)}")
    devs = []
    for s in PAGES:
        if not landing.get(s):
            continue
        miss = constant - chrome_by_page[s]
        if miss:
            devs.append((s, sorted(miss)))
    if not devs:
        print("  VERDICT: chrome strip is present + identical on every page -> "
              "two-class model FOUNDATION SOUND")
    else:
        print("  pages missing part of the constant strip:")
        for s, m in devs:
            print(f"    {s}: missing {m}")

    # ── CLAIM 2: TRUE contextual adjacency (landing UNION detail) ──
    print("\n" + "=" * 72)
    print("CLAIM 2 — contextual cross-page edges (landing + detail state)")
    print("=" * 72)
    adjacency = {}
    for s in PAGES:
        x = defaultdict(set)
        for kind, tgt, params, *_ in allrows[s]:
            if kind == "contextual_xpage" and tgt:
                x[tgt] |= (params & ENTITY_PARAMS)
        adjacency[s] = x
    deadends = []
    for s in PAGES:
        x = adjacency[s]
        d = detail_url.get(s, "(none)")
        if x:
            edges = ", ".join(f"{t}({'/'.join(sorted(p))})" for t, p in sorted(x.items()))
            print(f"  {s}  -->  {edges}   [detail crawled: {d}]")
        else:
            print(f"  {s}  -->  (none)   [detail crawled: {d}]")
            deadends.append(s)
    print(f"\n  Pages with NO cross-page entity edge (even on detail): {deadends}")

    # ── CLAIM 3: regression (exclude chart-export + home chrome) ──
    print("\n" + "=" * 72)
    print("CLAIM 3 — regression: real broken nav hrefs")
    print("=" * 72)
    CHART = {"Save as SVG", "Save as PNG", "View Source", "View Compiled Vega",
             "Open in Vega Editor", "View in Vega Editor"}
    real_broken = []
    for s in PAGES:
        for kind, tgt, params, href, text in allrows[s]:
            if kind == "broken" and text not in CHART:
                real_broken.append((s, href, text))
    if real_broken:
        for s, href, text in real_broken:
            print(f"  BROKEN {s}: {href!r} (text={text!r})")
    else:
        print("  no real broken nav hrefs (all '#' are Vega/Altair chart-export chrome)")

    # ── FIX spot-checks ──
    print("\n  FIX spot-checks (on detail states):")
    mo = adjacency.get("member-overview", {})
    print(f"    member-overview --> rankings-legislation(bill): "
          f"{'bill' in mo.get('rankings-legislation', set())}")
    pp = adjacency.get("rankings-public-payments", {})
    print(f"    rankings-public-payments --> company(supplier): "
          f"{'supplier' in pp.get('company', set())}")


if __name__ == "__main__":
    main()
