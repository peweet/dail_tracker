"""Crawl every page of the running app, harvest <a href>, flag dead links.

Internal hrefs are checked against the registered url_path slugs in
utility/app.py. External hrefs are HEAD-checked (deduped, capped per domain).
Anchors with no href / href="#" / href containing None|nan are flagged.

Run with the dev server already running at localhost:8631.
"""

from __future__ import annotations

import sys
import time
from collections import defaultdict
from urllib.parse import urlparse

import requests
from playwright.sync_api import sync_playwright

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://localhost:8631"

SLUGS = {
    "", "home", "member-overview", "rankings-attendance", "rankings-votes",
    "rankings-interests", "rankings-committees", "rankings-payments",
    "rankings-election-spending", "rankings-procurement", "rankings-public-payments",
    "company", "rankings-legislation", "rankings-statutory-instruments",
    "rankings-corporate", "rankings-judiciary", "rankings-lobbying",
    "rankings-appointments", "glossary",
}

PAGES = [
    "/member-overview", "/rankings-attendance", "/rankings-votes",
    "/rankings-interests", "/rankings-committees", "/rankings-payments",
    "/rankings-election-spending", "/rankings-procurement",
    "/rankings-public-payments", "/rankings-legislation",
    "/rankings-statutory-instruments", "/rankings-corporate",
    "/rankings-judiciary", "/rankings-lobbying", "/rankings-appointments",
    "/glossary",
]

HEAD_CAP_PER_DOMAIN = 8


def main() -> None:
    suspicious: list[tuple[str, str, str]] = []  # (page, href, reason)
    external: dict[str, set[str]] = defaultdict(set)  # url -> pages

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(viewport={"width": 1440, "height": 900})
        page = ctx.new_page()
        for path in PAGES:
            try:
                page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
                time.sleep(7)
                hrefs = page.evaluate(
                    """() => [...document.querySelectorAll('a')].map(a => ({
                        href: a.getAttribute('href'),
                        text: (a.textContent || '').trim().slice(0, 50)
                    }))"""
                )
            except Exception as e:  # noqa: BLE001
                print(f"{path}: PAGE LOAD FAILED {e}")
                continue
            seen = set()
            for a in hrefs:
                href = a["href"]
                key = (href, a["text"])
                if key in seen:
                    continue
                seen.add(key)
                if href is None or href in ("", "#"):
                    suspicious.append((path, str(href), f"empty href (text={a['text']!r})"))
                    continue
                if "None" in href or "/nan" in href or href.endswith("=nan"):
                    suspicious.append((path, href, f"null-ish href (text={a['text']!r})"))
                    continue
                if href.startswith("http"):
                    external[href].add(path)
                    continue
                if href.startswith("?") or href.startswith("mailto:"):
                    continue
                slug = href.lstrip("/").split("?")[0].rstrip("/")
                if slug not in SLUGS:
                    suspicious.append((path, href, f"internal slug not registered (text={a['text']!r})"))
        browser.close()

    print("\n== SUSPICIOUS ANCHORS ==")
    for pg, href, why in suspicious:
        print(f"  {pg}  {href!r}  {why}")
    if not suspicious:
        print("  (none)")

    print(f"\n== EXTERNAL URLS ({len(external)} unique) ==")
    per_domain: dict[str, int] = defaultdict(int)
    sess = requests.Session()
    sess.headers["User-Agent"] = "Mozilla/5.0 (link audit; local dev)"
    for url in sorted(external):
        dom = urlparse(url).netloc
        if per_domain[dom] >= HEAD_CAP_PER_DOMAIN:
            continue
        per_domain[dom] += 1
        try:
            r = sess.head(url, allow_redirects=True, timeout=15)
            status = r.status_code
            if status in (403, 405):  # some servers reject HEAD
                r = sess.get(url, allow_redirects=True, timeout=15, stream=True)
                status = r.status_code
                r.close()
        except Exception as e:  # noqa: BLE001
            status = f"ERR {type(e).__name__}"
        flag = "" if status == 200 else "   <-- CHECK"
        print(f"  [{status}] {url}  (on {', '.join(sorted(external[url]))}){flag}")


if __name__ == "__main__":
    main()
