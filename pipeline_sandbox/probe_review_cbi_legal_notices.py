"""Read-only tractability probe for the CBI "regulated-entity intelligence" brief.

Part of the 2026-06-05 review of doc/dail_tracker_cbi_second_pass_reconciliation.md
(review written to doc/CBI_SECOND_PASS_REVIEW.md). DOES NOT write any parquet,
touch gold/silver, or modify the pipeline. Run manually if you want to re-confirm
the source-tractability findings the review cites.

Findings captured at review time (2026-06-05, via WebFetch, not this script):

  Warning notices  https://www.centralbank.ie/news-media/warning-notices
    - HTML list: per-item date + "Warning Notice" category + article link.
    - Pagination is ASP.NET __doPostBack (same family as the registers page the
      current extractor already drives — reusable VIEWSTATE postback code).
    - Article body is clean readable HTML. Sample (Callanor, 19 Mar 2026) exposed
      firm name, website, email, phone, date, authorisation statement inline.
    - NO natural persons named in the unauthorised-firm warnings sampled.
    - Page advertises a "News and Media" RSS feed (exact URL TODO via DevTools;
      /rss/news-and-media 404s — confirm the real path before relying on it).

  Enforcement actions  https://www.centralbank.ie/news-media/legal-notices/enforcement-actions
    - The /enforcement-actions URL itself is a HUB page (links out to prohibition,
      adverse-assessment, post-assessment pages) — the records live on news
      article pages, not this hub.
    - Sampled article (Cantor Fitzgerald, 25 Feb 2025) exposed firm name, fine
      (gross + 30% settlement discount + net), legal regime (MAR Art 16(2)),
      multi-category breach narrative, dates — ALL inline HTML.
    - Only named person was a CBI official (Director of Enforcement) — safe.
    - Older settlements also have a public-statement PDF under
      /docs/.../settlement-agreements/ but the recent article HTML is self-contained.

  Prohibition notices  https://www.centralbank.ie/news-media/legal-notices/prohibition-notices
    - PDF-centric, and these DO name private natural persons (fitness & probity).
    - This is the family that collides with the personal-insolvency privacy
      precedent — see review's Devil's Advocate section.

This script only enumerates the warning-notice postback targets (read-only GET +
one POST round-trip) so a future builder can see the shape. It writes nothing.
"""
from __future__ import annotations

import re
import sys

import requests
from bs4 import BeautifulSoup

WARNING_URL = "https://www.centralbank.ie/news-media/warning-notices"
H = {"User-Agent": "Mozilla/5.0 dail_extractor review-probe (read-only)"}


def probe_warning_list() -> None:
    """GET the warning-notices list; report article links + postback pagination."""
    r = requests.get(WARNING_URL, headers=H, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "html.parser")

    article_links = [
        a.get("href")
        for a in soup.find_all("a", href=True)
        if "/news/article/" in (a.get("href") or "")
    ]
    postbacks = [
        m.group(1)
        for a in soup.find_all("a", href=True)
        if (m := re.search(r"__doPostBack\('([^']+)'", a.get("href") or ""))
    ]
    rss = [
        a.get("href")
        for a in soup.find_all("a", href=True)
        if "rss" in (a.get("href") or "").lower() or "feed" in (a.get("href") or "").lower()
    ]

    print(f"warning-notices page: {len(article_links)} article links on page 1")
    for href in article_links[:10]:
        print(f"  article  {href}")
    print(f"\n{len(postbacks)} __doPostBack pagination targets (ASP.NET, reuse register code)")
    for t in postbacks[:6]:
        print(f"  postback {t}")
    print(f"\nRSS/feed candidates found in markup: {rss or '(none — confirm via DevTools)'}")


if __name__ == "__main__":
    sys.exit(probe_warning_list())
