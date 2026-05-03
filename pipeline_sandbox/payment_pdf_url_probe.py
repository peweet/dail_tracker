"""
payment_pdf_url_probe.py — parlparse-style URL discovery for Oireachtas PDFs

STATUS: SANDBOX. Not wired into pipeline.py. Test by running directly.

PURPOSE
-------
Demonstrates the construction-with-index-fallback pattern used by mySociety's
parlparse for UK Hansard. The same pattern is what dail_tracker should use to
discover new PDFs (payments, attendance, interests) without requiring a human
to drag files into a folder.

WHY TWO STRATEGIES, NOT ONE
---------------------------
Construction-only (predict the URL from a pattern) fails when:
  - publication date slips a few days
  - the URL slug changes (we've already seen psa → caighdeanOifigiul once)
  - month-name slugs vary (`for-march-2026` vs `for-1-31-march-2026` for
    multi-period payments — see payment_29_30_nov_2024 in
    pdf_endpoint_check.py)

Index-only (scrape the publications listing) works but is slower and
depends on the Oireachtas index page being stable.

The robust answer is BOTH: try construction first (cheap when it works),
fall back to scraping the topic-filtered index when construction misses.

ARCHITECTURE (pseudocode-leaning, not production code)
------------------------------------------------------
This file is intentionally simplified. It demonstrates shape, not edge cases.
Production version belongs under pipeline/sources/ once the Layer A reorg
in v4 §4.2 happens.

USAGE
-----
    python pipeline_sandbox/payment_pdf_url_probe.py 2026 4
    # → Tries to find the Parliamentary Standard Allowance PDF for April 2026.
    # → Constructs candidate URLs based on historical lag pattern.
    # → HEAD-checks each. Returns the first 200, or falls back to index scrape.

PROVENANCE
----------
URL pattern lifted from existing pdf_endpoint_checky.
Lag analysis from same file's URL history.
Fallback strategy adapted from mysociety/parlparse pyscraper architecture.
"""

from __future__ import annotations

import calendar
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterator

import requests
from bs4 import BeautifulSoup  # only imported in fallback path


# -----------------------------------------------------------------------------
# Identity — see v4 §4.3 (web citizenship)
# -----------------------------------------------------------------------------

USER_AGENT = (
    "dail-tracker-bot/0.1 (+https://github.com/<owner>/dail-extractor; "
    "mailto:<contact>)"
)

DEFAULT_TIMEOUT = (10, 30)  # connect, read


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


# -----------------------------------------------------------------------------
# Pattern from existing pdf_endpoint_check.py
# -----------------------------------------------------------------------------

PSA_BASE = "https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/psa"
# Observed alternate folder seen in `payment_nov_td_2025`. Used as fallback.
ALT_BASE = "https://data.oireachtas.ie/ie/oireachtas/caighdeanOifigiul"

# Topic-filtered listing page used as discovery fallback.
PUBLICATIONS_INDEX = (
    "https://www.oireachtas.ie/en/publications/"
    "?topic[]=parliamentary-allowances&resultsPerPage=50"
)

MONTH_NAMES = {
    1: "january", 2: "february", 3: "march", 4: "april", 5: "may", 6: "june",
    7: "july", 8: "august", 9: "september", 10: "october", 11: "november", 12: "december",
}


# -----------------------------------------------------------------------------
# Lag analysis — derived empirically from existing URL history
# -----------------------------------------------------------------------------
# Pub-date offsets from end of data month (in days), observed in
# pdf_endpoint_check.py for the last several cycles:
#   Jan 2026 → +34
#   Feb 2026 → +32
#   Dec 2025 → +46
#   Feb 2024 → +32
#   Mar 2024 → +31
#
# Window: end_of_month + LAG_MIN to end_of_month + LAG_MAX captures the
# observed distribution with margin. Wider window = more HEAD requests but
# higher confidence the PDF is found.

LAG_MIN_DAYS = 25
LAG_MAX_DAYS = 60


# -----------------------------------------------------------------------------
# Construction strategy
# -----------------------------------------------------------------------------

@dataclass
class CandidateUrl:
    url: str
    pub_date: date
    folder_variant: str  # "psa" or "alt" — for diagnostics


def _end_of_month(year: int, month: int) -> date:
    last_day = calendar.monthrange(year, month)[1]
    return date(year, month, last_day)


def construct_candidates(data_year: int, data_month: int) -> Iterator[CandidateUrl]:
    """
    Yield candidate URLs in priority order: most likely publication date first.

    Priority order:
      1. PSA folder, dates around the median observed lag (~32 days), preferring
         working-week days (Tue–Thu — historically most common).
      2. PSA folder, wider date window (LAG_MIN..LAG_MAX).
      3. Same window via the alternate folder (caighdeanOifigiul).

    NOTE: this does not enumerate every day in the window. The publication
    date is *not* random; tightening the priority list reduces HEAD-request
    count significantly.
    """
    month_name = MONTH_NAMES[data_month]
    eom = _end_of_month(data_year, data_month)

    # Tier 1: most likely (32-day median, weekdays)
    tier_1_offsets = [32, 33, 34, 31, 30, 35, 29, 28, 36, 37]
    # Tier 2: wider window (skip duplicates from Tier 1)
    tier_2_offsets = [d for d in range(LAG_MIN_DAYS, LAG_MAX_DAYS + 1)
                      if d not in tier_1_offsets]

    seen: set[str] = set()
    for tier in (tier_1_offsets, tier_2_offsets):
        for offset in tier:
            pub_date = eom + timedelta(days=offset)
            for base, variant in ((PSA_BASE, "psa"), (ALT_BASE, "alt")):
                url = (
                    f"{base}/{pub_date.year}/"
                    f"{pub_date:%Y-%m-%d}_parliamentary-standard-allowance-"
                    f"payments-to-deputies-for-{month_name}-{data_year}_en.pdf"
                )
                if url in seen:
                    continue
                seen.add(url)
                yield CandidateUrl(url=url, pub_date=pub_date, folder_variant=variant)


def head_check(session: requests.Session, url: str) -> int:
    """Return HTTP status code for a HEAD request. Conditional-GET headers omitted
    here because we only care about existence, not content-changed semantics.
    Production version (pipeline/sources/_http.py) should add If-Modified-Since.
    """
    try:
        resp = session.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        return resp.status_code
    except requests.RequestException:
        return -1


def try_construction(
    data_year: int,
    data_month: int,
    session: requests.Session,
    max_attempts: int = 80,
) -> str | None:
    """First strategy: construct candidate URLs and HEAD-check each.

    `max_attempts` caps HEAD requests so a fully-missing PDF doesn't generate
    50+ upstream calls. Tier 1 alone is 10 candidates × 2 variants = 20.
    """
    attempts = 0
    for candidate in construct_candidates(data_year, data_month):
        if attempts >= max_attempts:
            break
        status = head_check(session, candidate.url)
        attempts += 1
        if status == 200:
            return candidate.url
        # Useful diagnostic; production should structured-log this.
        print(f"  [{status}] {candidate.url}")
    return None


# -----------------------------------------------------------------------------
# Index-fallback strategy — parlparse pattern
# -----------------------------------------------------------------------------
# When construction misses, we don't give up — we ask the index page what
# publications exist in this topic, then look for a match.
#
# This is the same shape as parlparse's pyscraper modules: try a known URL
# template, fall back to scraping the listing when the template misses.
# -----------------------------------------------------------------------------

# def discover_via_index(
#     data_year: int,
#     data_month: int,
#     session: requests.Session,
# ) -> str | None:
#     """Second strategy: scrape the topic-filtered publications listing for an
#     entry matching `<month>-<year>` and return its PDF link.

#     This is intentionally simple. Production version should:
#       - paginate beyond the first page (recent items only matter for new-asset
#         detection — usually pages 1–3 cover everything published this month)
#       - cache the listing's last-fetched ETag/Last-Modified
#       - tolerate minor HTML structure changes via more permissive selectors
#       - distinguish payment publications from interest/attendance publications
#         if the topic filter ever blends them (it currently does not)
#     """
#     month_name = MONTH_NAMES[data_month]
#     target_phrase = f"{month_name} {data_year}"  # e.g. "april 2026"

#     print(f"\nFalling back to index scrape: looking for '{target_phrase}'...")
#     resp = session.get(PUBLICATIONS_INDEX, timeout=DEFAULT_TIMEOUT)
#     if resp.status_code != 200:
#         print(f"  index returned {resp.status_code}; giving up")
#         return None

#     soup = BeautifulSoup(resp.text, "html.parser")

#     # The publications listing renders results as cards/list items. The exact
#     # selector will need adjustment after inspecting the live HTML; treat the
#     # selector below as illustrative of the pattern, not as a final answer.
#     for entry in soup.select(".c-publication-card, article.publication"):
#         title = entry.get_text(" ", strip=True).lower()
#         if target_phrase not in title:
#             continue
#         link = entry.find("a", href=True)
#         if not link:
#             continue
#         href = link["href"]
#         # Resolve relative links
#         if href.startswith("/"):
#             href = "https://www.oireachtas.ie" + href
#         # The card link often points to a landing page; the actual PDF may be
#         # one click deeper. Production version should follow once.
#         return href

#     print(f"  no match for '{target_phrase}' on the first page of the index")
#     return None


# -----------------------------------------------------------------------------
# Public entry point
# -----------------------------------------------------------------------------

def find_payment_pdf(data_year: int, data_month: int) -> str | None:
    """Try construction first, then index-scrape fallback."""
    session = _session()

    print(f"\nFinding PSA payment PDF for {MONTH_NAMES[data_month]} {data_year}")
    print(f"  data month ends: {_end_of_month(data_year, data_month)}")
    print(f"  expected pub window: {LAG_MIN_DAYS}–{LAG_MAX_DAYS} days after that")
    print()

    print("Strategy 1: URL construction")
    found = try_construction(data_year, data_month, session)
    if found:
        print(f"\n✓ found via construction: {found}")
        return found

    print("\nStrategy 1 missed.")
    # found = discover_via_index(data_year, data_month, session)
    # if found:
    #     print(f"\n✓ found via index: {found}")
    #     return found

    print(f"\n✗ neither strategy found a PDF for "
          f"{MONTH_NAMES[data_month]} {data_year}")
    print("  reasons this might be expected:")
    print("    - PDF not yet published (check pub-date lag)")
    print("    - URL slug changed (re-inspect pattern)")
    print("    - index page HTML structure changed (update selector)")
    return None


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python payment_pdf_url_probe.py <year> <month>")
        print("Example: python payment_pdf_url_probe.py 2026 4")
        sys.exit(2)

    year = 2026
    month = 4
    # year = int(sys.argv[1])
    # month = int(sys.argv[2])
    url = find_payment_pdf(year, month)
    sys.exit(0 if url else 1)
