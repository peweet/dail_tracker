"""P0-2 — OGP central arrangements / frameworks catalogue (SCAFFOLD — BLOCKED).

STATUS as of 2026-06-28: NOT ingestible by static scrape.
  * gov.ie returns HTTP 403 after a few automated requests (gov.ie WAF — the
    same wall documented for the ministerial-diaries extractor).
  * The /en/ogp-frameworks/ listing is JS-rendered: the static HTML contains
    no framework-detail links and no embedded JSON/API state.

To complete this source, ONE of:
  1. Identify the gov.ie content/search API the listing calls at runtime
     (capture it from browser devtools network tab), then fetch JSON directly
     with a respectful rate limit + the gov.ie WAF headers.
  2. Render with a headless browser (Playwright) and parse the hydrated DOM.

HARD RULE: ingest only the PUBLIC catalogue fields (arrangement name, CPB,
category, framework/DPS/panel type, lots, start/expiry, route-to-market). The
supplier MEMBER LISTS live behind Buyer Zone (buyerzone.gov.ie, gov.ie-domain
accounts only) — DO NOT scrape gated content.

Proposed gold schema: procurement_frameworks(arrangement_id, name, cpb,
category, type, lot, start_date, expiry_date, days_to_expiry, eligibility,
route_to_market, source_url, + provenance). Optional child
procurement_framework_suppliers(arrangement_id, supplier_norm) ONLY where the
member list is public.
"""
from __future__ import annotations

CANDIDATE_LINKS = {
    "public_catalogue": "https://www.gov.ie/en/ogp-frameworks/?sort_by=published_date",
    "schedule": "https://ogp.gov.ie/schedule-of-frameworks-and-contracts/",  # 403 to bots
    "buyer_zone_GATED": "https://www.buyerzone.gov.ie/",  # do NOT scrape
}


def run() -> None:
    print("OGP frameworks: BLOCKED (gov.ie WAF 403 + JS-rendered listing).")
    print("Needs the gov.ie content API endpoint or a headless browser. No-op.")
    for k, v in CANDIDATE_LINKS.items():
        print(f"  {k}: {v}")


if __name__ == "__main__":
    run()
