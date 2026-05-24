"""Fetch every Bill (Government + Private Member) from /v1/legislation, unscoped.

The per-TD legislation scenario (build_legislation_urls + run_member_scenario)
calls /v1/legislation once per TD with member_id=<uri>. That filter only
returns bills sponsored by an individual member — Government bills, sponsored
by a Minister "in capacity as Minister", are never returned.

This module fetches the same endpoint without member_id, paginating on skip,
so the resulting file covers both Government and Private Member bills.

Shape mirrors services/votes.py — a list of one synthetic page payload:
    [{"head": {"counts": {"resultCount": N}}, "results": [bill, bill, ...]}]

The downstream flattener in legislation.py iterates it as
    for page in data["results"]: bills.extend(page)
"""

from __future__ import annotations

import logging
from collections import Counter

from services.http_engine import fetch_json
from services.urls import build_legislation_unscoped_url

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000  # API server cap; larger values are silently clamped


def fetch_all_bills() -> tuple[list[dict], int]:
    """Sequential skip/limit pagination. Returns (synthetic-payload-list, bytes).

    Returns one synthetic payload preserving the on-disk shape consumed by
    legislation.py. Asserts the cumulative count matches
    head.counts.resultCount so silent truncation cannot recur.
    """
    all_bills: list[dict] = []
    total_bytes = 0
    expected: int | None = None
    skip = 0

    while True:
        page, raw_bytes = fetch_json(build_legislation_unscoped_url(skip=skip))
        total_bytes += raw_bytes

        if expected is None:
            expected = page["head"]["counts"]["resultCount"]
            logger.info(f"Bill pagination | expected={expected} | page_size={PAGE_SIZE}")

        page_results = page.get("results", [])
        all_bills.extend(page_results)
        logger.info(
            f"Bill page | skip={skip} | got={len(page_results)} | running_total={len(all_bills)} | bytes={raw_bytes:,}"
        )

        if len(page_results) < PAGE_SIZE or len(all_bills) >= expected:
            break
        skip += PAGE_SIZE

    assert len(all_bills) >= expected, f"Bill pagination drift: got {len(all_bills)} of {expected} expected"

    sources = Counter(b["bill"].get("source") for b in all_bills)
    logger.info(
        f"Finished unscoped legislation fetch | bills={len(all_bills)} | "
        f"bytes={total_bytes:,} | source breakdown: {dict(sources)}"
    )

    payload = {
        "head": {"counts": {"resultCount": expected}},
        "results": all_bills,
    }
    return [payload], total_bytes


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    bills, total = fetch_all_bills()
    logger.info(f"Fetched {len(bills)} payload(s) | total {total:,} bytes")
