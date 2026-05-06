"""Fetch every Bill (Government + Private Member) from /v1/legislation, unscoped.

Why this exists
---------------
The production pipeline calls /v1/legislation once per TD with `member_id=...`
(see services/urls.py::build_legislation_urls). That filter only returns bills
where the TD is listed as an individual sponsor, so Government bills — sponsored
by a Minister "in their capacity as Minister" rather than as a member — are
never returned. Bronze diagnostic at time of writing: 1,001 raw bill rows /
642 unique bills, 100 % of which carry `bill.source = "Private Member"`.

This sandbox script fetches the same endpoint with no `member_id`, paginating
on `skip`, so the resulting file contains both Government and Private Member
bills. Output is written to a sibling file alongside the existing per-TD JSON,
so nothing in the production pipeline changes.

Run
---
    python -m pipeline_sandbox.legislation_unscoped_fetch

Output
------
    data/bronze/legislation/legislation_results_unscoped.json

Shape mirrors services/votes.py — a list of one synthetic page payload:
    [{"head": {"counts": {"resultCount": N}}, "results": [bill, bill, ...]}]

so a downstream flattener can iterate it the same way legislation.py iterates
the per-TD file (`for page in data["results"]: bills.extend(page)`).

See `legislation_unscoped_integration_plan.md` for graduation steps.
"""

from __future__ import annotations

import json
import logging
from collections import Counter

from config import LEGISLATION_DIR
from services.dail_config import API_BASE
from services.http_engine import fetch_json

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000  # API server cap — same as votes
DATE_START = "2014-01-01"  # matches build_legislation_urls
DATE_END = "2099-01-01"
OUTPUT_PATH = LEGISLATION_DIR / "legislation_results_unscoped.json"


def build_url(skip: int) -> str:
    return (
        f"{API_BASE}/legislation"
        f"?date_start={DATE_START}"
        f"&date_end={DATE_END}"
        f"&limit={PAGE_SIZE}"
        f"&skip={skip}"
        f"&chamber_id="
        f"&lang=en"
    )


def fetch_all_bills() -> tuple[list[dict], int, int]:
    """Sequential skip/limit pagination. Returns (bills, expected_count, bytes)."""
    all_bills: list[dict] = []
    total_bytes = 0
    expected: int | None = None
    skip = 0

    while True:
        page, raw_bytes = fetch_json(build_url(skip))
        total_bytes += raw_bytes

        if expected is None:
            expected = page["head"]["counts"]["resultCount"]
            logger.info(f"Bill pagination | expected={expected} | page_size={PAGE_SIZE}")

        page_results = page.get("results", [])
        all_bills.extend(page_results)
        logger.info(
            f"Bill page | skip={skip} | got={len(page_results)} | "
            f"running_total={len(all_bills)} | bytes={raw_bytes:,}"
        )

        if len(page_results) < PAGE_SIZE or len(all_bills) >= expected:
            break
        skip += PAGE_SIZE

    assert len(all_bills) >= expected, (
        f"Bill pagination drift: got {len(all_bills)} of {expected} expected"
    )
    return all_bills, expected, total_bytes


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logger.info("Fetching unscoped /v1/legislation (Government + Private Member bills)")

    bills, expected, total_bytes = fetch_all_bills()

    payload = [{"head": {"counts": {"resultCount": expected}}, "results": bills}]
    LEGISLATION_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload), encoding="utf-8")

    sources = Counter(b["bill"].get("source") for b in bills)
    logger.info(f"Wrote {OUTPUT_PATH} | bills={len(bills)} | bytes={total_bytes:,}")
    logger.info(f"bill.source breakdown: {dict(sources)}")


if __name__ == "__main__":
    main()
