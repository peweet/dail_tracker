import logging

from services.dail_config import API_BASE, VOTES_DATE_START
from services.http_engine import fetch_json

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000  # API server cap; larger values are silently clamped


def build_vote_url() -> str:
    """Single base URL for the post-cutoff Dáil vote query.

    Trailing &outcome= is required: omitting it triggers a hidden default
    filter that returns ~10 of ~1078 records. Sort/order are explicit so
    paging is stable across calls.
    """
    return (
        f"{API_BASE}/votes"
        f"?chamber_type=house"
        f"&chamber_id="
        f"&chamber=dail"
        f"&date_start={VOTES_DATE_START}"
        f"&sort=date"
        f"&order=desc"
        f"&outcome="
    )


def fetch_votes() -> tuple[list[dict], int]:
    """Fetch every vote since VOTES_DATE_START via skip/limit pagination.

    Returns one synthetic payload (a list of one dict) preserving the
    on-disk shape consumed by transform_votes.py. Asserts the cumulative
    count matches head.counts.resultCount so silent truncation cannot recur.
    """
    base_url = build_vote_url()
    all_results: list[dict] = []
    total_bytes = 0
    expected = None
    skip = 0

    while True:
        page_url = f"{base_url}&limit={PAGE_SIZE}&skip={skip}"
        page, raw_bytes = fetch_json(page_url)
        total_bytes += raw_bytes

        if expected is None:
            expected = page["head"]["counts"]["resultCount"]
            logger.info(
                f"Vote pagination | expected={expected} | page_size={PAGE_SIZE}"
            )

        page_results = page.get("results", [])
        all_results.extend(page_results)
        logger.info(
            f"Vote page | skip={skip} | got={len(page_results)} | "
            f"running_total={len(all_results)} | bytes={raw_bytes:,}"
        )

        if len(page_results) < PAGE_SIZE or len(all_results) >= expected:
            break
        skip += PAGE_SIZE

    assert len(all_results) >= expected, (
        f"Vote pagination drift: got {len(all_results)} of {expected} expected"
    )
    logger.info(
        f"Finished votes fetch | divisions={len(all_results)} | "
        f"bytes={total_bytes:,}"
    )

    payload = {
        "head": {"counts": {"resultCount": expected}},
        "results": all_results,
    }
    return [payload], total_bytes


if __name__ == "__main__":
    votes, total_bytes = fetch_votes()
    logger.info(
        f"Fetched {len(votes)} vote payload(s) | total size {total_bytes:,} bytes"
    )
