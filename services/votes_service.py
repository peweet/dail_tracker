import logging

from services.dail_config import API_BASE
from services.http_engine import fetch_json

logger = logging.getLogger(__name__)


def build_vote_queries() -> list[tuple[str, str]]:
    """Return the vote query set."""
    return [
        (
            "votes_query1",
            f"{API_BASE}/votes"
            f"?chamber_type=house"
            f"&chamber_id="
            f"&chamber=dail"
            f"&date_start=2020-01-01"
            f"&limit=10000"
            f"&outcome=",
        ),
        (
            "votes_query2",
            f"{API_BASE}/votes"
            f"?chamber_type=house"
            f"&chamber_id="
            f"&chamber=dail"
            f"&date_start=2020-01-01"
            f"&limit=10000",
        ),
        (
            "votes_query3",
            f"{API_BASE}/votes"
            f"?chamber_type=house"
            f"&chamber_id="
            f"&chamber=dail"
            f"&date_end=2019-12-31"
            f"&limit=10000"
            f"&outcome=",
        ),
    ]


def fetch_votes() -> tuple[list[dict], int]:
    """Fetch votes sequentially using the shared HTTP engine.

    Returns:
        vote_payloads, total_downloaded_bytes
    """
    votes: list[dict] = []
    total_bytes = 0

    for name, url in build_vote_queries():
        logger.info(f"Starting {name}: {url}")
        payload, raw_bytes = fetch_json(url)
        total_bytes += raw_bytes

        logger.info(
            f"Finished {name} | rows={len(payload.get('results', []))} | "
            f"bytes={raw_bytes:,}"
        )
        votes.append(payload)

    logger.info(f"Finished votes fetch | batches={len(votes)} | bytes={total_bytes:,}")
    return votes, total_bytes

if __name__ == "__main__":
    votes, total_bytes = fetch_votes()
    logger.info(f"Fetched {len(votes)} vote batches with total size {total_bytes:,} bytes")