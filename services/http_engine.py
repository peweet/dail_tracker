import concurrent.futures
import logging
import time

import requests
from requests.adapters import HTTPAdapter

from services.schema_validation import validate_if_known

logger = logging.getLogger(__name__)

session = requests.Session()
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Transient-fault retry policy. The Oireachtas API (and the gov endpoints sharing
# this session) intermittently drop connections, time out mid-read on large pages,
# or return 5xx/429 under load. Without retry, a single blip aborts a whole
# pagination scenario or silently drops a member from bronze (see
# member_paginated.py) — exactly what a marginal connection produced in practice.
# We retry those with exponential backoff; permanent 4xx still raise immediately.
RETRY_MAX_ATTEMPTS = 3  # total attempts (1 initial + 2 retries)
RETRY_BACKOFF_BASE = 0.5  # seconds; sleep before retry N = BASE * 2 ** (N - 1)
RETRY_STATUS_FORCELIST = frozenset({429, 500, 502, 503, 504})
_RETRYABLE_EXC = (requests.exceptions.ConnectionError, requests.exceptions.Timeout)


def _sleep_backoff(attempt: int) -> None:
    time.sleep(RETRY_BACKOFF_BASE * (2 ** (attempt - 1)))


def fetch_json(url: str, timeout: tuple[int, int] = (10, 60)) -> tuple[dict, int]:
    """Fetch one URL using the shared session, retrying transient faults.

    Retries (exponential backoff) on connection errors, timeouts, and retryable
    status codes (429 + 5xx). Permanent 4xx responses raise on the first attempt.
    Once RETRY_MAX_ATTEMPTS is exhausted the final error propagates, preserving
    the "raises on failure" contract fetch_all relies on to count failures.
    """
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_MAX_ATTEMPTS + 1):
        try:
            response = session.get(url, timeout=timeout)
            if response.status_code in RETRY_STATUS_FORCELIST and attempt < RETRY_MAX_ATTEMPTS:
                logger.warning(
                    "fetch_json %s -> HTTP %s (attempt %d/%d), retrying",
                    url,
                    response.status_code,
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                )
                _sleep_backoff(attempt)
                continue
            response.raise_for_status()
            raw_bytes = len(response.content)
            payload = response.json()
            # Validate-at-fetch: assert the Oireachtas API envelope still matches
            # its registered schema before any flattener reads it (DAIL-019/020).
            # No-op for unrecognised hosts/endpoints. Raises SchemaValidationError
            # on drift; the caller lets it propagate so a break fails loudly.
            validate_if_known(url, payload)
            return payload, raw_bytes
        except _RETRYABLE_EXC as exc:
            last_exc = exc
            if attempt < RETRY_MAX_ATTEMPTS:
                logger.warning(
                    "fetch_json %s -> %s (attempt %d/%d), retrying",
                    url,
                    type(exc).__name__,
                    attempt,
                    RETRY_MAX_ATTEMPTS,
                )
                _sleep_backoff(attempt)
                continue
            raise
    # Unreachable in practice: the final attempt either returns, raises
    # raise_for_status (forcelist status), or re-raises the transient exception.
    raise last_exc if last_exc is not None else RuntimeError("fetch_json: retry loop exited unexpectedly")


def fetch_all(urls: list[str], max_workers: int = 5) -> tuple[list[dict], int, int]:
    """Fetch many URLs concurrently.

    Returns:
        results, total_downloaded_bytes, failure_count
    """
    results: list[dict] = []
    total_bytes = 0
    failures = 0

    if not urls:
        logger.warning("No URLs provided to fetch_all().")
        return results, total_bytes, failures

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_url = {executor.submit(fetch_json, url): url for url in urls}

        for i, future in enumerate(concurrent.futures.as_completed(future_to_url), start=1):
            url = future_to_url[future]
            try:
                data, raw_bytes = future.result()
                results.append(data)
                total_bytes += raw_bytes

                if i % 10 == 0 or i == len(urls):
                    logger.info(
                        f"Fetched {i}/{len(urls)} URLs | "
                        f"successes={len(results)} | failures={failures} | "
                        f"last_payload={raw_bytes:,} bytes | "
                        f"total_downloaded={total_bytes:,} bytes"
                    )
            except Exception as exc:
                failures += 1
                logger.error(f"API call failed for {url}: {exc}")

    logger.info(f"Finished fetch_all | results={len(results)} | failures={failures} | downloaded={total_bytes:,} bytes")
    return results, total_bytes, failures


if __name__ == "__main__":
    # Example usage
    test_urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3",
    ]
    fetch_all(test_urls, max_workers=3)
