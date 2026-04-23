import concurrent.futures
import logging

import requests
from requests.adapters import HTTPAdapter

logger = logging.getLogger(__name__)

session = requests.Session()
adapter = HTTPAdapter(pool_connections=20, pool_maxsize=20)
session.mount("http://", adapter)
session.mount("https://", adapter)


def fetch_json(url: str, timeout: tuple[int, int] = (10, 60)) -> tuple[dict, int]:
    """Fetch one URL using the shared session."""
    response = session.get(url, timeout=timeout)
    response.raise_for_status()
    raw_bytes = len(response.content)
    return response.json(), raw_bytes


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

    logger.info(
        f"Finished fetch_all | results={len(results)} | failures={failures} | "
        f"downloaded={total_bytes:,} bytes"
    )
    return results, total_bytes, failures
if __name__ == "__main__":
    # Example usage
    test_urls = [
        "https://jsonplaceholder.typicode.com/posts/1",
        "https://jsonplaceholder.typicode.com/posts/2",
        "https://jsonplaceholder.typicode.com/posts/3",
    ]
    fetch_all(test_urls, max_workers=3)