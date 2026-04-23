import logging
from urllib.parse import quote

import polars as pl

from services.dail_config import API_BASE

logger = logging.getLogger(__name__)


def _to_absolute_member_uri(relative_member_uri: str) -> str:
    if str(relative_member_uri).startswith("http"):
        return str(relative_member_uri)
    return f"https://data.oireachtas.ie{relative_member_uri}"


def build_legislation_urls(member_df: pl.DataFrame) -> list[str]:
    """Build one legislation URL per member."""
    if member_df.is_empty():
        return []

    urls = []
    for member_uri in member_df.get_column("member_uri").to_list():
        full_member_uri = _to_absolute_member_uri(member_uri)
        encoded_member_id = quote(full_member_uri, safe="")

        url = (
            f"{API_BASE}/legislation"
            f"?date_start=2014-01-01"
            f"&date_end=2099-01-01"
            f"&limit=1000"
            f"&member_id={encoded_member_id}"
            f"&chamber_id="
            f"&lang=en"
        )
        urls.append(url)

    logger.info(f"Built {len(urls)} legislation URLs")
    return urls


def build_questions_urls(member_df: pl.DataFrame) -> list[str]:
    """Build one questions URL per member."""
    if member_df.is_empty():
        return []

    urls = []
    for member_uri in member_df.get_column("member_uri").to_list():
        encoded_member_id = quote(member_uri, safe="")

        url = (
            f"{API_BASE}/questions"
            f"?date_start=2020-01-01"
            f"&skip=0"
            f"&limit=1000"
            f"&qtype=oral,written"
            f"&member_id={encoded_member_id}"
        )
        urls.append(url)

    logger.info(f"Built {len(urls)} questions URLs")
    return urls

if __name__ == "__main__":
    # Example usage
    test_df = pl.DataFrame(
        {
            "member_uri": [
                "/ie/oireachtas/member/id/123",
                "https://data.oireachtas.ie/ie/oireachtas/member/id/456",
            ]
        }
    )
    legislation_urls = build_legislation_urls(test_df)
    questions_urls = build_questions_urls(test_df)

    print("Legislation URLs:")
    for url in legislation_urls:
        print(url)

    print("\nQuestions URLs:")
    for url in questions_urls:
        print(url)