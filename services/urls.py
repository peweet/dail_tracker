import logging
from urllib.parse import quote

import polars as pl

from services.dail_config import API_BASE

DEBATES_CHAMBERS = ("dail", "seanad")

logger = logging.getLogger(__name__)


def _to_absolute_member_uri(relative_member_uri: str) -> str:
    if str(relative_member_uri).startswith("http"):
        return str(relative_member_uri)
    return f"https://data.oireachtas.ie{relative_member_uri}"


def build_legislation_url(member_uri: str, skip: int = 0) -> str:
    """One /v1/legislation URL for a single member at a given pagination offset.

    The PAGE_SIZE=1000 is the API server cap (larger values are silently
    clamped — see services/legislation_unscoped.py:28). For members whose
    sponsored-bill history exceeds 1000, callers must loop with skip+=1000;
    see services/member_paginated.py.
    """
    full_member_uri = _to_absolute_member_uri(member_uri)
    encoded_member_id = quote(full_member_uri, safe="")
    return (
        f"{API_BASE}/legislation"
        f"?date_start=2014-01-01"
        f"&date_end=2099-01-01"
        f"&limit=1000"
        f"&skip={skip}"
        f"&member_id={encoded_member_id}"
        f"&chamber_id="
        f"&lang=en"
    )


def build_legislation_urls(member_df: pl.DataFrame) -> list[str]:
    """Legacy list builder: one URL per member at skip=0.

    DEPRECATED for production fetching — silently drops results past the
    1000-row cap. Kept for tests and for any caller that knows it's
    operating below the cap. New code should call
    services/member_paginated.fetch_all_member_paginated() instead.
    """
    if member_df.is_empty():
        return []

    urls = [build_legislation_url(member_uri, skip=0) for member_uri in member_df.get_column("member_uri").to_list()]
    logger.info(f"Built {len(urls)} legislation URLs")
    return urls


def build_legislation_unscoped_url(skip: int = 0) -> str:
    return (
        f"{API_BASE}/legislation?date_start=2014-01-01&date_end=2099-01-01&limit=1000&skip={skip}&chamber_id=&lang=en"
    )


def debate_section_url(context_date: str, debate_section_id: str) -> str:
    """Reconstruct the internal AKN debate-section URI dropped from questions.parquet.

    Reproduces what was stored as `debate_section_uri` before the URI cleanup
    documented in pipeline_sandbox/zstd_typing_notes.md §8.2. Useful for UI
    code that needs a deep-link to the akoma-ntoso XML payload.
    """
    return f"https://data.oireachtas.ie/akn/ie/debateRecord/dail/{context_date}/debate/{debate_section_id}"


def build_questions_url(member_uri: str, skip: int = 0) -> str:
    """One /v1/questions URL for a single member at a given pagination offset.

    PAGE_SIZE=1000 is the API server cap. Empirically (May 2026) 79 of 174
    sitting Dáil members have head.counts.resultCount > 1000 — the most
    prolific are at ~3,000+. Callers must paginate via skip+=1000; see
    services/member_paginated.py.
    """
    encoded_member_id = quote(member_uri, safe="")
    return (
        f"{API_BASE}/questions"
        f"?date_start=2020-01-01"
        f"&skip={skip}"
        f"&limit=1000"
        f"&qtype=oral,written"
        f"&member_id={encoded_member_id}"
    )


def build_questions_urls(member_df: pl.DataFrame) -> list[str]:
    """Legacy list builder: one URL per member at skip=0.

    DEPRECATED for production fetching — silently drops every question past
    the 1000-row cap. 79/174 members hit that cap as of May 2026, losing
    ~150k question-rows in aggregate. Kept for the URL-shape tests in
    test/test_url_builders.py. New code should call
    services/member_paginated.fetch_all_member_paginated() instead.
    """
    if member_df.is_empty():
        return []

    urls = [build_questions_url(member_uri, skip=0) for member_uri in member_df.get_column("member_uri").to_list()]
    logger.info(f"Built {len(urls)} questions URLs")
    return urls


def build_debates_day_urls(
    date_chamber_pairs: list[tuple[str, str]],
) -> list[str]:
    """Build one /v1/debates day-window URL per (date, chamber) pair.

    Identity is composite — the same dbsect_* recurs every sitting day, so
    the worklist must be deduped on (date, chamber) by the caller. The API
    returns every dbsect that sat in that chamber on that date in a single
    response (~35 KB), so total worklist size is ~700 today, not 3,008.
    """
    if not date_chamber_pairs:
        return []

    seen: set[tuple[str, str]] = set()
    urls: list[str] = []
    for date, chamber in date_chamber_pairs:
        if not date or not chamber:
            continue
        if chamber not in DEBATES_CHAMBERS:
            continue
        key = (str(date), str(chamber))
        if key in seen:
            continue
        seen.add(key)
        urls.append(f"{API_BASE}/debates?date_start={date}&date_end={date}&chamber={quote(chamber)}&limit=200&lang=en")

    logger.info(f"Built {len(urls)} debates day-window URLs")
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
