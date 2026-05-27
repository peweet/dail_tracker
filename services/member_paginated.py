"""Per-member sequential pagination over Oireachtas /v1/{questions,legislation}.

The Oireachtas API caps any single response at 1000 results (server-side;
larger limit= values are silently clamped — see services/legislation_unscoped.py:28).
Historically the per-member URL builders in services/urls.py hardcoded
skip=0 and dropped everything beyond the first page. For questions this
truncated 79 of 174 sitting members, losing ~150k question-rows in
aggregate (May 2026 figures).

This module fixes that by paginating per member: for each member it fires
one URL, reads head.counts.resultCount, and loops skip+=1000 until either
the page is short (< PAGE_SIZE) or the running total reaches resultCount.
Members are processed concurrently (sequential pagination per member,
parallel across members) to keep wall-clock close to the single-fetch
version.

Output shape preserves the previous bronze contract:
    list[ {"head": {"counts": {"resultCount": N}}, "results": [...]} ]
one dict per member. The downstream flatteners (questions.py, legislation.py)
read this shape unchanged — they iterate every member's results list, so
they tolerate per-member lists of any length.

Truncation assertion:
    For each member, we assert running_total >= head.counts.resultCount.
    Silent truncation is exactly the bug this module exists to prevent.
"""

from __future__ import annotations

import concurrent.futures
import logging
from collections.abc import Callable

import polars as pl

from services.http_engine import fetch_json

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000  # API server cap; larger values are silently clamped


def _fetch_one_member(
    member_uri: str,
    url_builder: Callable[[str, int], str],
    scenario_label: str,
) -> tuple[int, list[dict], int]:
    """Paginate /v1/<scenario> for a single member. Returns (resultCount, results, bytes).

    Sequential — the first page reveals resultCount, so subsequent pages
    can't be issued in parallel without speculative over-fetching.
    """
    results: list[dict] = []
    expected: int | None = None
    raw_bytes_total = 0
    skip = 0

    while True:
        url = url_builder(member_uri, skip)
        page, raw_bytes = fetch_json(url)
        raw_bytes_total += raw_bytes

        if expected is None:
            expected = int(page.get("head", {}).get("counts", {}).get("resultCount", 0))

        page_results = page.get("results", []) or []
        results.extend(page_results)

        if len(page_results) < PAGE_SIZE or len(results) >= expected:
            break
        skip += PAGE_SIZE

    if expected is None:
        expected = len(results)

    # Hard guard: this is the bug-class the module exists to prevent. If the
    # server says resultCount=N and we got fewer than N rows, something is
    # wrong (rate-limit truncation, mid-loop schema change, network flake)
    # and downstream silver/gold would silently underestimate activity.
    assert len(results) >= expected, (
        f"{scenario_label} pagination drift for {member_uri}: "
        f"got {len(results)} rows, head.counts.resultCount={expected}"
    )

    return expected, results, raw_bytes_total


def fetch_all_member_paginated(
    member_df: pl.DataFrame,
    url_builder: Callable[[str, int], str],
    scenario_label: str,
    max_workers: int = 5,
) -> tuple[list[dict], int]:
    """Per-member sequential pagination, parallel across members.

    Args:
        member_df: Must carry a `member_uri` column.
        url_builder: Singular URL builder, signature (member_uri, skip) -> url.
                     E.g. services.urls.build_questions_url or build_legislation_url.
        scenario_label: For logging — "questions" / "legislation" etc.
        max_workers: Concurrent member-workers (each does its own sequential
                     pagination loop). Matches the existing http_engine.fetch_all
                     default of 5.

    Returns:
        (payloads, total_bytes) where payloads is one dict per member preserving
        the previous bronze shape: {"head": {"counts": {"resultCount": N}},
        "results": [...]}. Output ordered by the input member_df row order.
    """
    if member_df.is_empty():
        logger.warning(f"{scenario_label}: empty member_df, nothing to fetch")
        return [], 0

    member_uris = member_df.get_column("member_uri").to_list()
    payloads_by_idx: dict[int, dict] = {}
    failures: dict[int, str] = {}
    total_bytes = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_idx = {
            executor.submit(_fetch_one_member, member_uri, url_builder, scenario_label): (idx, member_uri)
            for idx, member_uri in enumerate(member_uris)
        }
        for i, future in enumerate(concurrent.futures.as_completed(future_to_idx), start=1):
            idx, member_uri = future_to_idx[future]
            try:
                expected, results, raw_bytes = future.result()
                total_bytes += raw_bytes
                payloads_by_idx[idx] = {
                    "head": {"counts": {"resultCount": expected}},
                    "results": results,
                }
                if i % 25 == 0 or i == len(member_uris):
                    logger.info(
                        f"{scenario_label} paginated | {i}/{len(member_uris)} members | "
                        f"downloaded={total_bytes:,} bytes"
                    )
            except Exception as exc:
                failures[idx] = str(exc)
                logger.error(f"{scenario_label} | {member_uri} | pagination failed: {exc}")

    # Restore input order. Members that failed are omitted — the bronze file
    # will be missing those entries, which is what the previous single-fetch
    # behaviour also did on failure. Counts logged below so it's visible.
    payloads = [payloads_by_idx[i] for i in range(len(member_uris)) if i in payloads_by_idx]

    grand_total_rows = sum(len(p["results"]) for p in payloads)
    capped_count = sum(1 for p in payloads if p["head"]["counts"]["resultCount"] > PAGE_SIZE)

    logger.info(
        f"{scenario_label} pagination complete | "
        f"members_fetched={len(payloads)}/{len(member_uris)} | "
        f"failures={len(failures)} | "
        f"total_rows={grand_total_rows:,} | "
        f"members_with_>1_page={capped_count} | "
        f"bytes={total_bytes:,}"
    )

    return payloads, total_bytes
