"""
Tests for services/votes.py — Oireachtas votes API pagination.

The vote-pagination bug ([SHORT_TERM_PLAN.md item 3.3](doc/SHORT_TERM_PLAN.md#L121))
is exactly the failure mode this catches: a regression that silently
truncates pages would mean only ~10 of ~1078 votes ever reach the pipeline.

What this catches:
  - URL construction (trailing &outcome= is load-bearing per the docstring).
  - Pagination terminates when results < PAGE_SIZE OR cumulative ≥ expected.
  - Cumulative count matches head.counts.resultCount (the truncation guard).
  - Empty result page (already at end) returns gracefully.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
import responses

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from services.votes import PAGE_SIZE, build_vote_url, fetch_votes

# ---------------------------------------------------------------------------
# URL construction — the trailing &outcome= is non-negotiable
# ---------------------------------------------------------------------------


def test_build_vote_url_includes_trailing_outcome_param():
    """Per the function docstring: omitting &outcome= triggers a hidden
    default filter on the API side that returns ~10 of ~1078 records.
    This is the silent-truncation case that's bitten the project before.
    """
    url = build_vote_url()
    assert "outcome=" in url
    # Trailing position matters — the param must be there even if empty.
    assert url.endswith("outcome=")


def test_build_vote_url_uses_explicit_sort_order():
    """Sort/order must be explicit so pagination is stable. Default sort
    order on the API has changed before, scrambling pagination state.
    """
    url = build_vote_url()
    assert "sort=date" in url
    assert "order=desc" in url


def test_build_vote_url_targets_dail_chamber():
    """Hardcoded chamber=dail filter — sanity check this hasn't drifted
    to seanad or empty.
    """
    url = build_vote_url()
    assert "chamber=dail" in url


# ---------------------------------------------------------------------------
# fetch_votes — pagination flow
# ---------------------------------------------------------------------------


def _page_response(results: list, result_count: int) -> dict:
    """Build a fake API page response matching the live API shape.

    Every real /v1/votes result item carries a ``division`` wrapper (see the
    bronze samples under test/fixtures/api/). Inject it on any item missing one
    so the page passes the validate-at-fetch schema check in fetch_json
    (services/schema_validation.py) — while leaving each item's other keys
    untouched so the pass-through assertions below still hold.
    """
    normalised = [{"division": item.get("division", {}), **item} for item in results]
    return {
        "head": {"counts": {"resultCount": result_count}},
        "results": normalised,
    }


@responses.activate
def test_fetch_votes_paginates_until_all_results_collected():
    """The classic pagination case: API has 3 pages of PAGE_SIZE each
    (last page may be partial). All pages stitched into a single result list.
    """
    # Build three pages: full, full, partial (= termination signal).
    page_1 = _page_response([{"vote_id": f"v{i}"} for i in range(PAGE_SIZE)], result_count=2500)
    page_2 = _page_response(
        [{"vote_id": f"v{i}"} for i in range(PAGE_SIZE, 2 * PAGE_SIZE)],
        result_count=2500,
    )
    page_3 = _page_response(
        [{"vote_id": f"v{i}"} for i in range(2 * PAGE_SIZE, 2500)],
        result_count=2500,
    )

    base_url = build_vote_url()
    responses.add(
        responses.GET,
        f"{base_url}&limit={PAGE_SIZE}&skip=0",
        json=page_1,
        status=200,
    )
    responses.add(
        responses.GET,
        f"{base_url}&limit={PAGE_SIZE}&skip={PAGE_SIZE}",
        json=page_2,
        status=200,
    )
    responses.add(
        responses.GET,
        f"{base_url}&limit={PAGE_SIZE}&skip={2 * PAGE_SIZE}",
        json=page_3,
        status=200,
    )

    payloads, total_bytes = fetch_votes()

    # fetch_votes returns one synthetic envelope wrapping all results.
    assert len(payloads) == 1
    assert len(payloads[0]["results"]) == 2500
    assert payloads[0]["head"]["counts"]["resultCount"] == 2500
    assert total_bytes > 0


@responses.activate
def test_fetch_votes_stops_when_first_page_below_page_size():
    """If results < PAGE_SIZE on the very first page, terminate without
    a second HTTP call. Verifies the loop's exit condition.
    """
    page_1 = _page_response([{"vote_id": f"v{i}"} for i in range(10)], result_count=10)

    base_url = build_vote_url()
    responses.add(
        responses.GET,
        f"{base_url}&limit={PAGE_SIZE}&skip=0",
        json=page_1,
        status=200,
    )

    payloads, _ = fetch_votes()

    assert len(payloads[0]["results"]) == 10
    # Only one call made — terminate condition fires immediately.
    assert len(responses.calls) == 1


@responses.activate
def test_fetch_votes_assertion_fires_on_truncation():
    """If the cumulative count is LESS than head.counts.resultCount,
    the explicit assert at the end of fetch_votes must raise. This is
    the silent-truncation guard the function was written specifically
    to enforce.
    """
    # API claims 5000 results but only returns 10 on the first page.
    page_1 = _page_response([{"vote_id": f"v{i}"} for i in range(10)], result_count=5000)

    base_url = build_vote_url()
    responses.add(
        responses.GET,
        f"{base_url}&limit={PAGE_SIZE}&skip=0",
        json=page_1,
        status=200,
    )

    with pytest.raises(AssertionError, match="Vote pagination drift"):
        fetch_votes()


@responses.activate
def test_fetch_votes_uses_expected_count_from_first_page_only():
    """`expected` is set on the first page; subsequent pages' result_count
    is ignored (could drift mid-pagination if the API is updating). The
    contract is "what page 1 said, must be what we end up with."
    """
    page_1 = _page_response([{"vote_id": f"v{i}"} for i in range(PAGE_SIZE)], result_count=1500)
    page_2 = _page_response(
        [{"vote_id": f"v{i}"} for i in range(PAGE_SIZE, 1500)],
        # Pretend the API revised its count partway through — must be ignored.
        result_count=99999,
    )

    base_url = build_vote_url()
    responses.add(
        responses.GET,
        f"{base_url}&limit={PAGE_SIZE}&skip=0",
        json=page_1,
        status=200,
    )
    responses.add(
        responses.GET,
        f"{base_url}&limit={PAGE_SIZE}&skip={PAGE_SIZE}",
        json=page_2,
        status=200,
    )

    payloads, _ = fetch_votes()

    # Synthetic envelope must use the FIRST page's expected count.
    assert payloads[0]["head"]["counts"]["resultCount"] == 1500
    assert len(payloads[0]["results"]) == 1500


@responses.activate
def test_fetch_votes_preserves_individual_result_dicts():
    """Each vote dict in the API response passes through unchanged so
    downstream transform_votes.py sees the same shape it has in
    production.
    """
    page_1 = _page_response(
        [
            {"vote_id": "v1", "date": "2026-01-15", "member": {"full_name": "Mary Murphy"}},
            {"vote_id": "v2", "date": "2026-01-16", "member": {"full_name": "Sean OBrien"}},
        ],
        result_count=2,
    )

    base_url = build_vote_url()
    responses.add(
        responses.GET,
        f"{base_url}&limit={PAGE_SIZE}&skip=0",
        json=page_1,
        status=200,
    )

    payloads, _ = fetch_votes()

    results = payloads[0]["results"]
    assert results[0]["vote_id"] == "v1"
    assert results[0]["member"]["full_name"] == "Mary Murphy"
    assert results[1]["date"] == "2026-01-16"
