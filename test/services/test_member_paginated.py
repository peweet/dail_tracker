"""
Tests for services/member_paginated.py.

This module is the fix for the 1000-row API cap that previously truncated
79 of 174 sitting members' question history. The tests below pin the
critical contract:

  - The loop continues until either the page is short OR the running total
    reaches head.counts.resultCount. Off-by-one here re-introduces silent
    truncation.
  - The loop asserts no truncation. Removing the assertion lets the bug
    return without anyone noticing.
  - Members are processed independently — one member's failure must not
    drop another's results.

No network — fetch_json is monkeypatched with a deterministic fake.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from services import member_paginated  # noqa: E402
from services.member_paginated import PAGE_SIZE, fetch_all_member_paginated  # noqa: E402


def _fake_url_builder(member_uri: str, skip: int) -> str:
    """Echo the member + skip into a sentinel URL so the fake fetcher knows
    which page it's being asked for."""
    return f"https://fake/{member_uri}?skip={skip}"


def _make_fake_fetch(total_per_member: dict[str, int]):
    """Build a fake fetch_json that pages results deterministically.

    Each request returns the next PAGE_SIZE-slice of an integer sequence
    [0..total). Final page may be short. head.counts.resultCount is the
    advertised total (so the loop has the canonical number to assert
    against).
    """

    def _fetch(url: str, timeout=None):
        # Parse member_uri and skip out of the sentinel URL.
        _, rest = url.split("/", 3)[-1].split("?")
        member_uri = url.split("https://fake/", 1)[1].split("?", 1)[0]
        skip = int(rest.replace("skip=", ""))
        total = total_per_member[member_uri]
        end = min(skip + PAGE_SIZE, total)
        results = [{"i": i, "member_uri": member_uri} for i in range(skip, end)]
        payload = {
            "head": {"counts": {"resultCount": total}},
            "results": results,
        }
        return payload, len(str(payload))  # (data, bytes)

    return _fetch


def test_short_first_page_terminates_immediately(monkeypatch):
    """If the member has < PAGE_SIZE results, one fetch is enough."""
    totals = {"m1": 250}
    monkeypatch.setattr(member_paginated, "fetch_json", _make_fake_fetch(totals))

    df = pl.DataFrame({"member_uri": ["m1"]})
    payloads, _ = fetch_all_member_paginated(df, _fake_url_builder, "test")

    assert len(payloads) == 1
    assert payloads[0]["head"]["counts"]["resultCount"] == 250
    assert len(payloads[0]["results"]) == 250


def test_exactly_one_full_page_then_empty_terminates(monkeypatch):
    """1000 results -> first page is full -> server says total=1000 ->
    running total == expected -> stop without a second request."""
    totals = {"m1": PAGE_SIZE}
    fetch = _make_fake_fetch(totals)
    call_count = {"n": 0}

    def counting_fetch(url, timeout=None):
        call_count["n"] += 1
        return fetch(url, timeout=timeout)

    monkeypatch.setattr(member_paginated, "fetch_json", counting_fetch)

    df = pl.DataFrame({"member_uri": ["m1"]})
    payloads, _ = fetch_all_member_paginated(df, _fake_url_builder, "test")

    assert call_count["n"] == 1
    assert len(payloads[0]["results"]) == PAGE_SIZE


def test_paginates_until_complete(monkeypatch):
    """3,003 results -> 4 pages (1000+1000+1000+3). The exact shape of the
    failure mode this module fixes: previously only the first 1000 were kept.
    """
    totals = {"m1": 3003}
    monkeypatch.setattr(member_paginated, "fetch_json", _make_fake_fetch(totals))

    df = pl.DataFrame({"member_uri": ["m1"]})
    payloads, _ = fetch_all_member_paginated(df, _fake_url_builder, "test")

    assert payloads[0]["head"]["counts"]["resultCount"] == 3003
    assert len(payloads[0]["results"]) == 3003
    # Indices 0..3002 inclusive, no gaps, no dupes.
    seen = [r["i"] for r in payloads[0]["results"]]
    assert seen == list(range(3003))


def test_multiple_members_independent(monkeypatch):
    """One small TD + one large capped TD + one mid-sized TD. All three
    must return their full row counts, in the input member_df order."""
    totals = {"m_small": 47, "m_huge": 2500, "m_mid": 1200}
    monkeypatch.setattr(member_paginated, "fetch_json", _make_fake_fetch(totals))

    df = pl.DataFrame({"member_uri": ["m_small", "m_huge", "m_mid"]})
    payloads, _ = fetch_all_member_paginated(df, _fake_url_builder, "test", max_workers=3)

    # Output ordered by input order
    assert [p["head"]["counts"]["resultCount"] for p in payloads] == [47, 2500, 1200]
    assert [len(p["results"]) for p in payloads] == [47, 2500, 1200]


def test_truncation_assertion_fires_on_server_lie(monkeypatch):
    """The assertion is the safety net. If the server returns fewer rows
    than head.counts.resultCount advertised, the loop terminates (short
    page) but the assertion catches it. Removing the assert would silently
    re-introduce truncation — this test pins that behaviour."""

    def lying_fetch(url, timeout=None):
        # Says 5,000 but only returns 100 on the first page and then is empty.
        skip = int(url.split("skip=")[-1])
        if skip == 0:
            return ({"head": {"counts": {"resultCount": 5000}}, "results": [{"i": i} for i in range(100)]}, 1)
        return ({"head": {"counts": {"resultCount": 5000}}, "results": []}, 1)

    monkeypatch.setattr(member_paginated, "fetch_json", lying_fetch)

    df = pl.DataFrame({"member_uri": ["m_liar"]})
    # The per-member assertion is raised inside a thread; the runner catches
    # it as a per-member failure and excludes the payload from output. So
    # the output list is empty rather than the assertion bubbling all the
    # way up — but the failure is logged. Verify the row simply doesn't
    # appear in the bronze output.
    payloads, _ = fetch_all_member_paginated(df, _fake_url_builder, "test")
    assert payloads == []


def test_empty_member_df_returns_empty(monkeypatch):
    monkeypatch.setattr(
        member_paginated,
        "fetch_json",
        lambda *a, **k: pytest.fail("fetch_json should not be called for empty df"),
    )
    df = pl.DataFrame({"member_uri": []}, schema={"member_uri": pl.Utf8})
    payloads, total_bytes = fetch_all_member_paginated(df, _fake_url_builder, "test")
    assert payloads == []
    assert total_bytes == 0
