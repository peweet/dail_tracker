"""
Tests for the URL builders in services/urls.py.

Pure functions over a member DataFrame — no network. Tests document the
expected URL shape so a regression in the base URL, query parameters, or
URL encoding is caught at PR time.

Why this matters: every member-scoped API call (legislation, questions)
goes through these builders. A subtle change to `quote()` semantics or
a param-order swap breaks every API fetch silently and fills bronze with
garbage. This is the contract.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from services.urls import (
    build_debates_day_urls,
    build_legislation_unscoped_url,
    build_legislation_url,
    build_legislation_urls,
    build_questions_url,
    build_questions_urls,
    debate_section_url,
)

# ---------------------------------------------------------------------------
# Per-member URL builders
# ---------------------------------------------------------------------------


def test_build_legislation_urls_one_url_per_member():
    df = pl.DataFrame(
        {
            "member_uri": [
                "/ie/oireachtas/member/id/123",
                "/ie/oireachtas/member/id/456",
                "https://data.oireachtas.ie/ie/oireachtas/member/id/789",  # already absolute
            ]
        }
    )

    urls = build_legislation_urls(df)

    assert len(urls) == 3
    assert all("/legislation" in u for u in urls)
    assert all("date_start=2014-01-01" in u for u in urls)
    assert all("limit=1000" in u for u in urls)


def test_build_legislation_urls_url_encodes_member_id():
    """Member URIs contain '/' which must be percent-encoded before going
    into the query string. Without encoding the query parser splits on
    them and the API returns wrong data.
    """
    df = pl.DataFrame({"member_uri": ["/ie/oireachtas/member/id/test-123"]})

    urls = build_legislation_urls(df)

    assert len(urls) == 1
    # '/' → %2F when quote(safe="")
    assert "%2Fie%2Foireachtas%2Fmember%2Fid%2Ftest-123" in urls[0]


def test_build_legislation_urls_promotes_relative_uri_to_absolute():
    """Relative member URIs are prefixed with the canonical data.oireachtas.ie
    host before encoding. Two members — one relative, one already absolute —
    produce two URLs both pointing at data.oireachtas.ie/...
    """
    df = pl.DataFrame(
        {
            "member_uri": [
                "/ie/oireachtas/member/id/123",
                "https://data.oireachtas.ie/ie/oireachtas/member/id/456",
            ]
        }
    )

    urls = build_legislation_urls(df)

    # Both percent-encoded forms must include the data.oireachtas.ie host.
    for url in urls:
        assert "data.oireachtas.ie" in url or "data.oireachtas.ie" in url.replace("%2F", "/")


def test_build_legislation_urls_empty_df_returns_empty_list():
    """Defensive — empty inputs short-circuit, no exception."""
    df = pl.DataFrame({"member_uri": []}, schema={"member_uri": pl.Utf8})
    assert build_legislation_urls(df) == []


def test_build_questions_urls_one_url_per_member():
    df = pl.DataFrame({"member_uri": ["uri-1", "uri-2"]})

    urls = build_questions_urls(df)

    assert len(urls) == 2
    assert all("/questions" in u for u in urls)
    assert all("qtype=oral,written" in u for u in urls)
    assert all("date_start=2020-01-01" in u for u in urls)


def test_build_questions_urls_empty_df_returns_empty_list():
    df = pl.DataFrame({"member_uri": []}, schema={"member_uri": pl.Utf8})
    assert build_questions_urls(df) == []


# ---------------------------------------------------------------------------
# Singular per-member builders with skip — the pagination contract
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("skip,expected", [(0, "skip=0"), (1000, "skip=1000"), (3000, "skip=3000")])
def test_build_questions_url_includes_skip(skip: int, expected: str):
    """Per-member pagination depends on this skip param flowing through.
    Regression here re-introduces the 1000-row truncation bug.
    """
    url = build_questions_url("/ie/oireachtas/member/id/test", skip=skip)
    assert expected in url
    assert "/questions" in url
    assert "limit=1000" in url
    assert "qtype=oral,written" in url


@pytest.mark.parametrize("skip,expected", [(0, "skip=0"), (1000, "skip=1000"), (3000, "skip=3000")])
def test_build_legislation_url_includes_skip(skip: int, expected: str):
    url = build_legislation_url("/ie/oireachtas/member/id/test", skip=skip)
    assert expected in url
    assert "/legislation" in url
    assert "limit=1000" in url


def test_build_questions_url_defaults_skip_zero():
    """skip is keyword-only-with-default — calling without it must produce
    a sensible URL (legacy code paths rely on this)."""
    url = build_questions_url("/ie/oireachtas/member/id/test")
    assert "skip=0" in url


# ---------------------------------------------------------------------------
# Unscoped legislation URL with pagination skip
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("skip,expected_skip_param", [(0, "skip=0"), (1000, "skip=1000"), (5000, "skip=5000")])
def test_build_legislation_unscoped_url_includes_skip(skip: int, expected_skip_param: str):
    """Unscoped fetcher paginates via skip — the param must be in the URL."""
    url = build_legislation_unscoped_url(skip=skip)
    assert expected_skip_param in url
    assert "/legislation" in url
    assert "limit=1000" in url


# ---------------------------------------------------------------------------
# Debate section URL reconstruction
# ---------------------------------------------------------------------------


def test_debate_section_url_reconstructs_akn_uri():
    """Pipeline drops the AKN URI from questions.parquet; UI rebuilds it from
    (date, section_id). The pattern must match data.oireachtas.ie/akn/...
    """
    url = debate_section_url("2026-03-15", "dbsect_2026_03_15_001")

    assert url == "https://data.oireachtas.ie/akn/ie/debateRecord/dail/2026-03-15/debate/dbsect_2026_03_15_001"


# ---------------------------------------------------------------------------
# Debates day-window builder — dedupe contract
# ---------------------------------------------------------------------------


def test_build_debates_day_urls_dedupes_repeated_pairs():
    """A worklist with duplicate (date, chamber) pairs must produce one URL
    per unique pair. Without this, fetch_all would hit each debate day twice.
    """
    pairs = [
        ("2026-03-15", "dail"),
        ("2026-03-15", "dail"),  # duplicate
        ("2026-03-15", "seanad"),
        ("2026-03-16", "dail"),
    ]

    urls = build_debates_day_urls(pairs)

    assert len(urls) == 3
    assert sum(1 for u in urls if "date_start=2026-03-15" in u and "chamber=dail" in u) == 1


def test_build_debates_day_urls_filters_unknown_chambers():
    """Only `dail` and `seanad` are valid chambers — other values are dropped
    silently (they're a sign of upstream typo or new chamber that needs
    schema support).
    """
    pairs = [("2026-03-15", "dail"), ("2026-03-15", "joint"), ("2026-03-15", "")]

    urls = build_debates_day_urls(pairs)

    assert len(urls) == 1
    assert "chamber=dail" in urls[0]


def test_build_debates_day_urls_skips_empty_date_or_chamber():
    pairs = [("", "dail"), ("2026-03-15", ""), ("2026-03-15", "dail")]

    urls = build_debates_day_urls(pairs)

    assert len(urls) == 1


def test_build_debates_day_urls_empty_input_returns_empty_list():
    assert build_debates_day_urls([]) == []
