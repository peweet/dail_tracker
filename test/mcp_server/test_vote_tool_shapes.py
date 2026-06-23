"""Behavioural contracts for the bounded vote-search MCP tools.

These lock the result *shape* fixes (2026-06-22): the overview tools must lead with
a compact, navigable summary and only emit the large per-member arrays on request,
and every overview row must carry the `vote_id` that chains into
``division_interest_breakdown`` / ``get_division``.

Data-gated: skips when the gold/silver vote views are absent (CI with no parquet),
mirroring test_core_cross_ref_queries.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("mcp")

from mcp_server import server  # noqa: E402


def _topic_or_skip(**kw):
    r = server.search_votes_by_topic("eviction, tenanc", **kw)
    if "divisions" not in r or not r["divisions"]:
        pytest.skip("vote views/data not available in this snapshot")
    return r


def test_topic_search_leads_with_divisions_carrying_vote_id():
    r = _topic_or_skip()
    # default: overview only, no heavy per-member array
    assert "votes" not in r
    for d in r["divisions"]:
        assert d["vote_id"]  # chains into division_interest_breakdown
        assert {"debate_title", "vote_date", "yes", "no"} <= set(d)


def test_topic_search_member_votes_are_opt_in():
    base = _topic_or_skip()
    full = server.search_votes_by_topic("eviction, tenanc", include_member_votes=True)
    assert "votes" in full and isinstance(full["votes"], list)
    # overview is unchanged by the opt-in
    assert len(full["divisions"]) == len(base["divisions"])


def test_topic_overview_keys_on_distinct_divisions():
    r = _topic_or_skip()
    ids = [d["vote_id"] for d in r["divisions"]]
    assert len(ids) == len(set(ids)), "one row per division"


def _vvi_or_skip(**kw):
    r = server.voting_vs_interests(keyword="rent", **kw)
    if "error" in r or r.get("match_count", 0) == 0:
        pytest.skip("votes×interests views/data not available in this snapshot")
    return r


def test_voting_vs_interests_summary_collapses_to_divisions():
    r = _vvi_or_skip(summary_only=True)
    assert "matches" not in r
    assert r["divisions"], "summary lists matching divisions"
    for d in r["divisions"]:
        assert d["vote_id"]
        assert d["matching_members"] == len(d["members"])
    # match_count reflects the FULL result, not the collapsed view
    assert r["match_count"] >= len(r["divisions"])


def test_voting_vs_interests_detail_is_capped_but_count_is_honest():
    r = _vvi_or_skip(limit=5)
    assert r["returned"] == len(r["matches"]) <= 5
    if r["match_count"] > 5:
        assert r["truncated"] is True
    # the headline counts are the full population, independent of the page
    assert r["match_count"] >= r["returned"]
