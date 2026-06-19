"""Unit tests for the media-mentions sandbox matcher.

Two layers:
  * Pure-logic tests on synthetic members — no IO, no network. These pin the
    CONSERVATIVE adjacency rule (full first+last only, whitespace-bounded,
    accent-insensitive, dedup-per-member, first-writer-wins on collision).
  * Real-world checks against the live historic roster (skipped if it hasn't
    been built). These verify the feature actually works on production data:
    former politicians are included, the no-collision invariant the matcher
    silently relies on still holds, and verified members carry the right
    `is_current` flag.

Run:  pytest test/pipeline_sandbox/test_news_mentions.py -q
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
NEWS_DIR = ROOT / "pipeline_sandbox" / "news_mentions"
# The module does `from feeds import FEEDS`; expose its own dir on sys.path.
sys.path.insert(0, str(NEWS_DIR))

extract = pytest.importorskip(
    "extract", reason="news_mentions sandbox not importable"
)
from extract import (  # noqa: E402
    HISTORIC_ROSTER,
    build_alias_map,
    load_members,
    match_members,
    member_aliases,
    norm,
    strip_accents,
)


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _member(code, first, last, full=None, **extra):
    m = {"unique_member_code": code, "first_name": first, "last_name": last,
         "full_name": full or f"{first} {last}", "house": "Dail",
         "is_current": True}
    m.update(extra)
    return m


def _index(members):
    amap = build_alias_map(members)
    aliases = sorted(amap, key=lambda a: -len(a))
    return aliases, amap


def _names(hits):
    return {m["unique_member_code"] for m, _ in hits}


# --------------------------------------------------------------------------- #
# norm / strip_accents
# --------------------------------------------------------------------------- #
def test_strip_accents_removes_fadas():
    assert strip_accents("Micheál Ó Súilleabháin") == "Micheal O Suilleabhain"


def test_norm_lowercases_strips_punct_and_collapses_space():
    assert norm("  O'Brien,  Mary-Lou! ") == "o brien mary lou"


def test_norm_handles_html_entities_and_none():
    assert norm("Se&aacute;n &amp; Co") == "sean co"
    assert norm(None) == ""


# --------------------------------------------------------------------------- #
# member_aliases
# --------------------------------------------------------------------------- #
def test_aliases_include_first_last_and_full_name():
    al = member_aliases(_member("x", "Jennifer", "MacNeill",
                                full="Jennifer Carroll MacNeill"))
    assert "jennifer macneill" in al
    assert "jennifer carroll macneill" in al


def test_aliases_drop_single_token_names():
    # a one-word full name and a missing first name must not yield a bare token
    assert member_aliases({"full_name": "Bono", "unique_member_code": "z"}) == set()
    assert member_aliases({"last_name": "Murphy", "unique_member_code": "z"}) == set()


def test_aliases_are_accent_normalised():
    assert "micheal martin" in member_aliases(_member("mm", "Micheál", "Martin"))


# --------------------------------------------------------------------------- #
# build_alias_map — first-writer-wins on collision
# --------------------------------------------------------------------------- #
def test_alias_map_first_writer_wins_on_collision():
    a = _member("A", "John", "Browne")
    b = _member("B", "John", "Browne")
    amap = build_alias_map([a, b])
    assert amap["john browne"]["unique_member_code"] == "A"


# --------------------------------------------------------------------------- #
# match_members — the conservative adjacency rule
# --------------------------------------------------------------------------- #
def test_match_full_name_in_title_sets_in_title_flag():
    aliases, amap = _index([_member("SH", "Simon", "Harris")])
    hits = match_members("Simon Harris announces budget", "", aliases, amap)
    assert _names(hits) == {"SH"}
    assert hits[0][1] is True  # match_in_title


def test_match_in_body_only_clears_in_title_flag():
    aliases, amap = _index([_member("SH", "Simon", "Harris")])
    hits = match_members("Budget announced", "Minister Simon Harris said today",
                         aliases, amap)
    assert _names(hits) == {"SH"}
    assert hits[0][1] is False


def test_surname_only_does_not_match():
    aliases, amap = _index([_member("SH", "Simon", "Harris")])
    assert match_members("Harris announces budget", "", aliases, amap) == []


def test_no_substring_false_positive():
    # "ryan" must not match inside "bryan"; whitespace-bounded only
    aliases, amap = _index([_member("ER", "Eamon", "Ryan")])
    assert match_members("Bryan Adams plays Cork", "", aliases, amap) == []


def test_accent_insensitive_match_on_article_text():
    aliases, amap = _index([_member("MM", "Micheál", "Martin")])
    # article drops the fada — must still match
    hits = match_members("Taoiseach Micheal Martin speaks", "", aliases, amap)
    assert _names(hits) == {"MM"}


def test_same_member_named_twice_dedups():
    aliases, amap = _index([_member("SH", "Simon", "Harris")])
    hits = match_members("Simon Harris and Simon Harris again", "", aliases, amap)
    assert len(hits) == 1


def test_distinct_members_both_returned():
    aliases, amap = _index([_member("SH", "Simon", "Harris"),
                            _member("MM", "Micheál", "Martin")])
    hits = match_members("Simon Harris meets Micheál Martin", "", aliases, amap)
    assert _names(hits) == {"SH", "MM"}


def test_empty_text_matches_nothing():
    aliases, amap = _index([_member("SH", "Simon", "Harris")])
    assert match_members("", "", aliases, amap) == []


# --------------------------------------------------------------------------- #
# real-world checks against the live historic roster
# --------------------------------------------------------------------------- #
needs_roster = pytest.mark.skipif(
    not HISTORIC_ROSTER.exists(),
    reason="historic roster not built (run historic_member_pull.py)",
)


@pytest.fixture(scope="module")
def real_members():
    return load_members()


@needs_roster
def test_real_roster_has_no_alias_collisions(real_members):
    """The matcher's first-writer-wins is only safe while no two members share a
    normalised name. Lock that invariant so a future roster regression is caught
    instead of silently dropping a member."""
    from collections import defaultdict
    by_alias = defaultdict(set)
    for m in real_members:
        for a in member_aliases(m):
            by_alias[a].add(m["unique_member_code"])
    collisions = {a: codes for a, codes in by_alias.items() if len(codes) > 1}
    assert not collisions, f"alias collisions would drop members: {collisions}"


@needs_roster
def test_real_roster_includes_former_members(real_members):
    """The whole point of dropping the sitting-only rule: former politicians are
    present (is_current False) alongside current ones."""
    cur = [m for m in real_members if m["is_current"]]
    former = [m for m in real_members if not m["is_current"]]
    assert former, "no former members loaded — sitting-only rule still in effect"
    assert cur, "no current members loaded"


@needs_roster
def test_real_roster_is_current_matches_verified_real_world(real_members):
    """Spot-check against the Oireachtas API ground truth (verified 2026-06-19):
    Donohoe's 34th-Dail membership ended 2025-11-21 (former); Harris sits (current)."""
    by_name = {m.get("full_name"): m for m in real_members}
    if "Simon Harris" in by_name:
        assert by_name["Simon Harris"]["is_current"] is True
    if "Paschal Donohoe" in by_name:
        assert by_name["Paschal Donohoe"]["is_current"] is False


@needs_roster
def test_real_roster_matches_a_known_member(real_members):
    aliases, amap = _index(real_members)
    hits = match_members("Tánaiste Simon Harris addresses the Dáil", "",
                         aliases, amap)
    assert "Simon Harris" in {m.get("full_name") for m, _ in hits}
