"""Unit tests for the experimental media-mentions feature.

Two surfaces, both pure (no network, no Streamlit runtime):

1. The MEMBER MATCHER (``pipeline_sandbox/news_mentions/extract.py``) — the
   firewall-critical, defamation-sensitive logic that decides *who* an article
   is about. A regression here attaches the wrong politician to a headline, so
   the conservative rule is locked down hard: full first+last only (never
   surname-only), accent-insensitive, word-bounded, deduped per member.

2. The DISPLAY SHAPER (``ui/media_mentions_experimental.py``) — filter to one
   member, newest-first, month labels.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
for _p in (str(PROJECT_ROOT), str(PROJECT_ROOT / "utility")):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from pipeline_sandbox.news_mentions.extract import (  # noqa: E402
    build_alias_map,
    match_members,
    member_aliases,
    norm,
    strip_accents,
    _parse_date,
)
from ui.media_mentions_experimental import (  # noqa: E402
    month_label,
    shape_member_mentions,
)


# ── fixtures ─────────────────────────────────────────────────────────────────
def _member(code, first, last, **extra):
    return {
        "unique_member_code": code, "first_name": first, "last_name": last,
        "full_name": f"{first} {last}", "party": extra.get("party"),
        "constituency_name": extra.get("constituency"), "house": extra.get("house", "Dail"),
        "is_current": extra.get("is_current", True),
    }


@pytest.fixture
def amap_aliases():
    members = [
        _member("M1", "Micheál", "Martin"),     # fada in given name? no — in nothing; surname Martin
        _member("M2", "Catherine", "Murphy"),    # shared surname with M3
        _member("M3", "Verona", "Murphy"),       # shared surname with M2
        _member("M4", "Simon", "Harris"),
        _member("M5", "Mary Lou", "McDonald"),   # multi-token given name
    ]
    amap = build_alias_map(members)
    aliases = sorted(amap, key=lambda a: -len(a))  # longest-first, as the extractor does
    return amap, aliases


def _codes(hits):
    return [m["unique_member_code"] for m, _ in hits]


# ── normalisation ────────────────────────────────────────────────────────────
def test_strip_accents_removes_fada():
    assert strip_accents("Micheál Ó Súilleabháin") == "Micheal O Suilleabhain"


def test_norm_lowercases_strips_punctuation():
    assert norm("O'Sullivan, T.D.!") == "o sullivan t d"


# ── alias building ───────────────────────────────────────────────────────────
def test_member_aliases_requires_two_tokens():
    assert member_aliases(_member("X", "Simon", "Harris")) == {"simon harris"}
    # no surname -> no usable alias (prevents single-token matches)
    assert member_aliases({"first_name": "Madonna", "last_name": None, "full_name": "Madonna"}) == set()


def test_build_alias_map_keys_are_normalised():
    amap = build_alias_map([_member("M4", "Simon", "Harris")])
    assert "simon harris" in amap and amap["simon harris"]["unique_member_code"] == "M4"


# ── the matcher: the critical behaviour ──────────────────────────────────────
def test_full_name_in_title_matches_and_flags_title(amap_aliases):
    amap, aliases = amap_aliases
    hits = match_members("Simon Harris announces budget", "", aliases, amap)
    assert _codes(hits) == ["M4"]
    assert hits[0][1] is True  # match_in_title


def test_accent_insensitive_match(amap_aliases):
    amap, aliases = amap_aliases
    # headline drops the fada — must still match Micheál Martin
    hits = match_members("Micheal Martin meets EU leaders", "", aliases, amap)
    assert _codes(hits) == ["M1"]


def test_surname_only_never_matches(amap_aliases):
    """The defamation guard: a bare surname must NOT attach to anyone."""
    amap, aliases = amap_aliases
    assert match_members("Murphy slams housing policy", "", aliases, amap) == []
    assert match_members("Harris under pressure", "Minister Harris", aliases, amap) == []


def test_substring_of_longer_name_does_not_match(amap_aliases):
    amap, aliases = amap_aliases
    # "Simon Harrison" must not trip "Simon Harris" (word boundary)
    assert match_members("Simon Harrison wins award", "", aliases, amap) == []


def test_body_only_match_sets_flag_false(amap_aliases):
    amap, aliases = amap_aliases
    hits = match_members("Budget reaction", "Simon Harris welcomed the measures", aliases, amap)
    assert _codes(hits) == ["M4"]
    assert hits[0][1] is False  # named in body, not title


def test_multiple_distinct_members_each_returned(amap_aliases):
    amap, aliases = amap_aliases
    hits = match_members("Simon Harris and Mary Lou McDonald clash", "", aliases, amap)
    assert set(_codes(hits)) == {"M4", "M5"}


def test_same_member_named_twice_deduped(amap_aliases):
    amap, aliases = amap_aliases
    hits = match_members("Simon Harris speaks; Simon Harris later clarified", "", aliases, amap)
    assert _codes(hits) == ["M4"]


def test_two_members_sharing_surname_only_matched_by_full_name(amap_aliases):
    amap, aliases = amap_aliases
    hits = match_members("Catherine Murphy questions minister", "", aliases, amap)
    assert _codes(hits) == ["M2"]  # not Verona Murphy


# ── date parsing ─────────────────────────────────────────────────────────────
def test_parse_date_rfc822():
    dt = _parse_date("Tue, 17 Jun 2026 09:30:00 +0000")
    assert dt is not None and dt.year == 2026 and dt.month == 6 and dt.day == 17


def test_parse_date_iso():
    dt = _parse_date("2026-06-17T09:30:00Z")
    assert dt is not None and dt.year == 2026


def test_parse_date_junk_returns_none():
    assert _parse_date("not a date") is None
    assert _parse_date("") is None


# ── display shaping ──────────────────────────────────────────────────────────
def _mentions_df():
    return pd.DataFrame([
        {"unique_member_code": "M4", "article_title": "older", "published_at": pd.Timestamp("2026-05-01"), "outlet_tier": "national"},
        {"unique_member_code": "M4", "article_title": "newer", "published_at": pd.Timestamp("2026-06-10"), "outlet_tier": "national"},
        {"unique_member_code": "M4", "article_title": "undated", "published_at": None, "outlet_tier": "national"},
        {"unique_member_code": "M1", "article_title": "other member", "published_at": pd.Timestamp("2026-06-01"), "outlet_tier": "national"},
    ])


def test_shape_filters_to_member():
    out = shape_member_mentions(_mentions_df(), "M4")
    assert set(out["unique_member_code"]) == {"M4"}
    assert len(out) == 3


def test_shape_orders_newest_first_undated_last():
    out = shape_member_mentions(_mentions_df(), "M4")
    assert list(out["article_title"]) == ["newer", "older", "undated"]


def test_shape_unknown_member_is_empty():
    assert shape_member_mentions(_mentions_df(), "ZZZ").empty


def test_shape_empty_input_is_empty():
    assert shape_member_mentions(pd.DataFrame(), "M4").empty


def test_month_label():
    assert month_label(pd.Timestamp("2026-06-17")) == "June 2026"
    assert month_label(None) == "Undated"
    assert month_label("garbage") == "Undated"
