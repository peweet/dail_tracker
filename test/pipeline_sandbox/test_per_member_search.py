"""Unit tests for the per-member Google-News search extractor
(pipeline_sandbox/news_mentions/per_member_search.py). Pure-logic, no network, no IO.

Pins: the search-URL builder (quoted name + recency + IE locale), the Google-News RSS parse
(headline cleanup, outlet from <source>, ParseError safety), the source→tier mapping, the
per-member row build (match_in_title flag, drift drop, schema), and the accumulate/dedup
(keep earliest fetched copy of an (article_url, member) pair).
"""
from __future__ import annotations

import sys
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
NEWS_DIR = ROOT / "pipeline_sandbox" / "news_mentions"
sys.path.insert(0, str(NEWS_DIR))

pms = pytest.importorskip("per_member_search", reason="news_mentions sandbox not importable")
from per_member_search import (  # noqa: E402
    _accumulate,
    _rows_for_member,
    _source_tier,
    gn_search_url,
    parse_gn_items,
)

_FETCHED = datetime(2026, 6, 19, 12, 0, tzinfo=UTC)


# --------------------------------------------------------------------------- URL builder
def test_gn_search_url_quotes_name_and_window():
    u = gn_search_url("Helen McEntee", 30)
    assert "news.google.com/rss/search" in u
    assert "%22Helen+McEntee%22" in u  # quoted, url-encoded
    assert "when%3A30d" in u
    assert "gl=IE" in u and "ceid=IE%3Aen" in u


def test_gn_search_url_respects_days():
    assert "when%3A7d" in gn_search_url("Jim Smith", 7)


# --------------------------------------------------------------------------- RSS parse
_GN_RSS = b"""<?xml version='1.0'?><rss version='2.0'><channel>
<item><title>Naughton welcomes funding for Galway piers - Galway Advertiser</title>
<link>https://news.google.com/rss/articles/ABC123</link>
<pubDate>Wed, 18 Jun 2026 09:00:00 GMT</pubDate>
<source url='https://www.galwayadvertiser.ie'>Galway Advertiser</source></item>
<item><title>A headline with no source element</title>
<link>https://news.google.com/rss/articles/DEF456</link>
<pubDate>Tue, 17 Jun 2026 09:00:00 GMT</pubDate></item>
<item><title></title><link>https://news.google.com/x</link></item>
</channel></rss>"""


def test_parse_gn_items_strips_source_suffix_and_reads_outlet():
    items = parse_gn_items(_GN_RSS)
    assert len(items) == 2  # the empty-title item is dropped
    first = items[0]
    assert first["title"] == "Naughton welcomes funding for Galway piers"  # " - Galway Advertiser" stripped
    assert first["outlet"] == "Galway Advertiser"
    assert first["link"].endswith("ABC123")


def test_parse_gn_items_handles_missing_source():
    items = parse_gn_items(_GN_RSS)
    second = items[1]
    assert second["title"] == "A headline with no source element"
    assert second["outlet"] == ""  # no <source> → blank, never crashes


def test_parse_gn_items_parse_error_is_safe():
    assert parse_gn_items(b"<<not xml>>") == []
    assert parse_gn_items(b"") == []


# --------------------------------------------------------------------------- source → tier
@pytest.mark.parametrize(
    "outlet,tier",
    [
        ("The Irish Times", "national"),
        ("RTÉ", "national"),
        ("Galway Bay FM", "local_radio"),
        ("Kildare Nationalist", "local_paper"),
        ("Agriland", "specialist"),
        ("Some Unknown Blog", "national"),  # default
        ("", "national"),
    ],
)
def test_source_tier(outlet, tier):
    assert _source_tier(outlet) == tier


# --------------------------------------------------------------------------- per-member rows
_MEMBER = {
    "unique_member_code": "abc", "full_name": "Helen McEntee", "last_name": "McEntee",
    "party": "Fine Gael", "constituency_name": "Meath East", "house": "Dail", "is_current": True,
}


def test_rows_match_in_title_true_when_full_name_in_headline():
    items = [{"title": "Helen McEntee announces reform", "link": "https://news.google.com/a",
              "outlet": "RTÉ", "pubDate": "Wed, 18 Jun 2026 09:00:00 GMT"}]
    rows = _rows_for_member(_MEMBER, items, _FETCHED)
    assert len(rows) == 1
    r = rows[0]
    assert r["match_in_title"] is True
    assert r["unique_member_code"] == "abc"
    assert r["matched_name"] == "Helen McEntee"
    assert r["outlet"] == "RTÉ"
    assert r["published_at"] is not None
    # schema the demo/UI relies on:
    assert {"article_id", "outlet_tier", "article_url", "article_title", "is_current"}.issubset(r)


def test_rows_match_in_title_false_for_body_only_mention():
    # Name not in headline but Google matched the article (kept, flagged body mention).
    items = [{"title": "Minister visits new Meath school", "link": "https://news.google.com/b",
              "outlet": "Meath Chronicle", "pubDate": ""}]
    rows = _rows_for_member(_MEMBER, items, _FETCHED)
    assert len(rows) == 1
    assert rows[0]["match_in_title"] is False
    assert rows[0]["published_at"] is not None  # falls back to fetched_at when pubDate missing


def test_rows_drop_drift_with_no_name_and_no_link():
    # Neither full name nor surname in the headline AND no link → Google topic-drift, dropped.
    items = [{"title": "Unrelated weather story", "link": "", "outlet": "RTÉ", "pubDate": ""}]
    assert _rows_for_member(_MEMBER, items, _FETCHED) == []


def test_rows_keep_surname_headline_even_without_link():
    items = [{"title": "McEntee defends the bill", "link": "", "outlet": "RTÉ", "pubDate": ""}]
    rows = _rows_for_member(_MEMBER, items, _FETCHED)
    assert len(rows) == 1
    assert rows[0]["match_in_title"] is False  # surname-only, not full name


# --------------------------------------------------------------------------- accumulate / dedup
def _row(url, code, fetched):
    return {"article_id": url[-3:], "unique_member_code": code, "matched_name": code,
            "party": None, "constituency": None, "house": "Dail", "is_current": True,
            "outlet": "RTÉ", "outlet_tier": "national", "article_title": "t",
            "article_url": url, "published_at": _FETCHED.replace(tzinfo=None),
            "match_in_title": True, "fetched_at": fetched}


def test_accumulate_dedups_on_url_and_member_keeping_earliest(tmp_path, monkeypatch):
    out = tmp_path / "nm.parquet"
    monkeypatch.setattr(pms, "OUT", out)
    early = datetime(2026, 6, 1)
    late = datetime(2026, 6, 19)
    # first run persists one row (fetched early) — main() writes; emulate that here
    pms._accumulate(pl.DataFrame([_row("https://x/1", "m1", early)])).write_parquet(out)
    # now a second run: same (url, member) fetched late + a brand-new row
    merged = _accumulate(pl.DataFrame([_row("https://x/1", "m1", late), _row("https://x/2", "m2", late)]))
    assert merged.height == 2  # the duplicate collapsed, the new one added
    dup = merged.filter((pl.col("article_url") == "https://x/1") & (pl.col("unique_member_code") == "m1"))
    assert dup.height == 1
    assert dup["fetched_at"].item() == early  # kept the EARLIEST fetched copy


def test_accumulate_same_article_different_members_both_kept(tmp_path, monkeypatch):
    monkeypatch.setattr(pms, "OUT", tmp_path / "nm.parquet")
    merged = _accumulate(pl.DataFrame([_row("https://x/9", "m1", _FETCHED), _row("https://x/9", "m2", _FETCHED)]))
    assert merged.height == 2  # one article naming two members = two rows
