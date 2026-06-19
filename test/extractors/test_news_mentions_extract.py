"""Unit + contract tests for the promoted news-mentions extractor + view
(extractors/news_mentions_extract.py → data/silver/parquet/news_mentions.parquet →
v_member_news_mentions). Pure-logic tests run in CI; the view contract skips if the parquet
hasn't been built.
"""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import pytest

from extractors.news_mentions_extract import (
    gn_search_url,
    norm,
    parse_gn_items,
    rows_for_member,
    source_tier,
)

ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / "data/silver/parquet/news_mentions.parquet"
_FETCHED = datetime(2026, 6, 19, 12, 0, tzinfo=UTC)
_MEMBER = {"unique_member_code": "abc", "full_name": "Helen McEntee", "last_name": "McEntee",
           "party": "Fine Gael", "constituency_name": "Meath East", "house": "Dail", "is_current": True}

_GN_RSS = b"""<?xml version='1.0'?><rss version='2.0'><channel>
<item><title>Naughton welcomes funding for Galway piers - Galway Advertiser</title>
<link>https://news.google.com/rss/articles/ABC123</link>
<pubDate>Wed, 18 Jun 2026 09:00:00 GMT</pubDate>
<source url='https://www.galwayadvertiser.ie'>Galway Advertiser</source></item>
<item><title>A headline with no source element</title>
<link>https://news.google.com/rss/articles/DEF456</link></item>
<item><title></title><link>https://news.google.com/x</link></item>
</channel></rss>"""


# --------------------------------------------------------------------------- pure logic
def test_gn_search_url_quotes_name_window_and_locale():
    u = gn_search_url("Helen McEntee", 30)
    assert "news.google.com/rss/search" in u
    assert "%22Helen+McEntee%22" in u and "when%3A30d" in u
    assert "gl=IE" in u and "ceid=IE%3Aen" in u


def test_norm_strips_fadas_and_punct():
    assert norm("Mícheál O'Súilleabháin") == "micheal o suilleabhain"


def test_parse_gn_items_strips_source_and_handles_missing():
    items = parse_gn_items(_GN_RSS)
    assert len(items) == 2  # empty-title dropped
    assert items[0]["title"] == "Naughton welcomes funding for Galway piers"
    assert items[0]["outlet"] == "Galway Advertiser"
    assert items[1]["outlet"] == ""  # no <source> → blank


def test_parse_gn_items_parse_error_safe():
    assert parse_gn_items(b"<<bad>>") == [] and parse_gn_items(b"") == []


@pytest.mark.parametrize("outlet,tier", [
    ("The Irish Times", "national"), ("RTÉ", "national"), ("Galway Bay FM", "local_radio"),
    ("Kildare Nationalist", "local_paper"), ("Agriland", "specialist"), ("Mystery Blog", "national"),
])
def test_source_tier(outlet, tier):
    assert source_tier(outlet) == tier


def test_rows_match_in_title_and_schema():
    items = [{"title": "Helen McEntee announces reform", "link": "https://news.google.com/a",
              "outlet": "RTÉ", "pubDate": "Wed, 18 Jun 2026 09:00:00 GMT"}]
    r = rows_for_member(_MEMBER, items, _FETCHED)[0]
    assert r["match_in_title"] is True and r["unique_member_code"] == "abc"
    assert {"article_id", "outlet_tier", "article_url", "published_at", "is_current"}.issubset(r)


def test_rows_body_only_kept_and_drift_dropped():
    body = [{"title": "Minister visits Meath school", "link": "https://news.google.com/b", "outlet": "x", "pubDate": ""}]
    assert rows_for_member(_MEMBER, body, _FETCHED)[0]["match_in_title"] is False
    drift = [{"title": "Unrelated weather story", "link": "", "outlet": "x", "pubDate": ""}]
    assert rows_for_member(_MEMBER, drift, _FETCHED) == []


def _row(url, code, fetched):
    return {"article_id": url[-3:], "unique_member_code": code, "matched_name": code, "party": None,
            "constituency": None, "house": "Dail", "is_current": True, "outlet": "RTÉ",
            "outlet_tier": "national", "article_title": "t", "article_url": url,
            "published_at": _FETCHED.replace(tzinfo=None), "match_in_title": True, "fetched_at": fetched}


def test_accumulate_dedups_keeping_earliest(tmp_path, monkeypatch):
    import extractors.news_mentions_extract as ext
    out = tmp_path / "nm.parquet"
    monkeypatch.setattr(ext, "OUT", out)
    early, late = datetime(2026, 6, 1), datetime(2026, 6, 19)
    ext.accumulate(pl.DataFrame([_row("https://x/1", "m1", early)])).write_parquet(out)
    merged = ext.accumulate(pl.DataFrame([_row("https://x/1", "m1", late), _row("https://x/2", "m2", late)]))
    assert merged.height == 2
    dup = merged.filter((pl.col("article_url") == "https://x/1") & (pl.col("unique_member_code") == "m1"))
    assert dup.height == 1 and dup["fetched_at"].item() == early  # earliest kept


# --------------------------------------------------------------------------- view/silver contract
@pytest.mark.integration
def test_view_contract_against_silver():
    if not SILVER.exists():
        pytest.skip("news_mentions.parquet not built")
    import duckdb

    from dail_tracker_core.connections import register_member_views
    conn = duckdb.connect()
    register_member_views(conn)
    cols = {r[1] for r in conn.execute("PRAGMA table_info('v_member_news_mentions')").fetchall()}
    assert {"unique_member_code", "article_title", "article_url", "outlet_tier",
            "published_at", "match_in_title"}.issubset(cols)
    n, members = conn.execute(
        "SELECT COUNT(*), COUNT(DISTINCT unique_member_code) FROM v_member_news_mentions"
    ).fetchone()
    assert n > 0 and members > 0
    # every surfaced row has a title (the view filters nulls)
    assert conn.execute("SELECT COUNT(*) FROM v_member_news_mentions WHERE article_title IS NULL").fetchone()[0] == 0
