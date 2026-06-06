"""
Unit tests for wikidata_socials_etl.

Covers the pure URL-derivation functions and the in-memory shape pipeline
(build_links_df). No SPARQL roundtrip — `fetch_wikidata` is the only piece
that touches the network and is mocked via a hand-built Polars frame in the
shape test.

Why this matters: the URL builders are the contract between Wikidata's raw
handle storage (P2002 = "MaryMurphyTD", not a URL) and the hrefs the
member-overview hero renders. A drift in those builders silently breaks
every chip on every TD profile.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from wikidata.wikidata_socials_etl import (
    bluesky_url,
    build_links_df,
    facebook_url,
    instagram_url,
    twitter_url,
)

# ---------------------------------------------------------------------------
# URL builders
# ---------------------------------------------------------------------------


def test_twitter_url_handles_at_prefix_and_blanks():
    assert twitter_url("MaryMurphyTD") == "https://x.com/MaryMurphyTD"
    assert twitter_url("@MaryMurphyTD") == "https://x.com/MaryMurphyTD"
    assert twitter_url("  spaced  ") == "https://x.com/spaced"
    assert twitter_url(None) is None
    assert twitter_url("") is None
    assert twitter_url("   ") is None


def test_bluesky_url_handles_handle_and_custom_domain():
    assert bluesky_url("mary.bsky.social") == "https://bsky.app/profile/mary.bsky.social"
    assert bluesky_url("@mary.bsky.social") == "https://bsky.app/profile/mary.bsky.social"
    # Custom-domain identities resolve through the same pattern.
    assert bluesky_url("marymurphytd.ie") == "https://bsky.app/profile/marymurphytd.ie"
    assert bluesky_url(None) is None


def test_facebook_url_accepts_vanity_or_numeric_id():
    assert facebook_url("marymurphytd") == "https://www.facebook.com/marymurphytd"
    assert facebook_url("123456789") == "https://www.facebook.com/123456789"
    assert facebook_url(None) is None


def test_instagram_url_appends_trailing_slash():
    assert instagram_url("marymurphytd") == "https://www.instagram.com/marymurphytd/"
    assert instagram_url("@marymurphytd") == "https://www.instagram.com/marymurphytd/"
    assert instagram_url(None) is None


# ---------------------------------------------------------------------------
# Shape pipeline (build_links_df)
# ---------------------------------------------------------------------------


def _raw(rows: list[dict]) -> pl.DataFrame:
    """Shape-stable mock of what `pl.read_csv(<sparql.csv>)` returns. Columns
    mirror the SELECT variables in the SPARQL query, including null-value
    holes.
    """
    return pl.DataFrame(
        rows,
        schema={
            "code": pl.Utf8,
            "td": pl.Utf8,
            "twitter": pl.Utf8,
            "bluesky": pl.Utf8,
            "facebook": pl.Utf8,
            "instagram": pl.Utf8,
            "website": pl.Utf8,
            "wikiArticle": pl.Utf8,
        },
    )


def test_build_links_df_filters_to_current_members():
    raw = _raw(
        [
            {
                "code": "Mary-Murphy.D.2020-02-08",
                "td": "http://www.wikidata.org/entity/Q111111",
                "twitter": "MaryMurphyTD",
                "bluesky": None,
                "facebook": None,
                "instagram": None,
                "website": None,
                "wikiArticle": "https://en.wikipedia.org/wiki/Mary_Murphy",
            },
            {
                "code": "Old-Senator.S.1999-01-01",
                "td": "http://www.wikidata.org/entity/Q999999",
                "twitter": "OldSenator",
                "bluesky": None,
                "facebook": None,
                "instagram": None,
                "website": None,
                "wikiArticle": None,
            },
        ]
    )

    out = build_links_df(raw, {"Mary-Murphy.D.2020-02-08"})
    codes = out["unique_member_code"].to_list()
    assert codes == ["Mary-Murphy.D.2020-02-08"]


def test_build_links_df_extracts_qid_and_derives_urls():
    raw = _raw(
        [
            {
                "code": "Mary-Murphy.D.2020-02-08",
                "td": "http://www.wikidata.org/entity/Q111111",
                "twitter": "MaryMurphyTD",
                "bluesky": "mary.bsky.social",
                "facebook": "marymurphytd",
                "instagram": "marymurphytd",
                "website": "https://marymurphytd.ie",
                "wikiArticle": "https://en.wikipedia.org/wiki/Mary_Murphy",
            }
        ]
    )

    out = build_links_df(raw, {"Mary-Murphy.D.2020-02-08"})
    row = out.row(0, named=True)

    assert row["wikidata_qid"] == "Q111111"
    assert row["wikipedia_url"] == "https://en.wikipedia.org/wiki/Mary_Murphy"
    assert row["twitter_url"] == "https://x.com/MaryMurphyTD"
    assert row["bluesky_url"] == "https://bsky.app/profile/mary.bsky.social"
    assert row["facebook_url"] == "https://www.facebook.com/marymurphytd"
    assert row["instagram_url"] == "https://www.instagram.com/marymurphytd/"
    assert row["website_url"] == "https://marymurphytd.ie"


def test_build_links_df_collapses_duplicates_deterministically():
    """A Wikidata person with two values for one property (e.g. personal and
    ministerial Twitter accounts) yields multiple SPARQL rows. The builder
    must pick exactly one per (member, property), the same one on every run.
    """
    raw = _raw(
        [
            {
                "code": "Mary-Murphy.D.2020-02-08",
                "td": "http://www.wikidata.org/entity/Q111111",
                "twitter": "MaryMurphyMinister",
                "bluesky": None,
                "facebook": None,
                "instagram": None,
                "website": None,
                "wikiArticle": None,
            },
            {
                "code": "Mary-Murphy.D.2020-02-08",
                "td": "http://www.wikidata.org/entity/Q111111",
                "twitter": "MaryMurphyTD",
                "bluesky": None,
                "facebook": None,
                "instagram": None,
                "website": None,
                "wikiArticle": None,
            },
        ]
    )

    out_a = build_links_df(raw, {"Mary-Murphy.D.2020-02-08"})
    out_b = build_links_df(raw, {"Mary-Murphy.D.2020-02-08"})
    assert out_a.row(0) == out_b.row(0)
    # One row in, one row out — deduped.
    assert out_a.height == 1


def test_build_links_df_empty_raw_returns_typed_empty_frame():
    """First-ever run before Wikidata returns any matches must still produce
    a shape-stable parquet so the SQL view's column list doesn't drift.
    """
    raw = _raw([])
    out = build_links_df(raw, set())
    assert out.height == 0
    assert "unique_member_code" in out.columns
    assert "twitter_url" in out.columns
