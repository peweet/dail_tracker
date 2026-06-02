#!/usr/bin/env python3
"""
wikidata_socials_etl.py

Builds a per-member social-media + external-profile lookup, sourced from
Wikidata, that the member-overview hero renders as chips ("Wikipedia",
"Twitter", "Bluesky", etc.).

SOURCE: Wikidata SPARQL.

The Oireachtas API exposes no social-media or Wikipedia fields. Wikidata,
however, carries P4690 "Oireachtas member ID" whose value is exactly our
`unique_member_code` (the same `memberCode` slug used by oireachtas.ie/en/
members/member/<slug>/). Wikidata's URL formatter for P4690 confirms this
mapping verbatim. So one SPARQL query gives us:

    P4690     Oireachtas member ID    → join key, no fuzzy match needed
    P2002     Twitter username
    P12361    Bluesky handle
    P2013     Facebook ID
    P2003     Instagram username
    P856      official website
    schema:about / isPartOf <en.wikipedia.org/>   → EN Wikipedia article URL

OUTPUT: data/silver/parquet/member_external_links.parquet — one row per
member, every social field nullable. Columns:

    unique_member_code,
    wikidata_qid,
    wikipedia_url,
    twitter_handle,   twitter_url,
    bluesky_handle,   bluesky_url,
    facebook_id,      facebook_url,
    instagram_handle, instagram_url,
    website_url

We store both the raw handle and the derived URL so:
- the handle is the canonical (Wikidata-sourced) value, replayable into
  any future URL pattern if a platform domain changes;
- the URL is what the UI consumes, computed once at ETL time instead of
  scattering URL-construction code across pages.

Coverage as of last probe (Dáil 34): ~95% of sitting TDs have a Wikidata
entry; ~56% have Twitter; Wikipedia coverage matches the Wikidata-entry
coverage. Seanad members are mostly absent from Wikidata — that's fine,
the UI just shows fewer chips.

PROVENANCE: the raw SPARQL CSV is cached to
data/bronze/wikidata/member_external_links_raw.csv on every run.

Usage:
    python wikidata_socials_etl.py
"""

from __future__ import annotations

import io
import logging
import time

import polars as pl
import requests

from config import BRONZE_DIR, SILVER_PARQUET_DIR

_MEMBERS_PARQUET = SILVER_PARQUET_DIR / "flattened_members.parquet"
_OUT = SILVER_PARQUET_DIR / "member_external_links.parquet"
_RAW_OUT = BRONZE_DIR / "wikidata" / "member_external_links_raw.csv"

_SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"
_USER_AGENT = "dail-tracker/1.0 (civic accountability data project)"

# Every Wikidata item with an Oireachtas member ID (P4690), with every
# external-profile property optionally attached. We over-fetch (all D./S.
# slugs ever) and intersect with current members locally — one trip to
# WDQS, no VALUES list to bust URL length limits.
_QUERY = """
SELECT ?code ?td ?twitter ?bluesky ?facebook ?instagram ?website ?wikiArticle WHERE {
  ?td wdt:P4690 ?code .
  OPTIONAL { ?td wdt:P2002  ?twitter. }
  OPTIONAL { ?td wdt:P12361 ?bluesky. }
  OPTIONAL { ?td wdt:P2013  ?facebook. }
  OPTIONAL { ?td wdt:P2003  ?instagram. }
  OPTIONAL { ?td wdt:P856   ?website. }
  OPTIONAL { ?wikiArticle schema:about ?td;
                          schema:isPartOf <https://en.wikipedia.org/>. }
}
"""

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# URL derivation — pure functions, unit-testable, no network
# ---------------------------------------------------------------------------


def twitter_url(handle: str | None) -> str | None:
    """Wikidata P2002 stores the bare username (no @, no URL). Build the
    canonical x.com URL — the redirect from twitter.com still works but
    x.com is the current primary host.
    """
    h = (handle or "").strip().lstrip("@")
    return f"https://x.com/{h}" if h else None


def bluesky_url(handle: str | None) -> str | None:
    """P12361 stores either a handle (`name.bsky.social`) or, occasionally,
    a custom domain. Either way, the profile URL pattern is the same.
    """
    h = (handle or "").strip().lstrip("@")
    return f"https://bsky.app/profile/{h}" if h else None


def facebook_url(fb_id: str | None) -> str | None:
    """P2013 stores Facebook's vanity username or numeric ID — both resolve
    on `facebook.com/<id>/`.
    """
    f = (fb_id or "").strip()
    return f"https://www.facebook.com/{f}" if f else None


def instagram_url(handle: str | None) -> str | None:
    """P2003 stores the Instagram username (no @)."""
    h = (handle or "").strip().lstrip("@")
    return f"https://www.instagram.com/{h}/" if h else None


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


def fetch_wikidata(attempts: int = 4) -> pl.DataFrame:
    """Run the SPARQL query; cache the raw CSV to bronze for provenance.

    WDQS sometimes drops connections or rate-limits aggressively (active
    outage windows throttle to 1 req / min). Backoff is intentionally long.
    """
    last_err: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = requests.post(
                _SPARQL_ENDPOINT,
                data={"query": _QUERY},
                headers={"User-Agent": _USER_AGENT, "Accept": "text/csv"},
                timeout=120,
            )
            resp.raise_for_status()
            _RAW_OUT.parent.mkdir(parents=True, exist_ok=True)
            _RAW_OUT.write_text(resp.text, encoding="utf-8")
            return pl.read_csv(io.StringIO(resp.text))
        except Exception as exc:  # noqa: BLE001 — retry every transient cause
            last_err = exc
            wait = 65 * attempt  # WDQS throttle is 1 req/min when active
            logger.warning(
                "Wikidata fetch attempt %d/%d failed: %s — sleeping %ds",
                attempt,
                attempts,
                exc,
                wait,
            )
            if attempt < attempts:
                time.sleep(wait)
    raise SystemExit(f"Wikidata SPARQL fetch failed after {attempts} attempts: {last_err}")


# ---------------------------------------------------------------------------
# Shape
# ---------------------------------------------------------------------------


def current_member_codes() -> set[str]:
    """`unique_member_code` values from the silver members parquet — the set
    we filter Wikidata results to. Missing parquet = empty set; the ETL still
    writes an empty output so downstream views don't break, but it logs loud.
    """
    if not _MEMBERS_PARQUET.exists():
        logger.warning(
            "flattened_members.parquet not found — output will be empty: %s",
            _MEMBERS_PARQUET,
        )
        return set()
    df = pl.read_parquet(_MEMBERS_PARQUET, columns=["unique_member_code"])
    return set(df["unique_member_code"].drop_nulls().unique().to_list())


def build_links_df(raw: pl.DataFrame, codes_now: set[str]) -> pl.DataFrame:
    """Filter raw SPARQL results to current members, dedupe, and derive URLs.

    Some Wikidata items have multiple values for the same property (rare —
    happens for ministers who keep both personal and ministerial Twitter
    accounts). We keep the first value per (member, property) by sorting
    on the URI lexicographically — deterministic and replayable.
    """
    if raw.height == 0:
        return _empty_links_df()

    # The Wikidata SELECT CSV uses the variable names as column headers.
    # Promote them to our schema and trim to the join set.
    raw = raw.rename({"td": "wikidata_qid_uri", "wikiArticle": "wikipedia_url"})

    raw = raw.filter(pl.col("code").is_in(list(codes_now)))

    # Q-id is the trailing path segment of the Wikidata URI.
    raw = raw.with_columns(
        pl.col("wikidata_qid_uri").str.extract(r"(Q\d+)$").alias("wikidata_qid"),
    )

    # Deterministic dedupe: one row per (code, property). Sort by URI before
    # group_by so .first() is stable across runs.
    raw = raw.sort(["code", "wikidata_qid_uri"])
    out = raw.group_by("code", maintain_order=True).agg(
        pl.col("wikidata_qid").first(),
        pl.col("wikipedia_url").drop_nulls().first(),
        pl.col("twitter").drop_nulls().first(),
        pl.col("bluesky").drop_nulls().first(),
        pl.col("facebook").drop_nulls().first(),
        pl.col("instagram").drop_nulls().first(),
        pl.col("website").drop_nulls().first(),
    )

    out = out.rename(
        {
            "code": "unique_member_code",
            "twitter": "twitter_handle",
            "bluesky": "bluesky_handle",
            "facebook": "facebook_id",
            "instagram": "instagram_handle",
            "website": "website_url",
        }
    )

    # Derive URLs from handles using the platform-specific builders. Polars
    # map_elements on Utf8 keeps the null-propagation correct (None → None).
    out = out.with_columns(
        pl.col("twitter_handle").map_elements(twitter_url, return_dtype=pl.Utf8).alias("twitter_url"),
        pl.col("bluesky_handle").map_elements(bluesky_url, return_dtype=pl.Utf8).alias("bluesky_url"),
        pl.col("facebook_id").map_elements(facebook_url, return_dtype=pl.Utf8).alias("facebook_url"),
        pl.col("instagram_handle").map_elements(instagram_url, return_dtype=pl.Utf8).alias("instagram_url"),
    )

    # Stable column order — matches the docstring schema so the parquet is
    # readable by `parquet-tools schema` without surprises.
    return out.select(
        "unique_member_code",
        "wikidata_qid",
        "wikipedia_url",
        "twitter_handle",
        "twitter_url",
        "bluesky_handle",
        "bluesky_url",
        "facebook_id",
        "facebook_url",
        "instagram_handle",
        "instagram_url",
        "website_url",
    ).sort("unique_member_code")


def _empty_links_df() -> pl.DataFrame:
    """Shape-stable empty output so downstream views don't break on first run
    before Wikidata returns any matches."""
    return pl.DataFrame(
        schema={
            "unique_member_code": pl.Utf8,
            "wikidata_qid": pl.Utf8,
            "wikipedia_url": pl.Utf8,
            "twitter_handle": pl.Utf8,
            "twitter_url": pl.Utf8,
            "bluesky_handle": pl.Utf8,
            "bluesky_url": pl.Utf8,
            "facebook_id": pl.Utf8,
            "facebook_url": pl.Utf8,
            "instagram_handle": pl.Utf8,
            "instagram_url": pl.Utf8,
            "website_url": pl.Utf8,
        }
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run() -> dict:
    raw = fetch_wikidata()
    codes_now = current_member_codes()
    logger.info(
        "Wikidata returned %d raw rows; current member set has %d codes",
        raw.height,
        len(codes_now),
    )

    out = build_links_df(raw, codes_now)

    _OUT.parent.mkdir(parents=True, exist_ok=True)
    out.write_parquet(_OUT, compression="zstd", compression_level=3, statistics=True)

    # Coverage breakdown — useful in logs to see if a platform's coverage
    # degrades silently after a refresh.
    def _filled(col: str) -> int:
        return int(out[col].is_not_null().sum()) if out.height else 0

    n = out.height
    logger.info("member_external_links: wrote %s (%d rows)", _OUT, n)
    logger.info("  with Wikipedia:  %d", _filled("wikipedia_url"))
    logger.info("  with Twitter:    %d", _filled("twitter_url"))
    logger.info("  with Bluesky:    %d", _filled("bluesky_url"))
    logger.info("  with Facebook:   %d", _filled("facebook_url"))
    logger.info("  with Instagram:  %d", _filled("instagram_url"))
    logger.info("  with Website:    %d", _filled("website_url"))

    return {
        "rows": n,
        "wikipedia": _filled("wikipedia_url"),
        "twitter": _filled("twitter_url"),
        "bluesky": _filled("bluesky_url"),
        "facebook": _filled("facebook_url"),
        "instagram": _filled("instagram_url"),
        "website": _filled("website_url"),
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run()
