"""Rewrite Google-News redirect URLs in news_mentions.parquet to real publisher URLs.

The news-mentions extractor stores the Google-News RSS ``<link>``, which is an opaque
``news.google.com/rss/articles/<id>`` redirect that lands on a consent wall instead of
the article (see extractors/_gnews_resolve.py for the why). This step resolves those
redirects to the real publisher URL IN PLACE, so the "Recent media mentions" card and
the "In the News" feed link straight to the article.

Idempotent: rows whose ``article_url`` is already a real (non-Google) URL are skipped,
so steady-state runs only resolve the day's newly-fetched redirects (cheap), while the
first run backfills the whole accumulated corpus (~13 min / a few thousand rows). The
``article_id`` key is left untouched (it is the dedup spine), so re-resolving never
fans out rows. Resolution failures keep the original redirect (no row is lost).

Run:
  ./.venv/Scripts/python.exe extractors/news_url_resolve.py                # resolve all pending
  ./.venv/Scripts/python.exe extractors/news_url_resolve.py --max 50       # smoke (cap)
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from extractors._gnews_resolve import is_gn_redirect, make_session, resolve_many  # noqa: E402
from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

log = logging.getLogger(__name__)
OUT = ROOT / "data/silver/parquet/news_mentions.parquet"


def run(max_resolve: int, delay: float) -> int:
    if not OUT.exists():
        log.warning("no news_mentions parquet at %s — nothing to resolve (run the extractor first)", OUT)
        return 0
    df = pl.read_parquet(OUT)
    pending = [u for u in df["article_url"].to_list() if is_gn_redirect(u)]
    if not pending:
        log.info("no Google-News redirect URLs pending — all %d rows already point at publishers", df.height)
        return 0
    log.info("resolving %d Google-News redirect URLs (%d total rows)%s",
             len(pending), df.height, f", capped at {max_resolve}" if max_resolve else "")

    mapping = resolve_many(pending, make_session(), delay=delay, max_resolve=max_resolve)
    if not mapping:
        log.warning("resolved 0 URLs — Google format/consent may have changed (extractors/_gnews_resolve.py)")
        return 0

    # Replace only the URLs we resolved; everything else (real URLs, failures) is unchanged.
    df = df.with_columns(
        pl.col("article_url").replace(mapping).alias("article_url")
    )
    still_pending = sum(1 for u in df["article_url"].to_list() if is_gn_redirect(u))
    save_parquet(df, OUT)
    log.info("rewrote %d URLs -> real publishers (%d still pending) -> %s", len(mapping), still_pending, OUT)
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Resolve Google-News redirect URLs in news_mentions.parquet.")
    ap.add_argument("--max", type=int, default=0, help="cap URLs resolved this pass (0 = all pending)")
    ap.add_argument("--delay", type=float, default=0.0, help="seconds between resolutions (politeness)")
    args = ap.parse_args()
    setup_standalone_logging("news_url_resolve")
    return run(args.max, args.delay)


if __name__ == "__main__":
    raise SystemExit(main())
