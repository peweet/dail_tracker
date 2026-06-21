"""
pq_parse_cached.py — EXPERIMENTAL: parse the ALREADY-CACHED written-answer XML
into the full disclosure parquet, resumably.

The full corpus (~221k sections) parses at ~120/sec, i.e. ~30 min — longer than a
single background window, so a one-shot run gets killed mid-pass. This splits the
work into index-based shards (default 25k each) and skips any shard already written,
so it survives interruption and resumes for free. Re-run until it reports "all N
shards present", then it concatenates them into the final parquet.

Reads only the disk cache (c:/tmp/pq_answer_cache) populated by
pq_answer_mine_experimental.py — no network. Meta (date/department/section_title)
is rebuilt from the bronze questions JSON.

Run (repeat until done):
    python -m pipeline_sandbox.pq_disclosures.pq_parse_cached
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from pipeline_sandbox.pq_disclosures.pq_answer_mine_experimental import (
    _CACHE_DIR,
    _section_uris_from_bronze,
    parse_section,
)
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

_SHARD_DIR = Path("data/_sandbox/_pq_shards")
_OUT = Path("data/_sandbox/pq_disclosures_full.parquet")
_BATCH = 10_000


def _key(uri: str) -> str:
    return uri.split("/debateRecord/", 1)[-1].replace("/", "_")


def main() -> int:
    setup_standalone_logging("pq_parse_cached")
    _SHARD_DIR.mkdir(parents=True, exist_ok=True)

    sections = _section_uris_from_bronze(None)
    n = len(sections)
    n_shards = (n + _BATCH - 1) // _BATCH
    logger.info("sections=%d, batch=%d -> %d shards", n, _BATCH, n_shards)

    for i in range(n_shards):
        shard = _SHARD_DIR / f"part_{i:04d}.parquet"
        if shard.exists():
            continue
        chunk = sections[i * _BATCH : (i + 1) * _BATCH]
        rows: list[dict] = []
        for sec in chunk:
            cache = _CACHE_DIR / _key(sec["xml_uri"])
            if not cache.exists():
                continue
            try:
                rows.extend(parse_section(cache.read_text(encoding="utf-8"), sec))
            except Exception as e:
                logger.warning("parse failed %s: %s", sec["xml_uri"], e)
        df = pd.DataFrame(rows)
        save_parquet(df, shard)
        logger.info("shard %d/%d written (%d rows) <- this run is making progress",
                    i + 1, n_shards, len(df))

    present = sorted(_SHARD_DIR.glob("part_*.parquet"))
    if len(present) < n_shards:
        logger.info("RESUME NEEDED: %d/%d shards done — re-run to continue",
                    len(present), n_shards)
        return 0

    logger.info("all %d shards present — concatenating", n_shards)
    full = pd.concat([pd.read_parquet(p) for p in present], ignore_index=True)
    full = full.drop_duplicates(subset=["question_ref"])
    save_parquet(full, _OUT)
    logger.info("=" * 60)
    logger.info("wrote %s (%d distinct-ref rows)", _OUT, len(full))
    return 0


if __name__ == "__main__":
    sys.exit(main())
