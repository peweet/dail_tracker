"""
pq_table_census.py — EXPERIMENTAL: corpus-wide census of the TABULAR half of
written answers, which pq_answer_mine_experimental.py silently discards.

Status: sandbox prototype, NOT wired into pipeline.py. Reads only the disk
cache (c:/tmp/pq_answer_cache, ~221k section XMLs) — no network. Writes to
isolated sandbox parquets.

Why
---
parse_section() in pq_answer_mine_experimental.py reads only <p> children of
<speech>: every <table> element and every supportingDocumentation attachment
link is dropped. The 2026-06-21 novelty probe that PARKED this whole seam
measured only euro figures in PROSE and concluded the corpus was mostly
redundant with budget books. That verdict never looked at tables. The IDA case
(2026-08-01) disproved it for at least one topic: a 317-row itemised site
register published nowhere else.

This census answers the prior question for every topic at once: which sections
disclose a table, how big, under what header, from which department. The table
HEADER ROW is the payload — a corpus of headers is a compact searchable index
of what ministers actually disclose in structured form.

Emits one row per TABLE (not per cell) plus one row per attachment, so the
output stays small enough to analyse directly.

Sharded + resumable: re-run until it reports all shards present, then it
concatenates. Shards let a killed run resume for free.

Run:
    python -m pipeline_sandbox.pq_disclosures.pq_table_census
"""

from __future__ import annotations

import services.runtime_env  # noqa: F401  # MUST be first: caps BLAS threads

import logging
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import polars as pl

from pipeline_sandbox.pq_disclosures.pq_answer_mine_experimental import (
    _CACHE_DIR,
    _strip_ns,
    _text,
)
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

logger = logging.getLogger(__name__)

_FULL_CORPUS = Path("data/_sandbox/pq_disclosures_full.parquet")
_SHARD_DIR = Path("data/_sandbox/_pq_table_census_shards")
_OUT_TABLES = Path("data/_sandbox/pq_table_census.parquet")
_OUT_ATTACH = Path("data/_sandbox/pq_attachment_census.parquet")
_BATCH = 20_000

# Cheap string pre-filter: parsing every section costs ~30 min, but only a
# minority contain a table. Substring test first, ElementTree only on hits.
_TABLE_MARK = "<table>"
_ATTACH_MARK = "supportingDocumentation"


def _sections() -> list[dict]:
    """Distinct written-answer sections, with their topic/department meta."""
    return (
        pl.scan_parquet(_FULL_CORPUS)
        .select(["xml_uri", "date", "department", "section_title"])
        .unique(subset=["xml_uri"])
        .sort("xml_uri")
        .collect()
        .to_dicts()
    )


def _cache_path(xml_uri: str) -> Path:
    return _CACHE_DIR / xml_uri.split("/debateRecord/", 1)[-1].replace("/", "_")


def _grid(table_el: ET.Element) -> list[list[str]]:
    rows = []
    for tr in table_el:
        if _strip_ns(tr.tag) != "tr":
            continue
        rows.append([_text(td) for td in tr if _strip_ns(td.tag) in ("td", "th")])
    return rows


def census_section(xml: str, meta: dict) -> tuple[list[dict], list[dict]]:
    """One row per table + one row per attachment link in this section."""
    tables: list[dict] = []
    attachments: list[dict] = []

    has_table = _TABLE_MARK in xml
    has_attach = _ATTACH_MARK in xml
    if not has_table and not has_attach:
        return tables, attachments

    try:
        root = ET.fromstring(xml)
    except ET.ParseError:
        return tables, attachments

    if has_table:
        for idx, el in enumerate(e for e in root.iter() if _strip_ns(e.tag) == "table"):
            grid = _grid(el)
            if not grid:
                continue
            header = grid[0]
            body = grid[1:]
            # Drop a trailing totals/footer row (blank first cell) from the
            # count — see pq_ida_land_tables.py, where it faked a 26th county.
            body = [r for r in body if r and r[0].strip()]
            tables.append(
                {
                    "xml_uri": meta["xml_uri"],
                    "date": meta["date"],
                    "department": meta.get("department"),
                    "section_title": meta.get("section_title"),
                    "table_index": idx,
                    "n_rows": len(body),
                    "n_cols": len(header),
                    "header": " | ".join(h.strip() for h in header)[:500],
                    "first_col_sample": " ; ".join(r[0].strip() for r in body[:3])[:300],
                }
            )

    if has_attach:
        for a in root.iter():
            if _strip_ns(a.tag) != "a":
                continue
            href = a.get("href") or ""
            if _ATTACH_MARK not in href:
                continue
            attachments.append(
                {
                    "xml_uri": meta["xml_uri"],
                    "date": meta["date"],
                    "department": meta.get("department"),
                    "section_title": meta.get("section_title"),
                    "attachment_url": href,
                    "ext": href.rsplit(".", 1)[-1].lower()[:10] if "." in href else "",
                    "link_text": _text(a)[:200],
                }
            )
    return tables, attachments


def main() -> int:
    setup_standalone_logging("pq_table_census")
    _SHARD_DIR.mkdir(parents=True, exist_ok=True)

    sections = _sections()
    n = len(sections)
    n_shards = (n + _BATCH - 1) // _BATCH
    logger.info("sections=%d batch=%d -> %d shards", n, _BATCH, n_shards)

    for i in range(n_shards):
        t_shard = _SHARD_DIR / f"tables_{i:04d}.parquet"
        a_shard = _SHARD_DIR / f"attach_{i:04d}.parquet"
        if t_shard.exists() and a_shard.exists():
            continue
        chunk = sections[i * _BATCH : (i + 1) * _BATCH]
        tables: list[dict] = []
        attachments: list[dict] = []
        missing = 0
        for sec in chunk:
            p = _cache_path(sec["xml_uri"])
            if not p.exists():
                missing += 1
                continue
            try:
                t, a = census_section(p.read_text(encoding="utf-8"), sec)
            except Exception as e:
                logger.warning("census failed %s: %s", sec["xml_uri"], e)
                continue
            tables.extend(t)
            attachments.extend(a)
        # Empty shards still get written, so the resume check stays truthful.
        save_parquet(pl.DataFrame(tables, schema=_TABLE_SCHEMA), t_shard)
        save_parquet(pl.DataFrame(attachments, schema=_ATTACH_SCHEMA), a_shard)
        logger.info(
            "shard %d/%d: %d tables, %d attachments (%d cache-missing)",
            i + 1, n_shards, len(tables), len(attachments), missing,
        )

    t_parts = sorted(_SHARD_DIR.glob("tables_*.parquet"))
    if len(t_parts) < n_shards:
        logger.info("RESUME NEEDED: %d/%d shards — re-run", len(t_parts), n_shards)
        return 0

    tables_df = pl.concat([pl.read_parquet(p) for p in t_parts])
    attach_df = pl.concat([pl.read_parquet(p) for p in sorted(_SHARD_DIR.glob("attach_*.parquet"))])
    save_parquet(tables_df, _OUT_TABLES)
    save_parquet(attach_df, _OUT_ATTACH)

    logger.info("=" * 60)
    logger.info("sections scanned      : %d", n)
    logger.info("tables found          : %d", tables_df.height)
    logger.info("sections with a table : %d", tables_df["xml_uri"].n_unique())
    logger.info("attachments found     : %d", attach_df.height)
    logger.info("wrote %s / %s", _OUT_TABLES, _OUT_ATTACH)
    return 0


_TABLE_SCHEMA = {
    "xml_uri": pl.Utf8, "date": pl.Utf8, "department": pl.Utf8,
    "section_title": pl.Utf8, "table_index": pl.Int64, "n_rows": pl.Int64,
    "n_cols": pl.Int64, "header": pl.Utf8, "first_col_sample": pl.Utf8,
}
_ATTACH_SCHEMA = {
    "xml_uri": pl.Utf8, "date": pl.Utf8, "department": pl.Utf8,
    "section_title": pl.Utf8, "attachment_url": pl.Utf8, "ext": pl.Utf8,
    "link_text": pl.Utf8,
}


if __name__ == "__main__":
    sys.exit(main())
