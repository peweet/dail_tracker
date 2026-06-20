"""Promote the VETTED ministerial-diary sandbox tables to the gold layer.

The diary chain (extract -> classify -> match -> overlap) builds three sandbox
tables in data/sandbox/enrichment/. This step is the sandbox->gold promotion
([[feedback_pipeline_changes_data_anchored_promotion]]): it reads those vetted
tables, applies the two promotion-time data-quality fixes the outlier audit found,
and writes the gold parquet the SQL views (and therefore the page) read.

Promotion-time fixes (NOT done in the sandbox tables — they keep raw provenance):
  1. MINISTER DE-FRAGMENTATION. The sandbox `minister` is a filename guess, so the
     same person appears as "Ryans"/"Ryan", "Martins"/"Martin". `minister_display`
     strips the trailing possessive 's' and title-cases, merging the duplicates
     (same rule the overlap's surname_key uses for the lobbying join).
  2. STATE-vs-OUTSIDE-INTEREST split. `is_state_body` is carried on the overlap so
     the page can separate government-agency access (IDA/HSE — expected, 0 returns)
     from outside-interest access (the real lobbying-overlap signal).

Gold outputs -> data/gold/parquet/
  ministerial_diary_engagements.parquet    one row per parsed engagement (+ minister_display)
  ministerial_diary_org_mentions.parquet   one row per (engagement x matched org)
  ministerial_diary_org_overlap.parquet    org-level ranking (+ is_state_body)

HONESTY/PRIVACY posture (surfaced in the view + page provenance, not silently):
  diaries are self-curated, non-exhaustive, published QUARTERLY-IN-ARREARS; a diary
  meeting is co-occurrence, NOT a lobbying return ([[feedback_no_inference_in_app]]).
  Primary views are ORG-AGGREGATE (no person data); the raw-subject drill-down is
  as-published public record. (Person-name review of subjects = a pre-launch gate.)

Run (after the diary chain): .venv/Scripts/python.exe extractors/diary_promote_gold.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

log = logging.getLogger(__name__)

ENR = Path("data/sandbox/enrichment")
GOLD = Path("data/gold/parquet")
STATE_SECTOR = "state-semi-state"


def minister_display(name: str | None) -> str | None:
    """Canonical display name from the filename-guess surname token.

    Strips a trailing possessive 's' (len>4 so "Ross"/"Burke" are untouched) and
    title-cases, so "Ryans"->"Ryan", "Martins"->"Martin" collapse to one person."""
    if not name:
        return None
    n = name.strip()
    if len(n) > 4 and n.lower().endswith("s") and not n.lower().endswith("ss"):
        n = n[:-1]
    return n[:1].upper() + n[1:]


def _read(name: str) -> pl.DataFrame:
    p = ENR / name
    if not p.exists():
        raise SystemExit(f"promote: missing sandbox input {p} — run the diary chain first.")
    return pl.read_parquet(p)


def main() -> int:
    setup_standalone_logging("diary_promote_gold")
    GOLD.mkdir(parents=True, exist_ok=True)

    engagements = _read("ministerial_diary_entries.parquet").with_columns(
        pl.col("minister").map_elements(minister_display, return_dtype=pl.String).alias("minister_display")
    )
    mentions = _read("diary_org_mentions.parquet").with_columns(
        pl.col("minister").map_elements(minister_display, return_dtype=pl.String).alias("minister_display")
    )
    overlap = _read("diary_lobbying_overlap_ranked.parquet").with_columns(
        (pl.col("sector") == STATE_SECTOR).alias("is_state_body")
    )

    # Floors: refuse to atomically replace good gold with a botched (tiny) promotion.
    save_parquet(engagements, GOLD / "ministerial_diary_engagements.parquet", min_rows=1000)
    save_parquet(mentions, GOLD / "ministerial_diary_org_mentions.parquet", min_rows=500)
    save_parquet(overlap, GOLD / "ministerial_diary_org_overlap.parquet", min_rows=100)

    n_min = engagements.filter(pl.col("minister_display").is_not_null())["minister_display"].n_unique()
    log.info(
        "GOLD promoted: %d engagements (%d distinct ministers) | %d mentions | %d overlap orgs (%d state-body)",
        len(engagements),
        n_min,
        len(mentions),
        len(overlap),
        int(overlap["is_state_body"].sum()),
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
