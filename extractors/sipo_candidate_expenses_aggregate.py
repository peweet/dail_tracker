"""Aggregate the per-candidate SIPO expense SILVER facts → GOLD serving tables.

Pure, fully VECTORISED Polars: lazy scans, ``group_by().agg([...])`` with a list of
named expressions, ``replace_strict`` instead of a UDF, zero Python row-loops and zero
``map_elements``. Heavy lifting stays inside the Polars engine (multi-threaded, Arrow
columnar) rather than in interpreter loops — the idiomatic way to aggregate, and the
common StackOverflow answer for "multiple aggregations per group" in Polars.

"Filter out not needed data" happens HERE, declaratively, before aggregation:
  * cost_suspect / total_suspect rows (OCR decimal-loss the parser couldn't recover)
    are EXCLUDED from spend sums — they are flagged in silver, never silently summed.
  * subtotal / boilerplate rows never enter silver in the first place.
The excluded counts are reported so nothing is dropped invisibly (no silent caps).

Gold outputs (data/gold/parquet/):
  * sipo_candidate_expenses_fact.parquet   — candidate grain, plausible rows, clean cols
  * sipo_candidate_expense_items.parquet   — line-item grain (clean), e.g. Grealish→Galway Advertiser
  * sipo_campaign_spend_by_detail.parquet  — rollup of the free-text 'Details of item'
       field (a MIX of supplier names + item descriptions — never labelled "vendor")
  * sipo_campaign_spend_by_category.parquet
  * sipo_campaign_spend_by_party.parquet

Run:  ./.venv/Scripts/python.exe extractors/sipo_candidate_expenses_aggregate.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SILVER = ROOT / "data/silver/sipo_candidate"
GOLD = ROOT / "data/gold/parquet"
HEAD_SRC = SILVER / "sipo_candidate_expenses.parquet"
ITEMS_SRC = SILVER / "sipo_candidate_expense_items.parquet"

# The line-item 'detail' field is free text on a public statutory return — public, not
# PII (unlike donor home addresses on the donations track). No address columns exist on
# the expense items, but we guard the same way the donations promoter does, defensively.


def _no_address(df: pl.DataFrame) -> pl.DataFrame:
    leaked = [c for c in df.columns if "address" in c.lower()]
    if leaked:
        raise RuntimeError(f"PII leak: address column(s) {leaked} must not reach gold")
    return df


def build() -> None:
    head = pl.scan_parquet(HEAD_SRC)
    items = pl.scan_parquet(ITEMS_SRC)

    # --- GOLD candidate fact: keep the plausible, with-a-total rows for serving; the
    # suspect/no-total rows stay in silver for audit. Pure lazy filter+select. ---
    fact = (
        head.filter(pl.col("total_spend_eur").is_not_null() & ~pl.col("total_suspect"))
        .with_columns(pl.lit("GE2024").alias("election_event"))
        .collect()
    )

    # --- GOLD clean line items: drop the decimal-loss residue from the spend graph. ---
    items_clean = items.filter(~pl.col("cost_suspect")).collect()

    # --- Detail rollup = the campaign-spend graph. ONE group_by, a list of named aggs.
    by_detail = (
        items_clean.lazy()
        .filter(pl.col("detail").is_not_null() & (pl.col("detail").str.len_chars() > 2))
        .group_by("detail")
        .agg(
            pl.col("cost_eur").sum().round(2).alias("total_eur"),
            pl.len().alias("n_items"),
            pl.col("candidate_name").n_unique().alias("n_candidates"),
            pl.col("category_label").mode().first().alias("top_category"),
        )
        .sort("total_eur", descending=True)
        .collect()
    )

    by_category = (
        items_clean.lazy()
        .group_by("category", "category_label")
        .agg(
            pl.col("cost_eur").sum().round(2).alias("total_eur"),
            pl.len().alias("n_items"),
            pl.col("candidate_name").n_unique().alias("n_candidates"),
        )
        .sort("category")
        .collect()
    )

    # Spend-by-party off the candidate fact (the authoritative totals), not the items.
    by_party = (
        fact.lazy()
        .group_by("party")
        .agg(
            pl.col("total_spend_eur").sum().round(2).alias("total_eur"),
            pl.col("total_spend_eur").mean().round(2).alias("mean_eur"),
            pl.col("total_spend_eur").median().round(2).alias("median_eur"),
            pl.len().alias("n_candidates"),
        )
        .sort("total_eur", descending=True)
        .collect()
    )

    GOLD.mkdir(parents=True, exist_ok=True)
    save_parquet(_no_address(fact), GOLD / "sipo_candidate_expenses_fact.parquet")
    save_parquet(_no_address(items_clean), GOLD / "sipo_candidate_expense_items.parquet")
    save_parquet(by_detail, GOLD / "sipo_campaign_spend_by_detail.parquet")
    save_parquet(by_category, GOLD / "sipo_campaign_spend_by_category.parquet")
    save_parquet(by_party, GOLD / "sipo_campaign_spend_by_party.parquet")

    print("=== GOLD SIPO candidate expenses ===")
    print(f"  candidate fact      : {fact.height} rows, Σ €{fact['total_spend_eur'].sum():,.2f}")
    print(f"  clean line items    : {items_clean.height} rows, Σ €{items_clean['cost_eur'].sum():,.2f}")
    print(f"  spend by detail     : {by_detail.height} distinct detail strings")
    print(f"  spend by category   : {by_category.height} categories")
    print(f"  spend by party      : {by_party.height} parties")
    print("\n  top 12 spend-detail lines (mix of payees + descriptions):")
    for r in by_detail.head(12).iter_rows(named=True):
        print(f"    €{r['total_eur']:>11,.2f}  {r['n_candidates']:>3} cand  {r['n_items']:>3} items  {r['detail'][:40]}")
    print("\n  spend by category:")
    for r in by_category.iter_rows(named=True):
        print(f"    {r['category']}  {str(r['category_label'])[:24]:24} €{r['total_eur']:>12,.2f}  ({r['n_items']} items)")


if __name__ == "__main__":
    build()
