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
sys.path.insert(0, str(Path(__file__).resolve().parent))  # sibling extractor import
from sipo_candidate_expenses_extract import canon_party_expr  # noqa: E402

from services.parquet_io import save_parquet  # noqa: E402
from shared.normalise_join_key import normalise_df_td_name  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

SILVER = ROOT / "data/silver/sipo_candidate"
GOLD = ROOT / "data/gold/parquet"
HEAD_SRC = SILVER / "sipo_candidate_expenses.parquet"
ITEMS_SRC = SILVER / "sipo_candidate_expense_items.parquet"
MEMBER_SRC = ROOT / "data/silver/parquet/flattened_members.parquet"  # sitting 34th-Dáil TDs

# The line-item 'detail' field is free text on a public statutory return — public, not
# PII (unlike donor home addresses on the donations track). No address columns exist on
# the expense items, but we guard the same way the donations promoter does, defensively.


def _no_address(df: pl.DataFrame) -> pl.DataFrame:
    leaked = [c for c in df.columns if "address" in c.lower()]
    if leaked:
        raise RuntimeError(f"PII leak: address column(s) {leaked} must not reach gold")
    return df


def roster_join(head: pl.DataFrame, members: pl.DataFrame) -> pl.DataFrame:
    """PURE join of candidates -> sitting TDs by the sorted-letters name key (the project's
    standard cross-source name join — order/accent/comma agnostic, so 'Codd, Jim' ==
    'Jim Codd'). ``members`` must have columns full_name, unique_member_code, member_party.
    Adds:
      * unique_member_code — the SQL-view-facing canonical member ID, for cross-linking
        campaign spend -> votes / interests / member-overview (NULL if not a sitting TD).
      * is_elected_td — matched to a current TD.
      * party (overwritten) — AUTHORITATIVE: the registry party (canonicalised) wins for
        elected members; non-elected keep their OCR-declared canonical party. This is what
        collapses most of the OCR '(unknown)' party bucket for the candidates who won.
    Members are deduped to one row per code AND per join_key so the join is 1:1 (no row
    explosion). No-inference: an unmatched candidate keeps its declared party, never a guess.
    """
    mk = (
        normalise_df_td_name(members.unique(subset=["unique_member_code"]), "full_name")
        .unique(subset=["join_key"])
        .select("join_key", "unique_member_code", "member_party")
    )
    return (
        normalise_df_td_name(head, "candidate_name")
        .join(mk, on="join_key", how="left")
        .drop("join_key")
        .with_columns(pl.col("unique_member_code").is_not_null().alias("is_elected_td"))
        .with_columns(
            pl.coalesce([
                canon_party_expr("member_party"),  # registry party, canonicalised
                pl.col("member_party"),            # registry party not covered by rules -> raw
                pl.col("party"),                   # not elected -> OCR-declared canonical
            ]).alias("party")
        )
        .drop("member_party")
    )


def roster_enrich(head: pl.DataFrame) -> pl.DataFrame:
    """I/O wrapper around roster_join: read the sitting-TD registry and apply the join.
    Falls back to NULL member code if the registry parquet is absent (CI-safe)."""
    if not MEMBER_SRC.exists():
        return head.with_columns(
            pl.lit(None, dtype=pl.Utf8).alias("unique_member_code"),
            pl.lit(False).alias("is_elected_td"),  # noqa: FBT003
        )
    members = pl.read_parquet(MEMBER_SRC).select(
        "unique_member_code", "full_name", pl.col("party").alias("member_party")
    )
    return roster_join(head, members)


def build() -> None:
    head = roster_enrich(pl.read_parquet(HEAD_SRC))
    items = pl.scan_parquet(ITEMS_SRC)

    # --- GOLD candidate fact: keep the plausible, with-a-total rows for serving; the
    # suspect/no-total rows stay in silver for audit. ---
    fact = (
        head.lazy()
        .filter(pl.col("total_spend_eur").is_not_null() & ~pl.col("total_suspect"))
        .with_columns(pl.lit("GE2024").alias("election_event"))
        .collect()
    )

    # --- GOLD clean line items: drop the decimal-loss residue, and replace the OCR party
    # with the roster-enriched party + member code (joined per candidate on media_id). ---
    member_map = head.select("media_id", "unique_member_code", "is_elected_td", "party")
    items_clean = (
        items.filter(~pl.col("cost_suspect")).collect()
        .drop("party", "party_declared")
        .join(member_map, on="media_id", how="left")
    )

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
    elected = fact.filter(pl.col("is_elected_td")).height
    print(f"  candidate fact      : {fact.height} rows, Σ €{fact['total_spend_eur'].sum():,.2f}")
    print(f"    {elected} linked to a sitting TD (unique_member_code), {fact.filter(pl.col('party').is_null()).height} still unknown-party")
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
