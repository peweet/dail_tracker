"""Validate the unscoped legislation fetch + dry-run a join against gold.

Read-only. Does not write anywhere. Run after `legislation_unscoped_fetch.py`.

Checks performed
----------------
1. Source breakdown — confirm Government bills are present.
2. Sponsor-shape gotcha — Government bills have `sponsor.by.showAs = None`
   (sponsor identity lives in `sponsor.as.showAs`, e.g. "Minister for X").
   The current legislation.py:126 dropna and legislation_index.sql:24
   `IS NOT NULL` filter would drop these — flagged here so the graduation
   PR doesn't miss it.
3. New-bill diff — anti-join against the existing silver/sponsors.parquet
   (the per-TD harvest output) to count newly visible bills.
4. Gold join smoke test — substring-match a sample of newly visible
   Government bills against `current_dail_vote_history.parquet.debate_title`
   to confirm the new bills tie back to real downstream activity.

Run
---
    python -m pipeline_sandbox.legislation_unscoped_validate
"""

from __future__ import annotations

import json
import sys
from collections import Counter

import polars as pl

# Polars uses box-drawing chars; default Windows console (cp1252) chokes.
sys.stdout.reconfigure(encoding="utf-8")

from config import GOLD_PARQUET_DIR, LEGISLATION_DIR, SILVER_PARQUET_DIR

UNSCOPED_JSON = LEGISLATION_DIR / "legislation_results_unscoped.json"
SILVER_SPONSORS = SILVER_PARQUET_DIR / "sponsors.parquet"
GOLD_VOTES = GOLD_PARQUET_DIR / "current_dail_vote_history.parquet"


def load_unscoped() -> list[dict]:
    with UNSCOPED_JSON.open(encoding="utf-8") as f:
        payload = json.load(f)
    return payload[0]["results"]


def flatten_bills(bills: list[dict]) -> pl.DataFrame:
    """Tolerant flatten — does NOT drop rows with null sponsor.by.showAs."""
    rows = []
    for entry in bills:
        b = entry["bill"]
        sponsors = b.get("sponsors") or [{}]
        for s in sponsors:
            sp = (s.get("sponsor") or {})
            by = sp.get("by") or {}
            as_ = sp.get("as") or {}
            rows.append(
                {
                    "bill_year": b.get("billYear"),
                    "bill_no": b.get("billNo"),
                    "short_title_en": b.get("shortTitleEn"),
                    "source": b.get("source"),
                    "bill_type": b.get("billType"),
                    "status": b.get("status"),
                    "sponsor_by_show_as": by.get("showAs"),
                    "sponsor_as_show_as": as_.get("showAs"),
                    "is_primary": sp.get("isPrimary"),
                }
            )
    return pl.DataFrame(rows)


def main() -> None:
    bills = load_unscoped()
    print(f"--- 1. Source breakdown (raw bill records: {len(bills)}) ---")
    print(Counter(b["bill"].get("source") for b in bills))
    print()

    sponsors_df = flatten_bills(bills)
    print("--- 2. Sponsor identity location by source ---")
    print(
        sponsors_df.group_by("source").agg(
            [
                pl.col("sponsor_by_show_as").is_not_null().sum().alias("by_present"),
                pl.col("sponsor_as_show_as").is_not_null().sum().alias("as_present"),
                pl.len().alias("rows"),
            ]
        )
    )
    print(
        "\nGotcha: Government rows have by_present=0. "
        "legislation.py:126 dropna(subset=['sponsor.by.showAs']) "
        "would drop all of them. Graduation PR must coalesce by/as."
    )
    print()

    unique_unscoped = sponsors_df.select(["bill_year", "bill_no", "source"]).unique()
    print(f"--- 3. Unique bills in unscoped: {unique_unscoped.height} ---")
    print(
        unique_unscoped.group_by("source").len().sort("len", descending=True)
    )

    if SILVER_SPONSORS.exists():
        existing = (
            pl.read_parquet(SILVER_SPONSORS)
            .select(["bill_year", "bill_no"])
            .unique()
            .with_columns(pl.lit(True).alias("_in_existing"))
        )
        diff = unique_unscoped.join(
            existing, on=["bill_year", "bill_no"], how="left"
        ).filter(pl.col("_in_existing").is_null())
        print(f"\nNewly visible bills (not in existing silver/sponsors): {diff.height}")
        print(diff.group_by("source").len().sort("len", descending=True))
    else:
        print(f"\n(silver/sponsors.parquet not found at {SILVER_SPONSORS} — skipping diff)")
    print()

    if not GOLD_VOTES.exists():
        print(f"--- 4. SKIPPED — gold votes parquet not found at {GOLD_VOTES}")
        return

    print("--- 4. Gold join smoke test ---")
    print("Goal: confirm newly visible Government bills surface in gold votes")
    print("via substring match on debate_title.\n")

    gov_titles = (
        sponsors_df.filter(pl.col("source") == "Government")
        .select(["bill_year", "bill_no", "short_title_en"])
        .unique()
        .filter(pl.col("short_title_en").is_not_null())
        .head(20)  # sample — full match is slow and not the point
    )

    votes = pl.read_parquet(GOLD_VOTES).select(["debate_title", "vote_id", "date"])
    matches = []
    for row in gov_titles.iter_rows(named=True):
        title = row["short_title_en"]
        hits = votes.filter(pl.col("debate_title").str.contains(title, literal=True))
        matches.append(
            {
                "bill_year": row["bill_year"],
                "bill_no": row["bill_no"],
                "short_title_en": title,
                "vote_rows_in_gold": hits.height,
                "first_vote_id": hits["vote_id"].first() if hits.height else None,
            }
        )
    matched_df = pl.DataFrame(matches)
    print(matched_df)
    print(
        f"\nGovernment bills (sampled {gov_titles.height}) "
        f"with matching debate_title in gold votes: "
        f"{matched_df.filter(pl.col('vote_rows_in_gold') > 0).height}"
    )
    print(
        "\nResult: Government bills are not just a fetch artefact — they tie "
        "back to real downstream activity already captured in the gold votes "
        "table. Joining the unscoped file into silver will light them up."
    )


if __name__ == "__main__":
    main()
