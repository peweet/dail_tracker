"""tools/patch_value_plausible_flag.py — backfill the additive `value_plausible` magnitude
guard onto the two procurement money facts in current gold, so it's available before the
next full re-harvest. Idempotent. Uses the SAME definition the extractors now persist
(services.deflator.value_plausible_expr) — one source of truth, no drift.

Additive only: never alters value_eur / amount_eur. Rewrites via save_parquet (atomic,
row-floor: min_rows = current row count, so a truncated read can never shrink the fact).
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.deflator import value_plausible_expr  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

GOLD = ROOT / "data/gold/parquet"
LARGE_AWARD_REVIEW_EUR = 50_000_000.0

TARGETS = [
    ("procurement_awards.parquet", "value_eur", LARGE_AWARD_REVIEW_EUR),
    ("procurement_payments_fact.parquet", "amount_eur", 5e8),
]


def main() -> int:
    for fname, col, hi in TARGETS:
        path = GOLD / fname
        if not path.exists():
            print(f"  SKIP {fname} (absent)")
            continue
        df = pl.read_parquet(path)
        n = df.height
        before_cols = set(df.columns)
        df = df.with_columns(value_plausible_expr(col, hi=hi).alias("value_plausible"))
        plausible = df["value_plausible"].sum()
        valued = df[col].is_not_null().sum()
        flagged = df.filter(df[col].is_not_null() & ~df["value_plausible"]).height
        save_parquet(df, path, min_rows=n)  # never shrink the fact
        added = "value_plausible" not in before_cols
        print(
            f"  {fname}: rows={n:,} valued={valued:,} plausible={plausible:,} "
            f"flagged_implausible={flagged:,}  ({'added' if added else 'refreshed'} value_plausible)"
        )
    print("done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
