#!/usr/bin/env python3
"""
charity_enriched.py — gold-layer enrichment of the Tier-A charity table.

Reads:
  data/silver/charities/charity_resolved.parquet — Tier-A (charity ⨝ CRO)
  data/silver/cro/companies.parquet              — extra CRO columns
  data/_meta/nace_v2_sections.csv                — NACE Rev. 2 section ranges

Writes:
  data/gold/parquet/charities_enriched.parquet   — one row per RCN

Columns added on top of charity_resolved (NULL for charities without a CRO
match — same convention as the upstream Tier-A join):

  Sector:
    nace_section_letter, nace_section_label
  Filing dates:
    cro_last_ar_date, cro_nard, cro_last_accounts_date
  Compliance flags:
    cro_annual_return_overdue, cro_accounts_overdue, cro_recent_distress,
    cro_no_registered_address, cro_recent_rename

Purely additive — does not modify any upstream script. Re-runnable; final
unique() on RCN is belt-and-braces in case any join introduces duplicates.
"""
from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

from config import GOLD_PARQUET_DIR, SILVER_DIR

_ROOT = Path(__file__).resolve().parent

CHARITY_RESOLVED = SILVER_DIR / "charities" / "charity_resolved.parquet"
CRO_COMPANIES    = SILVER_DIR / "cro" / "companies.parquet"
NACE_REFERENCE   = _ROOT / "data" / "_meta" / "nace_v2_sections.csv"
OUTPUT           = GOLD_PARQUET_DIR / "charities_enriched.parquet"

# CRO columns we want on top of what charity_resolved already pulls,
# renamed to the cro_* prefix used in the gold contract.
_EXTRA_CRO_COLS = {
    "last_ar_date":               "cro_last_ar_date",
    "nard":                       "cro_nard",
    "last_accounts_date":         "cro_last_accounts_date",
    "annual_return_overdue_flag": "cro_annual_return_overdue",
    "accounts_overdue_flag":      "cro_accounts_overdue",
    "recent_distress_flag":       "cro_recent_distress",
    "no_registered_address_flag": "cro_no_registered_address",
    "recent_rename_flag":         "cro_recent_rename",
}


def main() -> int:
    for p in (CHARITY_RESOLVED, CRO_COMPANIES, NACE_REFERENCE):
        if not p.exists():
            raise SystemExit(f"missing prerequisite: {p}")

    resolved = pl.read_parquet(CHARITY_RESOLVED)
    cro = (
        pl.read_parquet(CRO_COMPANIES)
        .select(["company_num", *_EXTRA_CRO_COLS.keys()])
        .rename(_EXTRA_CRO_COLS)
        # The CRO dedup in cro_normalise already collapses to one row per
        # company_num, but enforce it here so the LEFT join below can't
        # multiply RCN rows even if upstream changes.
        .unique(subset=["company_num"], keep="first")
    )
    nace = pl.read_csv(NACE_REFERENCE)

    # ── 1. Add the extra CRO columns by Tier-A key ───────────────────────
    enriched = resolved.join(
        cro, left_on="cro_number", right_on="company_num", how="left"
    )

    # ── 2. NACE section labels via chained when/then over division ranges ─
    # 4-digit class // 100 → 2-digit division. 21 sections, one branch each.
    # No cross-join, no risk of duplicates.
    div = pl.col("nace_v2_code") // 100
    letter_expr: pl.Expr = pl.lit(None, dtype=pl.Utf8)
    label_expr:  pl.Expr = pl.lit(None, dtype=pl.Utf8)
    for r in nace.iter_rows(named=True):
        cond = (div >= r["division_min"]) & (div <= r["division_max"])
        letter_expr = pl.when(cond).then(pl.lit(r["section_letter"])).otherwise(letter_expr)
        label_expr  = pl.when(cond).then(pl.lit(r["section_label"])).otherwise(label_expr)
    enriched = enriched.with_columns(
        letter_expr.alias("nace_section_letter"),
        label_expr.alias("nace_section_label"),
    )

    # ── 3. Belt-and-braces dedup on RCN (primary key) ────────────────────
    before = enriched.height
    enriched = enriched.unique(subset=["rcn"], keep="first")
    if before != enriched.height:
        print(f"WARNING: dedup collapsed {before - enriched.height:,} duplicate RCN rows")

    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    enriched.write_parquet(
        OUTPUT, compression="zstd", compression_level=3, statistics=True
    )

    matched  = enriched.filter(pl.col("link_method").is_not_null()).height
    has_nace = enriched.filter(pl.col("nace_section_label").is_not_null()).height
    has_fil  = enriched.filter(pl.col("cro_last_accounts_date").is_not_null()).height
    distress = enriched.filter(pl.col("cro_recent_distress").fill_null(False)).height
    print(f"[charity_enriched] wrote {OUTPUT}")
    print(f"  rows={enriched.height:,}  cols={enriched.width}")
    print(f"  CRO-matched:              {matched:,}")
    print(f"  with NACE section:        {has_nace:,}")
    print(f"  with last_accounts_date:  {has_fil:,}")
    print(f"  recently distressed:      {distress:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
