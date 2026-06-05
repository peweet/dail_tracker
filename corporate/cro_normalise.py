#!/usr/bin/env python3
"""
CRO companies normaliser.

Reads the most recent CRO bulk CSV from data/bronze/cro/ and writes the
normalised silver parquet at data/silver/cro/companies.parquet.

Implements the CRO half of CRO/INTEGRATION_PLAN.md §10 (NORMALISE per source).

CLEANS:
- "********NO ADDRESS DETAILS*******" placeholder → null on company_address_1
- Trailing whitespace on company_status (e.g. "Normal " → "Normal")
- company_reg_date < 1900-01-01 → null + reg_date_invalid_flag = true
- nace_v2_code float ("6420.0") → integer
- Eircode → routing_key (first 3 chars, upper-cased, alphanumeric only)

DERIVES:
- name_norm: upper, strip punctuation, drop common legal suffixes / corporate
  fillers, collapse whitespace. The rule lives in shared/name_norm.py
  (name_norm_expr) so the procurement / TED / corporate-xref enrichers share
  the IDENTICAL company-name key. It is distinct from the TD-name rule in
  shared/normalise_join_key.py (the CRO universe is too dissimilar to share one).
- entity_age_years: today − company_reg_date (null when reg_date is null/invalid)
- status_pill_value: collapsed status enum for the UI
    Normal → active
    Liquidation, Strike Off Listed, Receivership → in_distress
    Dissolved, Strike Off, Deregistered → dead
    everything else → other
- Warning flags consumed by the lobbyist POC view:
    annual_return_overdue_flag   nard < today AND status='Normal' (s.725 path)
    accounts_overdue_flag        last_accounts_date < today − 18m AND status='Normal'
    recent_distress_flag         status_pill in (in_distress,dead) AND status_date within last 12m
    no_registered_address_flag   company_address_1 null after the placeholder cleanse
    recent_rename_flag           company_name_eff_date within last 24m

DEDUPE:
- 1,704 duplicate company_num rows reflect amalgamated history. Per RCN
  group, keep the row whose company_name_eff_date is most recent (ties broken
  by company_status_date desc, then natural order). Quarantined rows are
  dropped from the silver output but counted in the run summary.
"""

from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

import polars as pl

from config import BRONZE_DIR, SILVER_DIR
from shared.name_norm import name_norm_expr

BRONZE_CRO_DIR = BRONZE_DIR / "cro"
SILVER_CRO_DIR = SILVER_DIR / "cro"
DEFAULT_SILVER = SILVER_CRO_DIR / "companies.parquet"

NO_ADDRESS_PATTERN = r"(?i)\*+\s*NO ADDRESS"
ADDRESS_COLS = ["company_address_1", "company_address_2", "company_address_3", "company_address_4"]
DATE_COLS = [
    "company_reg_date",
    "last_ar_date",
    "comp_dissolved_date",
    "nard",
    "last_accounts_date",
    "company_status_date",
    "company_name_eff_date",
    "company_type_eff_date",
]
STATUS_BUCKETS = {
    "active": {"Normal"},
    "in_distress": {"Liquidation", "Receivership", "Strike Off Listed"},
    "dead": {"Dissolved", "Strike Off", "Deregistered"},
}


def latest_bronze_csv() -> Path:
    """Pick the most recent companies_*.csv in the bronze CRO directory.

    CRO bulk exports are filename-dated (companies_YYYYMMDD.csv) so lexical sort
    gives chronological order. Raises SystemExit if no candidate exists so the
    pipeline reports a clean failure instead of a silent skip.
    """
    candidates = sorted(BRONZE_CRO_DIR.glob("companies_*.csv"))
    if not candidates:
        raise SystemExit(f"no companies_*.csv files in {BRONZE_CRO_DIR}")
    return candidates[-1]


def status_pill_expr() -> pl.Expr:
    expr = pl.lit("other")
    for bucket, members in STATUS_BUCKETS.items():
        expr = pl.when(pl.col("company_status").is_in(list(members))).then(pl.lit(bucket)).otherwise(expr)
    return expr.alias("status_pill_value")


def load_bronze(path: Path) -> pl.DataFrame:
    df = pl.read_csv(
        path,
        infer_schema_length=20_000,
        try_parse_dates=False,
        ignore_errors=False,
    )
    expected = 21
    if df.width != expected:
        raise SystemExit(f"schema drift: expected {expected} columns, got {df.width}: {df.columns}")
    return df


def normalise(df: pl.DataFrame) -> tuple[pl.DataFrame, dict]:
    today = dt.date.today()
    address_clean = [
        pl.when(pl.col(c).str.contains(NO_ADDRESS_PATTERN, literal=False))
        .then(None)
        .otherwise(pl.col(c).str.strip_chars())
        .alias(c)
        for c in ADDRESS_COLS
    ]
    date_parsed = [pl.col(c).str.strptime(pl.Date, format="%Y-%m-%d", strict=False).alias(c) for c in DATE_COLS]

    df = df.with_columns(
        pl.col("company_num").cast(pl.Int64, strict=False),
        pl.col("company_status").str.strip_chars().alias("company_status"),
        pl.col("company_type").str.strip_chars().alias("company_type"),
        pl.col("nace_v2_code").cast(pl.Int64, strict=False).alias("nace_v2_code"),
        pl.col("eircode").str.strip_chars().alias("eircode"),
        *address_clean,
        *date_parsed,
    )

    invalid_reg = pl.col("company_reg_date") < dt.date(1900, 1, 1)
    df = df.with_columns(
        invalid_reg.fill_null(False).alias("reg_date_invalid_flag"),
        pl.when(invalid_reg).then(None).otherwise(pl.col("company_reg_date")).alias("company_reg_date"),
    )
    df = df.with_columns(
        name_norm_expr("company_name").alias("name_norm"),
        ((pl.lit(today) - pl.col("company_reg_date")).dt.total_days() // 365).cast(pl.Int32).alias("entity_age_years"),
        pl.col("eircode").str.replace_all(r"[^A-Z0-9]", "").str.slice(0, 3).alias("routing_key"),
        status_pill_expr(),
    )

    cutoff_18m = today - dt.timedelta(days=int(365 * 1.5))
    cutoff_12m = today - dt.timedelta(days=365)
    cutoff_24m = today - dt.timedelta(days=2 * 365)
    df = df.with_columns(
        ((pl.col("nard") < pl.lit(today)) & (pl.col("company_status") == "Normal"))
        .fill_null(False)
        .alias("annual_return_overdue_flag"),
        ((pl.col("last_accounts_date") < pl.lit(cutoff_18m)) & (pl.col("company_status") == "Normal"))
        .fill_null(False)
        .alias("accounts_overdue_flag"),
        (
            (pl.col("company_status_date") >= pl.lit(cutoff_12m))
            & (pl.col("status_pill_value").is_in(["in_distress", "dead"]))
        )
        .fill_null(False)
        .alias("recent_distress_flag"),
        pl.col("company_address_1").is_null().alias("no_registered_address_flag"),
        (pl.col("company_name_eff_date") >= pl.lit(cutoff_24m)).fill_null(False).alias("recent_rename_flag"),
    )

    n_before = df.height
    df_dedup = df.sort(
        ["company_num", "company_name_eff_date", "company_status_date"],
        descending=[False, True, True],
        nulls_last=True,
    ).unique(subset=["company_num"], keep="first")
    n_after = df_dedup.height
    summary = {
        "rows_in": n_before,
        "rows_out": n_after,
        "duplicates_collapsed": n_before - n_after,
        "invalid_reg_dates": int(df["reg_date_invalid_flag"].sum()),
        "no_address_rows": int(df.select(pl.col("company_address_1").is_null()).to_series().sum()),
        "annual_return_overdue": int(df_dedup["annual_return_overdue_flag"].sum()),
        "accounts_overdue": int(df_dedup["accounts_overdue_flag"].sum()),
        "recent_distress": int(df_dedup["recent_distress_flag"].sum()),
        "no_registered_address": int(df_dedup["no_registered_address_flag"].sum()),
        "recent_rename": int(df_dedup["recent_rename_flag"].sum()),
    }
    return df_dedup, summary


def main() -> int:
    p = argparse.ArgumentParser(description="CRO companies normaliser")
    p.add_argument(
        "--bronze", type=Path, default=None, help="bronze CSV path; defaults to the most recent companies_*.csv"
    )
    p.add_argument("--silver", type=Path, default=DEFAULT_SILVER)
    args = p.parse_args()

    bronze_path = args.bronze or latest_bronze_csv()
    if not bronze_path.exists():
        raise SystemExit(f"bronze input not found: {bronze_path}")
    print(f"[cro_normalise] reading {bronze_path}")

    df_raw = load_bronze(bronze_path)
    df, summary = normalise(df_raw)

    args.silver.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(args.silver, compression="zstd", compression_level=3, statistics=True)

    print(f"[cro_normalise] wrote {args.silver}  rows={df.height}  cols={df.width}")
    for k, v in summary.items():
        print(f"  {k}: {v:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
