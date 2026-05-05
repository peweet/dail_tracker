#!/usr/bin/env python3
"""
Charity-resolved (Tier A) join builder (sandbox).

STATUS: SANDBOX. Self-contained — does not import from pipeline.py / enrich.py /
normalise_join_key.py. Reads the silver parquets produced by the two normaliser
scripts and writes a single joined parquet.

Implements CRO/INTEGRATION_PLAN.md §4.5 (Tier A — deterministic, high
confidence: charity.cro_number → cro.company_num) plus §10 step 6+7 (build
charity_resolved including the latest financial snapshot per RCN).

PREREQUISITES:
- pipeline_sandbox/cro_normalise.py     →  data/silver/cro/companies.parquet
- pipeline_sandbox/charity_normalise.py →  data/silver/charities/register.parquet
                                           data/silver/charities/charity_latest.parquet

OUTPUT:
- data/silver/charities/charity_resolved.parquet  (one row per charity, RCN PK)

JOIN LOGIC:
- LEFT join charity.register (every charity) ⨝ cro.companies on
  cro_number = company_num. Many charities have no CRO Number (~60% of register)
  and the integer types match exactly — no normalisation needed for Tier A.
- LEFT join charity_latest (per-RCN financial snapshot) onto the result.
- link_method = 'cro_number_exact' when both sides matched; null otherwise.

VALIDATION:
- §4.5 spec: of charities that publish a CRO number, ≥97% must match a CRO row.
- The plan reports 99.2% (5,686 / 5,731). This script asserts ≥97% and exits
  non-zero if the rate drops below the threshold.
- Also reports: distinct cro_number values seen, duplicate-collision count
  (a single CRO row matched to multiple RCNs is unusual — it would mean two
  charities both claim the same company number).

USAGE:
    python pipeline_sandbox/charity_resolved.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import polars as pl

DEFAULT_CHARITY_DIR = Path("data/silver/charities")
DEFAULT_CRO_DIR = Path("data/silver/cro")

MIN_TIER_A_MATCH_RATE = 0.97


def main() -> int:
    p = argparse.ArgumentParser(description="Tier A charity↔CRO join builder (sandbox)")
    p.add_argument("--charity-dir", type=Path, default=DEFAULT_CHARITY_DIR)
    p.add_argument("--cro-dir", type=Path, default=DEFAULT_CRO_DIR)
    p.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_CHARITY_DIR / "charity_resolved.parquet",
    )
    args = p.parse_args()

    register_path = args.charity_dir / "register.parquet"
    latest_path = args.charity_dir / "charity_latest.parquet"
    cro_path = args.cro_dir / "companies.parquet"
    for label, path in (("register", register_path), ("charity_latest", latest_path), ("cro_companies", cro_path)):
        if not path.exists():
            raise SystemExit(f"missing prerequisite ({label}): {path}  — run the normalisers first")

    register = pl.read_parquet(register_path).select([
        "rcn", "registered_charity_name", "also_known_as", "name_norm", "aka_norm",
        "status", "governing_form", "classification_primary",
        "classification_secondary", "classification_sub", "country_established",
        "county", "cro_number", "has_cro_number_flag",
    ])

    cro = pl.read_parquet(cro_path).select([
        pl.col("company_num"),
        pl.col("company_name"),
        pl.col("name_norm").alias("company_name_norm"),
        pl.col("company_status"),
        pl.col("status_pill_value").alias("company_status_pill_value"),
        pl.col("company_type"),
        pl.col("company_reg_date"),
        pl.col("comp_dissolved_date"),
        pl.col("entity_age_years"),
        pl.col("nace_v2_code"),
        pl.col("eircode"),
        pl.col("routing_key"),
    ])

    latest = pl.read_parquet(latest_path)

    resolved = (
        register.join(cro, left_on="cro_number", right_on="company_num", how="left")
        .with_columns(
            pl.when(pl.col("cro_number").is_not_null() & pl.col("company_name").is_not_null())
            .then(pl.lit("cro_number_exact"))
            .otherwise(None)
            .alias("link_method"),
        )
        .join(latest, on="rcn", how="left")
    )

    charities_with_cro = int(register["has_cro_number_flag"].sum())
    matched = int(resolved.filter(pl.col("link_method") == "cro_number_exact").height)
    match_rate = matched / charities_with_cro if charities_with_cro else 0.0

    cro_collisions = (
        resolved.filter(pl.col("link_method") == "cro_number_exact")
        .group_by("cro_number")
        .agg(pl.len().alias("n"))
        .filter(pl.col("n") > 1)
    )

    args.out.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_parquet(args.out, compression="zstd")

    print(f"[charity_resolved] wrote {args.out}  rows={resolved.height}  cols={resolved.width}")
    print(f"  charities_with_cro_number: {charities_with_cro:,}")
    print(f"  matched (Tier A):          {matched:,}")
    print(f"  match_rate:                {match_rate:.4f}")
    print(f"  cro_number_collisions:     {cro_collisions.height:,}")
    print(f"  state_adjacent in result:  {int(resolved['state_adjacent_flag'].fill_null(False).sum()):,}")

    if match_rate < MIN_TIER_A_MATCH_RATE:
        print(
            f"[charity_resolved] FAIL: Tier A match rate {match_rate:.4f} "
            f"< required {MIN_TIER_A_MATCH_RATE:.2f}",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
