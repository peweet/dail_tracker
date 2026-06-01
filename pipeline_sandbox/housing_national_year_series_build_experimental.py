"""Housing NATIONAL year-series — companion to housing_la_master / la_year_series.

These three policy-table parquets are national-aggregate, not LA-keyed:
  - current_capital_target_vs_output_2016_2021 (Build/Acq/Lease × Target/Output × year)
  - housing_commission_inspections_2015_2022 (PRTB-style inspection enforcement)
  - housing_commission_cost_rental_targets (AHB / LA / Total × year, 2024–2030)

Consolidated as a vertical-stack long-format:
  (year, metric_family, category, metric, value, __source)

Easy to filter on the app side: a national-trend chart asks for a single
(metric_family, metric) and plots `value` over `year`.

Reads  : data/gold/parquet/housing_commission_*.parquet,
         data/gold/parquet/current_capital_target_vs_output_2016_2021.parquet
Writes : data/gold/parquet/housing_national_year_series.parquet
"""
from __future__ import annotations

import argparse
import contextlib
import sys
from pathlib import Path

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]

import polars as pl

_ROOT = Path(__file__).resolve().parents[1]
_PARQ = _ROOT / "data" / "gold" / "parquet"
_OUT = _PARQ / "housing_national_year_series.parquet"


def _stack_cc_target_output() -> pl.DataFrame:
    """Build/Acquisition/Lease × Target/Output × year."""
    df = pl.read_parquet(_PARQ / "current_capital_target_vs_output_2016_2021.parquet")
    return df.select([
        pl.col("year").cast(pl.Int64),
        pl.lit("social_housing_delivery").alias("metric_family"),
        pl.col("category").alias("category"),
        pl.col("metric").str.to_lowercase().alias("metric"),
        pl.col("value").cast(pl.Int64).alias("value"),
        pl.lit("Current_Capital_SocialHousing.pdf p11 [Rebuilding Ireland 2016-2021]").alias("__source"),
    ])


def _stack_hc_inspections() -> pl.DataFrame:
    """SSRRA private-rental inspection enforcement 2015-2022."""
    df = pl.read_parquet(_PARQ / "housing_commission_inspections_2015_2022.parquet")
    metric_aliases = {
        "No. of dwellings inspected": "inspected",
        "No. of dwellings inspected not meeting regulatory requirements": "non_compliant",
        "Improvement notices served on landlords for improvements to be carried out": "improvement_notices",
        "Legal action initiated": "legal_action",
    }
    parts = []
    for col, alias in metric_aliases.items():
        if col not in df.columns:
            continue
        parts.append(df.select([
            pl.col("year").cast(pl.Int64),
            pl.lit("rental_inspections").alias("metric_family"),
            pl.lit("national").alias("category"),
            pl.lit(alias).alias("metric"),
            pl.col(col).cast(pl.Int64).alias("value"),
            pl.lit("Housing Commission Report 2024 p128").alias("__source"),
        ]))
    return pl.concat(parts) if parts else pl.DataFrame()


def _stack_hc_cost_rental() -> pl.DataFrame:
    """Cost-rental delivery targets 2024-2030 (AHB / LA / synthesised Total)."""
    df = pl.read_parquet(_PARQ / "housing_commission_cost_rental_targets.parquet")
    base = df.select([
        pl.col("year").cast(pl.Int64),
        pl.col("provider").alias("category"),
        pl.col("units").cast(pl.Int64).alias("value"),
    ])
    totals = base.group_by("year").agg(pl.col("value").sum()).with_columns(
        pl.lit("Total").alias("category"),
    ).select(["year", "category", "value"])
    stacked = pl.concat([base, totals])
    return stacked.with_columns(
        pl.lit("cost_rental_targets").alias("metric_family"),
        pl.lit("units").alias("metric"),
        pl.lit("Housing Commission Report 2024 p123 [forward targets to 2030; Total synthesised]").alias("__source"),
    ).select(["year", "metric_family", "category", "metric", "value", "__source"])


def build() -> pl.DataFrame:
    parts = [
        _stack_cc_target_output(),
        _stack_hc_inspections(),
        _stack_hc_cost_rental(),
    ]
    return pl.concat([p for p in parts if not p.is_empty()])


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": len(df)}
    families = sorted(df["metric_family"].unique().to_list())
    rpt["checks"]["1_families_present"] = {
        "families": families,
        "pass": set(families) == {"social_housing_delivery", "rental_inspections", "cost_rental_targets"},
    }
    rpt["checks"]["2_row_count_plausible"] = {
        "rows": len(df), "pass": 50 <= len(df) <= 200,
    }
    # Year span sanity
    yr_min, yr_max = df["year"].min(), df["year"].max()
    rpt["checks"]["3_year_span"] = {
        "min": yr_min, "max": yr_max, "pass": yr_min >= 2015 and yr_max <= 2031,
    }
    # No negative values for any non-change metric
    bad = df.filter(pl.col("value") < 0).height
    rpt["checks"]["4_no_negatives"] = {"negatives": bad, "pass": bad == 0}
    # Cost-rental 2024 total: AHB (1024) + LA/Other (1052) ≈ 2076
    cr24 = df.filter(
        (pl.col("metric_family") == "cost_rental_targets") &
        (pl.col("year") == 2024) &
        (pl.col("category") == "Total")
    )
    cr24_val = cr24["value"].item(0) if len(cr24) else None
    rpt["checks"]["5_cost_rental_2024_total"] = {
        "value": cr24_val, "pass": cr24_val is not None and 1800 <= cr24_val <= 2400,
    }
    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    print("Building housing NATIONAL year-series …")
    df = build()
    print(f"Shape: {df.shape}  cols: {df.columns}")

    rpt = fidelity_check(df)
    print("\nFidelity:")
    for n, chk in rpt["checks"].items():
        print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {n}: {chk}")
    print(f">>> {'GREEN' if rpt['green'] else 'AMBER'}")

    if args.write and rpt["green"]:
        _write_parquet(df, _OUT)
        print(f"\nWrote {_OUT.relative_to(_ROOT)} ({_OUT.stat().st_size:,} bytes)")

    # Show one slice per family
    print("\nSample — social_housing_delivery, Build, Target vs Output:")
    print(df.filter(
        (pl.col("metric_family") == "social_housing_delivery") &
        (pl.col("category") == "Build")
    ).sort(["year", "metric"]))


if __name__ == "__main__":
    main()
