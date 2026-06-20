"""Sandbox: anonymized land-acquisition / CPO compensation by area × year.

A new MONEY LAYER for the planning-permission feature (doc/PLANNING_PERMISSION_SCOPING.md). It is
the inverse of §19 development contributions: contributions = what a *developer pays the council* for
a grant; this = what the *State pays a landowner* to acquire land (compulsory purchase orders, dwelling
/ land-bank purchases, road-scheme land). Read live from the consolidated public-body payment fact
(data/gold/parquet/procurement_payments_fact.parquet, built by procurement_payments_consolidate.py).

PRIVACY — why this is safe (and is the whole point of this script):
  The SOURCE rows are councils' own published "Payments over €20,000" lists (Circular 07/2012 / FOI),
  which ALREADY publish payee NAME + amount + year. Many CPO/land payees are private individuals, so
  they sit quarantined (public_display=False) in the gold fact and never surface in the app. This
  script keeps that quarantine intact while extracting the *non-identifying* facts the planning
  feature needs: the COMPENSATION FIGURE by YEAR and LOCATION (acquiring body = council/dept), with
  the payee IDENTITY DROPPED. Output carries NO supplier name/normalised key — so it is STRICTLY MORE
  PRIVATE than the council's own published list (name removed, figures aggregated). A runtime invariant
  refuses to write if any name-like column leaks in.

NO-INFERENCE: acquisition_type is keyword-derived from the body's OWN published description (raw kept
for audit in the sample dump); amounts are summed as published; nothing is imputed.

LOCATION granularity = the acquiring body (council/department). The descriptions rarely carry a
townland/road, so this is an honest council-level cost layer; the planning feature's fine (lon/lat)
location comes from the separate ArcGIS applications feed and joins to this at council level.

Run:
    python pipeline_sandbox/planning_cpo_compensation.py            # build
    python pipeline_sandbox/planning_cpo_compensation.py --sample   # also print an anonymized sample
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
from pathlib import Path

import polars as pl

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_cpo_compensation")

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"
OUT_DIR = ROOT / "data/silver/parquet"
OUT = OUT_DIR / "cpo_land_acquisition_by_area_year.parquet"
OUT_COV = ROOT / "data/_meta/cpo_land_acquisition_coverage.json"

# A land-acquisition payment, matched on the body's OWN published purpose text (description first,
# then spend_category). Deliberately specific to land/property ACQUISITION — not generic "property"
# (which also catches rent/lease/management). Order matters in _acq_type (most-specific first).
_ACQ_MATCH = (
    r"cpo|compulsory purchase|land purchase|land acquisition|acquisition of land|purchase of land|"
    r"purchase of dwelling|dwelling asset|land bank|land-purchase|land - purchase|land purchase-|"
    r"roadwidening|land for road|new road works"
)
# payee_type from supplier_class — a CLASS, never a name. Private CPO payees are the sensitive set;
# this lets the feature split "land bought from private owners (CPO)" vs "from companies/developers".
_PAYEE_TYPE = {
    "sole_trader_or_individual": "individual",
    "sole_trader": "individual",
    "company": "company",
    "foreign_company": "company",
    "public_body": "public_body",
    "id_code": "anonymised_code",
    "unknown": "unknown",
}


def _acq_type(text: pl.Expr) -> pl.Expr:
    """Source-grounded acquisition type from the published description (no-inference, most-specific
    branch first). 'road_land' before 'land_general'; 'cpo' is an explicit compulsory-purchase flag."""
    t = text.str.to_lowercase()
    return (
        pl.when(t.str.contains("dwelling"))
        .then(pl.lit("dwelling"))
        .when(t.str.contains("land bank|land-bank"))
        .then(pl.lit("land_bank"))
        .when(t.str.contains("roadwidening|road works|for road|new road"))
        .then(pl.lit("road_land"))
        .when(t.str.contains("cpo|compulsory"))
        .then(pl.lit("cpo"))
        .otherwise(pl.lit("land_general"))
    )


def build(sample: bool = False) -> pl.DataFrame:
    if not SRC.exists():
        raise SystemExit(f"source fact missing: {SRC} (run procurement_payments_consolidate.py first)")
    df = pl.read_parquet(SRC)

    text = pl.coalesce([pl.col("description"), pl.lit("")]) + " " + pl.coalesce([pl.col("spend_category"), pl.lit("")])
    land = df.filter(
        text.str.to_lowercase().str.contains(_ACQ_MATCH)
        & pl.col("amount_eur").is_not_null()
        & (pl.col("amount_eur") > 0)
    )
    if land.is_empty():
        raise SystemExit("no land-acquisition rows matched — check the source fact / match pattern")

    land = land.with_columns(
        _acq_type(text).alias("acquisition_type"),
        pl.col("supplier_class").replace_strict(_PAYEE_TYPE, default="unknown").alias("payee_type"),
    )

    LOG.info(
        "matched %d land-acquisition rows / %d payees / €%.1fm (%d–%d)",
        land.height,
        land["supplier_normalised"].n_unique(),
        land["amount_eur"].sum() / 1e6,
        land["year"].min(),
        land["year"].max(),
    )

    if sample:  # audit only — printed, never written; names shown only to the operator at build time
        s = land.select("publisher_name", "year", "amount_eur", "acquisition_type", "payee_type", "description")
        LOG.info("sample (audit):\n%s", s.sort("amount_eur", descending=True).head(15))

    # ── AGGREGATE: drop every identifying field; keep figure × year × location (council). ──
    agg = (
        land.group_by("publisher_name", "publisher_type", "sector", "year", "acquisition_type", "payee_type")
        .agg(
            pl.len().alias("n_payments"),
            pl.col("supplier_normalised").n_unique().alias("n_distinct_payees"),  # count only — NO names
            pl.col("amount_eur").sum().round(2).alias("total_compensation_eur"),
            pl.col("value_safe_to_sum").all().alias("all_sum_safe"),
        )
        .rename({"publisher_name": "acquiring_body"})
        # low_count: a cell of one payee/payment reflects a single already-published figure with the
        # name removed. Flagged so the UI can band/caveat it if a stricter line is wanted — NOT
        # suppressed here (the figure is strictly more private than the council's own published list).
        .with_columns((pl.col("n_distinct_payees") <= 1).alias("low_count"))
        .sort("total_compensation_eur", descending=True)
    )

    # PRIVACY INVARIANT (runtime, -O-proof): no name-like column may reach the output.
    forbidden = {"supplier", "supplier_raw", "supplier_normalised", "payee", "name"}
    leaked = forbidden & set(agg.columns)
    if leaked:
        raise SystemExit(f"privacy invariant breached: name-like columns {leaked} in CPO output — refusing to write")

    return agg


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample", action="store_true", help="also print an anonymized audit sample")
    args = ap.parse_args()
    setup_standalone_logging("planning_cpo_compensation")

    agg = build(sample=args.sample)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    save_parquet(agg, OUT)

    cov = {
        "generated_utc": dt.datetime.now(dt.UTC).isoformat(),
        "layer": "sandbox",
        "source": "data/gold/parquet/procurement_payments_fact.parquet",
        "purpose": "anonymized land-acquisition / CPO compensation by area (council) x year, for the planning feature",
        "privacy": "payee identity dropped; figures aggregated; strictly more private than the council's own published >€20k list",
        "n_cells": agg.height,
        "n_acquiring_bodies": int(agg["acquiring_body"].n_unique()),
        "year_min": int(agg["year"].min()),
        "year_max": int(agg["year"].max()),
        "total_compensation_eur": round(float(agg["total_compensation_eur"].sum()), 2),
        "n_payments_total": int(agg["n_payments"].sum()),
        "low_count_cells": int(agg["low_count"].sum()),
        "by_acquisition_type": {
            r["acquisition_type"]: round(r["total_compensation_eur"], 2)
            for r in agg.group_by("acquisition_type").agg(pl.col("total_compensation_eur").sum()).iter_rows(named=True)
        },
    }
    OUT_COV.parent.mkdir(parents=True, exist_ok=True)
    OUT_COV.write_text(json.dumps(cov, indent=2))
    LOG.info("wrote %d cells -> %s", agg.height, OUT)
    LOG.info("coverage -> %s", OUT_COV)
    LOG.info(
        "total compensation €%.1fm across %d bodies, %d–%d",
        cov["total_compensation_eur"] / 1e6,
        cov["n_acquiring_bodies"],
        cov["year_min"],
        cov["year_max"],
    )


if __name__ == "__main__":
    main()
