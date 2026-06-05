"""PROBE (throwaway): is eTenders/OGP procurement data usable, and can awarded
suppliers be matched to CRO companies (review plan Phase 5)?

Source: data.gov.ie 'Contract Notices Published on eTenders', CC-BY 4.0, one CSV
(~43MB). Downloads once to c:/tmp, then measures:
  - award coverage (rows with an awarded supplier vs all notices)
  - supplier -> CRO exact name_norm match rate (reuses probe A's CRO matcher)
  - sole-trader / individual privacy risk (supplier names with no company suffix)
  - contracting-authority (public body) cardinality for a Department crosswalk

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_etenders_procurement.py
Reads CRO silver; downloads the open CSV; writes only a cached copy to c:/tmp.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

from shared.name_norm import name_norm_expr  # noqa: E402

URL = "https://assets.gov.ie/static/documents/7ba65f1b/Public_Procurement_Opendata_Dataset.csv"
CACHE = Path("c:/tmp/etenders_opendata.csv")
CRO = ROOT / "data/silver/cro/companies.parquet"

COMPANY_SUFFIX_RE = re.compile(r"\b(limited|ltd|dac|plc|clg|uc|llp|teoranta|teo|unlimited|company|holdings|group)\b", re.I)
SPLIT_RE = re.compile(r";|\s/\s|\n|\sand\s")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def ensure_csv() -> Path:
    if CACHE.exists() and CACHE.stat().st_size > 1_000_000:
        return CACHE
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    print("downloading eTenders CSV (~43MB)…")
    with requests.get(URL, headers={"User-Agent": "dail-tracker research probe"}, timeout=180, stream=True) as r:
        r.raise_for_status()
        with open(CACHE, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 16):
                f.write(chunk)
    print("saved", CACHE)
    return CACHE


def main() -> None:
    path = ensure_csv()
    df = pl.read_csv(path, infer_schema_length=0, truncate_ragged_lines=True, ignore_errors=True)
    # normalise the BOM'd header
    df = df.rename({c: c.replace("﻿", "").strip() for c in df.columns})
    hr("DATASET")
    print(f"rows (all notices): {df.height:,}")
    print(f"columns: {len(df.columns)}")

    sup_col = "Awarded Suppliers"
    val_col = "Awarded Value (€)" if "Awarded Value (€)" in df.columns else [c for c in df.columns if "Awarded Value" in c][0]
    auth_col = "Contracting Authority"
    date_col = [c for c in df.columns if "Published" in c and "Date" in c]
    date_col = date_col[0] if date_col else None

    awards = df.filter(pl.col(sup_col).is_not_null() & (pl.col(sup_col).str.strip_chars() != "") & (pl.col(sup_col) != "NULL"))
    hr("AWARD COVERAGE")
    print(f"notices with an awarded supplier: {awards.height:,}  ({awards.height / df.height:.1%} of all notices)")
    print(f"award-value column: {val_col!r}")
    print(f"date column      : {date_col!r}")

    # explode multi-supplier cells
    sup = (
        awards.select([sup_col, auth_col])
        # the Awarded Suppliers field separates multiple suppliers with '|'
        .with_columns(pl.col(sup_col).str.replace_all(r";", "|").str.split("|").alias("supplier_list"))
        .explode("supplier_list")
        .with_columns(pl.col("supplier_list").str.strip_chars().alias("supplier"))
        .filter(pl.col("supplier").str.len_chars() >= 3)
    )
    sup = sup.with_columns(name_norm_expr("supplier").alias("nn"))
    distinct_sup = sup.select(["supplier", "nn"]).unique(subset=["nn"]).filter(pl.col("nn").str.len_chars() >= 4)
    hr("AWARDED SUPPLIERS")
    print(f"awarded-supplier mentions (exploded): {sup.height:,}")
    print(f"distinct normalised suppliers      : {distinct_sup.height:,}")

    # privacy: company-suffix vs apparent individual / sole trader
    distinct_sup = distinct_sup.with_columns(
        pl.col("supplier").map_elements(lambda s: bool(COMPANY_SUFFIX_RE.search(s or "")), return_dtype=pl.Boolean).alias("has_company_suffix")
    )
    n_company = distinct_sup.filter(pl.col("has_company_suffix")).height
    hr("PRIVACY: sole-trader / individual risk")
    print(f"suppliers with a company suffix (Ltd/DAC/…): {n_company:,}  ({n_company / distinct_sup.height:.1%})")
    print(f"NO company suffix (possible sole trader / individual / public body): {distinct_sup.height - n_company:,}")
    print("  => sole-trader names are personal data; a real build should match/keep COMPANY-suffix")
    print("     suppliers and quarantine bare personal names.")
    print(distinct_sup.filter(~pl.col("has_company_suffix")).select("supplier").head(8))

    # CRO match (company-suffix suppliers only — the safe, matchable subset)
    cro = pl.read_parquet(CRO).select(["name_norm", "company_num"])
    co_sup = distinct_sup.filter(pl.col("has_company_suffix"))
    per = (
        co_sup.join(cro, left_on="nn", right_on="name_norm", how="left")
        .group_by("nn")
        .agg(pl.col("company_num").drop_nulls().n_unique().alias("n_cro"))
    )
    no = per.filter(pl.col("n_cro") == 0).height
    one = per.filter(pl.col("n_cro") == 1).height
    many = per.filter(pl.col("n_cro") > 1).height
    tot = per.height
    hr("SUPPLIER -> CRO (company-suffix suppliers, exact name_norm)")
    print(f"company-suffix suppliers: {tot:,}")
    print(f"  0 CRO match : {no:,}  ({no / tot:.1%})")
    print(f"  1 CRO match : {one:,}  ({one / tot:.1%})  <- clean")
    print(f"  >1 CRO match: {many:,}  ({many / tot:.1%})  <- disambiguate")

    hr("CONTRACTING AUTHORITY (public body -> Department crosswalk feasibility)")
    print(f"distinct contracting authorities: {df.select(auth_col).n_unique():,}")
    print(df.group_by(auth_col).len().sort("len", descending=True).head(10))

    hr("VERDICT")
    print(f"  award rows: {awards.height:,} | distinct suppliers: {distinct_sup.height:,}")
    print(f"  supplier->CRO clean 1:1 (company-suffix): {one / tot:.1%}")
    print("  procurement is usable + CC-BY; supplier->CRO reuses the validated matcher.")
    print("  privacy: keep company-suffix suppliers; quarantine bare personal names (sole traders).")


if __name__ == "__main__":
    main()
