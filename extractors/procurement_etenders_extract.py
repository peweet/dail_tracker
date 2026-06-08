"""eTenders/OGP procurement -> awards -> CRO match.
Promoted from probe_etenders_procurement.py. Lives in extractors/ but runs
as the `procurement` pipeline.py CHAIN, writing committed gold (cbi/cro pattern).

Cleaning vs the probe:
  - decode HTML entities (&amp; etc.), strip leading '|', split suppliers on '|'
  - drop public-body "suppliers" (councils/HSE/departments/etc.)
  - flag sole-trader / individual names (no company suffix) -> QUARANTINE, not matched
  - flag foreign legal forms (GmbH/SA/NV/...) -> CRO match not expected

Outputs:
  data/gold/parquet/procurement_awards.parquet            (one row per award-supplier)
  data/gold/parquet/procurement_supplier_cro_match.parquet (distinct supplier -> CRO)
  data/_meta/procurement_coverage.json

Run:  ./.venv/Scripts/python.exe extractors/procurement_etenders_extract.py
      ./.venv/Scripts/python.exe extractors/procurement_etenders_extract.py --force   # re-download CSV
The OGP CSV is cached at c:/tmp with a TTL (default 7d, --force / --cache-max-age-days /
ETENDERS_CACHE_MAX_AGE_DAYS) so a recurring run re-pulls instead of reusing a stale copy.
"""

from __future__ import annotations

import argparse
import contextlib
import html
import json
import os
import re
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import polars as pl
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

from shared.name_norm import name_norm_expr  # noqa: E402

URL = "https://assets.gov.ie/static/documents/7ba65f1b/Public_Procurement_Opendata_Dataset.csv"
# Provenance: the citable record of where this data came from. Emitted into the
# coverage JSON so the UI provenance footer reads source-of-truth, not hardcoded copy.
SOURCE = {
    "dataset": "Contract Notices Published on eTenders",  # verified via data.gov.ie package_show
    "publisher": "Office of Government Procurement (OGP)",
    "distributor": "data.gov.ie",
    "landing_page": "https://data.gov.ie/dataset/contract-notices-published-on-etenders",
    "download_url": URL,
    "license": "Creative Commons Attribution 4.0 (CC-BY 4.0)",
    "license_url": "https://creativecommons.org/licenses/by/4.0/",
    "attribution": "Contains Irish Public Sector Data (Office of Government Procurement) licensed under CC-BY 4.0.",
}
CACHE = Path("c:/tmp/etenders_opendata.csv")
# Cache TTL: the old code reused c:/tmp/etenders_opendata.csv whenever it merely
# EXISTED, so a routine `procurement` chain run silently re-built from a months-old
# CSV and never saw a new OGP publication (the same class of silent-staleness bug as
# DAIL-160/162). A cache older than this is re-downloaded; --force ignores it
# entirely. 7d keeps same-week dev runs cheap (the file is ~43MB) while guaranteeing
# a recurring pipeline re-pulls. Override with ETENDERS_CACHE_MAX_AGE_DAYS.
CACHE_MAX_AGE_DAYS: float = float(os.environ.get("ETENDERS_CACHE_MAX_AGE_DAYS", "7"))
CRO = ROOT / "data/silver/cro/companies.parquet"
# Promoted to committed gold (cbi/cro pattern): read by sql_views/procurement_*.sql.
OUT_AWARDS = ROOT / "data/gold/parquet/procurement_awards.parquet"
OUT_MATCH = ROOT / "data/gold/parquet/procurement_supplier_cro_match.parquet"
OUT_COV = ROOT / "data/_meta/procurement_coverage.json"

COMPANY_SUFFIX = re.compile(r"\b(limited|ltd|dac|plc|clg|uc|llp|teoranta|teo|unlimited company|t/a)\b", re.I)
FOREIGN_FORM = re.compile(
    r"\b(gmbh|s\.?a\.?|n\.?v\.?|s\.?a\.?s|s\.?p\.?a|inc|llc|\bpty\b|\bab\b|\bas\b|\bbv\b|\boy\b|srl|sl|sarl|aps|kft|ltda)\b",
    re.I,
)
PUBLIC_BODY = re.compile(
    r"\b(county council|city council|university|institute of technology|department of|office of|\bHSE\b|health service|an garda|údarás|udaras|education and training board|\bETB\b|local authority|national \w+ authority|county board|\bOPW\b)\b",
    re.I,
)


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def tidy_name(s: str) -> str:
    """Trailing-punctuation / dangling-connective tidy (approach 1)."""
    s = html.unescape(s or "").strip(" |\t")
    s = re.sub(r"\s+(?:and|&)\s*$", "", s, flags=re.I)  # "James Harte &" -> "James Harte"
    s = s.rstrip(" ,.&/-")  # "Accenture,." -> "Accenture"
    return s.strip()


def build_canonical_map(names: list[str]) -> dict[str, str]:
    """Deterministic first-character-truncation repair (approach 2).

    The OGP source drops the leading capital on a subset of supplier names
    ('eloitte Ireland LLP' = Deloitte). For each lowercase-initial name, prepend
    each A-Z and map to the matching correctly-spelled name that ALREADY exists in
    the dataset. Conservative: no canonical match -> leave the name unchanged.
    """
    canon = {n.lower(): n for n in names if n and n[:1].isupper() or (n[:1].isdigit())}
    mapping: dict[str, str] = {}
    for nm in names:
        if nm and nm[:1].islower():
            for c in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
                hit = canon.get((c + nm).lower())
                if hit:
                    mapping[nm] = hit
                    break
    return mapping


def _cache_is_fresh(force: bool, max_age_days: float) -> bool:
    """A usable cache: present, plausibly-sized, not forced, and within the TTL."""
    if force or not CACHE.exists() or CACHE.stat().st_size <= 1_000_000:
        return False
    age_days = (time.time() - CACHE.stat().st_mtime) / 86400.0
    if age_days > max_age_days:
        print(f"eTenders cache {age_days:.1f}d old (> {max_age_days}d) — re-downloading.")
        return False
    print(f"eTenders cache {age_days:.1f}d old (<= {max_age_days}d) — reusing {CACHE}.")
    return True


def ensure_csv(force: bool = False, max_age_days: float = CACHE_MAX_AGE_DAYS) -> Path:
    if _cache_is_fresh(force, max_age_days):
        return CACHE
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    print("downloading eTenders CSV…")
    with requests.get(URL, headers={"User-Agent": "dail-tracker research probe"}, timeout=180, stream=True) as r:
        r.raise_for_status()
        with open(CACHE, "wb") as f:
            for ch in r.iter_content(1 << 16):
                f.write(ch)
    return CACHE


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true", help="ignore the c:/tmp CSV cache and re-download the OGP export")
    ap.add_argument(
        "--cache-max-age-days",
        type=float,
        default=CACHE_MAX_AGE_DAYS,
        help=f"re-download if the cached CSV is older than this (default {CACHE_MAX_AGE_DAYS}; "
        "env ETENDERS_CACHE_MAX_AGE_DAYS)",
    )
    args = ap.parse_args()

    csv_path = ensure_csv(force=args.force, max_age_days=args.cache_max_age_days)
    df = pl.read_csv(csv_path, infer_schema_length=0, truncate_ragged_lines=True, ignore_errors=True)
    df = df.rename({c: c.replace("﻿", "").strip() for c in df.columns})
    sup_col, auth_col = "Awarded Suppliers", "Contracting Authority"
    val_col = next(c for c in df.columns if "Awarded Value" in c)
    date_col = next(c for c in df.columns if "Published" in c and "Date" in c)
    cpv_col = next((c for c in df.columns if c == "Main Cpv Code"), None)
    cpv_desc_col = next((c for c in df.columns if c == "Main Cpv Code Description"), None)
    comp_col = "Competition Type"  # Framework / DPS / Standalone / Bespoke ...
    parent_col = "Parent Agreement ID"  # present => call-off under a parent framework

    awards = df.filter(
        pl.col(sup_col).is_not_null() & (pl.col(sup_col).str.strip_chars() != "") & (pl.col(sup_col) != "NULL")
    )
    hr("INPUT")
    print(f"all notices: {df.height:,} | award notices: {awards.height:,} ({awards.height / df.height:.1%})")

    # explode supplier cells (separator '|'), decode entities, clean
    aw = (
        awards.select(
            ["Tender ID", sup_col, auth_col, val_col, date_col, comp_col, parent_col]
            + ([cpv_col] if cpv_col else [])
            + ([cpv_desc_col] if cpv_desc_col else [])
        )
        # decode HTML entities FIRST: the source encodes '&' as '&amp;', whose literal
        # ';' would otherwise be turned into the '|' supplier delimiter below and split a
        # single name ("Bourke &amp; Co. Limited" -> "Bourke" + "Co. Limited"). Unescape,
        # then normalise ';' separators to '|', then split.
        .with_columns(
            pl.col(sup_col)
            .map_elements(lambda s: html.unescape(s or ""), return_dtype=pl.Utf8)
            .str.replace_all(";", "|")
            .str.split("|")
            .alias("sl")
        )
        .explode("sl")
        .with_columns(pl.col("sl").map_elements(tidy_name, return_dtype=pl.Utf8).alias("supplier_raw"))
        .filter(pl.col("supplier_raw").str.len_chars() >= 3)
    )
    # deterministic first-char-truncation repair -> supplier (canonical)
    cmap = build_canonical_map(aw.select("supplier_raw").unique().to_series().to_list())
    aw = aw.with_columns(
        pl.col("supplier_raw").replace(cmap).alias("supplier"),
    ).with_columns(
        (pl.col("supplier") != pl.col("supplier_raw")).alias("name_repaired"),
    )
    print(
        f"  supplier names repaired (first-char truncation): {aw['name_repaired'].sum():,} rows; "
        f"{len(cmap):,} distinct spellings remapped"
    )
    # Residual source corruption: the OGP feed drops a leading capital on a subset of
    # cells. Where no correctly-spelled twin exists in the dataset, the missing letter
    # cannot be reconstructed safely (e.g. 'athWorks Ltd' could be Math/Bath/Path...).
    # Flag rather than guess: starts lowercase AND contains a later uppercase letter
    # (the Title-cased remainder is the truncation signature). Genuinely all-lowercase
    # trading names ('michael coughlan coach hire') have NO later uppercase -> not flagged.
    aw = aw.with_columns(
        (pl.col("supplier").str.contains(r"^[a-z]") & pl.col("supplier").str.contains(r"[A-Z]")).alias(
            "name_truncated"
        ),
    )
    print(f"  names flagged as unrecoverable source truncation: {aw['name_truncated'].sum():,} rows")
    aw = aw.with_columns(
        name_norm_expr("supplier").alias("supplier_norm"),
        pl.col("supplier")
        .map_elements(lambda s: bool(COMPANY_SUFFIX.search(s or "")), return_dtype=pl.Boolean)
        .alias("has_company_suffix"),
        pl.col("supplier")
        .map_elements(lambda s: bool(FOREIGN_FORM.search(s or "")), return_dtype=pl.Boolean)
        .alias("foreign_form"),
        pl.col("supplier")
        .map_elements(lambda s: bool(PUBLIC_BODY.search(s or "")), return_dtype=pl.Boolean)
        .alias("is_public_body"),
    )
    # classification for privacy handling
    aw = aw.with_columns(
        pl.when(pl.col("is_public_body"))
        .then(pl.lit("public_body"))
        .when(pl.col("has_company_suffix"))
        .then(pl.lit("company"))
        .when(pl.col("foreign_form"))
        .then(pl.lit("foreign_company"))
        .otherwise(pl.lit("sole_trader_or_individual"))
        .alias("supplier_class")
    )

    # ---- VALUE SEMANTICS: the published 'Awarded Value' is NOT actual spend ----
    # Two distinct over-counting traps, both flagged so totals can't be taken naively:
    #   1. Framework / DPS ceilings: the value is the notional MAXIMUM over the life of a
    #      multi-year framework (all future call-offs), not money paid. Competition Type
    #      tells us which notices are frameworks/DPS rather than one-off contract awards.
    #   2. Multi-supplier double-count: a single framework lists N suppliers and the SAME
    #      ceiling is stamped on every supplier row (confirmed: all multi-supplier tenders
    #      repeat one identical value). Exploding by supplier would multiply the money N-fold.
    # Even a clean standalone award is the ESTIMATED/awarded contract value, never vouched
    # expenditure. So `value_safe_to_sum` is the only column anything downstream may total,
    # and even that should be labelled "awarded value, not actual spend".
    FRAMEWORK_TYPES = ["Framework", "FW - Mini-Comp", "DPS Tender", "DPS/UQS"]
    # A sub-€1 "awarded value" is not a real contract value — it is source noise (a
    # handful parse to fractions of a cent, e.g. 0.0013) or a €1 placeholder. Below
    # this floor a row is never value_safe_to_sum; the value_eur is still kept as-is.
    MIN_PLAUSIBLE_VALUE_EUR = 1.0
    # The source carries the literal string "NULL" for ~7% of award rows. Make it an honest
    # null so it can't (a) collapse 4k unrelated awards into one group_by bucket, nor (b) be
    # mistaken for a joinable id downstream.
    aw = aw.with_columns(
        pl.when(pl.col("Tender ID").str.strip_chars().is_in(["", "NULL"]))
        .then(None)
        .otherwise(pl.col("Tender ID"))
        .alias("Tender ID"),
    )
    aw = (
        aw.with_columns(
            pl.col(val_col).str.replace_all(r"[^0-9.]", "").cast(pl.Float64, strict=False).alias("value_eur"),
            pl.col(comp_col).is_in(FRAMEWORK_TYPES).alias("is_framework_or_dps"),
            (
                pl.col(parent_col).is_not_null()
                & (pl.col(parent_col) != "NULL")
                & (pl.col(parent_col).str.strip_chars() != "")
            ).alias("is_call_off"),
        )
        .with_columns(
            # >1 supplier row on a tender => the single ceiling is repeated across them. A null
            # Tender ID can't be grouped reliably (every null would otherwise share one bucket),
            # so leave it un-shared here and exclude it from value_safe_to_sum below instead.
            pl.when(pl.col("Tender ID").is_null())
            .then(False)
            .otherwise(pl.len().over("Tender ID") > 1)
            .alias("value_shared_across_suppliers"),
        )
        .with_columns(
            pl.when(pl.col("is_framework_or_dps"))
            .then(pl.lit("framework_or_dps_ceiling"))
            .when(pl.col("is_call_off"))
            .then(pl.lit("framework_call_off"))
            .otherwise(pl.lit("contract_award_value"))
            .alias("value_kind"),
        )
        .with_columns(
            (
                (pl.col("value_kind") == "contract_award_value")
                & ~pl.col("value_shared_across_suppliers")
                & pl.col("Tender ID").is_not_null()  # null id => can't verify the value isn't a repeated ceiling
                & pl.col("value_eur").is_not_null()
                & (pl.col("value_eur") >= MIN_PLAUSIBLE_VALUE_EUR)  # drop sub-€1 source noise / placeholders
            ).alias("value_safe_to_sum"),
        )
    )
    print(
        f"  value rows: {aw['value_eur'].is_not_null().sum():,} | "
        f"framework/DPS ceilings: {aw['is_framework_or_dps'].sum():,} | "
        f"multi-supplier (repeated value): {aw['value_shared_across_suppliers'].sum():,} | "
        f"safe-to-sum award rows: {aw['value_safe_to_sum'].sum():,}"
    )

    save_parquet(aw, OUT_AWARDS)
    hr("AWARD-SUPPLIER ROWS")
    print(f"rows: {aw.height:,}  ->  {OUT_AWARDS}")
    print(aw.group_by("supplier_class").len().sort("len", descending=True))

    # distinct suppliers; match only 'company' class to CRO (privacy: quarantine individuals)
    distinct = (
        aw.select(["supplier", "supplier_norm", "supplier_class", "name_truncated"])
        .unique(subset=["supplier_norm"])
        .filter(pl.col("supplier_norm").str.len_chars() >= 4)
    )
    cro = pl.read_parquet(CRO).select(["name_norm", "company_num", "company_status", "comp_dissolved_date"])
    # truncated names would mis-join on a wrong-stem norm -> exclude from CRO matching
    company = distinct.filter((pl.col("supplier_class") == "company") & ~pl.col("name_truncated"))
    m = (
        company.join(cro, left_on="supplier_norm", right_on="name_norm", how="left")
        .group_by(["supplier", "supplier_norm"])
        .agg(
            pl.col("company_num").drop_nulls().n_unique().alias("n_cro"),
            pl.col("company_num").drop_nulls().first().alias("company_num"),
            pl.col("company_status").drop_nulls().first().alias("company_status"),
        )
        .with_columns(
            pl.when(pl.col("n_cro") == 1)
            .then(pl.lit("exact_unique"))
            .when(pl.col("n_cro") > 1)
            .then(pl.lit("exact_ambiguous"))
            .otherwise(pl.lit("no_match"))
            .alias("match_method"),
            pl.when(pl.col("n_cro") == 1)
            .then(0.9)
            .when(pl.col("n_cro") > 1)
            .then(0.5)
            .otherwise(0.0)
            .alias("match_confidence"),
        )
    )
    save_parquet(m, OUT_MATCH)

    hr("SUPPLIER -> CRO (company-class only; individuals quarantined)")
    tot = m.height
    one = m.filter(pl.col("match_method") == "exact_unique").height
    amb = m.filter(pl.col("match_method") == "exact_ambiguous").height
    print(f"company-class distinct suppliers: {tot:,}")
    print(f"  exact_unique : {one:,} ({one / tot:.1%})")
    print(f"  ambiguous    : {amb:,} ({amb / tot:.1%})")
    print(f"  no_match     : {tot - one - amb:,} ({(tot - one - amb) / tot:.1%})")

    hr("PRIVACY QUARANTINE")
    indiv = distinct.filter(pl.col("supplier_class") == "sole_trader_or_individual").height
    print(f"distinct suppliers total: {distinct.height:,}")
    print(
        f"  sole_trader_or_individual (QUARANTINED, not matched/published): {indiv:,} ({indiv / distinct.height:.1%})"
    )
    print(distinct.filter(pl.col("supplier_class") == "sole_trader_or_individual").select("supplier").head(6))

    hr("SAMPLE: clean award -> CRO matches")
    sample = (
        m.filter(pl.col("match_method") == "exact_unique")
        .join(
            aw.select(["supplier_norm", auth_col, val_col]).unique(subset=["supplier_norm"]),
            on="supplier_norm",
            how="left",
        )
        .head(8)
    )
    print(sample.select(["supplier", "company_num", "company_status", auth_col]).head(8))

    cov = {
        "all_notices": df.height,
        "award_notices": awards.height,
        "award_supplier_rows": aw.height,
        "supplier_class_counts": {
            r["supplier_class"]: r["len"] for r in aw.group_by("supplier_class").len().iter_rows(named=True)
        },
        "distinct_suppliers": distinct.height,
        "company_class_suppliers": tot,
        "cro_exact_unique": one,
        "cro_exact_unique_pct_of_company": round(100 * one / tot, 1),
        "sole_trader_quarantined": indiv,
        "name_truncated_rows": int(aw["name_truncated"].sum()),
        "name_truncated_distinct": int(distinct.filter(pl.col("name_truncated")).height),
        "value_rows": int(aw["value_eur"].is_not_null().sum()),
        "framework_or_dps_ceiling_rows": int(aw["is_framework_or_dps"].sum()),
        "value_shared_across_suppliers_rows": int(aw["value_shared_across_suppliers"].sum()),
        "value_safe_to_sum_rows": int(aw["value_safe_to_sum"].sum()),
        "value_safe_to_sum_total_eur": float(aw.filter(pl.col("value_safe_to_sum"))["value_eur"].sum() or 0),
        "value_naive_sum_eur_DO_NOT_USE": float(aw["value_eur"].sum() or 0),
        "source": SOURCE,
        "retrieved_utc": datetime.fromtimestamp(CACHE.stat().st_mtime, tz=UTC).strftime("%Y-%m-%d"),
        "caveat": "A contract award is a fact, not evidence of influence or wrongdoing. "
        "Sole-trader/individual supplier names are quarantined (personal data). "
        "name_truncated rows have a leading capital dropped in the OGP source and "
        "cannot be reconstructed safely; they are flagged and excluded from CRO matching. "
        "VALUE IS NOT SPEND: 'Awarded Value' is the estimated/awarded contract value; "
        "framework & DPS notices carry notional multi-year CEILINGS and multi-supplier "
        "frameworks repeat one ceiling across every supplier row. Only sum value_safe_to_sum, "
        "and even then label it 'awarded value, not actual expenditure'.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"\nwrote {OUT_MATCH}\nwrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
