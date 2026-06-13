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
# Organisation-form words that denote a firm/plurality and can NEVER be a lone private individual —
# the same conservative set the payments consolidation uses (extractors/procurement_payments_consolidate
# .py `_ORG_FORM_RE`). A sole-trader-classed supplier carrying one of these (or an exact CRO match) is
# reclassified to a company below, so the suffix-less firms the COMPANY_SUFFIX regex misses are still
# matched, ranked and surfaced. Kept in sync with the payments sibling.
ORG_FORM = r"(?i)\b(bros|brothers|sons|solicitors|barristers|partners|associates|contractors|developments|enterprises|industries)\b"
# A CRO exact name match below this normalised length is treated as a coincidental short-name
# collision, not evidence of a company (mirrors the payments floor).
_CRO_UPGRADE_MIN_LEN = 5
FOREIGN_FORM = re.compile(
    r"\b(gmbh|s\.?a\.?|n\.?v\.?|s\.?a\.?s|s\.?p\.?a|inc|llc|\bpty\b|\bab\b|\bas\b|\bbv\b|\boy\b|srl|sl|sarl|aps|kft|ltda)\b",
    re.I,
)
PUBLIC_BODY = re.compile(
    r"\b(county council|city council|university|institute of technology|department of|office of|\bHSE\b|health service|an garda|údarás|udaras|education and training board|\bETB\b|local authority|national \w+ authority|county board|\bOPW\b)\b",
    re.I,
)


# The source CSV writes these literal strings for missing values (it mixes "NULL",
# "Null" and "n/a"). Matched case-insensitively after trim — everywhere a sentinel
# check happens (row filter, exploded-supplier filter, final column coercion) so the
# three sites can't drift. Unit-tested in test/extractors/test_procurement_etenders_sentinels.py.
NULL_SENTINELS = ("null", "n/a")


def null_sentinel_expr(col: pl.Expr) -> pl.Expr:
    """True where the string column holds a literal null-sentinel ("NULL"/"Null"/"n/a"/...)."""
    return col.str.strip_chars().str.to_lowercase().is_in(list(NULL_SENTINELS))


def coerce_null_sentinels(df: pl.DataFrame) -> pl.DataFrame:
    """Replace literal null-sentinel strings with honest nulls across every Utf8 column."""
    str_cols = [c for c, t in zip(df.columns, df.dtypes, strict=False) if t == pl.Utf8]
    return df.with_columns(
        [pl.when(null_sentinel_expr(pl.col(c))).then(None).otherwise(pl.col(c)).alias(c) for c in str_cols]
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
    ('eloitte Ireland LLP' = Deloitte). The dropped capital also strands names on
    a punctuation initial ("&L Goodbody" = A&L Goodbody, "'FLYNN EXHAMS" =
    O'Flynn Exhams, ".M Morris" = ?.M Morris). For each lowercase- or
    punctuation-initial name, prepend each A-Z and map to the matching
    correctly-spelled name that ALREADY exists in the dataset. Conservative:
    no canonical match -> leave the name unchanged.
    """
    canon = {n.lower(): n for n in names if n and (n[:1].isupper() or n[:1].isdigit())}
    mapping: dict[str, str] = {}
    for nm in names:
        if nm and (nm[:1].islower() or nm[:1] in "&'."):
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
    # Detail columns (2026-06-12): the source carries far more per-notice detail than the
    # original 9-column select — most importantly "Tender/Contract Name" (the actual
    # contract title, 100% filled on award rows) where the old feed left a line item with
    # nothing but its generic CPV label. Fill rates on award rows: title 100%, awarded-SME
    # count 100%, procedure 94%, additional CPVs 79%, spend category / contract type /
    # bid counts 72%, duration 51%, TED links ~25% (above-EU-threshold subset only).
    # "Spend Category" doubles as the classification fallback: Main Cpv Code is filled on
    # only ~30% of award rows, and ~69% of the CPV-less rows carry a spend category.
    # Carried verbatim (per-notice scalars — the supplier explode just repeats them);
    # literal "NULL"/"n/a" strings become honest nulls in coerce_null_sentinels below.
    title_col = "Tender/Contract Name"
    DETAIL_COLS = [
        title_col,
        "Spend Category",
        "Contract Type",
        "Procedure",
        "Contract Duration (Months)",
        "No of Bids Received",
        "No of SMEs Bids Received",
        "No of Awarded SMEs",
        "Additional CPV Codes on CFT",
        "TED Notice Link",
        "TED CAN Link",
    ]
    detail_cols = [c for c in DETAIL_COLS if c in df.columns]
    # The estimated-value header embeds a '€' — find it by substring (same defence as the
    # awarded-value column) and parse it to a float here so no downstream SQL has to quote
    # a non-ASCII column name.
    est_col = next((c for c in df.columns if "Estimated Value" in c), None)

    awards = df.filter(
        pl.col(sup_col).is_not_null() & (pl.col(sup_col).str.strip_chars() != "") & ~null_sentinel_expr(pl.col(sup_col))
    )
    hr("INPUT")
    print(f"all notices: {df.height:,} | award notices: {awards.height:,} ({awards.height / df.height:.1%})")

    # explode supplier cells (separator '|'), decode entities, clean
    aw = (
        awards.select(
            ["Tender ID", sup_col, auth_col, val_col, date_col, comp_col, parent_col]
            + ([cpv_col] if cpv_col else [])
            + ([cpv_desc_col] if cpv_desc_col else [])
            + detail_cols
            + ([est_col] if est_col else [])
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
        # length gate + null-sentinel gate: a multi-supplier cell can carry "Null"/"n/a"
        # as one of its exploded entries even when the cell as a whole passed the row filter
        .filter((pl.col("supplier_raw").str.len_chars() >= 3) & ~null_sentinel_expr(pl.col("supplier_raw")))
        # The source pads a subset of authority cells with ~220 trailing spaces, so the
        # same body ranks as two distinct authorities ("The Office of Government
        # Procurement" appeared twice). Strip + collapse whitespace; decode entities.
        .with_columns(
            pl.col(auth_col)
            .map_elements(lambda s: html.unescape(s or ""), return_dtype=pl.Utf8)
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .alias(auth_col)
        )
    )
    # Contract titles carry the same source defects as authority cells: HTML entities
    # ("&amp;") and padded/stray whitespace. Clean in place — the title is display copy
    # downstream (null titles stay null; never coerced to "").
    if title_col in detail_cols:
        aw = aw.with_columns(
            pl.col(title_col)
            .map_elements(lambda s: html.unescape(s) if s else s, return_dtype=pl.Utf8)
            .str.replace_all(r"\s+", " ")
            .str.strip_chars()
            .alias(title_col)
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
    # A punctuation initial ("&L Goodbody", "'FLYNN EXHAMS", ".M Morris") is the same
    # dropped-capital defect stranded on the second character -> always flagged.
    aw = aw.with_columns(
        (
            (pl.col("supplier").str.contains(r"^[a-z]") & pl.col("supplier").str.contains(r"[A-Z]"))
            | pl.col("supplier").str.contains(r"^[&'.]")
        ).alias("name_truncated"),
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

    # Reclassify suffix-less firms the COMPANY_SUFFIX regex mis-binned as sole traders — the same
    # two conservative signals the payments consolidation uses, neither of which can be a lone
    # private individual: (1) an exact CRO name_norm match (a hard registration identifier, length-
    # floored against short collisions, never on a source-truncated name that would mis-join), or
    # (2) an organisation-form word (ORG_FORM). Upgraded firms then flow into the awards drill-down,
    # the rankings, and the CRO match table below (which re-filters on supplier_class=='company').
    _cro_names = pl.read_parquet(CRO).select("name_norm").unique().with_columns(pl.lit(True).alias("_cro_hit"))
    aw = aw.join(_cro_names, left_on="supplier_norm", right_on="name_norm", how="left")
    _upgrade = (pl.col("supplier_class") == "sole_trader_or_individual") & (
        (
            pl.col("_cro_hit").fill_null(False)  # noqa: FBT003 — polars fill, not a bool positional
            & (pl.col("supplier_norm").str.len_chars() >= _CRO_UPGRADE_MIN_LEN)
            & ~pl.col("name_truncated")
        )
        | pl.col("supplier").fill_null("").str.contains(ORG_FORM)
    )
    _n_up = int(aw.select(_upgrade.sum()).item())
    aw = aw.with_columns(
        pl.when(_upgrade).then(pl.lit("company")).otherwise(pl.col("supplier_class")).alias("supplier_class")
    ).drop("_cro_hit")
    print(f"  reclassified {_n_up:,} award rows sole_trader_or_individual -> company (CRO exact match / org-form word)")

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
    # Upper guard mirroring TED (ted_enrich.LARGE_AWARD): a single-supplier "contract award"
    # at or above this is, in practice, almost always a multi-year operating/framework CEILING
    # mislabelled as a one-off award (Go-Ahead bus €1.486bn, Applus NCT €650m, Valero fuel
    # €280m, PFH IT €475m). Without it, ~50 such rows = 38% of the entire "safe" total and
    # dominate every supplier-by-value ranking. Flag them for review, keep them OUT of
    # value_safe_to_sum, so eTenders and TED apply the identical honesty rail.
    LARGE_AWARD_REVIEW_EUR = 50_000_000.0
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
            # Mega single-supplier "awards" are almost always multi-year operating/framework
            # ceilings — flag for review and exclude from value_safe_to_sum (TED-consistent).
            (pl.col("value_eur") >= LARGE_AWARD_REVIEW_EUR).alias("is_large_award_review"),
        )
        .with_columns(
            (
                (pl.col("value_kind") == "contract_award_value")
                & ~pl.col("value_shared_across_suppliers")
                & ~pl.col("is_large_award_review")  # mega single-award ceilings excluded (TED-consistent)
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
        f"large-award (>=€50m) review: {aw['is_large_award_review'].sum():,} | "
        f"safe-to-sum award rows: {aw['value_safe_to_sum'].sum():,}"
    )

    # Pre-award estimate (notice header, ~27% filled): parsed exactly like value_eur but
    # kept as a SEPARATE column — it is an estimate published before tendering, never
    # summed and never substituted into value_eur. The raw column is dropped so the
    # parquet never carries a '€' in a column name.
    if est_col:
        aw = aw.with_columns(
            pl.col(est_col).str.replace_all(r"[^0-9.]", "").cast(pl.Float64, strict=False).alias("estimated_value_eur")
        ).drop(est_col)

    # Root data-quality fix (2026-06-11): the source CSV stores the literal string "NULL"
    # for missing values across MANY columns — not just Tender ID (handled above) but also
    # Main Cpv Code (~71% of rows), Main Cpv Code Description, Contracting Authority, Parent
    # Agreement ID and the raw value column. Left as a string it (a) forms a bogus "NULL"
    # group in every CPV / authority / supplier rollup and (b) reads as a real value
    # downstream. Coerce every "NULL" (trimmed) string column to an honest null, AFTER the
    # value/flag derivations above have already consumed the raw values. This makes the
    # per-view `WHERE ... <> 'NULL'` guards belt-and-suspenders rather than load-bearing.
    # Matching is case-insensitive and also catches "n/a" — see NULL_SENTINELS.
    aw = coerce_null_sentinels(aw)
    n_cpv = aw["Main Cpv Code"].is_not_null().sum() if "Main Cpv Code" in aw.columns else 0
    print(f"  coerced literal 'NULL' strings -> null across text cols; real CPV now: {n_cpv:,}")

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
        "large_award_review_rows_ge_50m": int(aw["is_large_award_review"].sum()),
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
