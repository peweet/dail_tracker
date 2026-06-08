"""Shared winner-row enrichment for TED silver layers (single source of truth).

Both the eForms API lane (ted_ireland_extract.py, 2024+) and the legacy per-notice-XML
lane (ted_ireland_winner_history_extract.py, 2016-2023) produce raw (notice x winner) rows
from different sources, then run the SAME enrichment: supplier-class classification ->
CRO match (by identifier then name) -> CRO-evidence privacy upgrade -> award-value safety
flags. Factoring it here keeps the two lanes byte-identical in classification so their
silver UNIONs cleanly (see doc/TED_ENRICHMENT.md §6).

Input df must carry: winner_name, winner_identifier_digits, award_value_eur (float|None),
value_kind, is_multi_supplier_framework (bool), is_pan_eu_outlier (bool).
enrich_winner_rows() adds: winner_name_norm, supplier_class, cro_company_num,
cro_match_method, cro_company_status, privacy_status, is_large_award_review,
value_safe_to_sum, source, retrieved_utc.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
import sys  # noqa: E402

sys.path.insert(0, str(ROOT))
from shared.name_norm import name_norm_expr  # noqa: E402

CRO = ROOT / "data/silver/cro/companies.parquet"
LARGE_AWARD = 50_000_000  # blunt ceiling guard: single awards above this are usually multi-year ceilings

COMPANY_SUFFIX = re.compile(
    r"\b(limited|ltd|dac|plc|clg|uc|llp|teoranta|teo|unlimited company|t/a|group|company|holdings|services|solutions|consult|partners|associates|university|institute|board|council|&)\b",
    re.I,
)
# TED winners skew FOREIGN (Bechtle AG, Proact IT Sweden AB, CloudFerro S.A., Vaisala Oyj) —
# without this they fall through COMPANY_SUFFIX and get mislabelled sole_trader, inflating the
# privacy flag. Mirrors the FOREIGN_FORM regex in procurement_etenders_extract.py.
FOREIGN_FORM = re.compile(
    r"\b(gmbh|ag|s\.?a\.?|n\.?v\.?|s\.?a\.?s|s\.?p\.?a|spa|inc|llc|\bpty\b|\bab\b|\bas\b|a/s|\bbv\b|\boy\b|oyj|srl|sl|sarl|aps|kft|ltda|s\.?r\.?o)\b",
    re.I,
)


def enrich_winner_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Apply the shared TED winner classification + CRO match + privacy + value flags."""
    # ---- winner classification + privacy (sole-trader quarantine flag, NOT dropped) ----
    df = (
        df.with_columns(
            name_norm_expr("winner_name").alias("winner_name_norm"),
            pl.col("winner_name")
            .map_elements(lambda s: bool(COMPANY_SUFFIX.search(s or "")), return_dtype=pl.Boolean)
            .alias("_co"),
            pl.col("winner_name")
            .map_elements(lambda s: bool(FOREIGN_FORM.search(s or "")), return_dtype=pl.Boolean)
            .alias("_for"),
        )
        .with_columns(
            pl.when(pl.col("winner_name").is_null())
            .then(pl.lit("unknown"))
            .when(pl.col("_co"))
            .then(pl.lit("company"))
            .when(pl.col("_for"))
            .then(pl.lit("foreign_company"))
            .otherwise(pl.lit("sole_trader_or_individual"))
            .alias("supplier_class"),
        )
        .drop(["_co", "_for"])
    )
    # privacy_status deferred until AFTER the CRO join — a CRO match is decisive evidence the
    # winner is a registered company, not an individual (see below).

    # ---- CRO match: by winner-identifier (exact reg number) THEN by normalised name ----
    cro = pl.read_parquet(CRO).select(["name_norm", "company_num", "company_status"])
    cro_num = (
        cro.select(
            pl.col("company_num")
            .cast(pl.Utf8)
            .str.replace_all(r"\D", "")
            .str.strip_chars_start("0")
            .alias("num_digits"),
            pl.col("company_num").alias("company_num_id"),
            pl.col("company_status").alias("status_by_id"),
        )
        .filter(pl.col("num_digits").str.len_chars() >= 4)
        .unique(subset=["num_digits"])
    )
    cro_name = cro.filter(pl.col("name_norm").str.len_chars() >= 4).unique(subset=["name_norm"])

    df = (
        df.join(cro_num, left_on="winner_identifier_digits", right_on="num_digits", how="left")
        .join(cro_name, left_on="winner_name_norm", right_on="name_norm", how="left")
        .with_columns(
            pl.coalesce(["company_num_id", "company_num"]).alias("cro_company_num"),
            pl.when(pl.col("company_num_id").is_not_null())
            .then(pl.lit("identifier"))
            .when(pl.col("company_num").is_not_null())
            .then(pl.lit("name"))
            .otherwise(pl.lit("none"))
            .alias("cro_match_method"),
            pl.coalesce(["status_by_id", "company_status"]).alias("cro_company_status"),
        )
        .drop(["company_num_id", "company_num", "status_by_id", "company_status"])
    )

    # CRO-evidence upgrade: a winner that joins the company register IS a registered company,
    # even if its TED name dropped the suffix word (Sweeney Consultancy, Three Ireland, Savills,
    # Cruinn Diagnostics...). Upgrade those from sole_trader_or_individual -> company so the
    # privacy flag isn't inflated by real firms. privacy_status computed AFTER this.
    df = df.with_columns(
        pl.when((pl.col("supplier_class") == "sole_trader_or_individual") & (pl.col("cro_match_method") != "none"))
        .then(pl.lit("company"))
        .otherwise(pl.col("supplier_class"))
        .alias("supplier_class"),
    ).with_columns(
        pl.when(pl.col("supplier_class") == "sole_trader_or_individual")
        .then(pl.lit("review_personal_data"))
        .otherwise(pl.lit("ok"))
        .alias("privacy_status"),
    )

    # ---- value flags ----------------------------------------------------------------
    # TED award values are ceiling/award-grade, not transactions. Even SINGLE-winner notices
    # above EU thresholds are routinely multi-year framework/operating CEILINGS. So a
    # "single-winner" test is NOT enough — gate large awards out of value_safe_to_sum and flag
    # them for review. Trustworthy metrics are COUNT and MEDIAN, never a naive sum.
    df = df.with_columns(
        (pl.col("award_value_eur") >= LARGE_AWARD).alias("is_large_award_review"),
    ).with_columns(
        (
            (pl.col("value_kind") == "contract_award_value")
            & ~pl.col("is_multi_supplier_framework")
            & ~pl.col("is_pan_eu_outlier")
            & ~pl.col("is_large_award_review")
            & pl.col("award_value_eur").is_not_null()
            & (pl.col("award_value_eur") > 0)
        ).alias("value_safe_to_sum"),
        pl.lit("TED").alias("source"),
        pl.lit(datetime.now(UTC).strftime("%Y-%m-%d")).alias("retrieved_utc"),
    )
    return df
