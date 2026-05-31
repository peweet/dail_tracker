"""
enrich.py — build gold enriched datasets from silver inputs.

Outputs (when all inputs present):
  - GOLD_DIR / master_td_list.csv                            (members only)
  - GOLD_DIR / enriched_td_attendance.csv                    (members + attendance)
  - GOLD_CSV_DIR / attendance_by_td_year.csv + parquet       (members + attendance)
  - GOLD_DIR / current_dail_vote_history.csv + parquet       (above + votes)
  - GOLD_CSV_DIR / current_td_payment_rankings.csv + parquet (master + payments)

Graceful per-input skips:
  - flattened_members.csv missing      → hard fail (rc=1)
  - td_attendance_fact_table missing   → skip enriched + by-year + vote-history
  - pretty_votes.csv missing           → skip vote-history (still write enriched + by-year)
  - payments_full_psa.parquet missing  → skip payment rankings
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import polars as pl

import normalise_join_key
from config import GOLD_CSV_DIR, GOLD_DIR, GOLD_PARQUET_DIR, SILVER_DIR

logger = logging.getLogger(__name__)


def _build_members_and_master(members_csv: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read flattened_members, build join_key + master_td_list."""
    members_wide_df = pl.read_csv(members_csv)
    members_wide_df = members_wide_df.with_columns(
        pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key")
    )
    members_wide_df = normalise_join_key.normalise_df_td_name(members_wide_df, "join_key")
    members_wide_df = members_wide_df.unique(subset=["join_key"], keep="first")
    logging.info("normalised members_wide_df  (API members) TD names")

    # year_elected is derived at silver layer by flatten_members_json_to_csv.py,
    # so members_wide_df already has it. No re-derivation needed.
    master_cols = [
        "unique_member_code",  # identifier
        "first_name",
        "last_name",
        "full_name",
        "year_elected",
        "ministerial_office",  # position
        "join_key",
        "constituency_name",  # constituency
        "party",
        "constituency_code",
    ]
    master_td_list = (
        members_wide_df.with_columns(pl.col("dail_number").cast(pl.Int32, strict=False))
        .sort("dail_number", descending=True, nulls_last=True)
        .select(master_cols)
        .unique(subset=["unique_member_code"], keep="first")
    )
    master_td_list = master_td_list.rename(
        {"unique_member_code": "identifier", "constituency_name": "constituency", "ministerial_office": "position"}
    )
    return members_wide_df, master_td_list


def _build_enriched_attendance(members_wide_df: pl.DataFrame, fact_csv: Path) -> pl.DataFrame:
    """Left-join members against attendance fact table; add year_elected."""
    member_profiles_df = pl.read_csv(fact_csv)
    member_profiles_df = member_profiles_df.with_columns(
        pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key")
    )
    member_profiles_df = normalise_join_key.normalise_df_td_name(member_profiles_df, "join_key")
    logging.info("normalised member_profiles_df  (PDF attendance) TD names")

    enriched_df = members_wide_df.join(member_profiles_df, on=["join_key"], how="left")
    enriched_df = enriched_df.with_columns(
        pl.col("unique_member_code").str.extract(r"\b\d{4}\b", 0).alias("year_elected")
    )
    return enriched_df


def _build_attendance_by_year(enriched_df: pl.DataFrame, csv_path: Path, parquet_path: Path) -> None:
    """Gold attendance summary — one row per (member, year)."""
    attendance_year = (
        enriched_df.filter(pl.col("year").is_not_null())
        .group_by(["full_name", "year"])
        .agg(
            pl.col("unique_member_code").first().alias("unique_member_code"),
            pl.col("identifier").first().alias("member_id"),
            pl.col("party").first().alias("party_name"),
            pl.col("constituency_name").first().alias("constituency"),
            pl.col("ministerial_office").first().alias("is_minister"),
            pl.col("sitting_days_count").max().alias("sitting_days"),
            pl.col("other_days_count").max().alias("other_days"),
        )
        .with_columns(
            [
                (pl.col("sitting_days") + pl.col("other_days")).alias("total_days"),
                pl.col("unique_member_code").fill_null(""),
            ]
        )
        .sort(["full_name", "year"])
    )
    attendance_year.write_csv(csv_path)
    attendance_year.write_parquet(
        parquet_path,
        compression="zstd",
        compression_level=3,
        statistics=True,
    )
    logging.info("Gold attendance_by_td_year.csv + parquet written.")


def _build_vote_history(
    votes_csv: Path,
    enriched_csv_on_disk: Path,
    out_csv: Path,
    out_parquet: Path,
) -> None:
    """Build current_dail_vote_history from pretty_votes + enriched_td_attendance.

    Re-reads enriched_td_attendance.csv from disk (matches original behaviour).
    """
    votes_df = pl.read_csv(votes_csv)
    enrich_vote = pl.read_csv(enriched_csv_on_disk)
    key_data = enrich_vote.select(
        [
            "join_key",
            "unique_member_code",
            "year_elected",
            "last_name",
            "dail_term",
            "dail_number",
            "full_name",
            "first_name",
            "party",
            "constituency_name",
        ]
    )
    key_data = key_data.unique(subset=["unique_member_code"])
    current_dail_vote_history_df = votes_df.join(key_data, on="unique_member_code", how="left")
    current_dail_vote_history_df = current_dail_vote_history_df.unique(
        subset=["unique_member_code", "date", "vote_id"]
    ).drop("join_key")

    current_dail_vote_history_df.write_csv(out_csv)
    logging.info("Enriched TD votes CSV created successfully.")
    current_dail_vote_history_df.write_parquet(
        out_parquet,
        compression="zstd",
        compression_level=3,
        statistics=True,
    )
    logging.info("Enriched TD votes Parquet created (check pipeline)")


def _build_payment_rankings(
    master_td_list: pl.DataFrame,
    payments_parquet: Path,
    out_csv: Path,
    out_parquet: Path,
) -> None:
    """Rank current TDs by total payments since 2020.

    Note: rank assignment is non-deterministic when amounts tie — Polars
    group_by() does not guarantee output order. (identifier → amount) is
    stable; rank position for tied amounts may differ between runs.
    """
    pay_raw = pl.read_parquet(payments_parquet)
    pay_keyed = normalise_join_key.normalise_df_td_name(
        pay_raw.filter(pl.col("date_paid") >= pl.lit("2020-01-01").str.to_date()).filter(
            pl.col("amount").is_not_null()
        ),
        "member_name",
    )
    pay_agg = pay_keyed.group_by("join_key").agg(pl.col("amount").sum().alias("total_amount_paid_since_2020"))
    member_lookup = master_td_list.select(["join_key", "identifier", "party", "constituency"]).unique(
        subset=["join_key"], keep="first"
    )
    current_rankings = (
        pay_agg.join(member_lookup, on="join_key", how="inner")
        .select(["join_key", "identifier", "party", "constituency", "total_amount_paid_since_2020"])
        .sort("total_amount_paid_since_2020", descending=True)
        .with_row_index(name="rank", offset=1)
    )
    current_rankings.write_csv(out_csv)
    current_rankings.write_parquet(
        out_parquet,
        compression="zstd",
        compression_level=3,
        statistics=True,
    )
    print(f"Current TD payment rankings written: {len(current_rankings)} TDs")


def main() -> int:
    """Build gold enriched datasets from silver inputs.

    Exit codes:
        0 — ok, or skipped sub-stages cleanly
        1 — flattened_members.csv missing (cannot proceed)
    """
    members_csv = SILVER_DIR / "flattened_members.csv"
    fact_csv = SILVER_DIR / "td_attendance_fact_table.csv"
    votes_csv = SILVER_DIR / "pretty_votes.csv"
    payments_parquet = GOLD_PARQUET_DIR / "payments_full_psa.parquet"

    master_csv_out = GOLD_DIR / "master_td_list.csv"
    enriched_csv_out = GOLD_DIR / "enriched_td_attendance.csv"
    attendance_year_csv = GOLD_CSV_DIR / "attendance_by_td_year.csv"
    attendance_year_parquet = GOLD_PARQUET_DIR / "attendance_by_td_year.parquet"
    vote_history_csv = GOLD_DIR / "current_dail_vote_history.csv"
    vote_history_parquet = GOLD_DIR / "parquet" / "current_dail_vote_history.parquet"
    payment_rankings_csv = GOLD_CSV_DIR / "current_td_payment_rankings.csv"
    payment_rankings_parquet = GOLD_PARQUET_DIR / "current_td_payment_rankings.parquet"

    # ── Hard requirement: members ────────────────────────────────────────────
    if not members_csv.exists():
        logger.error(
            "Cannot enrich: %s missing (Members API + flatten_members must run first).",
            members_csv,
        )
        print(f"ERROR: {members_csv} missing — cannot enrich.")
        return 1

    members_wide_df, master_td_list = _build_members_and_master(members_csv)
    master_td_list.write_csv(master_csv_out)
    print(f"Master TD list written to {master_csv_out} with {master_td_list.height} rows.")

    # ── Optional: attendance enrichment ──────────────────────────────────────
    if fact_csv.exists():
        enriched_df = _build_enriched_attendance(members_wide_df, fact_csv)
        enriched_df.write_csv(enriched_csv_out)
        logging.info("Enriched TD attendance CSV created successfully.")
        _build_attendance_by_year(enriched_df, attendance_year_csv, attendance_year_parquet)
    else:
        logger.warning(
            "%s missing — skipping enriched_td_attendance + attendance_by_td_year.",
            fact_csv,
        )
        print(f"WARN: {fact_csv} missing — skipping enrichment + attendance_by_td_year.")

    # ── Optional: vote history (needs both pretty_votes AND enriched on disk) ─
    if votes_csv.exists() and enriched_csv_out.exists():
        _build_vote_history(votes_csv, enriched_csv_out, vote_history_csv, vote_history_parquet)
    elif not votes_csv.exists():
        logger.warning("%s missing — skipping current_dail_vote_history.", votes_csv)
        print(f"WARN: {votes_csv} missing — skipping vote history.")
    else:
        logger.warning("enriched_td_attendance.csv missing — skipping current_dail_vote_history.")
        print("WARN: enriched_td_attendance.csv missing — skipping vote history.")

    # ── Optional: payment rankings ───────────────────────────────────────────
    if payments_parquet.exists():
        _build_payment_rankings(
            master_td_list, payments_parquet, payment_rankings_csv, payment_rankings_parquet
        )
    else:
        print(f"WARN: {payments_parquet} not found — skipping current TD payment rankings")

    return 0


if __name__ == "__main__":
    rc = main()
    if rc == 0:
        print("Enriched TD datasets created successfully and saved to enriched_td_attendance.csv.")
    sys.exit(rc)
