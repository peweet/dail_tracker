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

from config import (
    GOLD_CSV_DIR,
    GOLD_DIR,
    GOLD_PARQUET_DIR,
    GOLD_SEANAD_VOTE_HISTORY_PARQUET,
    SEANAD_ATTENDANCE_BY_YEAR_PARQUET,
    SEANAD_PAYMENTS_PARQUET,
    SILVER_DIR,
)
from services.parquet_io import save_parquet
from shared import normalise_join_key

logger = logging.getLogger(__name__)


def _build_members_and_master(members_csv: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Read flattened_members, build join_key + master_td_list."""
    members_wide_df = pl.read_csv(members_csv)
    members_wide_df = members_wide_df.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
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


# Junk "member" rows the PDF name-detection occasionally emits (e.g. a
# "Member Services" administrative footer parsed as a person). Excluded from the
# attendance gold — they are not TDs/Senators.
_NON_MEMBER_IDENTIFIER = "memberservices"


def _historic_meta(historic_csv: Path) -> pl.DataFrame | None:
    """Fallback member metadata from the HISTORIC (former-member) roster, same sorted-letter
    join_key. The current API roster is current members only (176), so a TD who sat in a given
    year but has since left the Dáil resolves to no unique_member_code off the current spine —
    100 of the 117 unmatched 2023-24 attendance rows (Coveney, Howlin, Collins, …) are exactly
    these former members and ARE in historic_members_dail. Joined after the current roster."""
    if not historic_csv.exists():
        return None
    h = pl.read_csv(historic_csv, infer_schema_length=None)
    h = h.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
    h = normalise_join_key.normalise_df_td_name(h, "join_key")
    return h.select(
        ["join_key", "unique_member_code", "full_name", "party", "constituency_name", "ministerial_office"]
    ).unique(subset=["join_key"], keep="first")


def _build_attendance_by_year(
    members_wide_df: pl.DataFrame, fact_csv: Path, csv_path: Path, parquet_path: Path, historic_csv: Path | None = None
) -> None:
    """Gold attendance summary — one row per (member, year).

    The ATTENDANCE FACT is the spine (left side), with member metadata joined on.
    A member who sat in a given year but is NOT on the current roster — a former
    TD who lost their seat or retired (e.g. at the 2024 election) — was a real
    member that year and must still appear; a members-spine join silently dropped
    them, leaving e.g. only 74 of the 126 TDs who sat in 2023. Roster fields
    (party, constituency, unique_member_code, ministerial office) are null for the
    unmatched, and full_name is reconstructed from the PDF identifier.

    Decoupled from the votes-feeding ``enriched_td_attendance`` frame on purpose:
    vote history is correctly scoped to current members, attendance is not.
    """
    fact = pl.read_csv(fact_csv)
    fact = fact.with_columns(pl.concat_str(pl.col(["first_name", "last_name"])).alias("join_key"))
    fact = normalise_join_key.normalise_df_td_name(fact, "join_key")

    meta = members_wide_df.select(
        ["join_key", "unique_member_code", "full_name", "party", "constituency_name", "ministerial_office"]
    ).unique(subset=["join_key"], keep="first")

    joined = fact.join(meta, on="join_key", how="left")

    # Fallback: resolve former members (off the current roster) against the historic roster,
    # coalescing the current-roster value first so current members are unaffected.
    hist = _historic_meta(historic_csv) if historic_csv else None
    if hist is not None:
        joined = joined.join(hist, on="join_key", how="left", suffix="_hist").with_columns(
            pl.coalesce(["unique_member_code", "unique_member_code_hist"]).alias("unique_member_code"),
            pl.coalesce(["party", "party_hist"]).alias("party"),
            pl.coalesce(["constituency_name", "constituency_name_hist"]).alias("constituency_name"),
            pl.coalesce(["ministerial_office", "ministerial_office_hist"]).alias("ministerial_office"),
            pl.coalesce(["full_name", "full_name_hist"]).alias("full_name"),
        )

    joined = joined.with_columns(
        # Still-unmatched members (pre-historic-roster former TDs, mojibake names) have no roster
        # full_name — reconstruct from the PDF identifier ("Lastname_Firstname") so they render.
        pl.coalesce([pl.col("full_name"), pl.col("identifier").str.replace_all("_", " ")]).alias("full_name")
    )
    joined = joined.filter(~pl.col("identifier").str.to_lowercase().str.contains(_NON_MEMBER_IDENTIFIER))

    attendance_year = (
        joined.filter(pl.col("year").is_not_null())
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
    save_parquet(attendance_year, parquet_path)
    logging.info("Gold attendance_by_td_year.csv + parquet written (%d member-years).", attendance_year.height)


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
    save_parquet(current_dail_vote_history_df, out_parquet)
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
        # Tie-break on the unique identifier so equal-amount members get a stable
        # rank/order across runs — otherwise the output parquet's content hash
        # churns run-to-run (non-reproducible gold).
        .sort(["total_amount_paid_since_2020", "identifier"], descending=[True, False])
        .with_row_index(name="rank", offset=1)
    )
    current_rankings.write_csv(out_csv)
    save_parquet(current_rankings, out_parquet)
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
        _build_attendance_by_year(
            members_wide_df,
            fact_csv,
            attendance_year_csv,
            attendance_year_parquet,
            SILVER_DIR / "historic_members_dail.csv",
        )
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
        _build_payment_rankings(master_td_list, payments_parquet, payment_rankings_csv, payment_rankings_parquet)
    else:
        print(f"WARN: {payments_parquet} not found — skipping current TD payment rankings")

    return 0


def main_seanad() -> int:
    """Build gold enriched Senator datasets — additive sibling of main().

    Reuses every _build_* helper verbatim with Senator inputs/outputs; main()
    (the Dáil path) is untouched. Same graceful per-input skips. Called by
    seanad_refresh.py after the Senator silver has been produced.

    Exit codes:
        0 — ok, or skipped sub-stages cleanly
        1 — flattened_seanad_members.csv missing (cannot proceed)
    """
    members_csv = SILVER_DIR / "flattened_seanad_members.csv"
    fact_csv = SILVER_DIR / "seanad_attendance_fact_table.csv"
    votes_csv = SILVER_DIR / "seanad_pretty_votes.csv"
    payments_parquet = SEANAD_PAYMENTS_PARQUET

    master_csv_out = GOLD_DIR / "seanad_master_list.csv"
    enriched_csv_out = GOLD_DIR / "enriched_senator_attendance.csv"
    attendance_year_csv = GOLD_CSV_DIR / "seanad_attendance_by_year.csv"
    attendance_year_parquet = SEANAD_ATTENDANCE_BY_YEAR_PARQUET
    vote_history_csv = GOLD_DIR / "current_seanad_vote_history.csv"
    vote_history_parquet = GOLD_SEANAD_VOTE_HISTORY_PARQUET
    payment_rankings_csv = GOLD_CSV_DIR / "current_senator_payment_rankings.csv"
    payment_rankings_parquet = GOLD_PARQUET_DIR / "current_senator_payment_rankings.parquet"

    if not members_csv.exists():
        logger.error("Cannot enrich Seanad: %s missing (flatten_members seanad must run first).", members_csv)
        print(f"ERROR: {members_csv} missing — cannot enrich Seanad.")
        return 1

    members_wide_df, master_td_list = _build_members_and_master(members_csv)
    master_td_list.write_csv(master_csv_out)
    print(f"Seanad master list written to {master_csv_out} with {master_td_list.height} rows.")

    if fact_csv.exists():
        enriched_df = _build_enriched_attendance(members_wide_df, fact_csv)
        enriched_df.write_csv(enriched_csv_out)
        _build_attendance_by_year(
            members_wide_df,
            fact_csv,
            attendance_year_csv,
            attendance_year_parquet,
            SILVER_DIR / "historic_members_seanad.csv",
        )
    else:
        logger.warning("%s missing — skipping Seanad enriched + attendance_by_year.", fact_csv)
        print(f"WARN: {fact_csv} missing — skipping Seanad enrichment + attendance_by_year.")

    if votes_csv.exists() and enriched_csv_out.exists():
        _build_vote_history(votes_csv, enriched_csv_out, vote_history_csv, vote_history_parquet)
    elif not votes_csv.exists():
        logger.warning("%s missing — skipping current_seanad_vote_history.", votes_csv)
        print(f"WARN: {votes_csv} missing — skipping Seanad vote history.")
    else:
        print("WARN: enriched_senator_attendance.csv missing — skipping Seanad vote history.")

    if payments_parquet.exists():
        _build_payment_rankings(master_td_list, payments_parquet, payment_rankings_csv, payment_rankings_parquet)
    else:
        print(f"WARN: {payments_parquet} not found — skipping Senator payment rankings")

    return 0


if __name__ == "__main__":
    rc = main()
    if rc == 0:
        print("Enriched TD datasets created successfully and saved to enriched_td_attendance.csv.")
    sys.exit(rc)
