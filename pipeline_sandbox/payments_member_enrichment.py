"""
payments_member_enrichment.py — ISOLATED SANDBOX SCRIPT

Enriches the payments gold fact table with party_name, constituency, and
unique_member_code by joining against flattened_members.csv.

unique_member_code is the canonical Oireachtas member identifier used across
all domains — adding it here enables cross-page politician profile linking.

Reads:   data/gold/parquet/payments_fact.parquet
         data/silver/flattened_members.csv
Writes:  data/gold/parquet/payments_fact.parquet   <- overwrites with enriched version
         data/gold/csv/payments_fact_enriched_preview.csv  <- first 200 rows for QA

DO NOT import or call this from any existing pipeline file.
Run independently: python pipeline_sandbox/payments_member_enrichment.py
Validate the preview CSV before treating the parquet as authoritative.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
from normalise_join_key import normalise_df_td_name

_ROOT        = Path(__file__).resolve().parents[1]
_PAYMENTS_IN = _ROOT / "data" / "gold" / "parquet" / "payments_fact.parquet"
_MEMBERS_CSV = _ROOT / "data" / "silver" / "flattened_members.csv"
_OUT_PARQUET = _ROOT / "data" / "gold" / "parquet" / "payments_fact.parquet"
_PREVIEW_CSV = _ROOT / "data" / "gold" / "csv" / "payments_fact_enriched_preview.csv"


def _build_member_ref(members_path: Path) -> pl.DataFrame:
    """
    One-row-per-normalised-join-key reference with party and constituency from the
    most recent Dáil term (highest dail_number) for each member.
    """
    raw = (
        pl.read_csv(members_path, infer_schema_length=0)
        .select(["full_name", "unique_member_code", "party", "constituency_name", "dail_number"])
        .with_columns(pl.col("dail_number").cast(pl.Int32, strict=False))
        .filter(pl.col("full_name").is_not_null() & (pl.col("full_name") != ""))
        .sort("dail_number", descending=True)
        .unique(subset=["full_name"], keep="first")
    )

    keyed = normalise_df_td_name(raw, "full_name").select(
        ["full_name", "join_key", "unique_member_code", "party", "constituency_name"]
    )
    keyed = (
        keyed
        .rename({"party": "party_name", "constituency_name": "constituency"})
        .unique(subset=["join_key"], keep="first")
    )
    return keyed


def run() -> None:
    if not _PAYMENTS_IN.exists():
        print("ERROR: payments_fact.parquet not found. Run payments_gold_etl.py first.")
        return
    if not _MEMBERS_CSV.exists():
        print(f"ERROR: flattened_members.csv not found at {_MEMBERS_CSV}")
        return

    payments = pl.read_parquet(_PAYMENTS_IN)
    print(f"Loaded {len(payments):,} payment rows")

    pay_keyed = normalise_df_td_name(payments, "member_name")

    member_ref = _build_member_ref(_MEMBERS_CSV)
    print(f"Member reference: {len(member_ref):,} unique join keys")

    enriched = pay_keyed.join(
        member_ref.select(["join_key", "unique_member_code", "party_name", "constituency"]),
        on="join_key",
        how="left",
    ).drop("join_key")

    enriched = enriched.with_columns([
        pl.col("unique_member_code").fill_null(""),
        pl.col("party_name").fill_null(""),
        pl.col("constituency").fill_null(""),
    ])

    matched   = enriched.filter(pl.col("party_name") != "").height
    unmatched = len(enriched) - matched
    print(f"Matched: {matched:,} rows | Unmatched: {unmatched:,} rows")

    if unmatched > 0:
        unmatched_names = (
            enriched
            .filter(pl.col("party_name") == "")
            .select("member_name")
            .unique()
            .sort("member_name")
        )
        print(f"Unmatched: {len(unmatched_names)} distinct member names (pre-34th Dail or name variant)")

    col_order = [c for c in payments.columns if c not in ("unique_member_code", "party_name", "constituency")]
    col_order += ["unique_member_code", "party_name", "constituency"]
    enriched = enriched.select([c for c in col_order if c in enriched.columns])

    enriched.write_parquet(_OUT_PARQUET)
    print(f"Written enriched parquet -> {_OUT_PARQUET}")

    enriched.head(200).write_csv(_PREVIEW_CSV)
    print(f"Preview CSV -> {_PREVIEW_CSV}")
    print("Done. Validate the preview CSV before treating the parquet as authoritative.")


if __name__ == "__main__":
    run()
