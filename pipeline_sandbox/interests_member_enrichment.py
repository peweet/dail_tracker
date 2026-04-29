"""
interests_member_enrichment.py — ISOLATED SANDBOX SCRIPT

Enriches the interests gold parquet with unique_member_code by joining against
flattened_members.csv. The interests ranking parquet already has party_name and
constituency — this adds the canonical Oireachtas member identifier needed for
cross-page politician profile linking.

Reads:   data/gold/parquet/interests_member_ranking.parquet
         data/silver/flattened_members.csv
Writes:  data/gold/parquet/interests_member_ranking.parquet  <- overwrites with enriched version
         data/gold/csv/interests_enriched_preview.csv        <- first 200 rows for QA

DO NOT import or call this from any existing pipeline file.
Run independently: python pipeline_sandbox/interests_member_enrichment.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
from normalise_join_key import normalise_df_td_name

_ROOT        = Path(__file__).resolve().parents[1]
_INTERESTS   = _ROOT / "data" / "gold" / "parquet" / "interests_member_ranking.parquet"
_MEMBERS_CSV = _ROOT / "data" / "silver" / "flattened_members.csv"
_OUT_PARQUET = _ROOT / "data" / "gold" / "parquet" / "interests_member_ranking.parquet"
_PREVIEW_CSV = _ROOT / "data" / "gold" / "csv" / "interests_enriched_preview.csv"


def _build_member_ref(members_path: Path) -> pl.DataFrame:
    raw = (
        pl.read_csv(members_path, infer_schema_length=0)
        .select(["full_name", "unique_member_code", "dail_number"])
        .with_columns(pl.col("dail_number").cast(pl.Int32, strict=False))
        .filter(pl.col("full_name").is_not_null() & (pl.col("full_name") != ""))
        .sort("dail_number", descending=True)
        .unique(subset=["full_name"], keep="first")
    )
    keyed = normalise_df_td_name(raw, "full_name").select(
        ["join_key", "unique_member_code"]
    ).unique(subset=["join_key"], keep="first")
    return keyed


def run() -> None:
    if not _INTERESTS.exists():
        print(f"ERROR: interests_member_ranking.parquet not found at {_INTERESTS}")
        return
    if not _MEMBERS_CSV.exists():
        print(f"ERROR: flattened_members.csv not found at {_MEMBERS_CSV}")
        return

    interests = pl.read_parquet(_INTERESTS)
    print(f"Loaded {len(interests):,} interests rows")

    int_keyed = normalise_df_td_name(interests, "member_name")

    member_ref = _build_member_ref(_MEMBERS_CSV)
    print(f"Member reference: {len(member_ref):,} unique join keys")

    enriched = int_keyed.join(
        member_ref,
        on="join_key",
        how="left",
    ).drop("join_key")

    enriched = enriched.with_columns(
        pl.col("unique_member_code").fill_null("")
    )

    matched   = enriched.filter(pl.col("unique_member_code") != "").height
    unmatched = len(enriched) - matched
    print(f"Matched: {matched:,} rows | Unmatched: {unmatched:,} rows")

    col_order = ["unique_member_code"] + [c for c in interests.columns if c != "unique_member_code"]
    enriched  = enriched.select([c for c in col_order if c in enriched.columns])

    enriched.write_parquet(_OUT_PARQUET)
    print(f"Written enriched parquet -> {_OUT_PARQUET}")

    enriched.head(200).write_csv(_PREVIEW_CSV)
    print(f"Preview CSV -> {_PREVIEW_CSV}")
    print("Done.")


if __name__ == "__main__":
    run()
