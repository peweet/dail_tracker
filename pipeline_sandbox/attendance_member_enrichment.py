"""
attendance_member_enrichment.py — ISOLATED SANDBOX SCRIPT

Enriches the attendance gold CSV with unique_member_code by joining against
flattened_members.csv (the canonical member reference).

The attendance gold CSV uses an internal member_id format (LastName_FirstName)
that is not consistent with the Oireachtas unique_member_code used in votes and
across the rest of the pipeline. This script adds unique_member_code via a
normalised name join so that the attendance domain can participate in the
cross-page politician profile.

Uses normalise_join_key.normalise_df_td_name — the same fuzzy join used across
the pipeline to handle diacritics, apostrophes, and spacing variants.

Reads:   data/gold/csv/attendance_by_td_year.csv
         data/silver/flattened_members.csv
Writes:  data/gold/csv/attendance_by_td_year.csv       <- overwrites with enriched version
         data/gold/parquet/attendance_by_td_year.parquet <- parquet for faster view reads
         data/gold/csv/attendance_enriched_preview.csv  <- first 200 rows for QA

DO NOT import or call this from any existing pipeline file.
Run independently: python pipeline_sandbox/attendance_member_enrichment.py
Validate the preview CSV before treating the output as authoritative.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import polars as pl
from normalise_join_key import normalise_df_td_name

_ROOT         = Path(__file__).resolve().parents[1]
_ATTENDANCE   = _ROOT / "data" / "gold" / "csv" / "attendance_by_td_year.csv"
_MEMBERS_CSV  = _ROOT / "data" / "silver" / "flattened_members.csv"
_OUT_CSV      = _ROOT / "data" / "gold" / "csv" / "attendance_by_td_year.csv"
_OUT_PARQUET  = _ROOT / "data" / "gold" / "parquet" / "attendance_by_td_year.parquet"
_PREVIEW_CSV  = _ROOT / "data" / "gold" / "csv" / "attendance_enriched_preview.csv"


def _build_member_ref(members_path: Path) -> pl.DataFrame:
    """unique_member_code lookup keyed by normalised full_name."""
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
    if not _ATTENDANCE.exists():
        print(f"ERROR: attendance_by_td_year.csv not found at {_ATTENDANCE}")
        return
    if not _MEMBERS_CSV.exists():
        print(f"ERROR: flattened_members.csv not found at {_MEMBERS_CSV}")
        return

    attendance = pl.read_csv(_ATTENDANCE)
    print(f"Loaded {len(attendance):,} attendance rows")

    att_keyed = normalise_df_td_name(attendance, "full_name")

    member_ref = _build_member_ref(_MEMBERS_CSV)
    print(f"Member reference: {len(member_ref):,} unique join keys")

    enriched = att_keyed.join(
        member_ref,
        on="join_key",
        how="left",
    ).drop("join_key")

    enriched = enriched.with_columns(
        pl.col("unique_member_code").fill_null("")
    )

    matched   = enriched.filter(pl.col("unique_member_code") != "").height
    unmatched = len(enriched) - matched
    print(f"Matched: {matched:,} rows | Unmatched: {unmatched:,} rows (pre-34th Dail, by design)")

    col_order = [c for c in attendance.columns if c != "unique_member_code"]
    col_order = ["unique_member_code"] + col_order
    enriched  = enriched.select([c for c in col_order if c in enriched.columns])

    enriched.write_csv(_OUT_CSV)
    print(f"Written enriched CSV -> {_OUT_CSV}")

    enriched.write_parquet(_OUT_PARQUET)
    print(f"Written parquet -> {_OUT_PARQUET}")

    enriched.head(200).write_csv(_PREVIEW_CSV)
    print(f"Preview CSV -> {_PREVIEW_CSV}")
    print("Done. Validate the preview CSV before treating outputs as authoritative.")


if __name__ == "__main__":
    run()
