"""Enrich payments_full_psa.parquet with unique_member_code, party_name, constituency.

Closes the bug where Member Overview's payments hero stat showed "Not on file"
for every TD because v_payments_base (sql_views/payments_base.sql) projected
unique_member_code as NULL — `SELECT SUM(amount_num) WHERE unique_member_code = ?`
could never match.

Match strategy: sort-letters fuzzy key via normalise_join_key.normalise_df_td_name,
which collapses "Last, First" ↔ "First Last" ↔ accents ↔ apostrophes by
lowercasing → NFD-stripping → removing non-alpha → sorting characters.

Coverage (verified 2026-05-31): 172 of 176 current TDs match. The 4 misses are
upstream name-shape mismatches between the Oireachtas members API and the PSA
payment publications:

  - Daniel Ennis           : zero payment rows on PSA register
  - Frankie Feighan        : PSA publishes as "Frank Feighan" (nickname)
  - Paul Nicholas Gogarty  : PSA publishes as "Paul Gogarty" (no middle name)
  - Conor D McGuinness     : PSA publishes as "Conor McGuinness" (no initial)

These four retain NULL unique_member_code and continue to show "Not on file"
on Member Overview — same behaviour as before this fix, but now affecting 4
members instead of all 176. Resolving them requires either a curated alias
table or a surname-disambiguated fallback matcher; deferred until requested.

Historic payment rows (former TDs no longer in the registry) also retain NULL.
That's correct — they have no Member Overview page to surface them on.

Run: `python payments_member_enrichment.py`
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import polars as pl

from config import GOLD_PARQUET_DIR, SILVER_PARQUET_DIR
from paths import PROJECT_ROOT as REPO_ROOT
from services.parquet_io import save_parquet

sys.path.insert(0, str(REPO_ROOT))

from shared.normalise_join_key import normalise_df_td_name  # noqa: E402

PAYMENTS_PARQUET = GOLD_PARQUET_DIR / "payments_full_psa.parquet"
MEMBERS_PARQUET = SILVER_PARQUET_DIR / "flattened_members.parquet"


def _build_member_lookup(members_parquet: Path = MEMBERS_PARQUET) -> pl.DataFrame:
    """One row per member: join_key → (unique_member_code, party, constituency)."""
    members = pl.read_parquet(members_parquet).select(["unique_member_code", "full_name", "party", "constituency_name"])
    normalised = normalise_df_td_name(members, "full_name")
    lookup = normalised.select(
        [
            pl.col("join_key"),
            pl.col("unique_member_code"),
            pl.col("party").alias("party_name_enriched"),
            pl.col("constituency_name").alias("constituency_enriched"),
        ]
    )
    # Guard: member-side collisions would make the lookup ambiguous. Verified
    # zero collisions on 2026-05-31; fail loud if a future members file ever
    # introduces them rather than silently joining payments to the wrong TD.
    dup_keys = lookup.group_by("join_key").len().filter(pl.col("len") > 1)
    if dup_keys.height:
        raise RuntimeError(
            f"member-side join_key collisions detected ({dup_keys.height} groups) — "
            f"refusing to enrich payments because the join would be ambiguous. "
            f"Sample: {dup_keys.head(3).to_dicts()}"
        )
    return lookup


def enrich(payments_parquet: Path = PAYMENTS_PARQUET, members_parquet: Path = MEMBERS_PARQUET) -> dict:
    """Read payments parquet, attach member metadata, write back. Returns coverage stats.

    Defaults reproduce the Dáil behaviour. The Senator chain passes the Senator
    payments + Senator members parquets to reuse the same fuzzy-key match.
    """
    if not payments_parquet.exists():
        raise FileNotFoundError(f"payments parquet not found: {payments_parquet}")
    if not members_parquet.exists():
        raise FileNotFoundError(f"members parquet not found: {members_parquet}")

    payments = pl.read_parquet(payments_parquet)
    n_rows_before = payments.height

    # Idempotency: if the parquet already has these enrichment columns from a
    # previous run, drop them so the join re-applies cleanly (the upstream ETL
    # may have overwritten the parquet without them since the last enrichment).
    payments = payments.drop([c for c in ("unique_member_code", "party_name", "constituency") if c in payments.columns])

    # Derive the same sorted-letters key on the payments side.
    payments_keyed = normalise_df_td_name(payments, "member_name")

    lookup = _build_member_lookup(members_parquet)
    enriched = (
        payments_keyed.join(lookup, on="join_key", how="left")
        .rename({"party_name_enriched": "party_name", "constituency_enriched": "constituency"})
        .drop("join_key")
    )

    if enriched.height != n_rows_before:
        raise RuntimeError(
            f"row count changed during enrichment ({n_rows_before} → {enriched.height}) — "
            f"would indicate an ambiguous many-to-many join that the lookup-side dedupe missed"
        )

    n_matched = enriched.filter(pl.col("unique_member_code").is_not_null()).height
    n_distinct_matched = enriched.filter(pl.col("unique_member_code").is_not_null())["unique_member_code"].n_unique()
    n_distinct_unmatched_names = enriched.filter(pl.col("unique_member_code").is_null())["member_name"].n_unique()

    # Atomic write + zstd convention, centralised in services.parquet_io.
    save_parquet(enriched, payments_parquet)

    return {
        "rows_total": n_rows_before,
        "rows_matched": n_matched,
        "rows_unmatched": n_rows_before - n_matched,
        "members_resolved": n_distinct_matched,
        "distinct_unmatched_names": n_distinct_unmatched_names,
    }


def main() -> int:
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("payments_member_enrichment")
    stats = enrich()
    pct = 100 * stats["rows_matched"] / max(stats["rows_total"], 1)
    logging.info("payments enrichment complete:")
    logging.info("  rows total            = %s", f"{stats['rows_total']:,}")
    logging.info("  rows matched          = %s (%.1f%%)", f"{stats['rows_matched']:,}", pct)
    logging.info("  rows unmatched        = %s", f"{stats['rows_unmatched']:,}")
    logging.info("  current TDs resolved  = %s", f"{stats['members_resolved']:,}")
    logging.info("  distinct unmatched    = %s", f"{stats['distinct_unmatched_names']:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
