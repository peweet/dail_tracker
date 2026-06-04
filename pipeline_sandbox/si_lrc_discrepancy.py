"""Investigate the 672 'we mark REVOKED but LRC lists as in-force' discrepancy.

Goal: decide whether the discrepancy is (a) our si_current_state over-marking,
(b) LRC lag/completeness, or (c) semantic nuance — WITHOUT assuming which source
is right. Read-only.
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

GOLD = ROOT / "data/gold/parquet/statutory_instruments.parquet"
STATE = ROOT / "data/gold/parquet/si_current_state.parquet"
LRC_RAW = ROOT / "pipeline_sandbox/_lrc_output/si_lrc_classlist_raw.parquet"


def hr(t):
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def main():
    gold = pl.read_parquet(GOLD)
    state = pl.read_parquet(STATE)
    lrc = pl.read_parquet(LRC_RAW).filter(
        pl.col("si_number").is_not_null() & pl.col("si_year").is_not_null()
    )
    lrc_set = lrc.select("si_year", "si_number").unique().with_columns(pl.lit(True).alias("lrc_listed"))

    st = state.unique(subset=["si_year", "si_number"])
    disc = (
        gold.select("si_year", "si_number", "si_title")
        .join(st, on=["si_year", "si_number"], how="inner")
        .join(lrc_set, on=["si_year", "si_number"], how="left")
        .filter((pl.col("current_state") == "revoked") & pl.col("lrc_listed").fill_null(False))
    )
    hr(f"672 check — discrepancy rows = {disc.height}")

    print("confidence of these revocations (low confidence => our call is shaky):")
    print(disc["confidence"].describe())

    print("\nstate_source breakdown:")
    print(disc["state_source"].value_counts(sort=True))

    print("\nhow_affected_raw — top phrasings (what made us say revoked?):")
    print(
        disc.with_columns(pl.col("how_affected_raw").str.slice(0, 45).alias("haf"))["haf"]
        .value_counts(sort=True)
        .head(12)
    )

    # KEY TEST: when did the revoking happen? If the revoking SI is NEWER than the
    # LRC "updated to" date, LRC simply hasn't caught up (LRC lag, not our error).
    # affecting_si_urls / affecting_sis hold the revoker; pull its year from text.
    hr("Revoker recency — is LRC just lagging?")
    disc2 = disc.with_columns(
        pl.col("affecting_sis").list.eval(pl.element().str.extract(r"of (\d{4})", 1).cast(pl.Int32)).alias("revoker_years")
    ).with_columns(pl.col("revoker_years").list.max().alias("latest_revoker_year"))
    print("latest revoker year distribution among the 672:")
    print(disc2["latest_revoker_year"].value_counts(sort=True).sort("latest_revoker_year"))

    hr("3 concrete cases (verify by hand against the live sources)")
    for r in disc.sort("si_year", descending=True).head(3).iter_rows(named=True):
        print(f"\nSI {r['si_number']}/{r['si_year']}: {r['si_title']}")
        print(f"  current_state : {r['current_state']}  (confidence {r['confidence']})")
        print(f"  how_affected  : {str(r['how_affected_raw'])[:120]}")
        print(f"  affecting_sis : {r['affecting_sis']}")
        print(f"  state_source  : {r['state_source_url']}")
        lr = lrc.filter((pl.col('si_year') == r['si_year']) & (pl.col('si_number') == r['si_number']))
        print(f"  LRC says      : subject={lr['lrc_subject_heading'].to_list()} leaf={lr['lrc_subheading_leaf'].to_list()}")


if __name__ == "__main__":
    main()
