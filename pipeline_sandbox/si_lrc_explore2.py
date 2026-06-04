"""LRC enrichment — SECOND PASS exploration (sandbox, read-only).

Three deeper questions the first spike didn't answer:
  A. What does SI gold ALREADY hold? (full null profile — esp. si_parent_legislation,
     the SI->Act link the brief wants LRC Revised Acts to supply.)
  B. WHY do 9.9% of SIs not match? (ephemeral vs parse-fail vs not-listed) +
     what are the 797 unparsed LRC entries?
  C. CROSS-VALIDATE: LRC lists only IN-FORCE legislation. si_current_state marks
     revoked/amended. Disagreements (LRC=in-force but we say revoked, or vice
     versa) are a dual-source data-quality signal.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

LRC_RAW = ROOT / "pipeline_sandbox/_lrc_output/si_lrc_classlist_raw.parquet"
GOLD = ROOT / "data/gold/parquet/statutory_instruments.parquet"
STATE = ROOT / "data/gold/parquet/si_current_state.parquet"


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def main() -> None:
    gold = pl.read_parquet(GOLD)
    lrc = pl.read_parquet(LRC_RAW)
    lrc_keyed = lrc.filter(pl.col("si_number").is_not_null() & pl.col("si_year").is_not_null())

    # ----------------------------------------------------------------- A. gold null profile
    hr("A. SI GOLD — null profile (what's already populated?)")
    prof = (
        gold.null_count()
        .transpose(include_header=True, header_name="col", column_names=["nulls"])
        .with_columns((pl.col("nulls") / gold.height).round(3).alias("null_rate"))
        .sort("null_rate")
    )
    for r in prof.iter_rows(named=True):
        bar = "█" * int(r["null_rate"] * 30)
        print(f"  {r['col']:28s} null={r['null_rate']:.3f} {bar}")

    hr("A2. si_parent_legislation — do we ALREADY have the SI->Act link?")
    pl_col = "si_parent_legislation"
    have = gold.filter(pl.col(pl_col).is_not_null() & (pl.col(pl_col).str.strip_chars() != ""))
    print(f"si_parent_legislation populated: {have.height}/{gold.height} ({have.height/gold.height:.1%})")
    print("samples:")
    for r in have.select("si_title", pl_col).head(6).iter_rows(named=True):
        print(f"  - {r['si_title'][:46]:46s} -> {str(r[pl_col])[:50]}")

    # ----------------------------------------------------------------- B. non-match diagnosis
    hr("B. NON-MATCH DIAGNOSIS")
    lrc_keys = lrc_keyed.select("si_year", "si_number").unique()
    g = gold.join(lrc_keys.with_columns(pl.lit(True).alias("in_lrc")), on=["si_year", "si_number"], how="left")
    unmatched = g.filter(pl.col("in_lrc").is_null())
    print(f"unmatched SIs: {unmatched.height}")
    # hypothesis 1: ephemeral instrument types (commencement/appointment/fee orders)
    # are spent and so NOT in an in-force list. classify by title keyword + si_form.
    def kw(col):
        t = pl.col(col).str.to_lowercase()
        return (
            pl.when(t.str.contains("commencement")).then(pl.lit("commencement order"))
            .when(t.str.contains("appoint")).then(pl.lit("appointment/establishment"))
            .when(t.str.contains("appointed day|vesting day|establishment day")).then(pl.lit("appointed day"))
            .when(t.str.contains(r"\bfees?\b|charges")).then(pl.lit("fees/charges"))
            .when(t.str.contains("revoc")).then(pl.lit("revocation"))
            .otherwise(pl.lit("other"))
        )
    print("\nunmatched by title-type (ephemeral types are expected to be absent from an in-force list):")
    print(unmatched.with_columns(kw("si_title").alias("kind"))["kind"].value_counts(sort=True))
    print("\nMATCHED by same title-type (contrast):")
    matched = g.filter(pl.col("in_lrc").is_not_null())
    print(matched.with_columns(kw("si_title").alias("kind"))["kind"].value_counts(sort=True))

    if "si_form" in gold.columns:
        print("\nunmatched by si_form:")
        print(unmatched["si_form"].value_counts(sort=True).head(10))

    # the 797 unparsed LRC entries: what are they?
    hr("B2. The unparsed LRC entries (no number/year)")
    unp = lrc.filter(pl.col("si_number").is_null() | pl.col("si_year").is_null())
    print(f"unparsed rows: {unp.height}")
    print("sample titles (are these Acts / EU regs / odd formats?):")
    for t in unp["lrc_entry_title"].head(12).to_list():
        print(f"  - {t[:80]}")

    # ----------------------------------------------------------------- C. cross-validate
    hr("C. CROSS-VALIDATE — LRC in-force listing vs si_current_state")
    state = pl.read_parquet(STATE)
    print("si_current_state.current_state values:")
    print(state["current_state"].value_counts(sort=True))
    # one row per SI from each source over the gold window
    st = state.select("si_year", "si_number", "current_state").unique(subset=["si_year", "si_number"])
    lrc_set = lrc_keyed.select("si_year", "si_number").unique().with_columns(pl.lit(True).alias("lrc_in_force_listed"))
    x = (
        gold.select("si_year", "si_number", "si_title")
        .join(st, on=["si_year", "si_number"], how="left")
        .join(lrc_set, on=["si_year", "si_number"], how="left")
        .with_columns(pl.col("lrc_in_force_listed").fill_null(False))
    )
    # DISCREPANCY: we mark revoked (fully) but LRC still lists it as in-force
    revoked_states = ["revoked"]
    disc = x.filter(pl.col("current_state").is_in(revoked_states) & pl.col("lrc_in_force_listed"))
    print(f"\nDISCREPANCY A: we mark REVOKED but LRC still lists as in-force: {disc.height}")
    for r in disc.select("si_number", "si_year", "si_title", "current_state").head(8).iter_rows(named=True):
        print(f"  {r['si_number']}/{r['si_year']}  {r['si_title'][:55]}")
    # CORROBORATION: in-force-as-made AND LRC-listed = two independent sources agree
    agree = x.filter((pl.col("current_state") == "in_force_as_made") & pl.col("lrc_in_force_listed"))
    print(f"\nCORROBORATION: state=in_force_as_made AND LRC-listed: {agree.height} (two sources agree)")
    # coverage of each source
    print(f"\nSIs with a current_state value : {x.filter(pl.col('current_state').is_not_null()).height}")
    print(f"SIs LRC-listed                 : {x.filter(pl.col('lrc_in_force_listed')).height}")
    print(f"SIs in NEITHER source          : {x.filter(pl.col('current_state').is_null() & ~pl.col('lrc_in_force_listed')).height}")


if __name__ == "__main__":
    main()
