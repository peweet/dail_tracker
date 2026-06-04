"""LRC enrichment — the VERDICT analysis (sandbox, read-only).

Maps the parsed LRC Classified List onto our SI gold and answers the only
questions that decide whether this enrichment is worth shipping:

  1. MATCH RATE  — of our 5,924 SIs (2016-26), how many get an LRC subject?
  2. GAP FILL    — of the 972 SIs with NULL si_policy_domain today, how many
                   does LRC classify? (the clearest data-quality win)
  3. TAXONOMY    — is the LRC subject a finer/different lens than si_policy_domain?
  4. MULTI-LABEL — how often is an SI filed under >1 subject? (richer browse)
  5. NOISE       — the unparsed rows, and any number/year collisions.

Reads only: pipeline_sandbox/_lrc_output/si_lrc_classlist_raw.parquet
            data/gold/parquet/statutory_instruments.parquet
Writes nothing to gold.
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

LRC = ROOT / "pipeline_sandbox/_lrc_output/si_lrc_classlist_raw.parquet"
GOLD = ROOT / "data/gold/parquet/statutory_instruments.parquet"


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def main() -> None:
    lrc = pl.read_parquet(LRC)
    gold = pl.read_parquet(GOLD)

    # --- clean LRC to (si_year, si_number) keyed, drop unparsed, dedup occurrences
    lrc_keyed = lrc.filter(pl.col("si_number").is_not_null() & pl.col("si_year").is_not_null())
    unparsed = lrc.height - lrc_keyed.height
    gyr_min, gyr_max = gold["si_year"].min(), gold["si_year"].max()

    hr("0. Shape")
    print(f"LRC occurrence rows           : {lrc.height}")
    print(f"  unparsed (no number/year)   : {unparsed}  ({unparsed/lrc.height:.1%})")
    print(f"LRC distinct SIs (all years)  : {lrc_keyed.select('si_year','si_number').n_unique()}")
    lrc_in_window = lrc_keyed.filter(pl.col("si_year").is_between(gyr_min, gyr_max))
    print(f"LRC distinct SIs in {gyr_min}-{gyr_max}  : {lrc_in_window.select('si_year','si_number').n_unique()}")
    print(f"SI gold rows ({gyr_min}-{gyr_max})       : {gold.height}")

    # --- 1. MATCH RATE: gold LEFT JOIN lrc (collapse LRC to one row per SI: list of subjects)
    lrc_by_si = (
        lrc_keyed.group_by("si_year", "si_number")
        .agg(
            pl.col("lrc_subject_heading").unique().alias("lrc_subjects"),
            pl.col("lrc_subheading_leaf").unique().alias("lrc_leaves"),
            pl.col("lrc_subheading_path_name").unique().alias("lrc_paths"),
            pl.len().alias("lrc_occurrences"),
        )
        .with_columns(pl.col("lrc_subjects").list.len().alias("n_subjects"))
    )
    j = gold.join(lrc_by_si, on=["si_year", "si_number"], how="left")
    matched = j.filter(pl.col("lrc_subjects").is_not_null())
    hr("1. MATCH RATE  (does our SI get an LRC subject?)")
    print(f"matched SIs : {matched.height} / {gold.height}  =  {matched.height/gold.height:.1%}")
    print("\nmatch rate by year:")
    by_year = (
        j.with_columns(pl.col("lrc_subjects").is_not_null().alias("m"))
        .group_by("si_year")
        .agg(pl.len().alias("n"), pl.col("m").sum().alias("matched"))
        .with_columns((pl.col("matched") / pl.col("n")).alias("rate"))
        .sort("si_year")
    )
    for r in by_year.iter_rows(named=True):
        print(f"  {r['si_year']}: {r['matched']:4d}/{r['n']:4d}  {r['rate']:.0%}")

    # --- 2. GAP FILL: SIs with NULL si_policy_domain that LRC classifies
    hr("2. GAP FILL  (LRC classifies SIs our taxonomy left NULL)")
    null_dom = j.filter(pl.col("si_policy_domain").is_null())
    null_dom_matched = null_dom.filter(pl.col("lrc_subjects").is_not_null())
    print(f"SIs with NULL si_policy_domain         : {null_dom.height}")
    print(f"  ...that LRC DOES classify            : {null_dom_matched.height}  ({null_dom_matched.height/max(null_dom.height,1):.1%})")
    print("  examples (title -> LRC subject):")
    for r in null_dom_matched.select("si_title", "lrc_subjects").head(8).iter_rows(named=True):
        print(f"    - {r['si_title'][:58]:58s} -> {', '.join(r['lrc_subjects'])}")

    # --- 3. TAXONOMY granularity
    hr("3. TAXONOMY  (LRC subheadings = finer lens than 18 policy domains)")
    print(f"existing si_policy_domain distinct values : {gold['si_policy_domain'].n_unique()}")
    print(f"LRC subject headings                      : {lrc['lrc_subject_heading'].n_unique()}")
    print(f"LRC subheading leaves (matched SIs)       : {matched['lrc_leaves'].explode().n_unique()}")
    print("\ntop LRC subheading leaves among OUR matched SIs:")
    leaf = matched.select("si_year", "si_number", "lrc_leaves").explode("lrc_leaves")
    print(leaf["lrc_leaves"].value_counts(sort=True).head(15))

    # --- 4. MULTI-LABEL
    hr("4. MULTI-LABEL  (SIs filed under >1 subject = richer browse)")
    print(matched["n_subjects"].value_counts(sort=True).sort("n_subjects"))
    multi = matched.filter(pl.col("n_subjects") > 1)
    print(f"\nmulti-subject SIs: {multi.height} ({multi.height/matched.height:.1%} of matched)")
    for r in multi.select("si_title", "lrc_subjects").head(5).iter_rows(named=True):
        print(f"    - {r['si_title'][:48]:48s} -> {', '.join(r['lrc_subjects'])}")

    # --- 5. cross-tab: where existing domain disagrees / LRC refines
    hr("5. CROSS-CHECK  (existing domain vs LRC subject on matched SIs)")
    xt = (
        matched.filter(pl.col("si_policy_domain").is_not_null())
        .with_columns(pl.col("lrc_subjects").list.first().alias("lrc_primary"))
        .group_by("si_policy_domain", "lrc_primary")
        .agg(pl.len().alias("n"))
        .sort("n", descending=True)
    )
    print("top (existing_domain, lrc_primary_subject) pairs:")
    print(xt.head(15))


if __name__ == "__main__":
    main()
