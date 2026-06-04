"""LRC enrichment — data-quality checks + topic-search simulation (sandbox).

Two jobs:
  A. Integrity: is the SI gold key (si_year, si_number) unique? (guards the
     90% match rate against double-counting), and are LRC->gold matches 1:1?
  B. User simulation: mimic a citizen browsing/searching SIs by TOPIC and show
     concretely what LRC unlocks that title-search-only cannot.

Read-only. Writes nothing.
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
    lrc = pl.read_parquet(LRC).filter(
        pl.col("si_number").is_not_null() & pl.col("si_year").is_not_null()
    )
    gold = pl.read_parquet(GOLD)

    # ----------------------------------------------------------------- A. integrity
    hr("A. INTEGRITY")
    dup = gold.height - gold.select("si_year", "si_number").n_unique()
    print(f"gold rows                         : {gold.height}")
    print(f"gold distinct (si_year,si_number) : {gold.select('si_year','si_number').n_unique()}")
    print(f"gold DUPLICATE key rows           : {dup}")
    if dup:
        d = (
            gold.group_by("si_year", "si_number")
            .agg(pl.len().alias("n"), pl.col("si_title").first())
            .filter(pl.col("n") > 1)
            .sort("n", descending=True)
        )
        print("  examples:")
        for r in d.head(6).iter_rows(named=True):
            print(f"    {r['si_number']}/{r['si_year']} x{r['n']}: {r['si_title'][:55]}")

    # honest match rate on DISTINCT gold keys
    gold_keys = gold.select("si_year", "si_number").unique()
    lrc_keys = lrc.select("si_year", "si_number").unique()
    matched_keys = gold_keys.join(lrc_keys, on=["si_year", "si_number"], how="inner")
    print(
        f"\nHONEST match rate (distinct keys)  : "
        f"{matched_keys.height}/{gold_keys.height} = {matched_keys.height/gold_keys.height:.1%}"
    )

    # LRC duplicate (same SI, same subject, same subheading) — would double a browse count
    lrc_exact_dup = lrc.height - lrc.select(
        "si_year", "si_number", "lrc_subject_heading", "lrc_subheading_path_num"
    ).n_unique()
    print(f"LRC exact-duplicate occurrence rows: {lrc_exact_dup}  (collapse before counting)")

    # ----------------------------------------------------------------- B. topic search
    hr("B. TOPIC-SEARCH SIMULATION  (citizen browses SIs by subject)")
    # one row per SI with its LRC leaves + the existing domain
    lrc_by_si = lrc.group_by("si_year", "si_number").agg(
        pl.col("lrc_subject_heading").unique().alias("subjects"),
        pl.col("lrc_subheading_leaf").unique().alias("leaves"),
    )
    g = gold.unique(subset=["si_year", "si_number"]).join(
        lrc_by_si, on=["si_year", "si_number"], how="left"
    )

    # A real citizen searches free text. Compare:
    #   (i)  title contains the word   vs   (ii) LRC subject/leaf matches the topic
    # to show LRC catches SIs whose TITLE never mentions the topic word.
    topics = [
        ("dogs / animals", "dog", ["Dogs", "Animal"]),
        ("rented housing", "rent", ["Housing", "Landlord and Tenant", "Residential Tenancies"]),
        ("vaping / tobacco", "tobacco", ["Tobacco", "Health"]),
        ("data protection", "data protection", ["Data Protection"]),
        ("fishing / sea", "fish", ["Sea Fisheries", "Merchant Shipping", "Fisheries"]),
        ("disability", "disab", ["Disability", "Equality"]),
    ]
    for label, word, leaf_terms in topics:
        title_hits = g.filter(pl.col("si_title").str.to_lowercase().str.contains(word.lower()))
        leaf_expr = pl.lit(False)
        for t in leaf_terms:
            leaf_expr = leaf_expr | pl.col("leaves").list.eval(
                pl.element().str.contains("(?i)" + t)
            ).list.any()
        subj_expr = pl.lit(False)
        for t in leaf_terms:
            subj_expr = subj_expr | pl.col("subjects").list.eval(
                pl.element().str.contains("(?i)" + t)
            ).list.any()
        lrc_hits = g.filter((leaf_expr | subj_expr).fill_null(False))
        # SIs LRC surfaces that a title keyword search would MISS
        only_lrc = lrc_hits.join(title_hits.select("si_year", "si_number"), on=["si_year", "si_number"], how="anti")
        print(f"\n  TOPIC: {label!r}")
        print(f"    title-keyword '{word}' hits : {title_hits.height}")
        print(f"    LRC subject/leaf hits       : {lrc_hits.height}")
        print(f"    found ONLY via LRC (title misses the word): {only_lrc.height}")
        for r in only_lrc.select("si_title", "leaves").head(3).iter_rows(named=True):
            lv = ", ".join(r["leaves"]) if r["leaves"] is not None else ""
            print(f"      + {r['si_title'][:54]:54s} [{lv[:40]}]")

    # ----------------------------------------------------------------- B2. browse depth
    hr("B2. BROWSE DEPTH  (how many SIs sit under a citizen-friendly leaf)")
    leaf = g.filter(pl.col("leaves").is_not_null()).explode("leaves")
    vc = leaf["leaves"].value_counts(sort=True)
    print("most populated leaves (good browse landing pages):")
    print(vc.head(12))
    thin = vc.filter(pl.col("count") == 1).height
    print(f"\nsingle-SI leaves (too thin to browse): {thin} of {vc.height}")


if __name__ == "__main__":
    main()
