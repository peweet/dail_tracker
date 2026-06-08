"""LRC enrichment SUMMARY builder — PROMOTED gold writer (PR1 scope).

Turns the raw classlist occurrences into a clean, deterministic, one-row-per-SI
enrichment GOLD table over our SI gold. Read by v_si_lrc_enrichment
(sql_views/legislation_si_lrc_enrichment.sql) and joined into
v_statutory_instruments_classified for the SI page's subject chip + topic browse.
Run after si_lrc_classlist_extract.py; wired into the iris chain as
iris_refresh.step_si_lrc_enrichment ([8/8]).

SAFE LANGUAGE (locked by test_si_lrc_enrichment.py + the SQL-view enum test):
  - status is matched_classified_list | not_matched  — NEVER "in_force".
  - "not_matched" does NOT mean the SI is not in force; it means the LRC
    Classified List does not list it (it may be ephemeral, spent, or simply
    not yet classified). A match is a source-linked classification, not a
    legal-status assertion.
  - match_method is exact_number_year only in PR1 (no fuzzy/title matching).

Grain: one row per SI in our gold (matched and unmatched both present).

Reads : extractors/_lrc_output/si_lrc_classlist_raw.parquet
        data/gold/parquet/statutory_instruments.parquet
Writes: data/gold/parquet/si_lrc_enrichment_summary.parquet   (git-tracked gold)
        data/_meta/si_lrc_enrichment_summary_coverage.json
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

LRC = ROOT / "extractors/_lrc_output/si_lrc_classlist_raw.parquet"
GOLD = ROOT / "data/gold/parquet/statutory_instruments.parquet"
OUT = ROOT / "data/gold/parquet/si_lrc_enrichment_summary.parquet"
COVERAGE = ROOT / "data/_meta/si_lrc_enrichment_summary_coverage.json"

# Catch-all subheadings that are poor browse landing pages — kept, but never
# chosen as the *primary* leaf if a more specific one exists.
CATCH_ALL_LEAVES = {"ECA Section 3 Statutory Instruments", "General"}

CAVEAT = (
    "Listed by the LRC Classified List of in-force legislation under this "
    "subject, subject to LRC accuracy warnings and number/year match. This is "
    "a source-linked research aid, not legal advice. 'Not matched' does not "
    "mean the SI is not in force."
)


def build() -> pl.DataFrame:
    lrc = pl.read_parquet(LRC).filter(pl.col("si_number").is_not_null() & pl.col("si_year").is_not_null())
    # collapse exact-duplicate occurrences (same SI + subject + path)
    lrc = lrc.unique(subset=["si_year", "si_number", "lrc_subject_heading", "lrc_subheading_path_num"])

    # rank leaves so a specific leaf beats a catch-all when picking "primary"
    lrc = lrc.with_columns(pl.col("lrc_subheading_leaf").is_in(list(CATCH_ALL_LEAVES)).alias("_is_catch_all"))
    by_si = (
        # Sort specific leaves first; the trailing keys are tie-breakers so the
        # .first() picks below (primary subject/leaf) are deterministic across
        # runs rather than depending on input/group iteration order.
        lrc.sort(["_is_catch_all", "lrc_subheading_path_num", "lrc_subject_heading", "lrc_subheading_leaf"])
        .group_by("si_year", "si_number")
        .agg(
            pl.col("lrc_subject_heading").unique().sort().alias("lrc_subjects"),
            pl.col("lrc_subheading_leaf").unique().sort().alias("lrc_leaves"),
            pl.col("lrc_subheading_path_name").unique().sort().alias("lrc_paths"),
            pl.col("lrc_subject_heading").first().alias("lrc_primary_subject"),
            # first leaf after the catch-all sort = most specific available
            pl.col("lrc_subheading_leaf").first().alias("lrc_primary_leaf"),
            pl.col("lrc_eisb_url").first().alias("lrc_eisb_url"),
            pl.col("lrc_list_updated_to").first().alias("lrc_list_updated_to"),
        )
        .with_columns(pl.col("lrc_subjects").list.len().alias("lrc_n_subjects"))
    )

    gold = pl.read_parquet(GOLD).select("si_year", "si_number", "si_title", "si_policy_domain", "eisb_url")
    summary = gold.join(by_si, on=["si_year", "si_number"], how="left")

    matched = pl.col("lrc_subjects").is_not_null()
    summary = summary.with_columns(
        pl.format("{}/{}", pl.col("si_number"), pl.col("si_year")).alias("si_number_year"),
        matched.alias("has_lrc_classified_list_match"),
        pl.when(matched)
        .then(pl.lit("matched_classified_list"))
        .otherwise(pl.lit("not_matched"))
        .alias("lrc_enrichment_status"),
        pl.when(matched).then(pl.lit("exact_number_year")).otherwise(None).alias("match_method"),
        pl.when(matched).then(pl.lit(1.0)).otherwise(None).alias("match_confidence"),
        # the concrete data-quality win: SI had no topic, LRC supplies one
        (matched & pl.col("si_policy_domain").is_null()).alias("lrc_fills_empty_domain"),
        pl.when(matched).then(pl.lit(CAVEAT)).otherwise(None).alias("lrc_caveat"),
    )
    # Deterministic row order so the git-tracked gold parquet doesn't churn
    # run-to-run. (A per-row build timestamp was removed for the same reason —
    # build-time provenance lives in the run manifest + coverage JSON, not in
    # every gold row; nothing read source_built_at.)
    return summary.sort(["si_year", "si_number"])


def main() -> None:
    df = build()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT)

    matched = df.filter(pl.col("has_lrc_classified_list_match"))
    cov = {
        "dataset": "si_lrc_enrichment_summary",
        "source": "Law Reform Commission — Classified List of In-Force Legislation",
        "source_urls": ["https://revisedacts.lawreform.ie/classlist/intro"],
        "lrc_list_updated_to": (matched["lrc_list_updated_to"].drop_nulls().first()),
        "row_count": df.height,
        "matched_si_count": matched.height,
        "match_rate": round(matched.height / df.height, 4),
        "fills_empty_domain_count": int(df["lrc_fills_empty_domain"].sum()),
        "distinct_subjects": int(matched["lrc_primary_subject"].n_unique()),
        "distinct_leaves": int(matched["lrc_primary_leaf"].n_unique()),
        "coverage_note": "Source-linked research aid; may be incomplete or out of date.",
        "legal_caveat": "Not legal advice. 'Not matched' does not mean 'not in force'.",
    }
    COVERAGE.write_text(json.dumps(cov, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"wrote {OUT.relative_to(ROOT)}  rows={df.height}")
    print(json.dumps(cov, indent=2, ensure_ascii=False))
    print("\nsample matched rows:")
    print(matched.select("si_number_year", "lrc_primary_subject", "lrc_primary_leaf", "lrc_enrichment_status").head(6))


if __name__ == "__main__":
    main()
