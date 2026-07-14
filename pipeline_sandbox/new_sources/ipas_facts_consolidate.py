"""Consolidate every per-document extraction into ONE canonical fact table.

This is the point of the canonical schema in ipas_doc_registry.py: each document was
extracted by its own script, but they all emit the same shape, so they union into a
single `ipas_facts` table that the UI (and any future loop) can read as one thing.

It also runs a QUALITY GUARD over the union — in the spirit of tools/check_extraction_quality.py:
row counts alone hide silent field degradation, so we check the FIELD-LEVEL health that
actually matters here (provenance completeness, unknown rate, category validity, the
never-sum invariant) and FAIL LOUDLY rather than quietly shipping a degraded corpus.

Nothing is invented in consolidation: source tables are mapped, never re-derived. A
column a source genuinely lacks becomes null, not a guess.
"""
from __future__ import annotations

import polars as pl

from _common import SILVER, now_iso
from ipas_doc_registry import CATEGORIES

CANON = [
    "fact_id", "doc_key", "doc_title", "page", "printed_page", "ref", "section",
    "category", "subject", "metric", "value_numeric", "value_text", "unit",
    "qualifier", "period", "scope", "is_unknown", "unknown_reason", "notes",
    "source_url", "source_document_hash", "extraction_method", "confidence",
    "privacy_tier", "value_safe_to_sum",
]

# source table -> (doc_key, doc_title). Each is mapped onto CANON below.
SOURCES = {
    "cag_ipas_chapter_figures": ("cag_roaps_2024_ch10", "C&AG RoAPS 2024 Ch.10 — IP accommodation contracts"),
    "cag_ipas_chart_recovery": ("cag_roaps_2024_ch10", "C&AG RoAPS 2024 Ch.10 — chart/glyph recovery"),
    "hiqa_ipas_figures": ("hiqa_ipas_overview_2024", "HIQA — Monitoring of IPAS centres in 2024 (overview)"),
    "hiqa_centre_facts": ("hiqa_inspection_reports", "HIQA — 101 individual centre inspection reports"),
    "igees_ipas_facts": ("igees_ipas_paper_2025", "IGEES — Managing IPAS Expenditure Pressures (Jun 2025)"),
    "cag_2015_direct_provision_facts": ("cag_roaps_2015_ch06", "C&AG 2015 Ch.6 — direct provision contracts"),
    # added by the final agent; included if present
    "accommodation_strategy_facts": ("accommodation_strategy", "Comprehensive Accommodation Strategy"),
    "national_standards_facts": ("national_standards", "National Standards (2021)"),
    "pid_facts": ("project_initiation_document", "Project Initiation Document (DCEDIY, 2021)"),
    "ipas_weekly_facts": ("ipas_weekly_stats", "IPAS weekly statistics"),
}

ALIASES = {  # source column -> canonical column
    "para_ref": "ref", "figure_id": "fact_id", "recovery": "section",
    "item": "metric", "status": "value_text", "report": "doc_title",
    "derived_at": None, "fetched_at": None,
}

# Per-document extractors DRIFTED from the canonical vocabulary (each invented its own
# labels). Crosswalk them back — a rename, never a reclassification of meaning. The guard
# below fails if any category survives outside the canonical list, so drift can't rot.
CATEGORY_MAP = {
    "compliance_standard": "compliance",
    "compliance_theme": "compliance",
    "metrics_quality_safety": "compliance",
    "metrics_capacity_capability": "compliance",
    "discussion_findings": "compliance",
    "judgment_descriptors": "standards",
    "resident_experience_adults": "resident_experience",
    "resident_experience_children": "resident_experience",
    "resident_engagement": "resident_experience",
    "accommodation_profile": "residents_centres",
    "centres_estate": "residents_centres",
    "context_national": "residents_centres",
    "monitoring_activity": "inspections",
    "information_received": "inspections",
    "notifications": "inspections",
}

# cag_ipas_chart_recovery carries no `category` — but its `section` (the recovery type)
# determines it deterministically. A mapping, not a guess.
SECTION_TO_CATEGORY = {
    "fig_10_4_supplier_payments": "expenditure",
    "fig_10_3_annual_expenditure": "expenditure",
    "annex_10a_compliance_grid": "due_diligence",
}


def to_canon(df: pl.DataFrame, doc_key: str, doc_title: str, name: str) -> pl.DataFrame:
    df = df.rename({k: v for k, v in ALIASES.items() if k in df.columns and v})
    out = {}
    for c in CANON:
        if c in df.columns:
            out[c] = pl.col(c)
        else:
            dtype = (pl.Float64 if c == "value_numeric"
                     else pl.Int64 if c == "page"
                     else pl.Boolean if c in ("is_unknown", "value_safe_to_sum")
                     else pl.Utf8)
            out[c] = pl.lit(None, dtype=dtype).alias(c)
    d = df.select([e.alias(c) for c, e in out.items()])
    # coerce + backfill the invariants
    d = d.with_columns([
        pl.col("value_numeric").cast(pl.Float64, strict=False),
        pl.col("page").cast(pl.Int64, strict=False),
        pl.col("is_unknown").cast(pl.Boolean, strict=False).fill_null(False),
        # NEVER-SUM INVARIANT: report/audit narrative figures are never summable
        pl.lit(False).alias("value_safe_to_sum"),
        pl.lit(doc_key).alias("doc_key"),
        pl.col("doc_title").fill_null(pl.lit(doc_title)),
    ])
    # stable fact_id
    d = d.with_columns(
        pl.when(pl.col("fact_id").is_null())
          .then(pl.format("{}-{}", pl.lit(doc_key), pl.int_range(pl.len()).cast(pl.Utf8).str.zfill(4)))
          .otherwise(pl.col("fact_id")).alias("fact_id"))
    return d.with_columns(pl.lit(name).alias("source_table"))


def main() -> None:
    frames, missing = [], []
    for name, (dk, dt) in SOURCES.items():
        p = SILVER / f"{name}.parquet"
        if not p.exists():
            missing.append(name)
            continue
        frames.append(to_canon(pl.read_parquet(p), dk, dt, name))

    facts = pl.concat(frames, how="vertical_relaxed").with_columns(
        pl.lit(now_iso()).alias("consolidated_at"))

    # normalise the vocabulary: crosswalk drifted categories, then derive the ones a
    # source left null but whose section determines them
    facts = facts.with_columns(
        pl.col("category").replace(CATEGORY_MAP).alias("category")
    ).with_columns(
        pl.when(pl.col("category").is_null())
          .then(pl.col("section").replace_strict(SECTION_TO_CATEGORY, default=None))
          .otherwise(pl.col("category")).alias("category")
    )
    # any row still without a value AND not flagged unknown is a SILENT GAP — make it
    # explicit rather than let it pass as if it held data
    facts = facts.with_columns([
        pl.when(pl.col("value_numeric").is_null() & pl.col("value_text").is_null()
                & ~pl.col("is_unknown"))
          .then(pl.lit(True)).otherwise(pl.col("is_unknown")).alias("is_unknown"),
        pl.when(pl.col("value_numeric").is_null() & pl.col("value_text").is_null()
                & ~pl.col("is_unknown") & pl.col("unknown_reason").is_null())
          .then(pl.lit("no value captured by the extractor; flagged at consolidation "
                       "rather than passed off as data"))
          .otherwise(pl.col("unknown_reason")).alias("unknown_reason"),
    ])
    out = SILVER / "ipas_facts.parquet"
    facts.write_parquet(out, compression="zstd", statistics=True)
    (SILVER / "_eyeball").mkdir(exist_ok=True)
    facts.write_csv(SILVER / "_eyeball" / "ipas_facts.csv")

    print(f"=== ipas_facts.parquet — {facts.height:,} facts from {len(frames)} tables ===")
    if missing:
        print(f"NOT YET BUILT (will fold in on next run): {', '.join(missing)}")
    with pl.Config(tbl_rows=15, fmt_str_lengths=42):
        print(facts.group_by("doc_key").agg(
            pl.len().alias("facts"), pl.col("is_unknown").sum().alias("unknown")
        ).sort("facts", descending=True))

    # ---------------- QUALITY GUARD ----------------
    print("\n=== QUALITY GUARD ===")
    fails = []
    n = facts.height

    unk = facts["is_unknown"].sum()
    print(f"  unknown rows: {unk:,} ({unk/n*100:.1f}%) — explicit, never guessed")

    prov = facts.filter(pl.col("source_url").is_null()).height
    print(f"  rows missing source_url: {prov}")
    if prov:
        fails.append(f"{prov} rows have no source_url (provenance is non-negotiable)")

    bad_cat = (facts.filter(pl.col("category").is_not_null() &
                            ~pl.col("category").is_in(CATEGORIES))
                    ["category"].unique().to_list())
    print(f"  categories outside the canonical list: {bad_cat or 'none'}")
    if bad_cat:
        fails.append(f"category drift not crosswalked: {bad_cat}")

    nocat = facts.filter(pl.col("category").is_null()).height
    print(f"  rows with NO category: {nocat}")
    if nocat:
        fails.append(f"{nocat} rows are unclassified — every fact must carry a category")

    unsummable = facts.filter(pl.col("value_safe_to_sum")).height
    print(f"  rows marked value_safe_to_sum=True: {unsummable} (MUST be 0)")
    if unsummable:
        fails.append("never-sum invariant violated")

    # a value must be present unless the row is explicitly unknown
    ghost = facts.filter(~pl.col("is_unknown") &
                         pl.col("value_numeric").is_null() &
                         pl.col("value_text").is_null()).height
    print(f"  rows with NO value and NOT flagged unknown: {ghost}")
    if ghost:
        print("     ^ these are silent gaps — they should be is_unknown=True with a reason")

    dupes = facts.height - facts.select("fact_id").unique().height
    print(f"  duplicate fact_id: {dupes}")

    print(f"\n  categories covered: {facts['category'].n_unique()} of {len(CATEGORIES)}")
    with pl.Config(tbl_rows=30):
        print(facts.group_by("category").len().sort("len", descending=True))

    print("\n" + ("QUALITY GUARD: FAIL — " + "; ".join(fails) if fails
                  else "QUALITY GUARD: PASS"))


if __name__ == "__main__":
    main()
