"""iris_silver_rebuild.py — delta-rebuild Iris silver/gold notice CSVs from bronze.

Reads the existing bronze CSV (iris_raw_lines_pymupdf.csv), extracts bronze
rows from any PDFs in data/bronze/iris_oifigiuil/ whose source_file isn't
yet in the bronze CSV, appends them, then regenerates the silver/gold notice
CSVs via the standard ETL chain (build_records → enrich_records →
add_quarantine).

This is the lightweight refresh path when the per-PDF shard cache
(`data/silver/iris_oifigiuil_shards`) isn't available — same effect as
re-running iris_oifigiuil_etl_polars.run() but without the shard machinery.
It produces identical silver CSVs to a full ETL re-run for the rows in
common, and adds any new bronze lines from newly-fetched PDFs.

CLI:
    python iris_silver_rebuild.py
    python iris_silver_rebuild.py --threshold 0.75

Programmatic:
    from iris_silver_rebuild import rebuild_silver_from_bronze
    summary = rebuild_silver_from_bronze()
"""
from __future__ import annotations

import argparse
from pathlib import Path

import polars as pl

from iris_oifigiuil_etl_polars import (
    add_quarantine,
    build_bronze_frame,
    build_records,
    enrich_records,
    extract_lines_raw,
    shape_for_gold,
)

_ROOT = Path(__file__).resolve().parent
_OUT = _ROOT / "data" / "silver" / "iris_oifigiuil"
_BRONZE_CSV = _OUT / "iris_raw_lines_pymupdf.csv"
_PDFS = _ROOT / "data" / "bronze" / "iris_oifigiuil"
_DEFAULT_THRESHOLD = 0.75

_OUTPUT_FILES = [
    "iris_notice_events_all.csv",
    "iris_notice_events_clean.csv",
    "iris_notice_events_quarantined.csv",
    "iris_si_taxonomy.csv",
]


def _rows(p: Path) -> int:
    return pl.read_csv(p, infer_schema_length=20000).height if p.exists() else -1


def rebuild_silver_from_bronze(threshold: float = _DEFAULT_THRESHOLD) -> dict:
    """Append-and-rebuild. Returns a per-file before→after row-count summary."""
    before = {f: _rows(_OUT / f) for f in _OUTPUT_FILES}

    print("  [silver] reading existing bronze CSV...")
    bronze = pl.read_csv(_BRONZE_CSV, infer_schema_length=20000)
    have = set(bronze["source_file"].unique().to_list())
    print(f"    existing bronze: {bronze.height:,} lines from {len(have):,} PDFs")

    all_pdfs = sorted(_PDFS.glob("*.pdf"))
    new_pdfs = [p for p in all_pdfs if p.name not in have]
    print(f"  [silver] PDFs on disk: {len(all_pdfs):,} | new: {len(new_pdfs)}")
    for p in new_pdfs:
        print(f"    + {p.name}")

    if new_pdfs:
        new_raw: list[dict] = []
        for p in new_pdfs:
            rows, _ = extract_lines_raw(str(p))
            new_raw.extend(rows)
        # Run through build_bronze_frame so column shape matches existing bronze.
        new_bronze = build_bronze_frame(pl.DataFrame(new_raw, infer_schema_length=None))
        # Align columns to existing bronze schema (existing CSV is the source of truth).
        common = [c for c in bronze.columns if c in new_bronze.columns]
        new_bronze = new_bronze.select(common)
        # Cast string-typed nullable columns to match (CSV round-trip may shift).
        for c in common:
            if bronze.schema[c] != new_bronze.schema[c]:
                new_bronze = new_bronze.with_columns(pl.col(c).cast(bronze.schema[c], strict=False))
        bronze = pl.concat([bronze, new_bronze], how="vertical_relaxed")
        bronze.write_csv(_BRONZE_CSV)
        print(f"    new bronze lines: {new_bronze.height:,} | combined: {bronze.height:,}")
    else:
        print("    (no new PDFs to extract)")

    print(f"  [silver] rebuilding silver notice CSVs (threshold={threshold})...")
    events = add_quarantine(enrich_records(build_records(bronze)), threshold)
    clean = events.filter(~pl.col("is_quarantined"))
    quarantine = events.filter(pl.col("is_quarantined"))
    si_tax = events.filter(pl.col("notice_category") == "statutory_instrument")

    shape_for_gold(events).write_csv(_OUT / "iris_notice_events_all.csv")
    shape_for_gold(clean).write_csv(_OUT / "iris_notice_events_clean.csv")
    shape_for_gold(quarantine).write_csv(_OUT / "iris_notice_events_quarantined.csv")
    shape_for_gold(si_tax).write_csv(_OUT / "iris_si_taxonomy.csv")

    summary = {}
    print("\n  silver file                              before ->  after")
    for f in _OUTPUT_FILES:
        after = _rows(_OUT / f)
        summary[f] = (before[f], after)
        print(f"    {f:38s} {before[f]:7,d} -> {after:7,d}")
    return summary


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--threshold", type=float, default=_DEFAULT_THRESHOLD,
                    help="extraction-confidence threshold for clean/quarantined split")
    args = ap.parse_args()
    rebuild_silver_from_bronze(args.threshold)


if __name__ == "__main__":
    main()
