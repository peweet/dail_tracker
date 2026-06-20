"""Build the off-box OCR queue for the ministerial-diary corpus.

CAUTION (why this only QUEUES, never runs OCR): PaddleOCR/Tesseract SIPO-style ETLs
HARD-CRASH the local Windows box ([[feedback_paddleocr_crashes_local_box]]) — OCR must
run OFF-BOX. This step is the safe, network-free half: read the committed sandbox index
(extractors/ministerial_diaries_extract.py output), pull out every PDF that is an image
scan, and emit a clean work-list so the OCR can run elsewhere (then the silver is copied
back + promoted).

It separates the THREE non-parsed buckets so we don't send born-digital files to OCR:
  * scanned_needs_offbox_ocr  -> OCR queue (genuinely image-only, the only OCR work)
  * download_failed           -> retry-download queue (WAF-throttled; paced re-fetch, NO OCR)
  * text_layout_unrecognised  -> parser-fix queue (born-digital, parser gap, NO OCR)

Output -> data/_meta/ministerial_diaries_ocr_queue.csv (git-tracked work-list).

Run: .venv/Scripts/python.exe extractors/diary_build_ocr_queue.py
"""

from __future__ import annotations

import logging
from pathlib import Path

import polars as pl

from services.logging_setup import setup_standalone_logging

log = logging.getLogger(__name__)

INDEX = Path("data/sandbox/enrichment/ministerial_diaries_index.parquet")
OUT = Path("data/_meta/ministerial_diaries_ocr_queue.csv")

_ACTION = {
    "scanned_needs_offbox_ocr": "ocr",
    "download_failed": "retry_download",
    "text_layout_unrecognised": "parser_fix",
}


def main() -> int:
    setup_standalone_logging("diary_build_ocr_queue")
    if not INDEX.exists():
        log.error("missing index %s — run ministerial_diaries_extract.py first", INDEX)
        return 1

    idx = pl.read_parquet(INDEX)
    queue = (
        idx.filter(pl.col("parse_status").is_in(list(_ACTION)))
        .with_columns(pl.col("parse_status").replace_strict(_ACTION, default="review").alias("action"))
        .select(
            "action",
            "department",
            "file_name",
            "file_url",
            "listing_url",
            "period_year_guess",
            "period_month_guess",
            "n_pages",
            "parse_status",
        )
        .sort(["action", "department", "file_name"])
        .unique(subset="file_url", keep="first")
        .sort(["action", "department", "file_name"])
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    queue.write_csv(OUT)

    summary = queue.group_by("action").agg(pl.len().alias("files")).sort("files", descending=True)
    log.info("OCR/remediation queue -> %s (%d files)", OUT, len(queue))
    for r in summary.iter_rows(named=True):
        log.info("  %-15s %d files", r["action"], r["files"])
    # OCR-only breakdown by department (the actual off-box workload)
    ocr_by_dept = (
        queue.filter(pl.col("action") == "ocr").group_by("department").agg(pl.len().alias("files")).sort("files", descending=True)
    )
    log.info("OCR files by department: %s", {r["department"]: r["files"] for r in ocr_by_dept.iter_rows(named=True)})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
