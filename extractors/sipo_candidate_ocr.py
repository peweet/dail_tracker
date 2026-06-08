"""OCR stage for the per-candidate GE2024 expense statements.

Companion to `sipo_candidate_expenses_crawl.py` (which downloads the PDFs +
`_manifest.csv`). This runs the EXPENSIVE, parser-independent step: render each
scanned page @300 DPI, PaddleOCR it, and cache the raw cells per page to
`_ckpt/<candidate_key>/cNNN.json`. The single-candidate form parser is a separate,
cheap pass that reads these cached cells (so parsing can iterate without re-OCR).

Reuses the proven OCR machinery from `sipo_expenses_paddle_etl.py`
(`ocr_page` + the PaddleOCR config + the retry/checkpoint pattern) so this file
owns only the per-candidate driving loop, not the OCR internals.

Scope: by default OCRs the `expense_statement` docs that the crawler downloaded
(it reads the manifest). Resumable: an already-cached page is skipped, so a
re-run continues where a previous run stopped. ~13 pages/PDF × ~606 PDFs ≈ 8k
pages — expect hours; run repeatedly (or in the background) until complete.

Usage:
    python -m extractors.sipo_candidate_ocr --limit 1          # validate on 1 PDF
    python -m extractors.sipo_candidate_ocr --only 768ce-grealish-noel
    python -m extractors.sipo_candidate_ocr                    # whole corpus (resumable)
    python -m extractors.sipo_candidate_ocr --dump <key>       # print cached OCR text
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import tempfile
from pathlib import Path

import fitz

from config import BRONZE_DIR, DATA_DIR
from extractors.sipo_expenses_paddle_etl import ocr_page
from services.logging_setup import setup_standalone_logging

log = logging.getLogger("sipo_candidate_ocr")

MANIFEST = BRONZE_DIR / "sipo_candidate_expenses" / "_manifest.csv"
CKPT_ROOT = DATA_DIR / "silver" / "sipo_candidate" / "_ckpt"
DPI = 300


def _doc_key(row: dict) -> str:
    """Stable per-document key matching the PDF filename stem."""
    return f"{row['candidate_slug']}__{row['media_id']}"


def _jobs(doc_types: set[str], only: str | None, limit: int | None) -> list[dict]:
    """Manifest rows that have a downloaded PDF of a wanted doc_type."""
    rows = list(csv.DictReader(MANIFEST.open(encoding="utf-8")))
    jobs = []
    for r in rows:
        if r["doc_type"] not in doc_types:
            continue
        if r["status"] not in ("DOWNLOADED", "CACHED"):
            continue
        if not r.get("local_path"):
            continue
        if only and only not in (_doc_key(r), r["candidate_slug"]):
            continue
        jobs.append(r)
    jobs.sort(key=lambda r: (r["constituency_slug"], r["candidate_slug"]))
    return jobs[:limit] if limit else jobs


def _resolve_pdf(row: dict) -> Path:
    """Absolute path to a manifest row's PDF (local_path is repo-root-relative)."""
    p = Path(row["local_path"])
    if p.is_absolute():
        return p
    return (BRONZE_DIR.parent / row["local_path"]).resolve()


def _pending_pages(key: str, pdf_path: Path) -> int:
    """How many pages of this document still lack a cNNN.json checkpoint."""
    doc = fitz.open(pdf_path)
    n = doc.page_count
    doc.close()
    ckpt = CKPT_ROOT / key
    if not ckpt.exists():
        return n
    return sum(1 for pno in range(1, n + 1) if not (ckpt / f"c{pno:03}.json").exists())


def ocr_document(ocr, key: str, pdf_path: Path) -> tuple[int, int]:
    """Cache raw OCR cells per page (cNNN.json) with a DPI retry ladder mirroring
    the party extractor. Returns (pages_ocrd_this_run, pages_total)."""
    doc = fitz.open(pdf_path)
    ckpt = CKPT_ROOT / key
    ckpt.mkdir(parents=True, exist_ok=True)
    tmp_png = Path(tempfile.gettempdir()) / f"sipo_cand_{key}.png"
    did = 0
    for pno, page in enumerate(doc, start=1):
        done = ckpt / f"c{pno:03}.json"
        attempt = ckpt / f"a{pno:03}.attempt"
        if done.exists():
            continue
        tries = json.loads(attempt.read_text(encoding="utf-8"))["tries"] if attempt.exists() else []
        if not tries or tries == [300]:
            dpi = 300
        elif tries == [300, 300]:
            dpi = 200
        else:
            log.warning("    %s page %d crashed at %s -> SKIP", key, pno, tries)
            done.write_text(json.dumps({"failed": True, "cells": []}), encoding="utf-8")
            attempt.unlink(missing_ok=True)
            continue
        attempt.write_text(json.dumps({"tries": tries + [dpi]}), encoding="utf-8")
        cells = ocr_page(ocr, page, tmp_png, dpi)
        done.write_text(json.dumps({"failed": False, "cells": cells}), encoding="utf-8")
        attempt.unlink(missing_ok=True)
        did += 1
    doc.close()
    return did, doc.page_count


def dump(key: str) -> None:
    """Print the cached OCR text for a document, page by page (parser design aid)."""
    ckpt = CKPT_ROOT / key
    pages = sorted(ckpt.glob("c*.json"))
    if not pages:
        print(f"no cached cells for {key} at {ckpt}")
        return
    for pj in pages:
        d = json.loads(pj.read_text(encoding="utf-8"))
        print(f"\n===== {pj.stem} ({'FAILED' if d.get('failed') else len(d['cells'])} cells) =====")
        for c in sorted(d["cells"], key=lambda c: (c["y0"], c["x0"])):
            print(f"  ({c['x0']:>4},{c['y0']:>4}) [{c['score']:.2f}] {c['text']}")


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--only", help="restrict to one candidate (slug or <slug>__<media_id> key)")
    ap.add_argument("--limit", type=int, help="OCR at most N documents (validation)")
    ap.add_argument("--doc-types", default="expense_statement",
                    help="comma-separated doc types to OCR (default expense_statement)")
    ap.add_argument("--dump", metavar="KEY", help="print cached OCR text for KEY and exit")
    args = ap.parse_args()

    setup_standalone_logging("sipo_candidate_ocr")
    if args.dump:
        dump(args.dump)
        return

    doc_types = {t.strip() for t in args.doc_types.split(",") if t.strip()}
    jobs = _jobs(doc_types, args.only, args.limit)
    log.info("%d documents in scope (doc_types=%s)", len(jobs), sorted(doc_types))
    if not jobs:
        return

    # Resolve PDFs and find which documents still have uncached pages. Crucially we
    # do NOT construct PaddleOCR (which segfaults on teardown -> rc=1) unless there
    # is real OCR work — otherwise an all-cached corpus would crash on exit and the
    # watchdog would relaunch forever. With no pending work, exit 0 cleanly here.
    pending = []
    missing = 0
    for r in jobs:
        pdf_path = _resolve_pdf(r)
        if not pdf_path.exists():
            log.warning("missing PDF for %s (%s)", _doc_key(r), r["local_path"])
            missing += 1
            continue
        if _pending_pages(_doc_key(r), pdf_path) > 0:
            pending.append((r, pdf_path))
    log.info("%d documents need OCR, %d already complete, %d missing PDFs",
             len(pending), len(jobs) - len(pending) - missing, missing)
    if not pending:
        log.info("ALL_DONE — nothing to OCR; exiting cleanly without building model")
        return

    from paddleocr import PaddleOCR
    ocr = PaddleOCR(lang="en", use_doc_orientation_classify=False, use_doc_unwarping=False,
                    use_textline_orientation=False, enable_mkldnn=False,
                    text_det_limit_side_len=1280, text_det_limit_type="max")

    total_pages_done = 0
    for i, (r, pdf_path) in enumerate(pending, 1):
        key = _doc_key(r)
        did, total = ocr_document(ocr, key, pdf_path)
        total_pages_done += did
        log.info("[%d/%d] %s — %s: +%d/%d pages (run total %d)",
                 i, len(pending), r["constituency_slug"], key, did, total, total_pages_done)

    log.info("done. OCR'd %d new pages this run across %d documents",
             total_pages_done, len(pending))


if __name__ == "__main__":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass
    main()
