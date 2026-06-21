"""GPU OCR for the SCANNED ministerial-diary files (off the OCR queue).

The diary corpus has ~237 image-only scans the born-digital parser can't read
(data/_meta/ministerial_diaries_ocr_queue.csv) — DPER (incl. MoS Ossian Smyth's
eGovernment/procurement diary) + Taoiseach + DCCS history etc. This runs PaddleOCR
on the GPU to recover their text, reconstructs reading order, and feeds the existing
parse_entries state machine — so OCR'd scans flow into the same sandbox as born-digital
files (then classify -> match -> overlap -> promote, with diary_merge_depts to land).

GPU + single-process is deliberate ([[feedback_ocr_use_gpu]], [[feedback_paddleocr_crashes_local_box]]):
PaddleOCR is run ONE process at a time (two crash the GPU driver), GPU because the user
validated it against SIPO (far faster, results good). Per-page cells are checkpointed so a
crash loses one page, not the run. Reuses the proven SIPO PaddleOCR init (paddle 3.3.1
Windows gotchas: enable_mkldnn=False, text_det_limit_side_len=1280).

Operates on CACHED PDFs only (C:/tmp/min_diaries_pdfs) — NO gov.ie traffic.

Run (smoke one file):  .venv/Scripts/python.exe extractors/diary_ocr.py --file 2021-minister-smyths-diary.pdf --max-pages 6
Run (a department):    .venv/Scripts/python.exe extractors/diary_ocr.py --depts DPER
"""

from __future__ import annotations

import argparse
import contextlib
import json
import logging
import re
from datetime import date
from pathlib import Path

import duckdb
import fitz

from extractors._diary_minister import resolve_minister
from extractors.diary_grid_parse import parse_day_grid, parse_grid
from extractors.ministerial_diaries_extract import _infer_default_year, parse_entries
from services.logging_setup import setup_standalone_logging

log = logging.getLogger(__name__)

INDEX = Path("data/sandbox/enrichment/ministerial_diaries_index.parquet")
PDF_CACHE = Path("C:/tmp/min_diaries_pdfs")
OCR_TXT = Path("C:/tmp/min_diaries_ocr")  # per-file reconstructed text (transient cache)
DPI = 300
_Y_BAND = 12  # rows within this many px share a line (left/right column cells stay on one row)


def _cache_name(file_name: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]", "_", file_name)[-100:]


def make_ocr(orient: bool = False):
    """Build the PaddleOCR pipeline. ``orient=True`` enables document-orientation +
    text-line-orientation + unwarping — needed ONLY for the minority of scans saved
    rotated/skewed (e.g. the Education 2021-2022 files OCR'd to mirrored garbage with
    these off). It is slower, so it's opt-in for a targeted re-OCR of those files; the
    bulk of clean upright scans use the fast path (all three off)."""
    from paddleocr import PaddleOCR

    return PaddleOCR(
        lang="en",
        use_doc_orientation_classify=orient,
        use_doc_unwarping=orient,
        use_textline_orientation=orient,
        enable_mkldnn=False,  # paddle 3.3.1 oneDNN/PIR bug on Windows
        text_det_limit_side_len=1280,  # full A4 @300 DPI segfaults the detector
        text_det_limit_type="max",
    )


CELLS = OCR_TXT / "cells"  # per-file OCR cell cache (geometry preserved → re-parse w/o re-OCR)


def _page_cells(ocr, page, tmp_png: Path) -> list[dict]:
    pix = page.get_pixmap(matrix=fitz.Matrix(DPI / 72, DPI / 72))
    pix.save(tmp_png)
    out: list[dict] = []
    for r in ocr.predict(input=str(tmp_png)):
        d = r if isinstance(r, dict) else {}
        boxes = d.get("rec_boxes")
        boxes = boxes.tolist() if hasattr(boxes, "tolist") else (boxes or [])
        for t, b in zip(d.get("rec_texts") or [], boxes, strict=False):
            if t and t.strip():
                out.append({"t": t.strip(), "x0": int(b[0]), "y0": int(b[1]), "x1": int(b[2]), "y1": int(b[3])})
    return out


def ocr_file_cells(ocr, pdf_path: Path, cache_key: str, max_pages: int | None = None) -> list[list[dict]]:
    """Per-page OCR cells (with geometry), CACHED to JSON. A cached file is loaded without
    re-OCR so the parser can be re-run/fixed for free ([[project_sipo_ocr]] crash+cost lesson)."""
    CELLS.mkdir(parents=True, exist_ok=True)
    cache = CELLS / f"{cache_key}.json"
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    tmp_png = OCR_TXT / "_page.png"
    doc = fitz.open(pdf_path)
    pages = [_page_cells(ocr, page, tmp_png) for i, page in enumerate(doc) if max_pages is None or i < max_pages]
    cache.write_text(json.dumps(pages), encoding="utf-8")
    return pages


def cells_to_text(pages: list[list[dict]]) -> str:
    """Linear fallback: reconstruct reading order (top-to-bottom, left-to-right within a y-band)
    for the daily-list scans that are NOT week grids."""
    lines: list[str] = []
    for cells in pages:
        cells = sorted(cells, key=lambda c: (c["y0"], c["x0"]))
        row_y = None
        for c in cells:
            if row_y is None or c["y0"] - row_y > _Y_BAND:
                lines.append(c["t"])
                row_y = c["y0"]
            else:
                lines[-1] = f"{lines[-1]} {c['t']}"
    return "\n".join(lines)


def _scanned_files(depts: set[str] | None) -> list[dict]:
    con = duckdb.connect()
    q = f"SELECT department, file_name, file_url, period_year_guess, period_month_guess FROM read_parquet('{INDEX.as_posix()}') WHERE parse_status='scanned_needs_offbox_ocr'"
    cols = ("department", "file_name", "file_url", "year", "month")
    rows = [dict(zip(cols, r, strict=False)) for r in con.execute(q).fetchall()]
    if depts:
        rows = [r for r in rows if r["department"] in depts]
    return rows


def _clamp_year_to_file(e: dict, file_year: int) -> dict:
    """Snap an entry whose OCR-read year is one off the file's known single year back to it."""
    d = e.get("entry_date")
    if isinstance(d, date) and abs(d.year - file_year) == 1:
        with contextlib.suppress(ValueError):  # 29 Feb in a non-leap target year — leave as-is
            e["entry_date"] = d.replace(year=file_year)
    return e


def run(
    depts: set[str] | None,
    only_file: str | None,
    max_files: int | None,
    max_pages: int | None,
    orient: bool = False,
) -> int:
    setup_standalone_logging("diary_ocr")
    files = _scanned_files(depts)
    if only_file:
        files = [f for f in files if f["file_name"] == only_file]
    if max_files:
        files = files[:max_files]
    if not files:
        log.error("no matching scanned files (depts=%s file=%s)", depts, only_file)
        return 1

    OCR_TXT.mkdir(parents=True, exist_ok=True)
    ocr = make_ocr(orient)
    all_entries: list[dict] = []
    for f in files:
        pdf = PDF_CACHE / _cache_name(f["file_name"])
        if not pdf.exists():
            log.warning("not cached (skip, would need download): %s", f["file_name"])
            continue
        pages = ocr_file_cells(ocr, pdf, _cache_name(f["file_name"]), max_pages=max_pages)
        text = cells_to_text(pages)
        year = f["year"] or _infer_default_year(text)
        # week-grid first (DPER/Taoiseach/older-DCCS 5-column Outlook exports); then the
        # 2-column day-pair weekly print (Education scans); else linear daily-list.
        entries = parse_grid(pages, year)
        mode = "grid"
        if not entries:
            entries, mode = parse_day_grid(pages, year), "day_grid"
        if not entries:
            entries, mode = parse_entries(text, year, f["month"]), "linear"
        # OCR sometimes misreads a date header's year (a "2024" Q4 scan read as "2025"), which
        # also mis-attributes the minister (Dec-2024 Harris would resolve as Jan-2025 Martin). Each
        # quarterly diary file is a single calendar year (its filename year), so snap any entry whose
        # year is off by exactly one back to the file's year — keep month/day, leave wilder dates for
        # inspection. Done BEFORE resolve_minister so attribution uses the corrected date.
        if f["year"]:
            entries = [_clamp_year_to_file(e, int(f["year"])) for e in entries]
        for e in entries:
            e.update(
                department=f["department"],
                minister=resolve_minister(f["file_name"], f["department"], e["entry_date"]),
                source_pdf_url=f["file_url"],
            )
        all_entries.extend(entries)
        log.info("OCR %-40s %4d entries [%s]", f["file_name"][:40], len(entries), mode)

    log.info("TOTAL OCR'd: %d files -> %d parsed entries", len(files), len(all_entries))
    # save a JSON sidecar of recovered entries for inspection before any merge
    if all_entries:
        out = OCR_TXT / "_ocr_entries.json"
        out.write_text(json.dumps([{**e, "entry_date": str(e["entry_date"])} for e in all_entries], indent=0), "utf-8")
        log.info("recovered entries -> %s", out)
    return 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="GPU OCR for scanned ministerial-diary files.")
    ap.add_argument("--depts", help="comma-separated dept labels (e.g. DPER); default = all scanned")
    ap.add_argument("--file", help="single cached file_name to OCR (smoke test)")
    ap.add_argument("--max-files", type=int, default=None)
    ap.add_argument("--max-pages", type=int, default=None, help="cap pages per file (smoke test)")
    ap.add_argument(
        "--orient",
        action="store_true",
        help="enable orientation/unwarp (slow) — for re-OCR of rotated scans; delete their cell cache first",
    )
    a = ap.parse_args()
    depts = {d.strip().upper() for d in a.depts.split(",")} if a.depts else None
    raise SystemExit(run(depts, a.file, a.max_files, a.max_pages, a.orient))
