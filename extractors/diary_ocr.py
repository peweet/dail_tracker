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
import json
import logging
import re
from pathlib import Path

import duckdb
import fitz

from extractors._diary_minister import resolve_minister
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


def make_ocr():
    from paddleocr import PaddleOCR

    return PaddleOCR(
        lang="en",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,  # paddle 3.3.1 oneDNN/PIR bug on Windows
        text_det_limit_side_len=1280,  # full A4 @300 DPI segfaults the detector
        text_det_limit_type="max",
    )


def _page_cells(ocr, page, tmp_png: Path) -> list[tuple[int, int, str]]:
    pix = page.get_pixmap(matrix=fitz.Matrix(DPI / 72, DPI / 72))
    pix.save(tmp_png)
    out: list[tuple[int, int, str]] = []
    for r in ocr.predict(input=str(tmp_png)):
        d = r if isinstance(r, dict) else {}
        texts = d.get("rec_texts") or []
        boxes = d.get("rec_boxes")
        boxes = boxes.tolist() if hasattr(boxes, "tolist") else (boxes or [])
        for t, b in zip(texts, boxes, strict=False):
            if t and t.strip():
                out.append((int(b[1]), int(b[0]), t.strip()))  # (y0, x0, text)
    return out


def ocr_pdf_to_text(ocr, pdf_path: Path, max_pages: int | None = None) -> str:
    """Render -> OCR -> reconstruct reading order (top-to-bottom, then left-to-right within a
    y-band) into newline-joined text the diary state machine can parse."""
    OCR_TXT.mkdir(parents=True, exist_ok=True)
    tmp_png = OCR_TXT / "_page.png"
    doc = fitz.open(pdf_path)
    lines: list[str] = []
    for i, page in enumerate(doc):
        if max_pages is not None and i >= max_pages:
            break
        cells = _page_cells(ocr, page, tmp_png)
        # group into rows by y-band so a split time/subject on one visual row stays together
        cells.sort(key=lambda c: (c[0], c[1]))
        row_y = None
        for y0, _x0, t in cells:
            if row_y is None or y0 - row_y > _Y_BAND:
                lines.append(t)
                row_y = y0
            else:
                lines[-1] = f"{lines[-1]} {t}"
    return "\n".join(lines)


def _scanned_files(depts: set[str] | None) -> list[dict]:
    con = duckdb.connect()
    q = f"SELECT department, file_name, file_url, period_year_guess, period_month_guess FROM read_parquet('{INDEX.as_posix()}') WHERE parse_status='scanned_needs_offbox_ocr'"
    cols = ("department", "file_name", "file_url", "year", "month")
    rows = [dict(zip(cols, r, strict=False)) for r in con.execute(q).fetchall()]
    if depts:
        rows = [r for r in rows if r["department"] in depts]
    return rows


def run(depts: set[str] | None, only_file: str | None, max_files: int | None, max_pages: int | None) -> int:
    setup_standalone_logging("diary_ocr")
    files = _scanned_files(depts)
    if only_file:
        files = [f for f in files if f["file_name"] == only_file]
    if max_files:
        files = files[:max_files]
    if not files:
        log.error("no matching scanned files (depts=%s file=%s)", depts, only_file)
        return 1

    ocr = make_ocr()
    all_entries: list[dict] = []
    for f in files:
        pdf = PDF_CACHE / _cache_name(f["file_name"])
        if not pdf.exists():
            log.warning("not cached (skip, would need download): %s", f["file_name"])
            continue
        text = ocr_pdf_to_text(ocr, pdf, max_pages=max_pages)
        (OCR_TXT / (f["file_name"] + ".txt")).write_text(text, encoding="utf-8")
        year = f["year"] or _infer_default_year(text)
        entries = parse_entries(text, year, f["month"])
        for e in entries:
            e.update(
                department=f["department"],
                minister=resolve_minister(f["file_name"], f["department"], e["entry_date"]),
                source_pdf_url=f["file_url"],
            )
        all_entries.extend(entries)
        log.info("OCR %-44s %3d entries (%d chars)", f["file_name"][:44], len(entries), len(text))

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
    a = ap.parse_args()
    depts = {d.strip().upper() for d in a.depts.split(",")} if a.depts else None
    raise SystemExit(run(depts, a.file, a.max_files, a.max_pages))
