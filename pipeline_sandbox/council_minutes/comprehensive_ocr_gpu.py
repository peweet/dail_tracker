"""GPU variant of comprehensive_ocr.py — SCANNED council minutes -> agenda items.

Same harvest + agenda parser as comprehensive_ocr.py, but OCR runs on PaddleOCR-GPU
(the proven SIPO/diary engine, ~1.2s/page on the RTX 3060) in a SINGLE serial process
(one PaddleOCR at a time, no worker pool — the crash-safety rule). Born-digital PDFs
still skip OCR via their text layer. Appends to meeting_history.jsonl (dedupe). Sandbox.

Run:  .venv/Scripts/python.exe -u pipeline_sandbox/council_minutes/comprehensive_ocr_gpu.py
"""
from __future__ import annotations

import json
import tempfile
from pathlib import Path
from urllib.parse import unquote

import fitz
import requests

# reuse harvest + parsing from the CPU script (module-level defs only; main() is guarded)
from comprehensive_ocr import H, SOURCES, agenda_items, harvest, mdate

HERE = Path(__file__).resolve().parent
MAX_PAGES = 20
DPI = 200
_Y_BAND = 8  # px: cells whose top-y is within this band share a reconstructed line


def make_ocr():
    from paddleocr import PaddleOCR

    return PaddleOCR(
        lang="en",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,  # paddle 3.3.1 oneDNN/PIR bug on Windows
        text_det_limit_side_len=1280,
        text_det_limit_type="max",
    )


def _page_text(ocr, page, tmp_png: Path) -> str:
    """OCR one page -> reading-order text (top-to-bottom, left-to-right within a y-band)."""
    pix = page.get_pixmap(matrix=fitz.Matrix(DPI / 72, DPI / 72))
    pix.save(tmp_png)
    cells = []
    for r in ocr.predict(input=str(tmp_png)):
        d = r if isinstance(r, dict) else {}
        boxes = d.get("rec_boxes")
        boxes = boxes.tolist() if hasattr(boxes, "tolist") else (boxes or [])
        for t, b in zip(d.get("rec_texts") or [], boxes, strict=False):
            if t and t.strip():
                cells.append((int(b[1]), int(b[0]), t.strip()))  # (y0, x0, text)
    cells.sort()
    lines: list[str] = []
    row_y = None
    for y0, _x0, t in cells:
        if row_y is None or y0 - row_y > _Y_BAND:
            lines.append(t)
            row_y = y0
        else:
            lines[-1] = f"{lines[-1]} {t}"
    return "\n".join(lines)


def ocr_doc(ocr, la: str, url: str, tmp_png: Path) -> dict:
    try:
        pdf = requests.get(url, headers=H, timeout=70).content
        doc = fitz.open(stream=pdf, filetype="pdf")
        native = sum(len(p.get_text().strip()) for p in doc)
        if native >= 80 * max(1, len(doc)):
            text = "\n".join(p.get_text() for p in doc)  # born-digital
            mode = "text"
        else:
            text = "\n".join(_page_text(ocr, p, tmp_png) for p in list(doc)[:MAX_PAGES])
            mode = "gpu-ocr"
        items = agenda_items(text)
        return {"council": la, "file": unquote(url.split("/")[-1]), "date": mdate(url),
                "agenda_items": items, "source_url": url, "n_items": len(items), "mode": mode}
    except Exception as e:  # noqa: BLE001
        return {"council": la, "file": url.split("/")[-1], "error": type(e).__name__, "n_items": 0, "mode": "err"}


def main():
    jobs = []
    for la, pages in SOURCES.items():
        urls = harvest(pages)
        print(f"{la}: {len(urls)} recent scanned/minutes docs to OCR", flush=True)
        jobs += [(la, u) for u in urls]
    print(f"\nGPU OCR'ing {len(jobs)} docs (serial, 1 PaddleOCR process) ...", flush=True)

    ocr = make_ocr()
    tmp_png = Path(tempfile.gettempdir()) / "council_gpu_page.png"
    rows = []
    for i, (la, u) in enumerate(jobs):
        r = ocr_doc(ocr, la, u, tmp_png)
        rows.append(r)
        tag = r["error"] if "error" in r else f"{r['n_items']} [{r['mode']}]"
        print(f"  [{i+1}/{len(jobs)}] {r['council']:14} {r.get('date','')[:12]:12} -> {tag}", flush=True)

    good = [r for r in rows if r.get("agenda_items")]
    mh_path = HERE / "meeting_history.jsonl"
    mh = [json.loads(l) for l in mh_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    seen = {(r["council"], r.get("date", ""), r.get("file", "")) for r in mh}
    added = 0
    for r in good:
        key = (r["council"], r["date"], r["file"])
        if key in seen:
            continue
        seen.add(key)
        mh.append({"council": r["council"], "file": r["file"], "date": r["date"],
                   "agenda_items": r["agenda_items"], "source_url": r["source_url"]})
        added += 1
    mh_path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in mh), encoding="utf-8")
    from collections import Counter
    print(f"\nOCR done. agendas extracted: {len(good)}/{len(jobs)}; added {added} new meetings.", flush=True)
    print("meeting_history now:", len(mh), "meetings;", len({r['council'] for r in mh}), "councils", flush=True)
    print("scanned-council counts:", {k: v for k, v in Counter(r['council'] for r in mh).items() if k in SOURCES}, flush=True)


if __name__ == "__main__":
    main()
