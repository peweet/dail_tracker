"""Council minutes → structured text (agenda / attendance / motions / decisions / votes).

EXPERIMENTAL sandbox. Per-PDF flow:
  1. Open with PyMuPDF (fitz). If the page has a real text layer → extract with fitz (NO OCR).
  2. If the page is a scanned image (no/low text layer) → OCR it.
     - Production engine = **PaddleOCR on GPU** (`--engine paddle-gpu`), high fidelity, validated
       off-box vs SIPO. RUN OFF-BOX: PaddleOCR hard-crashes the local Windows box
       (feedback_paddleocr_crashes_local_box) — GPU job belongs off-box (feedback_ocr_use_gpu).
     - Safe local fallback = `--engine rapidocr` (ONNX, CPU) for sampling/demo only.
  3. Parse the resulting text into a light structure (agenda items, attendance, proposer/seconder
     motions, decisions, any roll-call vote tallies).

This is the "OCR all council minutes" worker. Drive it from a harvested list of minutes PDF URLs
(see council_minutes_harvest.py → minutes_sources.csv). Honest by design: a scanned page that OCRs
to noise is flagged low-confidence, not silently dropped.

Usage (off-box, full fidelity):
    python council_minutes_pipeline.py --engine paddle-gpu --sources minutes_sources.csv --out out/

Usage (local safe sample):
    python council_minutes_pipeline.py --engine rapidocr --url <pdf> --pages 0-3
"""
from __future__ import annotations

import argparse
import io
import json
import re
import sys
from pathlib import Path

import requests

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/120 Safari/537.36")
HDRS = {"User-Agent": UA}

# A page with at least this many real text chars is treated as born-digital (use fitz, skip OCR).
TEXT_LAYER_MIN_CHARS = 80


# ── OCR engines ────────────────────────────────────────────────────────────────
def _make_ocr(engine: str):
    """Return a callable(png_bytes) -> list[str] of text lines."""
    if engine == "paddle-gpu":
        # Production / off-box. High fidelity. use_gpu=True targets CUDA (RTX present on box,
        # but DO NOT run this locally unsupervised — see module docstring).
        from paddleocr import PaddleOCR  # noqa: PLC0415
        _p = PaddleOCR(use_angle_cls=True, lang="en", use_gpu=True, show_log=False)

        def run(png: bytes) -> list[str]:
            import numpy as np  # noqa: PLC0415
            from PIL import Image  # noqa: PLC0415
            img = np.array(Image.open(io.BytesIO(png)).convert("RGB"))
            res = _p.ocr(img, cls=True)
            out = []
            for block in (res or []):
                for line in (block or []):
                    out.append(line[1][0])
            return out
        return run

    if engine == "rapidocr":
        # Safe local fallback (CPU, ONNX). Lower fidelity than Paddle but does not crash the box.
        from rapidocr_onnxruntime import RapidOCR  # noqa: PLC0415
        _r = RapidOCR()

        def run(png: bytes) -> list[str]:
            res, _ = _r(png)
            return [t for _, t, _ in res] if res else []
        return run

    raise SystemExit(f"unknown engine {engine!r} (use paddle-gpu | rapidocr)")


# ── per-PDF extraction ───────────────────────────────────────────────────────--
def extract_pdf(pdf_bytes: bytes, ocr, dpi: int = 200) -> dict:
    """Return {pages:[{n, kind, text}], n_text, n_ocr}. fitz for text pages, OCR for scanned."""
    import fitz  # noqa: PLC0415
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    pages, n_text, n_ocr = [], 0, 0
    for i, page in enumerate(doc):
        native = page.get_text().strip()
        if len(native) >= TEXT_LAYER_MIN_CHARS:
            pages.append({"n": i, "kind": "text", "text": native})
            n_text += 1
        else:
            png = page.get_pixmap(dpi=dpi).tobytes("png")
            lines = ocr(png) if ocr else []
            pages.append({"n": i, "kind": "ocr", "text": "\n".join(lines)})
            n_ocr += 1
    return {"pages": pages, "n_text": n_text, "n_ocr": n_ocr, "n_pages": len(doc)}


# ── light structural parse (engine-agnostic over the extracted text) ─────────────
_ITEM = re.compile(r"ITEM\s*N[O0]\.?\s*\d+[^\n]{0,90}", re.I)
_MOTION = re.compile(r"PROPOSAL of[^\n]{0,120}?(?:SECONDED|seconded) by[^\n]{0,80}", re.I)
_DECISION = re.compile(r"\b(AGREED|N[O0]TED|CARRIED|LOST|ADOPTED|DEFERRED|APPROVED|REJECTED)\b")
_VOTE = re.compile(r"(roll[\s-]?call|in favou?r|\bagainst\b|abstain|\bT[áa]\b|\bN[íi]l\b|division|"
                   r"voted for|show of hands)", re.I)


def parse_structure(full_text: str) -> dict:
    return {
        "agenda_items": [re.sub(r"\s+", " ", m).strip() for m in _ITEM.findall(full_text)],
        "motions": [re.sub(r"\s+", " ", m).strip() for m in _MOTION.findall(full_text)],
        "decision_markers": len(_DECISION.findall(full_text)),
        "vote_markers": len(_VOTE.findall(full_text)),
        "has_named_rollcall": bool(re.search(r"roll[\s-]?call", full_text, re.I)),
    }


def process_url(url: str, ocr, pages_spec: str | None = None) -> dict:
    pdf = requests.get(url, headers=HDRS, timeout=90).content
    res = extract_pdf(pdf, ocr)
    if pages_spec:  # subset for sampling
        lo, hi = (pages_spec.split("-") + [pages_spec])[:2]
        keep = range(int(lo), int(hi) + 1)
        res["pages"] = [p for p in res["pages"] if p["n"] in keep]
    full = "\n".join(p["text"] for p in res["pages"])
    return {"url": url, **res, "structure": parse_structure(full), "full_text": full}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--engine", default="rapidocr", choices=["paddle-gpu", "rapidocr"])
    ap.add_argument("--url", help="single PDF url")
    ap.add_argument("--sources", help="csv with a 'minutes_url' column")
    ap.add_argument("--pages", help="page subset e.g. 0-3 (sampling)")
    ap.add_argument("--out", default="out")
    args = ap.parse_args()

    ocr = _make_ocr(args.engine)
    Path(args.out).mkdir(parents=True, exist_ok=True)

    urls = []
    if args.url:
        urls = [args.url]
    elif args.sources:
        import csv
        with open(args.sources, encoding="utf-8") as fh:
            urls = [r["minutes_url"] for r in csv.DictReader(fh) if r.get("minutes_url")]
    else:
        return print("need --url or --sources") or 2

    for i, u in enumerate(urls):
        try:
            r = process_url(u, ocr, args.pages)
            name = re.sub(r"\W+", "_", u.split("/")[-1])[:60] or f"doc{i}"
            Path(args.out, f"{name}.json").write_text(
                json.dumps(r, ensure_ascii=False, indent=1), encoding="utf-8")
            s = r["structure"]
            print(f"[{i+1}/{len(urls)}] {u.split('/')[-1][:50]} "
                  f"text={r['n_text']} ocr={r['n_ocr']} items={len(s['agenda_items'])} "
                  f"motions={len(s['motions'])} votes={s['vote_markers']}")
        except Exception as e:  # noqa: BLE001
            print(f"[{i+1}/{len(urls)}] ERR {type(e).__name__} {u}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
