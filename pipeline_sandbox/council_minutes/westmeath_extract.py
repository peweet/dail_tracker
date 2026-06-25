"""Westmeath County Council - meeting agenda extractor (sandbox, read-only).

Downloads recent (2024+) full-council Agenda PDFs and extracts the tabled
agenda items. Born-digital via PyMuPDF; falls back to rapidocr-onnxruntime
if a page is scanned (near-zero text). Never writes to data/gold.
"""
import io
import re
import json
import requests
import fitz  # PyMuPDF

H = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}

# (date, agenda_pdf_url) for the 6 most recent 2024+ meetings that have an Agenda
TARGETS = [
    ("2026-06-29", "http://www.westmeathcoco.ie/en/media/AgendaJune2026.pdf"),
    ("2026-04-27", "http://www.westmeathcoco.ie/en/media/Agenda April 2026.pdf"),
    ("2026-03-30", "http://www.westmeathcoco.ie/en/media/Agenda March 2026.pdf"),
    ("2026-02-23", "http://www.westmeathcoco.ie/en/media/Agenda Feb 2026.pdf"),
    ("2026-01-26", "http://www.westmeathcoco.ie/en/media/Agenda January 2026.pdf"),
    ("2025-12-15", "http://www.westmeathcoco.ie/en/media/Agenda Dec 2025.pdf"),
]


def get_pdf_text(url):
    r = requests.get(url, headers=H, timeout=60)
    r.raise_for_status()
    doc = fitz.open(stream=io.BytesIO(r.content), filetype="pdf")
    pages_text = [p.get_text() for p in doc]
    full = "\n".join(pages_text)
    scanned = False
    if len(full.strip()) < 40:
        # scanned -> OCR with rapidocr
        scanned = True
        from rapidocr_onnxruntime import RapidOCR
        ocr = RapidOCR()
        chunks = []
        for p in doc:
            pix = p.get_pixmap(dpi=200)
            import numpy as np
            from PIL import Image
            img = Image.open(io.BytesIO(pix.tobytes("png")))
            res, _ = ocr(np.array(img))
            if res:
                chunks.append("\n".join(t[1] for t in res))
        full = "\n".join(chunks)
    doc.close()
    return full, scanned


def main():
    out = []
    for date, url in TARGETS:
        rec = {"date": date, "url": url, "scanned": None, "raw_len": 0, "text": ""}
        try:
            text, scanned = get_pdf_text(url)
            rec["scanned"] = scanned
            rec["raw_len"] = len(text)
            rec["text"] = text
        except Exception as e:
            rec["error"] = repr(e)
        out.append(rec)
    print(json.dumps(out, ensure_ascii=False))


if __name__ == "__main__":
    main()
