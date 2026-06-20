"""
probe_2012_ocr_smoke.py  (SANDBOX smoke test)
---------------------------------------------
OCR a FEW pages of the scanned 2012 Dáil Register of Interests with the proven
SIPO PaddleOCR settings and reconstruct text lines, so we can eyeball legibility
(names "LAST, First", "(Constituency)", category headings "1. Occupations")
BEFORE committing to a full 102-page run.

Run:  python -m pipeline_sandbox.historic_members.probe_2012_ocr_smoke 1 2 3
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import fitz
from paddleocr import PaddleOCR

PDF = Path("data/bronze/interests/2013-02-28_register-of-members-interests-dail-eireann_en.pdf")
DPI = 300


def ocr_page(ocr, page, tmp_png: Path) -> list[dict]:
    pix = page.get_pixmap(matrix=fitz.Matrix(DPI / 72, DPI / 72))
    pix.save(tmp_png)
    res = ocr.predict(input=str(tmp_png))
    cells = []
    for r in res:
        d = r if isinstance(r, dict) else {}
        texts = d.get("rec_texts") or []
        scores = d.get("rec_scores") or []
        boxes = d.get("rec_boxes")
        boxes = boxes.tolist() if hasattr(boxes, "tolist") else (boxes or [])
        for t, s, b in zip(texts, scores, boxes, strict=False):
            cells.append({"text": t, "score": float(s), "x0": int(b[0]), "y0": int(b[1])})
    return cells


def reconstruct_lines(cells: list[dict], y_tol: int = 12) -> list[str]:
    """Cluster cells into lines by y, order each line left->right."""
    cells = sorted(cells, key=lambda c: (c["y0"], c["x0"]))
    lines: list[list[dict]] = []
    for c in cells:
        if lines and abs(c["y0"] - lines[-1][0]["y0"]) <= y_tol:
            lines[-1].append(c)
        else:
            lines.append([c])
    out = []
    for ln in lines:
        ln = sorted(ln, key=lambda c: c["x0"])
        out.append("  ".join(c["text"] for c in ln))
    return out


def main() -> None:
    pages = [int(a) for a in sys.argv[1:]] or [1, 2, 3]
    print(f"OCR smoke test on {PDF.name}, pages {pages}\n")
    ocr = PaddleOCR(
        lang="en",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,
        text_det_limit_side_len=1280,
        text_det_limit_type="max",
    )
    doc = fitz.open(PDF)
    with tempfile.TemporaryDirectory() as td:
        for p in pages:
            cells = ocr_page(ocr, doc[p], Path(td) / f"p{p}.png")
            lines = reconstruct_lines(cells)
            avg = sum(c["score"] for c in cells) / max(len(cells), 1)
            print(f"===== PAGE {p}  ({len(cells)} cells, mean conf {avg:.2f}) =====")
            for ln in lines:
                print("  ", ln)
            print()
    doc.close()


if __name__ == "__main__":
    main()
