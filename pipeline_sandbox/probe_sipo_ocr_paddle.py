"""PROBE v7 (throwaway): does a FREE, LOCAL, modern OCR engine (PaddleOCR,
Apache-2.0) recover the cells that Tesseract/ocrmypdf mangled on the SAME crisp
scan? The headline correction (doc, 2026-06-03) showed the image is clean —
`€ 17,844.78` is legible but Tesseract recorded `feireaa7e`; `856.51`->`85651`;
`2,492.07`->`249207`. If PaddleOCR reads them correctly, the engine swap (not the
`/100` repair heuristics) is the real fix, and this script is the scaffold for the
future enrichment: render page -> PaddleOCR (word boxes + confidence) -> feed the
EXISTING geometry/anchor/cap layer (probe_sipo_ocr_columns/repair).

This validates on page 3 of the already-OCR'd PDF (we only use its raster image;
we ignore its bad text layer and re-OCR with PaddleOCR).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_ocr_paddle.py
Writes only throwaway PNGs under the system temp dir; no repo data touched.
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

import fitz  # PyMuPDF

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

OCR_PDF = ROOT / "data/bronze/scan_pdf/output/ff_sipo_ge_2024_expenses-ocr.pdf"
# values Tesseract destroyed on p.3 (see the doc headline correction)
TESS_FAILED = {
    "17,844.78": "feireaa7e (filed as unrecoverable garbage)",
    "856.51": "85651 (decimal dropped)",
    "2,492.07": "249207 (comma + decimal dropped)",
    "889.10": "88910 / earlier corrupt",
}


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def render(page, clip, dpi, out: Path) -> Path:
    pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72), clip=clip)
    pix.save(out)
    return out


def run_paddle(img_path: Path):
    from paddleocr import PaddleOCR

    ocr = PaddleOCR(
        lang="en",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        # WINDOWS FIX: paddle 3.3.1's oneDNN/PIR path throws
        # "ConvertPirAttribute2RuntimeAttribute not support"; the plain CPU path
        # works. enable_mkldnn=False is forwarded through paddleocr -> paddlex.
        enable_mkldnn=False,
    )
    res = ocr.predict(input=str(img_path))
    texts: list[tuple[str, float]] = []
    for r in res:
        d = r if isinstance(r, dict) else getattr(r, "json", {}).get("res", {})
        rec_texts = d.get("rec_texts") or []
        rec_scores = d.get("rec_scores") or [None] * len(rec_texts)
        for t, s in zip(rec_texts, rec_scores):
            texts.append((t, s))
    return texts


def main() -> None:
    doc = fitz.open(OCR_PDF)
    page = doc[2]  # page 3
    tmp = Path(tempfile.gettempdir())
    # amount column (right) holds the known-bad cells
    amt_clip = fitz.Rect(page.rect.width * 0.62, 150, page.rect.width, 800)
    amt_png = render(page, amt_clip, 300, tmp / "sipo_paddle_amount.png")
    print("rendered:", amt_png)

    hr("PaddleOCR on the AMOUNT column (first run downloads models)")
    texts = run_paddle(amt_png)
    for t, s in texts:
        sc = f"{s:.2f}" if isinstance(s, float) else "?"
        print(f"  conf={sc}  {t!r}")

    joined = " ".join(t for t, _ in texts)
    hr("VERDICT: did PaddleOCR recover what Tesseract destroyed?")
    recovered = 0
    for good, tess in TESS_FAILED.items():
        hit = good in joined or good.replace(",", "") in joined.replace(",", "")
        recovered += hit
        print(f"  {'✓ RECOVERED' if hit else '✗ missed   '}  {good:<10} (Tesseract: {tess})")
    print(f"\n{recovered}/{len(TESS_FAILED)} known-bad cells recovered by PaddleOCR.")
    if recovered >= 3:
        print("=> engine swap confirmed as the real fix; build the enrichment on PaddleOCR.")
    else:
        print("=> inconclusive on this crop; try higher DPI / per-cell crops before deciding.")


if __name__ == "__main__":
    main()
