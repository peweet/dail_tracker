"""
ocr_2012_register.py  (SANDBOX -> produces a reusable OCR text artifact)
------------------------------------------------------------------------
OCR the scanned 2012 Dáil Register of Interests (102 pages, no text layer) with
the proven SIPO PaddleOCR settings and emit a flat, reading-order list[str] of
lines — the same shape member_interests.extract_raw_lines() yields for a
born-digital PDF. The downstream parser (split_embedded_names -> group_lines ->
parse_members -> clean_interests) then runs UNCHANGED.

Crash-safety (PaddleOCR on Windows is unstable): per-page checkpoints in
_ckpt_2012/pNNN.json, so a crash/stop loses ONE page and the run resumes. Run it
again to continue; it skips pages already checkpointed.

Output: data/silver/interests_ocr/2012_dail_lines.json  (built once all pages done)

Run:  python -m pipeline_sandbox.historic_members.ocr_2012_register
      python -m pipeline_sandbox.historic_members.ocr_2012_register --max-pages 3   # smoke
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

import fitz

PDF = Path("data/bronze/interests/2013-02-28_register-of-members-interests-dail-eireann_en.pdf")
CKPT_DIR = Path(__file__).parent / "_ckpt_2012"
OUT = Path("data/silver/interests_ocr/2012_dail_lines.json")
DPI = 300


def make_ocr():
    import paddle
    from paddleocr import PaddleOCR

    use_gpu = paddle.is_compiled_with_cuda() and paddle.device.cuda.device_count() > 0
    print(f"OCR device: {'gpu' if use_gpu else 'cpu'}")
    return PaddleOCR(
        lang="en",
        device="gpu" if use_gpu else "cpu",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,  # no-op on GPU; on CPU avoids the paddle 3.3.1 oneDNN/PIR bug
        text_det_limit_side_len=1280,  # full A4 @300 DPI segfaults the CPU detector
        text_det_limit_type="max",
    )


def ocr_cells(ocr, page, tmp_png: Path) -> list[dict]:
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
    """Cluster cells into visual lines by y, order each line left->right."""
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
        # Two spaces between cells preserves the name/(constituency) and
        # category/value gaps the prod parser splits on (\s{2,}).
        out.append("  ".join(c["text"] for c in ln))
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-pages", type=int, default=None, help="smoke: OCR only first N pages")
    args = ap.parse_args()

    CKPT_DIR.mkdir(parents=True, exist_ok=True)
    doc = fitz.open(PDF)
    total = doc.page_count
    n = total if args.max_pages is None else min(args.max_pages, total)
    print(f"OCR {PDF.name}: {n}/{total} pages, ckpt -> {CKPT_DIR}")

    ocr = None
    with tempfile.TemporaryDirectory() as td:
        for p in range(n):
            ckpt = CKPT_DIR / f"p{p:03d}.json"
            if ckpt.exists():
                continue
            if ocr is None:
                ocr = make_ocr()  # lazy: skip model load if everything checkpointed
            cells = ocr_cells(ocr, doc[p], Path(td) / f"p{p}.png")
            lines = reconstruct_lines(cells)
            conf = round(sum(c["score"] for c in cells) / max(len(cells), 1), 4)
            ckpt.write_text(json.dumps({"page": p, "conf": conf, "lines": lines}, ensure_ascii=False), encoding="utf-8")
            print(f"  p{p:03d}: {len(lines):>3} lines, conf {conf:.2f}")
    doc.close()

    # Assemble final artifact only when every page is checkpointed.
    done = sorted(CKPT_DIR.glob("p*.json"))
    if args.max_pages is None and len(done) == total:
        all_lines: list[str] = []
        confs = []
        for ck in done:
            d = json.loads(ck.read_text(encoding="utf-8"))
            all_lines.extend(d["lines"])
            confs.append(d["conf"])
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps({"year_declared": 2012, "mean_conf": round(sum(confs) / len(confs), 4),
                                   "n_pages": len(done), "lines": all_lines}, ensure_ascii=False, indent=1),
                       encoding="utf-8")
        print(f"\nDONE: {len(all_lines)} lines, mean conf {sum(confs)/len(confs):.2f} -> {OUT}")
    else:
        print(f"\n{len(done)}/{total} pages checkpointed; re-run to continue (artifact not yet written).")


if __name__ == "__main__":
    main()
