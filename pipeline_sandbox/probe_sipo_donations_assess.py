"""PROBE (throwaway): assess `2024_election_donations.pdf` (105pp, NO text layer)
for OCR extraction with the SAME PaddleOCR engine validated on the expenses file.

Unlike the expenses PDF (a tabular candidate summary -> geometry/anchor/cap layer),
the donations PDF is a per-party FORM packet:
  - a "General Information" page (party name + appropriate officer), then
  - a declaration page, then
  - N "Details of Donations" pages, ONE donation per page, key/value layout
    (donor name+address, value, nature, description, date received, receipt,
     Irish-citizen flag, receipt issuer).

So extraction here is label->value spatial mapping, NOT row clustering. This probe:
  1. classifies every page (general-info / donation-detail / declaration / blank),
  2. on donation-detail pages, pulls the printed labels' neighbouring values,
  3. reports party count, donation count, amount legibility, and confidence,
to judge whether a real extractor is worth building.

Read-only except a throwaway JSON dump under pipeline_sandbox/_sipo_output/.
Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_donations_assess.py
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
from pathlib import Path

import fitz  # PyMuPDF

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PDF = ROOT / "data/bronze/scan_pdf/2024_election_donations.pdf"
OUT = ROOT / "pipeline_sandbox/_sipo_output"
OUT.mkdir(parents=True, exist_ok=True)
DPI = 250


def make_ocr():
    from paddleocr import PaddleOCR

    return PaddleOCR(
        lang="en",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,  # WINDOWS FIX (paddle 3.3.1 oneDNN/PIR crash)
        text_det_limit_side_len=1280,  # full-A4 @300DPI segfaults the detector
        text_det_limit_type="max",
    )


def ocr_page(ocr, page) -> list[tuple[str, float, tuple[float, float]]]:
    pix = page.get_pixmap(matrix=fitz.Matrix(DPI / 72, DPI / 72))
    tmp = Path(tempfile.gettempdir()) / "sipo_don_tmp.png"
    pix.save(tmp)
    res = ocr.predict(input=str(tmp))
    items: list[tuple[str, float, tuple[float, float]]] = []
    for r in res:
        d = r if isinstance(r, dict) else getattr(r, "json", {}).get("res", {})
        texts = d.get("rec_texts") or []
        scores = d.get("rec_scores") or [None] * len(texts)
        polys = d.get("rec_polys") or d.get("dt_polys") or [None] * len(texts)
        for t, s, p in zip(texts, scores, polys):
            cx = cy = 0.0
            if p is not None:
                xs = [pt[0] for pt in p]
                ys = [pt[1] for pt in p]
                cx, cy = sum(xs) / len(xs), sum(ys) / len(ys)
            items.append((t, float(s) if s is not None else 0.0, (cx, cy)))
    return items


AMOUNT_RE = re.compile(r"€?\s*([\d][\d,]*(?:\.\d{2})?)")


def classify(items) -> str:
    blob = " ".join(t.lower() for t, _, _ in items)
    if not blob.strip() or len(blob) < 30:
        return "blank"
    if "general information" in blob or "name of political party" in blob:
        return "general_info"
    if "details of donations" in blob or "value of donation" in blob:
        # a blank photocopied detail page still has the printed labels; treat as
        # donation only if a value/donor field looks filled
        return "donation_detail"
    if "statutory declaration" in blob or "certificate of monetary" in blob:
        return "declaration"
    return "other"


def main() -> None:
    doc = fitz.open(PDF)
    ocr = make_ocr()
    pages_meta = []
    current_party = None
    donations = []

    for i in range(doc.page_count):
        items = ocr_page(ocr, doc[i])
        kind = classify(items)
        texts = [(t, s) for t, s, _ in items]

        if kind == "general_info":
            # party name is the value near the "Name of Political Party" label
            party = None
            for j, (t, _, _) in enumerate(items):
                if "name of political party" in t.lower() and j + 1 < len(items):
                    party = items[j + 1][0]
                    break
            current_party = party or "(unparsed)"

        donor = value = nature = date = None
        if kind == "donation_detail":
            for j, (t, _, _) in enumerate(items):
                low = t.lower()
                nxt = items[j + 1][0] if j + 1 < len(items) else ""
                if "postal" in low or ("name and full" in low):
                    donor = nxt
                if "value of donation" in low:
                    m = AMOUNT_RE.search(nxt)
                    value = nxt if m else nxt
                if "date" in low and "received" in low:
                    date = nxt
            # only count as a real donation if it has a parsed value or donor
            has_value = any("value of donation" in t.lower() for t, _, _ in items)
            filled = bool(
                (donor and len(donor) > 2)
                or any(AMOUNT_RE.fullmatch(t.strip()) for t, _, _ in items if "€" in t)
            )
            if has_value and filled:
                donations.append(
                    {
                        "page": i + 1,
                        "party": current_party,
                        "donor": donor,
                        "value": value,
                        "nature": nature,
                        "date": date,
                        "min_conf": min((s for _, s in texts), default=0.0),
                    }
                )

        pages_meta.append(
            {
                "page": i + 1,
                "kind": kind,
                "n_text": len(texts),
                "min_conf": round(min((s for _, s in texts), default=0.0), 2),
                "mean_conf": round(
                    sum(s for _, s in texts) / len(texts) if texts else 0.0, 2
                ),
            }
        )
        print(
            f"p{i + 1:>3} {kind:<15} n={len(texts):>3} "
            f"mean_conf={pages_meta[-1]['mean_conf']:.2f}"
            + (f"  party={current_party}" if kind == "general_info" else "")
        )

    from collections import Counter

    kinds = Counter(p["kind"] for p in pages_meta)
    print("\n=== SUMMARY ===")
    print("page kinds:", dict(kinds))
    print("donations parsed:", len(donations))
    parties = sorted({d["party"] for d in donations if d["party"]})
    print(f"parties with donations ({len(parties)}):", parties)
    if donations:
        confs = [d["min_conf"] for d in donations]
        print(f"donation min-conf: lo={min(confs):.2f} avg={sum(confs) / len(confs):.2f}")
        print("\nsample donations:")
        for d in donations[:12]:
            print(f"  p{d['page']:>3} {str(d['party'])[:18]:<18} "
                  f"{str(d['donor'])[:24]:<24} val={d['value']} date={d['date']}")

    (OUT / "donations_assess_pages.json").write_text(
        json.dumps({"pages": pages_meta, "donations": donations}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print("\nwrote", OUT / "donations_assess_pages.json")


if __name__ == "__main__":
    main()
