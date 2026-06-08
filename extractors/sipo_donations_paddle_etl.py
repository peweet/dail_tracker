"""SIPO 2024 election DONATIONS extractor — cache+parse.

Source: data/bronze/scan_pdf/2024_election_donations.pdf (105pp scanned, NO text
layer). Assessment (probe_sipo_donations_assess.py) showed the scan is crisp and
PaddleOCR reads it at ~0.98-0.99. Structure is a per-PARTY form packet, NOT a
table:
  * a "General Information" page  -> party name + appropriate officer
  * a declaration page
  * N "Details of Donations" pages -> ONE donation each, key/value layout:
      donor name+address, value, nature, description of donor, date received,
      requested?, corporate-approval-form?, receipt issued?, Irish-citizen?,
      receipt issuer.

Same two-stage design as the Part-3 expenses ETL (so the expensive OCR is cached
once and the parser can be re-iterated freely WITHOUT re-OCR — the lesson from the
€0.00 column bug):
  1. CACHE  : OCR every page -> _ckpt_donations/c<NNN>.json  (raw cells + boxes),
              crash/hang-proof via the .attempt retry ladder + the donations
              watchdog.
  2. PARSE  : read cached cells -> classify pages -> carry party down from each
              General-Information page -> extract donation fields -> parquet.

⚠️ PRIVACY: donations name PRIVATE donors + home addresses (legally published but
personal data — see project memory feedback_personal_insolvency_privacy). The
extractor captures donor_address_raw, but any app surface must SUPPRESS the home
address; show donor name + value + party + date + source link only, and never
imply influence (feedback_no_inference_in_app). OCR-derived -> "verify vs the
official SIPO PDF p.N".

Run:
  cache:   ./.venv/Scripts/python.exe extractors/sipo_donations_paddle_etl.py cache
  parse:   ./.venv/Scripts/python.exe extractors/sipo_donations_paddle_etl.py parse
  both:    ./.venv/Scripts/python.exe extractors/sipo_donations_paddle_etl.py
(the watchdog calls `cache`; `parse` is cheap and re-runnable.)
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

PDF = ROOT / "data/bronze/scan_pdf/2024_election_donations.pdf"
OUT_DIR = ROOT / "data/silver/sipo"
CKPT = OUT_DIR / "_ckpt_donations"
OUT_PARQUET = OUT_DIR / "sipo_donations_fact.parquet"
DPI = 300
LOW_CONF = 0.85


# ---------------------------------------------------------------- OCR stage ----
def ocr_page(ocr, page, tmp_png: Path, dpi: int = DPI) -> list[dict]:
    import fitz

    pix = page.get_pixmap(matrix=fitz.Matrix(dpi / 72, dpi / 72))
    pix.save(tmp_png)
    res = ocr.predict(input=str(tmp_png))
    cells = []
    for r in res:
        d = r if isinstance(r, dict) else {}
        texts = d.get("rec_texts") or []
        scores = d.get("rec_scores") or []
        boxes = d.get("rec_boxes")
        boxes = boxes.tolist() if hasattr(boxes, "tolist") else (boxes or [])
        for t, s, b in zip(texts, scores, boxes):
            cells.append(
                {
                    "text": t,
                    "score": round(float(s), 4),
                    "x0": int(b[0]),
                    "y0": int(b[1]),
                    "x1": int(b[2]),
                    "y1": int(b[3]),
                }
            )
    return cells


def cache_stage() -> None:
    """OCR all pages -> _ckpt_donations/c<NNN>.json. Resumes from checkpoints;
    .attempt retry ladder (2x300 -> 1x200 -> skip) survives segfaults/hangs."""
    import fitz
    from paddleocr import PaddleOCR

    CKPT.mkdir(parents=True, exist_ok=True)
    ocr = PaddleOCR(
        lang="en",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,  # REQUIRED on Windows (paddle 3.3.1 oneDNN/PIR bug)
        text_det_limit_side_len=1280,  # full A4 @300 DPI segfaults the detector
        text_det_limit_type="max",
    )
    doc = fitz.open(PDF)
    tmp_png = Path(tempfile.gettempdir()) / "sipo_don.png"
    for pno, page in enumerate(doc, start=1):
        done = CKPT / f"c{pno:03}.json"
        attempt = CKPT / f"a{pno:03}.attempt"
        if done.exists():
            continue
        tries = json.loads(attempt.read_text(encoding="utf-8"))["tries"] if attempt.exists() else []
        if not tries or tries == [300]:
            dpi = 300
        elif tries == [300, 300]:
            dpi = 200
        else:
            print(f"    donations page {pno}: crashed at {tries} -> SKIP", flush=True)
            done.write_text(json.dumps({"failed": True, "cells": []}), encoding="utf-8")
            attempt.unlink(missing_ok=True)
            continue
        print(f"    OCR donations page {pno}/{doc.page_count} @ {dpi}dpi...", flush=True)
        attempt.write_text(json.dumps({"tries": tries + [dpi]}), encoding="utf-8")
        cells = ocr_page(ocr, page, tmp_png, dpi)
        done.write_text(json.dumps({"failed": False, "cells": cells}), encoding="utf-8")
        attempt.unlink(missing_ok=True)


# -------------------------------------------------------------- parse stage ----
def yc(c: dict) -> float:
    return (c["y0"] + c["y1"]) / 2


def classify(cells: list[dict]) -> str:
    blob = " ".join(c["text"].lower() for c in cells)
    if len(blob.strip()) < 30:
        return "blank"
    if "general information" in blob or "name of political party" in blob:
        return "general_info"
    if "details of donations" in blob or "value of donation" in blob:
        return "donation_detail"
    if "statutory declaration" in blob or "certificate of monetary" in blob:
        return "declaration"
    return "other"


def find_label(cells: list[dict], *subs: str) -> dict | None:
    """First cell whose lowercased text contains any of the given substrings."""
    for c in cells:
        t = c["text"].lower()
        if any(s in t for s in subs):
            return c
    return None


def value_right(cells: list[dict], label: dict | None, ytol: int = 28) -> list[dict]:
    """Value cells sitting to the RIGHT of a label cell, on the same y-band."""
    if label is None:
        return []
    ly = yc(label)
    return sorted(
        (c for c in cells if c["x0"] >= label["x1"] - 5 and abs(yc(c) - ly) <= ytol),
        key=lambda c: c["x0"],
    )


def value_block(cells: list[dict], label: dict | None, y_until: float, x_min: int) -> list[dict]:
    """Value cells in the right column from a label's y down to y_until (the next
    field's y) — used for the multi-line donor name+address block."""
    if label is None:
        return []
    ly = yc(label)
    return sorted(
        (c for c in cells if c["x0"] >= x_min and ly - 20 <= yc(c) <= y_until - 10),
        key=lambda c: (yc(c), c["x0"]),
    )


def field_value(cells: list[dict], label: dict | None, x_min: int = 900, ywin: int = 95) -> str | None:
    """Value-column entry nearest a label's y — handles General-Information fields
    whose value is offset vertically from the label (party name sits above OR below
    'Name of Political Party:'). Joins cells on the nearest value-row."""
    if label is None:
        return None
    ly = yc(label)
    vcol = sorted((c for c in cells if c["x0"] > x_min and abs(yc(c) - ly) <= ywin), key=lambda c: abs(yc(c) - ly))
    if not vcol:
        return None
    row_y = yc(vcol[0])
    row = sorted((c for c in vcol if abs(yc(c) - row_y) <= 18), key=lambda c: c["x0"])
    return re.sub(r"\s+", " ", " ".join(c["text"] for c in row)).strip(" .,-:") or None


def join(cells: list[dict]) -> str:
    return re.sub(r"\s+", " ", " ".join(c["text"] for c in cells)).strip(" .,-")


def parse_money(text: str) -> float | None:
    t = text.replace("€", "").replace(",", "").replace(" ", "").strip("_.\\")
    m = re.search(r"\d+(?:\.\d{1,2})?", t)
    return float(m.group(0)) if m else None


def parse_donation_page(cells: list[dict], pno: int) -> dict | None:
    """FIRST-PASS field extractor (iterate against cached cells, no re-OCR needed).
    Anchors on the printed field labels and reads the value to their right."""
    val_lbl = find_label(cells, "value of donation")
    donor_lbl = find_label(cells, "full postal", "postal address of donor", "name and full")
    if val_lbl is None and donor_lbl is None:
        return None  # not a filled donation page

    nature_lbl = find_label(cells, "nature of")
    # donor block runs from the donor label down to the "requested?" row
    requested_lbl = find_label(cells, "was donation requested")
    y_until = yc(requested_lbl) if requested_lbl else (yc(val_lbl) if val_lbl else 99999)
    x_min = (donor_lbl["x1"] - 5) if donor_lbl else 0
    donor_cells = value_block(cells, donor_lbl, y_until, x_min)
    donor_text = [c["text"] for c in donor_cells]

    val_cells = value_right(cells, val_lbl)
    value_eur = next((parse_money(c["text"]) for c in val_cells if parse_money(c["text"]) is not None), None)

    def rv(*subs: str) -> str | None:
        v = join(value_right(cells, find_label(cells, *subs)))
        return v or None

    confs = [c["score"] for c in cells] or [0.0]
    return {
        "donation_page": pno,
        "donor_name": donor_text[0] if donor_text else None,
        "donor_address_raw": " ".join(donor_text[1:]) if len(donor_text) > 1 else None,  # PII
        "value_eur": value_eur,
        "value_raw": join(val_cells) or None,
        "nature": join(value_right(cells, nature_lbl)) or None,
        "description_of_donor": rv("description of donor"),
        "date_received_raw": rv("date donation", "date of donation"),
        "requested": rv("was donation requested"),
        "corporate_approval_form": rv("statement of approval"),
        "receipt_issued": rv("did receipt issue"),
        "donor_irish_citizen": rv("irish citizen"),
        "receipt_issuer": rv("name of person who issued"),
        "min_confidence": round(min(confs), 3),
    }


def parse_stage() -> pl.DataFrame:
    if not CKPT.exists():
        print("  no cached cells yet — run the cache stage first.")
        return pl.DataFrame()
    party = officer = None
    rows: list[dict] = []
    page_kinds: dict[int, str] = {}
    for f in sorted(CKPT.glob("c*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        pno = int(f.stem[1:])
        if d.get("failed") or not d.get("cells"):
            page_kinds[pno] = "failed"
            continue
        cells = d["cells"]
        kind = classify(cells)
        page_kinds[pno] = kind
        if kind == "general_info":
            # each general-info page starts a NEW party packet -> reset
            pl_lbl = find_label(cells, "name of political party")
            of_lbl = find_label(cells, "appropriate officer", "name of appropriate")
            party = field_value(cells, pl_lbl)
            officer = field_value(cells, of_lbl)
            continue
        if kind != "donation_detail":
            continue
        rec = parse_donation_page(cells, pno)
        if rec and (rec["donor_name"] or rec["value_eur"] is not None):
            flag = "ok"
            if rec["value_eur"] is None:
                flag = "no_value"
            elif rec["min_confidence"] < LOW_CONF:
                flag = "low_confidence_verify"
            rows.append(
                {
                    "party": party,
                    "appropriate_officer": officer,
                    **rec,
                    "flag": flag,
                    "source_pdf": PDF.name,
                    "source_page": pno,
                }
            )
    from collections import Counter

    print("page kinds:", dict(Counter(page_kinds.values())))
    return pl.DataFrame(rows)


def main() -> None:
    mode = sys.argv[1] if len(sys.argv) > 1 else "both"
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    if mode in ("cache", "both"):
        print("=== CACHE (OCR) stage ===", flush=True)
        cache_stage()
    if mode in ("parse", "both"):
        print("=== PARSE stage ===", flush=True)
        df = parse_stage()
        if df.height:
            save_parquet(df, OUT_PARQUET)
            print(f"\n{df.height} donations parsed -> {OUT_PARQUET.relative_to(ROOT)}")
            print(
                "by party:",
                df.group_by("party")
                .agg(pl.len().alias("donations"), pl.col("value_eur").sum().alias("total_eur"))
                .sort("party")
                .to_dicts(),
            )
            print("flags:", dict(zip(*df["flag"].value_counts().sort("flag").to_dict(as_series=False).values())))
        else:
            print("no donations parsed (run cache first, or refine the parser).")


if __name__ == "__main__":
    main()
