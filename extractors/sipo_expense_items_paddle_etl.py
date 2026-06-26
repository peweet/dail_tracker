"""SIPO GE2024 national-agent ITEMISED-EXPENSES extractor.

Companion to sipo_expenses_paddle_etl.py (which does the Part-3 candidate-summary
totals). This one extracts Part 4 of the same National-Agent returns:

  * Part 4 LINE ITEMS (pages ~11-20): each spending heading (4A Advertising,
    4B Publicity, 4C Election Posters, 4D Other Election Material, 4E Office and
    Stationery, 4F Transport and Travel, 4G Market Research, 4H Campaign Workers)
    is itemised as  Ref | Expenditure Item | Cost(€)  — e.g.
    `A16 | Meta ads | €24,853.05`. The Ref prefix letter (A..H) deterministically
    encodes the heading, so categorisation needs no header parsing.
  * Part 4 CATEGORY TOTALS ("4. Expenses Review" page, ~p21): the 8 heading totals
    + the Overall Expense total.

Validation (engine-independent, the role the constituency/cap anchors play in the
sibling ETL): Σ(line items in a heading) should reconcile to that heading's total
on the review page. Mismatch => an OCR miss or a split-page item, and gets flagged.

Scope: GE2024 only (the 8 party PDFs in data/bronze/scan_pdf/). Per-party parquet
-> _sipo_output/by_party/<key>_items.parquet + <key>_categories.parquet, combined
into sipo_expense_items_fact.parquet + sipo_expense_categories_fact.parquet.
Silver tier — stays silver until a view consumes it (the Part-3 summary + donations
are the tracks promoted to gold today via sipo_promote_to_gold.py).

Run (party keys; default all):
  ./.venv/Scripts/python.exe extractors/sipo_expense_items_paddle_etl.py ff
  ./.venv/Scripts/python.exe extractors/sipo_expense_items_paddle_etl.py
"""

from __future__ import annotations

import json
import re
import sys
import tempfile
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
import contextlib  # noqa: E402

from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

OUT_DIR = ROOT / "data/silver/sipo"
BY_PARTY_DIR = OUT_DIR / "by_party"
CKPT_ITEMS_DIR = BY_PARTY_DIR / "_ckpt_items"  # separate from Part-3's _ckpt
ITEMS_PARQUET = OUT_DIR / "sipo_expense_items_fact.parquet"
CATS_PARQUET = OUT_DIR / "sipo_expense_categories_fact.parquet"
SCAN_DIR = ROOT / "data/bronze/scan_pdf"

# key -> (pdf path, display party). Mirrors the sibling candidate-summary ETL.
PARTY_JOBS: dict[str, tuple[Path, str]] = {
    "ff": (SCAN_DIR / "output/ff_sipo_ge_2024_expenses-ocr.pdf", "Fianna Fáil"),
    "fg": (SCAN_DIR / "fg_sipo_ge_2024_expenses.pdf", "Fine Gael"),
    "sf": (SCAN_DIR / "sf_sipo_ge_2024_expenses.pdf", "Sinn Féin"),
    "lab": (SCAN_DIR / "lab_sipo_ge_2024_expenses.pdf", "Labour"),
    "green": (SCAN_DIR / "green_sipo_ge_2024_expenses.pdf", "Green Party"),
    "socdem": (SCAN_DIR / "socdem_sipo_ge_2024_expenses.pdf", "Social Democrats"),
    "pbp": (SCAN_DIR / "pbp_sipo_ge_2024_expenses.pdf", "People Before Profit/Solidarity"),
    "aontu": (SCAN_DIR / "aontu_sipo_ge_2024_expenses.pdf", "Aontú"),
    # Minor-party national-agent returns (mirrors sipo_expenses_paddle_etl.py PARTY_JOBS so
    # the Part-4 items watchdog can address them by key instead of silently falling back to
    # ALL parties). All scanned -> PaddleOCR; run one at a time via _sipo_items_watchdog.py.
    "centre_party": (SCAN_DIR / "centre_party_sipo_ge_2024_expenses.pdf", "The Centre Party of Ireland"),
    "i4c": (SCAN_DIR / "i4c_sipo_ge_2024_expenses.pdf", "Independents 4 Change"),
    "indep_ireland": (SCAN_DIR / "indep_ireland_sipo_ge_2024_expenses.pdf", "Independent Ireland"),
    "ireland_first": (SCAN_DIR / "ireland_first_sipo_ge_2024_expenses.pdf", "Ireland First"),
    "irish_freedom": (SCAN_DIR / "irish_freedom_sipo_ge_2024_expenses.pdf", "Irish Freedom Party"),
    "irish_people": (SCAN_DIR / "irish_people_sipo_ge_2024_expenses.pdf", "The Irish People"),
    "redress100": (SCAN_DIR / "redress100_sipo_ge_2024_expenses.pdf", "100% Redress Party"),
    "right_to_change": (SCAN_DIR / "right_to_change_sipo_ge_2024_expenses.pdf", "Right to Change"),
}

# Ref-prefix letter -> (section code, heading). Part 4 of the standard SIPO
# National-Agent return; verbatim from the "Expenses Review" page.
REF_CATEGORY: dict[str, tuple[str, str]] = {
    "A": ("4A", "Advertising"),
    "B": ("4B", "Publicity"),
    "C": ("4C", "Election Posters"),
    "D": ("4D", "Other Election Material"),
    "E": ("4E", "Office and Stationery"),
    "F": ("4F", "Transport and Travel"),
    "G": ("4G", "Market Research"),
    "H": ("4H", "Campaign Workers"),
}
SECTION_NAME = {code: name for code, name in REF_CATEGORY.values()}

DPI = 300
LOW_CONF = 0.85
REF_RE = re.compile(r"^([A-H])(\d{1,3})$")
SECTION_RE = re.compile(r"^4([A-H])\b")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def parse_money(text: str) -> float | None:
    """Parse a PaddleOCR money cell, honouring an explicit 2-dp tail. Handles the
    occasional OCR thousands-dot ('€ 4.305.00' -> 4305.00) and no-space '€8,860.76'."""
    t = text.replace("€", "").replace("(", "").replace(")", "").replace(" ", "").strip("_,.\\")
    if not any(ch.isdigit() for ch in t):
        return None
    m = re.match(r"^(\d[\d,. ]*?)([.,]\d{2})$", t)
    if m:
        whole = re.sub(r"\D", "", m.group(1))
        return float(f"{whole}.{m.group(2)[1:]}") if whole else None
    digits = re.sub(r"\D", "", t)
    return float(digits) if digits else None


def ocr_page(ocr, page, tmp_png: Path, dpi: int = DPI) -> tuple[list[dict], int]:
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
        for t, s, b in zip(texts, scores, boxes, strict=False):
            cells.append({"text": t, "score": float(s), "x0": b[0], "y0": b[1], "x1": b[2], "y1": b[3]})
    return cells, pix.width


def cluster_rows(cells: list[dict], y_tol: int) -> list[list[dict]]:
    rows: list[list[dict]] = []
    for c in sorted(cells, key=lambda c: ((c["y0"] + c["y1"]) / 2, c["x0"])):
        yc = (c["y0"] + c["y1"]) / 2
        if rows and abs(((rows[-1][0]["y0"] + rows[-1][0]["y1"]) / 2) - yc) <= y_tol:
            rows[-1].append(c)
        else:
            rows.append([c])
    for r in rows:
        r.sort(key=lambda c: c["x0"])
    return rows


def rightmost_money(row, width, min_frac=0.55):
    best = None
    for c in row:
        if c["x0"] < width * min_frac:
            continue
        if "€" in c["text"] or re.search(r"\d", c["text"]):
            v = parse_money(c["text"])
            if v is not None and (best is None or c["x0"] > best[0]["x0"]):
                best = (c, v)
    return best


def parse_item_row(row, width) -> dict | None:
    """A Part-4 line item: a Ref cell (A1..H99) on the left + a Cost cell on the
    right. Item text is whatever sits between them."""
    ref_cell = ref = letter = None
    for c in row:
        t = c["text"].replace(" ", "").strip(".")
        m = REF_RE.fullmatch(t)
        if m and c["x0"] < width * 0.30:
            ref_cell, ref, letter = c, f"{m.group(1)}{m.group(2)}", m.group(1)
            break
    if ref_cell is None:
        return None
    money = rightmost_money(row, width)
    cost_cell, cost = money if money else (None, None)
    item_cells = [
        c["text"]
        for c in row
        if c is not ref_cell
        and c is not cost_cell
        and "€" not in c["text"]
        and not REF_RE.fullmatch(c["text"].replace(" ", "").strip("."))
        and ref_cell["x1"] <= c["x0"]
    ]
    item = re.sub(r"\s+", " ", " ".join(item_cells)).strip(" .,-")
    section, category = REF_CATEGORY[letter]
    return {
        "section": section,
        "category": category,
        "ref": ref,
        "item_description": item or None,
        "cost_eur": round(cost, 2) if cost is not None else None,
        "cost_confidence": round(cost_cell["score"], 3) if cost_cell else None,
        "row_min_confidence": round(min(c["score"] for c in row), 3),
    }


def parse_summary_row(row, width) -> dict | None:
    """A row on the 'Expenses Review' page: '4A - Advertising' + total, or the
    'Overall Expense total:' grand total."""
    label = " ".join(c["text"] for c in row if c["x0"] < width * 0.55).strip()
    money = rightmost_money(row, width)
    if money is None:
        return None
    cost_cell, total = money
    norm = label.replace(" ", "")
    ms = SECTION_RE.match(norm)
    if ms:
        code = f"4{ms.group(1)}"
        return {
            "section": code,
            "category": SECTION_NAME[code],
            "category_total_eur": round(total, 2),
            "total_confidence": round(cost_cell["score"], 3),
            "is_overall": False,
        }
    if "overall" in label.lower() and "total" in label.lower():
        return {
            "section": "TOTAL",
            "category": "Overall Expense total",
            "category_total_eur": round(total, 2),
            "total_confidence": round(cost_cell["score"], 3),
            "is_overall": True,
        }
    return None


def process_party(ocr, key, pdf_path, party):
    """Crash-proof per-page (mirrors the Part-3 ETL): each page's parsed items +
    category rows are checkpointed to _ckpt_items/<key>/p<N>.json. PaddleOCR can
    SEGFAULT (uncatchable, kills the process) or HANG — on restart we skip done
    pages and walk a DPI retry ladder, skipping a page that keeps crashing us."""
    doc = fitz.open(pdf_path)
    y_tol = int(DPI / 72 * 9)
    tmp_png = Path(tempfile.gettempdir()) / f"sipo_items_{key}.png"
    ckpt = CKPT_ITEMS_DIR / key
    ckpt.mkdir(parents=True, exist_ok=True)
    items: list[dict] = []
    cats: list[dict] = []

    for pno, page in enumerate(doc, start=1):
        done = ckpt / f"p{pno:03}.json"
        attempt = ckpt / f"p{pno:03}.attempt"
        if done.exists():
            d = json.loads(done.read_text(encoding="utf-8"))
            items += d.get("items", [])
            cats += d.get("cats", [])
            continue
        # retry ladder: 2x@300 (transient segfaults), then 1x@200, then skip
        tries = json.loads(attempt.read_text(encoding="utf-8"))["tries"] if attempt.exists() else []
        if not tries or tries == [300]:
            dpi = 300
        elif tries == [300, 300]:
            dpi = 200
        else:
            print(f"    [{party}] page {pno}: crashed at {tries} -> SKIP (failed)", flush=True)
            done.write_text(json.dumps({"failed": True, "tries": tries, "items": [], "cats": []}), encoding="utf-8")
            attempt.unlink(missing_ok=True)
            continue
        print(f"    [{party}] OCR page {pno}/{doc.page_count} @ {dpi}dpi...", flush=True)
        attempt.write_text(json.dumps({"tries": tries + [dpi]}), encoding="utf-8")  # mark BEFORE the call
        cells, width = ocr_page(ocr, page, tmp_png, dpi)
        rows = cluster_rows(cells, y_tol)
        blob = " ".join(c["text"].lower() for c in cells)
        is_summary = "expenses review" in blob or (sum(1 for r in rows if parse_summary_row(r, width)) >= 5)
        page_items: list[dict] = []
        page_cats: list[dict] = []
        for r in rows:
            if is_summary:
                s = parse_summary_row(r, width)
                if s:
                    page_cats.append({**s, "source_pdf": pdf_path.name, "source_page": pno})
            it = parse_item_row(r, width)
            if it and (it["cost_eur"] is not None or it["item_description"]):
                page_items.append({"party": party, **it, "source_pdf": pdf_path.name, "source_page": pno})
        done.write_text(json.dumps({"failed": False, "items": page_items, "cats": page_cats}), encoding="utf-8")
        attempt.unlink(missing_ok=True)
        items += page_items
        cats += page_cats

    # flag line items
    for it in items:
        if it["cost_eur"] is None:
            it["flag"] = "no_cost"
        elif (it["cost_confidence"] or 1) < LOW_CONF or it["row_min_confidence"] < LOW_CONF:
            it["flag"] = "low_confidence_verify"
        else:
            it["flag"] = "ok"

    # reconcile line-item sums to the review-page category totals
    for c in cats:
        c["party"] = party
        if c["is_overall"]:
            c["items_sum_eur"] = round(sum(i["cost_eur"] or 0 for i in items), 2)
        else:
            c["items_sum_eur"] = round(sum(i["cost_eur"] or 0 for i in items if i["section"] == c["section"]), 2)
        tot = c["category_total_eur"]
        c["reconciles"] = bool(tot and abs(c["items_sum_eur"] - tot) <= max(1.0, tot * 0.01))

    return pl.DataFrame(items), pl.DataFrame(cats)


def main() -> None:
    keys = [k for k in sys.argv[1:] if k in PARTY_JOBS] or list(PARTY_JOBS)
    from paddleocr import PaddleOCR

    ocr = PaddleOCR(
        lang="en",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,  # REQUIRED on Windows (paddle 3.3.1 oneDNN/PIR bug)
        text_det_limit_side_len=1280,
        text_det_limit_type="max",
    )

    BY_PARTY_DIR.mkdir(parents=True, exist_ok=True)
    for key in keys:
        pdf_path, party = PARTY_JOBS[key]
        hr(f"PROCESSING {key} — {party}")
        if not pdf_path.exists():
            print(f"  !! missing PDF: {pdf_path}")
            continue
        items_df, cats_df = process_party(ocr, key, pdf_path, party)
        if items_df.height:
            save_parquet(items_df.sort(["section", "ref"]), BY_PARTY_DIR / f"{key}_items.parquet")
        if cats_df.height:
            save_parquet(cats_df, BY_PARTY_DIR / f"{key}_categories.parquet")
        n_cost = items_df.filter(pl.col("cost_eur").is_not_null()).height if items_df.height else 0
        tot = cats_df.filter(pl.col("is_overall"))["category_total_eur"].sum() if cats_df.height else 0
        print(
            f"  {party}: {items_df.height} line items ({n_cost} with cost), "
            f"{cats_df.height} category rows, overall total €{tot:,.2f}"
        )
        if cats_df.height:
            recon = cats_df.filter(~pl.col("is_overall"))
            ok = recon.filter(pl.col("reconciles")).height
            print(f"    reconciliation: {ok}/{recon.height} headings match line-item sums")

    # combine
    item_parts = [pl.read_parquet(p) for p in sorted(BY_PARTY_DIR.glob("*_items.parquet"))]
    cat_parts = [pl.read_parquet(p) for p in sorted(BY_PARTY_DIR.glob("*_categories.parquet"))]
    if item_parts:
        combined = pl.concat(item_parts, how="vertical_relaxed").sort(["party", "section", "ref"])
        save_parquet(combined, ITEMS_PARQUET)
        hr("COMBINED LINE ITEMS")
        print(
            combined.group_by("party")
            .agg(
                pl.len().alias("items"),
                pl.col("cost_eur").sum().alias("sum_eur"),
            )
            .sort("party")
        )
        print(f"wrote {ITEMS_PARQUET.relative_to(ROOT)} ({combined.height} rows)")
    if cat_parts:
        combined_c = pl.concat(cat_parts, how="vertical_relaxed").sort(["party", "section"])
        save_parquet(combined_c, CATS_PARQUET)
        print(f"wrote {CATS_PARQUET.relative_to(ROOT)} ({combined_c.height} rows)")


if __name__ == "__main__":
    main()
