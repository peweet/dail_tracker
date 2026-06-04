"""Build SIPO Part-4 (itemised + category totals) WITHOUT running OCR.

Three zero-OCR sources (the rest of the parties need the OCR extractor later):
  * FF  : its Part-4 pages are already OCR'd and cached as *parsed* items+cats in
          _ckpt_items/ff/p*.json -> just aggregate.
  * SF, Aontú : BORN-DIGITAL returns -> read the embedded text layer and reuse the
          OCR extractor's own parse_item_row / parse_summary_row (no PaddleOCR).

Reuses sipo_expense_items_paddle_etl (imported; its PaddleOCR import is inside
main(), so importing the module does NOT load paddle). Writes the same per-party +
combined parquets the OCR extractor would, so a later OCR run for the scanned
parties just adds to them. Sandbox only.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline_sandbox"))
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

import sipo_expense_items_paddle_etl as E  # noqa: E402  (PaddleOCR import is lazy)

BY = ROOT / "pipeline_sandbox/_sipo_output/by_party"
CKPT_ITEMS = BY / "_ckpt_items"
OUT_DIR = ROOT / "pipeline_sandbox/_sipo_output"
SCAN = ROOT / "data/bronze/scan_pdf"

BORN_DIGITAL = {
    "sf": (SCAN / "sf_sipo_ge_2024_expenses.pdf", "Sinn Féin"),
    "aontu": (SCAN / "aontu_sipo_ge_2024_expenses.pdf", "Aontú"),
}


def text_layer_cells(page) -> list[dict]:
    cells = []
    for block in page.get_text("dict")["blocks"]:
        for line in block.get("lines", []):
            spans = line["spans"]
            text = "".join(s["text"] for s in spans).strip()
            if not text:
                continue
            xs0 = [s["bbox"][0] for s in spans]; ys0 = [s["bbox"][1] for s in spans]
            xs1 = [s["bbox"][2] for s in spans]; ys1 = [s["bbox"][3] for s in spans]
            cells.append({"text": text, "score": 1.0,
                          "x0": min(xs0), "y0": min(ys0), "x1": max(xs1), "y1": max(ys1)})
    return cells


def aggregate_ff_from_cache() -> tuple[pl.DataFrame, pl.DataFrame]:
    items: list[dict] = []
    cats: list[dict] = []
    for f in sorted((CKPT_ITEMS / "ff").glob("p*.json")):
        j = json.loads(f.read_text(encoding="utf-8"))
        items += j.get("items", [])
        cats += j.get("cats", [])
    _flag_and_reconcile(items, cats, "Fianna Fáil")
    return pl.DataFrame(items), pl.DataFrame(cats)


def parse_born_digital(pdf: Path, party: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    doc = fitz.open(pdf)
    items: list[dict] = []
    cats: list[dict] = []
    for pno, page in enumerate(doc, start=1):
        cells = text_layer_cells(page)
        if not cells:
            continue
        width = page.rect.width
        rows = E.cluster_rows(cells, y_tol=4)  # text-layer rows are ~10-12pt tall
        blob = " ".join(c["text"].lower() for c in cells)
        is_summary = "expenses review" in blob or sum(1 for r in rows if E.parse_summary_row(r, width)) >= 5
        for r in rows:
            if is_summary:
                s = E.parse_summary_row(r, width)
                if s:
                    cats.append({**s, "source_pdf": pdf.name, "source_page": pno})
            it = E.parse_item_row(r, width)
            if it and (it["cost_eur"] is not None or it["item_description"]):
                items.append({"party": party, **it, "source_pdf": pdf.name, "source_page": pno})
    _flag_and_reconcile(items, cats, party)
    return pl.DataFrame(items), pl.DataFrame(cats)


def _flag_and_reconcile(items: list[dict], cats: list[dict], party: str) -> None:
    for it in items:
        if it.get("cost_eur") is None:
            it["flag"] = "no_cost"
        elif (it.get("cost_confidence") or 1) < E.LOW_CONF or it.get("row_min_confidence", 1) < E.LOW_CONF:
            it["flag"] = "low_confidence_verify"
        else:
            it["flag"] = "ok"
    for c in cats:
        c["party"] = party
        if c.get("is_overall"):
            c["items_sum_eur"] = round(sum(i.get("cost_eur") or 0 for i in items), 2)
        else:
            c["items_sum_eur"] = round(sum(i.get("cost_eur") or 0 for i in items if i["section"] == c["section"]), 2)
        tot = c.get("category_total_eur")
        c["reconciles"] = bool(tot and abs(c["items_sum_eur"] - tot) <= max(1.0, tot * 0.01))


def write_party(key: str, items_df: pl.DataFrame, cats_df: pl.DataFrame) -> None:
    if items_df.height:
        items_df.sort(["section", "ref"]).write_parquet(
            BY / f"{key}_items.parquet", compression="zstd", compression_level=3, statistics=True)
    if cats_df.height:
        # SIPO prints the Expenses Review twice (expenses NOT met / MET from public
        # funds), so a heading + the Overall total can appear twice. Keep, per section,
        # the row with the larger total (the real 'not met from public funds' figures;
        # the 'met' copy is typically €0/blank). Mirrors the Part-3 twice-printed dedup.
        cats_df = cats_df.sort("category_total_eur", descending=True).unique(
            subset=["section"], keep="first").sort("section")
        cats_df.write_parquet(
            BY / f"{key}_categories.parquet", compression="zstd", compression_level=3, statistics=True)
    overall = (cats_df.filter(pl.col("is_overall"))["category_total_eur"].sum() if cats_df.height else 0)
    n_cost = items_df.filter(pl.col("cost_eur").is_not_null()).height if items_df.height else 0
    print(f"  {key}: {items_df.height} items ({n_cost} w/cost), {cats_df.height} cat rows, "
          f"overall €{overall:,.2f}")


def main() -> None:
    print("=== FF (from cache) ===")
    write_party("ff", *aggregate_ff_from_cache())
    for key, (pdf, party) in BORN_DIGITAL.items():
        print(f"=== {key} — {party} (born-digital text layer) ===")
        write_party(key, *parse_born_digital(pdf, party))

    # rebuild combined
    item_parts = [pl.read_parquet(p) for p in sorted(BY.glob("*_items.parquet"))]
    cat_parts = [pl.read_parquet(p) for p in sorted(BY.glob("*_categories.parquet"))]
    if item_parts:
        pl.concat(item_parts, how="vertical_relaxed").sort(["party", "section", "ref"]).write_parquet(
            OUT_DIR / "sipo_expense_items_fact.parquet", compression="zstd", compression_level=3, statistics=True)
    if cat_parts:
        pl.concat(cat_parts, how="vertical_relaxed").sort(["party", "section"]).write_parquet(
            OUT_DIR / "sipo_expense_categories_fact.parquet", compression="zstd", compression_level=3, statistics=True)

    print("\n=== category overall totals (the real per-party national-agent spend) ===")
    if cat_parts:
        comb = pl.concat(cat_parts, how="vertical_relaxed")
        print(comb.filter(pl.col("is_overall")).select(["party", "category_total_eur"]).sort("party"))


if __name__ == "__main__":
    main()
