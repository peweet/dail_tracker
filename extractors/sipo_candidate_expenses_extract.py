"""Parse the per-candidate GE2024 SIPO Election-Expenses statements → SILVER facts.

This is the PARSE pass (no OCR). It reads the cached OCR cells written by
``extractors/sipo_candidate_ocr.py`` (``data/silver/sipo_candidate/_ckpt/<key>/cNNN.json``,
one file per page: ``{"failed": bool, "cells": [{"text","score","x0","y0","x1","y1"}]}``)
and emits two SILVER parquet facts at TWO DIFFERENT GRAINS (Kimball: never mix grains):

  1. candidate grain  ->  data/silver/sipo_candidate/sipo_candidate_expenses.parquet
       one row per candidate statement: identity + totals + the 8-category (5A-5H) grid.
  2. line-item grain  ->  data/silver/sipo_candidate/sipo_candidate_expense_items.parquet
       one row per Part-5 line item, e.g. Noel Grealish -> "Galway Advertiser" €2,799.48.

INCREMENTAL BY DESIGN (functional/idempotent data engineering): it processes whatever
is currently in the cell-cache and is safe to re-run — as OCR fills in the remaining
~30% of statements, re-running picks them up with no double counting (output is a full
overwrite keyed off the cache). So we do NOT wait for OCR to finish.

QUALITY POSTURE (no-inference): figures are reported with a per-row ``reconciles`` flag
(Σ categories ≈ overall total) and a ``min_confidence``; rows that don't reconcile are
FLAGGED, never dropped or repaired with an invented number. Legal boilerplate and
subtotal rows are filtered out (they don't match the label/ref anchors), not modelled.

Source provenance: the validated prototype was ``c:/tmp/sipo_candidate_parse_proto.py``
([[project_sipo_candidate_expenses_corpus]]); this is its production promotion.

Run:  ./.venv/Scripts/python.exe extractors/sipo_candidate_expenses_extract.py
"""

from __future__ import annotations

import contextlib
import csv
import json
import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

CKPT = ROOT / "data/silver/sipo_candidate/_ckpt"
MANIFEST = ROOT / "data/bronze/sipo_candidate_expenses/_manifest.csv"
OUT_DIR = ROOT / "data/silver/sipo_candidate"
OUT_HEAD = OUT_DIR / "sipo_candidate_expenses.parquet"
OUT_ITEMS = OUT_DIR / "sipo_candidate_expense_items.parquet"

# A "complete enough" statement is the standard ~13pp fillable template; fewer pages
# means OCR is still in flight for that doc (we still emit it, flagged not-complete).
MIN_COMPLETE_PAGES = 13

# Statutory candidate spending limit GE2024 (Electoral Act 1997 as amended): the
# 5-seat constituency cap is the highest. A single LINE ITEM is necessarily <= the
# candidate total <= this cap, so any parsed item above it is an OCR decimal-loss
# artefact (e.g. €287.84 mis-read as "28784"). We FLAG these (cost_suspect), never
# silently drop or "repair" them — no-inference. Gold sums exclude flagged rows.
STATUTORY_MAX_EUR = 58_350.0

CATS = ["5A", "5B", "5C", "5D", "5E", "5F", "5G", "5H"]
CAT_LABELS = {
    "5A": "Advertising",
    "5B": "Publicity",
    "5C": "Election Posters",
    "5D": "Other Election Material",
    "5E": "Office and Stationery",
    "5F": "Transport and Travel",
    "5G": "Market Research",
    "5H": "Campaign Workers",
}


_SEPS = ".,-"  # OCR renders the SIPO decimal mark inconsistently as '.', ',' or '-'


# Canonical party from the noisy OCR'd "declared party" field on page 1. The field is
# unreliable — it also captures the MS-Word fillable placeholder ("Click here to enter
# text."), SIPO footer contact info, and (where OCR grabbed the wrong cell) candidate
# NAMES. So this is a contains-match (tolerates case/accent/spacing OCR variants) and
# anything unmatched maps to NULL — we canonicalise a known party but NEVER invent one
# (no-inference). ORDER MATTERS: specific rules first (Independent Ireland before the
# generic independent -> Non-Party). The patterns deliberately stop before accented
# letters ("fianna f", "sinn f", "aont") so fé/fé/fa all match without de-accenting.
PARTY_RULES: list[tuple[str, str]] = [
    ("independent ireland", "Independent Ireland"),
    ("independents4change", "Independents 4 Change"),
    ("independents 4 change", "Independents 4 Change"),
    ("fine gael", "Fine Gael"),
    ("fianna f", "Fianna Fáil"),
    ("sinn f", "Sinn Féin"),
    ("green party", "Green Party"),
    ("comhaontas", "Green Party"),
    ("social democrat", "Social Democrats"),
    ("people before profit", "People Before Profit/Solidarity"),
    ("solidarity", "People Before Profit/Solidarity"),
    ("labour", "Labour"),
    ("aont", "Aontú"),
    ("irish people", "The Irish People"),
    ("irish freedom", "Irish Freedom Party"),
    ("100% redress", "100% Redress Party"),
    ("redress", "100% Redress Party"),
    ("rdr", "100% Redress Party"),  # registry abbreviates "100% RDR"
    ("liberty republic", "Liberty Republic"),
    ("animal welfare", "Party for Animal Welfare"),
    ("rabharta", "Rabharta"),
    ("non-party", "Non-Party"),
    ("non party", "Non-Party"),
    ("nonparty", "Non-Party"),
    ("independ", "Non-Party"),  # independent / independant — AFTER Independent Ireland
]


def canon_party_expr(col: str = "party_declared") -> pl.Expr:
    """Vectorised canonicalisation of a party-name column -> party (no UDF). Builds a
    when/then chain folded so the FIRST matching PARTY_RULES entry wins; unmatched -> NULL.
    Reused in the gold step to canonicalise the authoritative member-registry party too."""
    norm = pl.col(col).str.to_lowercase().str.strip_chars()
    expr: pl.Expr = pl.lit(None, dtype=pl.Utf8)
    for sub, canon in reversed(PARTY_RULES):
        expr = pl.when(norm.str.contains(sub, literal=True)).then(pl.lit(canon)).otherwise(expr)
    return expr.alias("party")


def parse_money(s: str) -> float | None:
    """Robust €-amount parse tolerant of OCR separator noise. Returns euros as float.

    The decimal rule: the LAST separator with exactly 2 trailing digits is the real
    decimal point; everything left of it is thousands and gets stripped. Otherwise the
    last separator is a thousands sep and all separators are stripped.

    Crucially, the recogniser captures the decimal mark correctly (>0.99 conf) but
    GLYPHS it inconsistently — ``€10,270.50`` comes through as ``'10270-50'`` (decimal
    point rendered as a hyphen) or ``'1.845.00'`` (comma read as a period). Treating
    '.', ',' AND '-' all as separators recovers the exact value deterministically — so
    these are NOT lost to re-OCR and need no /100 heuristic. Verified against the raw
    cells (O'Donoghue C1 '10270-50' @0.993).
    """
    s = s.replace("€", "").replace("E", "").replace("£", "").strip()
    s = re.sub(r"[^\d.,\-]", "", s).strip(_SEPS)
    if not s:
        return None
    seps = [i for i, ch in enumerate(s) if ch in _SEPS]
    if seps:
        last = seps[-1]
        dec = s[last + 1:]
        if len(dec) == 2:  # noqa: SIM108 — genuine 2dp decimal; everything left = thousands
            s = re.sub(r"[.,\-]", "", s[:last]) + "." + dec
        else:  # last separator was a thousands sep -> strip all
            s = re.sub(r"[.,\-]", "", s)
    return float(s) if s else None


def load_pages(key: str) -> dict[str, list[dict]]:
    d = CKPT / key
    return {pj.stem: json.loads(pj.read_text(encoding="utf-8")).get("cells", [])
            for pj in sorted(d.glob("c*.json"))}


def page_type(cells: list[dict]) -> str | None:
    """Classify a summary page by the header nearest the top. The form has a TWIN
    summary: p11 'not met out of public funds' (the real campaign spend) vs p12 'met
    out of public funds' (usually all n/a). We keep both, labelled."""
    top = [c for c in cells if c["y0"] < 700]
    txt = " ".join(c["text"].lower() for c in top)
    if "not met out of public funds" in txt or "not met out of public" in txt:
        return "not_public"
    if "met out of public funds" in txt:
        return "public"
    return None


def parse_grid(cells: list[dict]) -> tuple[dict[str, float | None], float | None]:
    """Pair each 5A-5H label (left col) and the 'Overall Expense total:' label to the
    value cell to its right. OCR mangles label spacing ('5D-', '5 B -') so match on
    space-stripped text. The overall is chosen to RECONCILE with the category sum when
    an ambiguous value is present (kills a stray '€0' beside the real total)."""
    labels: dict[str, dict] = {}
    for c in cells:
        tn = c["text"].replace(" ", "").strip()
        for cat in CATS:
            if tn.startswith(cat + "-") or tn == cat:
                labels[cat] = c
        if c["text"].strip().startswith("Overall Expense total"):
            labels["OVERALL"] = c
    vals = [c for c in cells
            if c["x0"] > 600 and (("€" in c["text"]) or re.search(r"\d", c["text"]))]

    def value_for(label_cell: dict, prefer: float | None = None) -> tuple[float | None, float]:
        cands = [(c, parse_money(c["text"])) for c in vals
                 if c["x0"] > label_cell["x0"] + 400 and abs(c["y0"] - label_cell["y0"]) < 60]
        cands = [(c, v) for c, v in cands if v is not None]
        if not cands:
            return None, 0.0
        if prefer:  # prefer a near-y value that reconciles with the category sum
            best, bv = min(cands, key=lambda cv: abs((cv[1] or 0) - prefer))
            if abs((bv or 0) - prefer) < max(5.0, prefer * 0.02):
                return bv, float(best.get("score", 0.0))
        best = min(cands, key=lambda cv: abs(cv[0]["y0"] - label_cell["y0"]))
        return best[1], float(best[0].get("score", 0.0))

    grid: dict[str, float | None] = {}
    confs: list[float] = []
    for cat in CATS:
        if cat in labels:
            v, conf = value_for(labels[cat])
            grid[cat] = v
            if v is not None:
                confs.append(conf)
        else:
            grid[cat] = None
    scat = sum(v for v in grid.values() if v)
    overall = None
    if "OVERALL" in labels:
        overall, oconf = value_for(labels["OVERALL"], prefer=scat or None)
        if overall is not None:
            confs.append(oconf)
    grid["_min_conf"] = min(confs) if confs else None  # type: ignore[assignment]
    return grid, overall


def parse_items(pages: dict[str, list[dict]]) -> list[dict]:
    """Part-5 line items: Ref (A1, B3, …) at left, the free-text 'Details of item' in
    the middle band, Cost at right. The details field is a MIX of supplier names
    ('Galway Advertiser') and item descriptions ('Posters', 'Meta ads') — it is NOT a
    clean vendor field, so we call it ``detail`` and never assert it is a payee.
    Subtotal rows (bare 'A' + 'Total:') don't match the Ref regex so they are naturally
    skipped (filtered, not modelled). Only rows with a real cost are kept."""
    items: list[dict] = []
    for page, cells in pages.items():
        refs = [c for c in cells if re.fullmatch(r"[A-H]\d{1,2}", c["text"].strip())]
        costs = [c for c in cells if c["x0"] > 1550 and parse_money(c["text"]) is not None]
        names = [c for c in cells if 400 < c["x0"] < 1500 and len(c["text"].strip()) > 2]
        for rc in refs:
            def row(c: dict, _rc: dict = rc) -> bool:
                return abs(c["y0"] - _rc["y0"]) < 45
            cost_c = min((c for c in costs if row(c)),
                         key=lambda c: abs(c["y0"] - rc["y0"]), default=None)
            cost = parse_money(cost_c["text"]) if cost_c else None
            if cost is None:
                continue
            nm = min((c for c in names if row(c) and c["x0"] > rc["x0"]),
                     key=lambda c: abs(c["y0"] - rc["y0"]), default=None)
            ref = rc["text"].strip()
            conf = min(float(rc.get("score", 0.0)), float(cost_c.get("score", 0.0)))
            # Identity (media_id, candidate, …) and category_label are NOT stamped here
            # per-row — they are added once, vectorised, after the DataFrame is built
            # (a join + replace, not a Python loop / UDF).
            items.append({
                "ref": ref, "category": "5" + ref[0],
                "detail": nm["text"].strip() if nm else None,
                "cost_eur": cost, "source_page": page,
                "item_confidence": round(conf, 4),
            })
    return items


def party_from_p1(p1: list[dict]) -> str | None:
    for c in p1:
        if c["x0"] > 800 and 1200 < c["y0"] < 1360 and c["text"] not in ("€",):
            return c["text"].strip()
    return None


def parse_candidate(key: str, mr: dict) -> tuple[dict, list[dict]]:
    pages = load_pages(key)
    n_pages = len(pages)
    rec: dict = {
        "media_id": mr.get("media_id"),
        "candidate_slug": mr.get("candidate_slug"),
        "candidate_name": mr.get("candidate_name"),
        "constituency_slug": mr.get("constituency_slug"),
        "constituency_name": mr.get("constituency_name"),
        "party_declared": party_from_p1(pages.get("c001", [])),
        "spend_not_public_eur": None,
        "spend_public_eur": None,
        "total_spend_eur": None,
        "reconciles": None,
        "min_confidence": None,
        "n_pages": n_pages,
        "ocr_complete": n_pages >= MIN_COMPLETE_PAGES,
        "source_pdf_url": mr.get("pdf_url"),
    }
    for cat in CATS:
        rec[f"cat_{cat}_eur"] = None

    grid_conf: float | None = None
    for cells in pages.values():
        labels = {c["text"].strip() for c in cells}
        if "Overall Expense total:" not in labels:
            continue
        pt = page_type(cells)
        grid, overall = parse_grid(cells)
        if pt == "not_public":
            rec["spend_not_public_eur"] = overall
            grid_conf = grid.pop("_min_conf", None)  # type: ignore[assignment]
            for cat in CATS:
                rec[f"cat_{cat}_eur"] = grid.get(cat)
            scat = sum(v for v in grid.values() if isinstance(v, (int, float)))
            rec["reconciles"] = (overall is not None and abs((scat or 0) - overall) < 1.0)
        elif pt == "public":
            rec["spend_public_eur"] = overall

    np_ = rec["spend_not_public_eur"] or 0
    pub = rec["spend_public_eur"] or 0
    rec["total_spend_eur"] = round(np_ + pub, 2) if (np_ or pub) else None
    rec["min_confidence"] = round(grid_conf, 4) if grid_conf is not None else None

    items = parse_items(pages)
    for it in items:  # stamp ONLY the join key; identity columns are joined on vectorised
        it["media_id"] = rec["media_id"]
    return rec, items


def main() -> None:
    rows = list(csv.DictReader(MANIFEST.open(encoding="utf-8")))
    # Only expense statements have the 5A-5H spend grid; donation statements are a
    # separate corpus/track. Key the manifest by <slug>__<media_id> like the cache dirs.
    mrow = {f"{r['candidate_slug']}__{r['media_id']}": r
            for r in rows if r.get("doc_type") == "expense_statement"}

    keys = [d.name for d in sorted(CKPT.glob("*"))
            if d.is_dir() and d.name in mrow and any(d.glob("c*.json"))]
    print(f"=== PARSE SIPO candidate expenses (silver) — {len(keys)} cached statements ===")

    head_rows: list[dict] = []
    item_rows: list[dict] = []
    for k in keys:
        rec, items = parse_candidate(k, mrow[k])
        head_rows.append(rec)
        item_rows.extend(items)

    # --- candidate fact: magnitude gate (VECTORISED). A page where EVERY figure lost
    # its decimal reconciles with itself (Σcats == overall, both x100), so `reconciles`
    # alone can't catch it — a total above the statutory cap is the tell. Flagged, not
    # dropped (no-inference); gold excludes total_suspect rows from spend sums.
    head = pl.DataFrame(head_rows).with_columns(
        (pl.col("total_spend_eur") > STATUTORY_MAX_EUR).fill_null(False).alias("total_suspect"),
        canon_party_expr(),  # party_declared (raw, kept) -> party (canonical, may be NULL)
    )

    # --- DEDUPE same-constituency double-filings. A candidate can run in only ONE
    # constituency, so two statements for the same (candidate, constituency) is a
    # double-filing (e.g. Daly-Finn, Roscommon-Galway ×2) that would double-count in
    # totals/rankings. Keep the better copy: reconciling first, then most OCR'd pages,
    # then higher total. The SAME name in DIFFERENT constituencies is left as-is (could
    # be two real people or a misfile — not auto-resolved, no-inference). ---
    n_before = head.height
    head = head.sort(
        ["reconciles", "n_pages", "total_spend_eur"], descending=True, nulls_last=True
    ).unique(subset=["candidate_slug", "constituency_slug"], keep="first", maintain_order=True)
    n_dropped = n_before - head.height
    kept_ids = head["media_id"]
    item_rows = [it for it in item_rows if it["media_id"] in set(kept_ids.to_list())]

    # --- line-item fact: all enrichment is VECTORISED (no UDF, no per-row loop) ---
    # 1. category_label  -> pl.col.replace (vectorised map), not a Python dict lookup
    # 2. identity columns -> a single join on media_id, not per-row stamping
    # 3. cost_suspect     -> a boolean expression bounding each item by the statutory cap
    identity = head.select(
        "media_id", "candidate_slug", "candidate_name", "constituency_name", "party_declared", "party",
    )
    items = (
        pl.DataFrame(item_rows)
        .with_columns(
            pl.col("category").replace_strict(CAT_LABELS, default=None).alias("category_label"),
            (pl.col("cost_eur") > STATUTORY_MAX_EUR).alias("cost_suspect"),
        )
        .join(identity, on="media_id", how="left")
    )

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    save_parquet(head, OUT_HEAD)
    save_parquet(items, OUT_ITEMS)

    n_recon = head.filter(pl.col("reconciles")).height
    n_complete = head.filter(pl.col("ocr_complete")).height
    n_amt = head.filter(pl.col("total_spend_eur").is_not_null()).height
    head_clean = head.filter(pl.col("total_spend_eur").is_not_null() & ~pl.col("total_suspect"))
    print(f"  candidate fact -> {OUT_HEAD.relative_to(ROOT)}  ({head.height} rows, "
          f"{n_dropped} same-constituency double-filing(s) deduped)")
    print(f"    {n_complete} OCR-complete, {n_amt} with a total, {n_recon} reconcile to the cent")
    print(f"    {head_clean.height} plausible (<= statutory cap), Σ €{head_clean['total_spend_eur'].sum():,.2f}; "
          f"{head['total_suspect'].sum()} total_suspect (decimal-lost page)")
    clean = items.filter(~pl.col("cost_suspect"))
    print(f"  line-item fact -> {OUT_ITEMS.relative_to(ROOT)}  ({items.height} rows)")
    print(f"    {clean.height} within statutory cap (Σ €{clean['cost_eur'].sum():,.2f}), "
          f"{items['cost_suspect'].sum()} flagged cost_suspect (OCR decimal-loss), "
          f"{items.filter(pl.col('detail').is_not_null()).height} with a detail")


if __name__ == "__main__":
    main()
