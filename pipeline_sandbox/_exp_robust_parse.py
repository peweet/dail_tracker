"""THROWAWAY EXPERIMENT (does NOT touch sipo_expenses_paddle_etl.py).

Question: can a single parser handle BOTH the aligned forms (Green) and the
vertically-offset forms (PBP), and be PROVEN right by reconciling the per-candidate
expenditure sum against the form's printed TOTAL row?

Strategy tested: ORDER-pairing within x-bands.
  - split money cells into 2 x-columns (assigned-left, expenditure-right)
  - drop the TOTAL row (keep its expenditure value as the checksum)
  - per column, sort remaining money by (page, y); pair assigned[i] <-> spend[i]
  - reconcile sum(spend) vs the TOTAL-row expenditure value
Order-pairing is offset-immune (the i-th right-band value by y is the i-th
candidate regardless of vertical stagger) — IF there are no blanks. The TOTAL
reconciliation tells us whether that holds.

OCR is cached to c:/tmp/exp_cells/<key>/cNNN.json so re-running the PARSE costs
nothing (the whole point — iterate parsing without re-OCR).

Run: ./.venv/Scripts/python.exe pipeline_sandbox/_exp_robust_parse.py
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import sipo_expenses_paddle_etl as etl  # reuse norm/parse_money/match_constituency/ocr_page

sys.stdout.reconfigure(encoding="utf-8")
ROOT = Path(__file__).resolve().parents[1]
CELLS = Path("c:/tmp/exp_cells")
# (key, pdf, page range to OCR) — front-matter + summary table
JOBS = [
    ("green", "green_sipo_ge_2024_expenses.pdf", range(2, 7)),
    ("pbp", "pbp_sipo_ge_2024_expenses.pdf", range(2, 12)),
    ("lab", "lab_sipo_ge_2024_expenses.pdf", range(2, 10)),
]


def xc(c):
    return (c["x0"] + c["x1"]) / 2


def yc(c):
    return (c["y0"] + c["y1"]) / 2


def ocr_cache(key, pdf, pages):
    """OCR the given pages once; cache cells. Returns {pno: cells}."""
    import fitz

    d = CELLS / key
    d.mkdir(parents=True, exist_ok=True)
    need = [p for p in pages if not (d / f"c{p:03}.json").exists()]
    if need:
        ocr = etl.PaddleOCR_singleton() if hasattr(etl, "PaddleOCR_singleton") else None
        from paddleocr import PaddleOCR

        ocr = PaddleOCR(lang="en", use_doc_orientation_classify=False, use_doc_unwarping=False,
                        use_textline_orientation=False, enable_mkldnn=False,
                        text_det_limit_side_len=1280, text_det_limit_type="max")
        doc = fitz.open(ROOT / "data/bronze/scan_pdf" / pdf)
        tmp = Path("c:/tmp") / f"exp_{key}.png"
        for p in need:
            print(f"  [{key}] OCR page {p}...", flush=True)
            cells = etl.ocr_page(ocr, doc[p - 1], tmp, 300)
            (d / f"c{p:03}.json").write_text(json.dumps(cells), encoding="utf-8")
    out = {}
    for p in pages:
        f = d / f"c{p:03}.json"
        if f.exists():
            out[p] = json.loads(f.read_text(encoding="utf-8"))
    return out


def experiment(key, pages_cells, norm_keys, norm_to_name):
    # gather, per page, money cells + constituency anchors + TOTAL row
    money = []          # (page, cell, value)
    anchors = []        # (page, cell, name)
    total_spend = None
    for pno, cells in pages_cells.items():
        page_money = [(c, v) for c in cells if (v := etl.parse_money(c["text"])) is not None
                      and ("€" in c["text"] or re.search(r"\d{3}", c["text"]) or "0.00" in c["text"] or c["text"].strip() == "0")]
        has_anchor = False
        for c in cells:
            nm, sc = etl.match_constituency(c["text"], norm_keys, norm_to_name)
            if nm:
                anchors.append((pno, c, nm)); has_anchor = True
        # TOTAL row — ONLY on summary pages (other sections have their own 'Total:'
        # rows that must not overwrite it). Label exactly 'TOTAL'/'TOTAL:' (NOT the
        # 'Total Expenditure ...' column header) -> rightmost money on its y.
        if has_anchor:
            for c in cells:
                if re.sub(r"[^a-z]", "", c["text"].lower()) == "total":
                    same = sorted([m for m in page_money if abs(yc(m[0]) - yc(c)) <= 25], key=lambda m: xc(m[0]))
                    if same:
                        total_spend = same[-1][1]
                        print(f"    [{key}] TOTAL row on p{pno}: rightmost €{total_spend:,.2f}  (row money: {[round(m[1],2) for m in same]})")
            money += [(pno, c, v) for c, v in page_money]

    if not money:
        return f"{key}: no money cells on summary pages"
    # x-band split across all summary money
    xs = sorted(xc(m[1]) for m in money)
    gaps = [(xs[i + 1] - xs[i], (xs[i + 1] + xs[i]) / 2) for i in range(len(xs) - 1)]
    span = xs[-1] - xs[0]
    g, split_x = max(gaps) if gaps else (0, None)
    if not (span and g > span * 0.15):
        split_x = None
    # exclude the TOTAL row's own money from per-candidate lists
    left = sorted([m for m in money if split_x and xc(m[1]) < split_x and "TOTAL" not in m[1]["text"].upper()],
                  key=lambda m: (m[0], yc(m[1])))
    right = sorted([m for m in money if split_x and xc(m[1]) >= split_x],
                   key=lambda m: (m[0], yc(m[1])))
    # drop a right-band TOTAL value if it equals total_spend (it's the checksum, not a candidate)
    right_vals = [m[2] for m in right]
    if total_spend in right_vals:
        right_vals.remove(total_spend)
    left_vals = [m[2] for m in left]

    sum_spend = round(sum(right_vals), 2)
    lines = [
        f"\n===== {key} =====",
        f"constituency anchors : {len(anchors)}",
        f"x-band split         : {'yes' if split_x else 'NO (single column!)'}",
        f"assigned values (left) : {len(left_vals)}  e.g. {left_vals[:6]}",
        f"spend values (right)   : {len(right_vals)}  e.g. {right_vals[:6]}",
        f"Σ spend (computed)   : €{sum_spend:,.2f}",
        f"TOTAL row spend      : €{total_spend:,.2f}" if total_spend else "TOTAL row spend      : NOT FOUND",
    ]
    if total_spend:
        diff = sum_spend - total_spend
        ok = abs(diff) <= max(1.0, total_spend * 0.005)
        lines.append(f"RECONCILES           : {'✅ YES' if ok else '❌ NO'}  (diff €{diff:,.2f})")

    # ---- validator A: ORDER-pairing (offset-immune, breaks on blanks) ----
    lv = [m for m in left if m[2] != total_spend]
    n = min(len(lv), len(right))
    ov = sum(1 for i in range(n) if right[i][2] > lv[i][2] * 1.02)
    lines.append(f"order-pair: {len(lv)} assigned vs {len(right)} spend; "
                 f"spend>assigned violations = {ov}/{n} {'✅' if ov == 0 and len(lv)==len(right) else '⚠️'}")

    # ---- validator B: NEAREST-ANCHOR pairing (handles blanks) ----
    # row height from anchor y spacing (per page)
    rights = [m for m in right if m[2] != total_spend]
    claimed = set()
    pairs = []
    for pno, acell, nm in anchors:
        ay = yc(acell)
        # assigned: nearest left-band money on same page
        cand_l = sorted([m for m in left if m[0] == pno and m[2] != total_spend],
                        key=lambda m: abs(yc(m[1]) - ay))
        assigned = cand_l[0][2] if cand_l and abs(yc(cand_l[0][1]) - ay) < 80 else None
        # expenditure: nearest UNCLAIMED right-band money on same page within ~1.2 row
        cand_r = sorted([(j, m) for j, m in enumerate(rights) if m[0] == pno and j not in claimed],
                        key=lambda t: abs(yc(t[1][1]) - ay))
        spend = None
        if cand_r and abs(yc(cand_r[0][1][1]) - ay) < 120:
            j, m = cand_r[0]; claimed.add(j); spend = m[2]
        if assigned or spend:
            pairs.append((nm, assigned, spend))
    na_viol = sum(1 for _, a, s in pairs if a and s and s > a * 1.02)
    na_sum = round(sum(s for _, a, s in pairs if s), 2)
    lines.append(f"nearest-anchor: {len(pairs)} pairs, Σspend €{na_sum:,.2f}, "
                 f"spend>assigned violations = {na_viol} {'✅' if na_viol == 0 else '⚠️'}")
    if total_spend:
        lines.append(f"  nearest-anchor reconcile: {'✅' if abs(na_sum-total_spend)<=max(1,total_spend*0.01) else '❌'} (diff €{na_sum-total_spend:,.2f})")
    return "\n".join(lines)


def main():
    constit = __import__("polars").read_parquet(etl.CONSTIT_PARQUET)
    norm_to_name = {etl.norm(n): n for n in constit["constituency_name"].to_list()}
    norm_keys = list(norm_to_name)
    for key, pdf, pages in JOBS:
        pc = ocr_cache(key, pdf, pages)
        print(experiment(key, pc, norm_keys, norm_to_name))


if __name__ == "__main__":
    main()
