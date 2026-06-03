"""SIPO GE2024 candidate-expenses extractor — MULTI-PARTY, TWO-STAGE (sandbox).

Validated approach (doc/SIPO_OCR_INVESTIGATION.md): the scans are crisp; Tesseract
mangled them. Re-OCR the raster with PaddleOCR (free, local, Apache-2.0) and feed
the cells into a geometry + 43-constituency closed-set anchor.

TWO STAGES (the key design point — parsing layout varies per party, so we must be
able to iterate parsing WITHOUT re-OCR'ing):
  1. OCR stage  : render @300 DPI -> PaddleOCR -> cache RAW CELLS per page to
                  _ckpt/<key>/cNNN.json. Expensive (~25 min/party), run via the
                  watchdog (pipeline_sandbox/_sipo_watchdog.py) which bounds
                  PaddleOCR's intermittent segfaults/hangs.
  2. PARSE stage: read cached cells -> rows -> by_party/<key>.parquet. Cheap,
                  re-runnable with  --parse-only  (no paddle, no OCR).

Anchors (party-independent): 43 constituencies + the STATUTORY spending limit
(€38,900/€48,600/€58,350 for 3/4/5-seat) as the validity bound — see
data/_meta/sipo_ge2024_expenses_sources.md. Money columns are read by x-band
(left = amount assigned to party, right = national-agent expenditure), and each
money cell is paired to its constituency anchor by nearest-y (tolerates the
vertical column offset seen on some parties' forms, e.g. Labour).

Run OCR (all or some parties; via watchdog in practice):
  ./.venv/Scripts/python.exe pipeline_sandbox/sipo_expenses_paddle_etl.py fg sf ...
Re-parse cached cells only (fast, iterate freely):
  ./.venv/Scripts/python.exe pipeline_sandbox/sipo_expenses_paddle_etl.py --parse-only
"""

from __future__ import annotations

import difflib
import json
import re
import statistics
import sys
import tempfile
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CONSTIT_PARQUET = ROOT / "data/gold/parquet/ec_constituency_pop_2022.parquet"
OUT_DIR = ROOT / "pipeline_sandbox/_sipo_output"
BY_PARTY_DIR = OUT_DIR / "by_party"
CKPT_ROOT = BY_PARTY_DIR / "_ckpt"
OUT_PARQUET = OUT_DIR / "sipo_expenses_fact.parquet"
SCAN_DIR = ROOT / "data/bronze/scan_pdf"

PARTY_JOBS: dict[str, tuple[Path, str]] = {
    "ff": (SCAN_DIR / "output/ff_sipo_ge_2024_expenses-ocr.pdf", "Fianna Fáil"),
    "fg": (SCAN_DIR / "fg_sipo_ge_2024_expenses.pdf", "Fine Gael"),
    "sf": (SCAN_DIR / "sf_sipo_ge_2024_expenses.pdf", "Sinn Féin"),
    "lab": (SCAN_DIR / "lab_sipo_ge_2024_expenses.pdf", "Labour"),
    "green": (SCAN_DIR / "green_sipo_ge_2024_expenses.pdf", "Green Party"),
    "socdem": (SCAN_DIR / "socdem_sipo_ge_2024_expenses.pdf", "Social Democrats"),
    "pbp": (SCAN_DIR / "pbp_sipo_ge_2024_expenses.pdf", "People Before Profit/Solidarity"),
    "aontu": (SCAN_DIR / "aontu_sipo_ge_2024_expenses.pdf", "Aontú"),
}

# Statutory candidate spending limit per constituency seat-count (GE2024).
# Verified verbatim vs the SIPO guidelines; provenance + per-party return URLs in
# data/_meta/sipo_ge2024_expenses_sources.md
STATUTORY_LIMIT = {3: 38900, 4: 48600, 5: 58350}
DPI = 300
LOW_CONF = 0.85
DIRECTIONS = {"north", "south", "east", "west", "central", "city", "county", "bay", "mid"}


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def norm(s: str) -> str:
    return re.sub(r"[^a-z]", "", s.lower())


def parse_money(text: str) -> float | None:
    t = text.replace("€", "").replace("(", "").replace(")", "").replace(" ", "").strip("_,.\\")
    if not any(ch.isdigit() for ch in t):
        return None
    m = re.match(r"^(\d[\d,. ]*?)([.,]\d{2})$", t)
    if m:
        whole = re.sub(r"\D", "", m.group(1))
        return float(f"{whole}.{m.group(2)[1:]}") if whole else None
    digits = re.sub(r"\D", "", t)
    return float(digits) if digits else None


def match_constituency(text: str, norm_keys, norm_to_name) -> tuple[str | None, float]:
    cand = norm(text)
    if len(cand) < 4 or cand in DIRECTIONS:
        return None, 0.0
    mm = difflib.get_close_matches(cand, norm_keys, n=1, cutoff=0.80)
    if not mm:
        return None, 0.0
    return norm_to_name[mm[0]], round(difflib.SequenceMatcher(None, cand, mm[0]).ratio(), 2)


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
            cells.append({"text": t, "score": round(float(s), 4),
                          "x0": int(b[0]), "y0": int(b[1]), "x1": int(b[2]), "y1": int(b[3])})
    return cells


def ocr_party(ocr, key: str, pdf_path: Path) -> None:
    """Cache raw OCR cells per page (cNNN.json). Crash/​hang-proof via the .attempt
    retry ladder (2x300 -> 1x200 -> skip); the watchdog kills a hung run & resumes."""
    import fitz

    doc = fitz.open(pdf_path)
    ckpt = CKPT_ROOT / key
    ckpt.mkdir(parents=True, exist_ok=True)
    tmp_png = Path(tempfile.gettempdir()) / f"sipo_etl_{key}.png"
    for pno, page in enumerate(doc, start=1):
        done = ckpt / f"c{pno:03}.json"
        attempt = ckpt / f"a{pno:03}.attempt"
        if done.exists():
            continue
        tries = json.loads(attempt.read_text(encoding="utf-8"))["tries"] if attempt.exists() else []
        if not tries:
            dpi = 300
        elif tries == [300]:
            dpi = 300
        elif tries == [300, 300]:
            dpi = 200
        else:
            print(f"    [{key}] page {pno}: crashed at {tries} -> SKIP", flush=True)
            done.write_text(json.dumps({"failed": True, "cells": []}), encoding="utf-8")
            attempt.unlink(missing_ok=True)
            continue
        print(f"    [{key}] OCR page {pno}/{doc.page_count} @ {dpi}dpi...", flush=True)
        attempt.write_text(json.dumps({"tries": tries + [dpi]}), encoding="utf-8")
        cells = ocr_page(ocr, page, tmp_png, dpi)
        done.write_text(json.dumps({"failed": False, "cells": cells}), encoding="utf-8")
        attempt.unlink(missing_ok=True)


# -------------------------------------------------------------- parse stage ----
def is_money(cell: dict) -> float | None:
    if "€" in cell["text"] or re.search(r"\d{3}", cell["text"]):
        return parse_money(cell["text"])
    return None


def xc(c: dict) -> float:
    return (c["x0"] + c["x1"]) / 2


def yc(c: dict) -> float:
    return (c["y0"] + c["y1"]) / 2


def column_split(money: list[dict]) -> float | None:
    """Find the x that splits the two money columns (assigned | expenditure) as the
    largest gap between sorted x-centres; None if there's only one column."""
    xs = sorted(xc(c) for c in money)
    if len(xs) < 2:
        return None
    gaps = [(xs[i + 1] - xs[i], (xs[i + 1] + xs[i]) / 2) for i in range(len(xs) - 1)]
    g, mid = max(gaps)
    span = xs[-1] - xs[0]
    return mid if span and g > span * 0.15 else None


def parse_page(cells: list[dict], pno: int, norm_keys, norm_to_name, name_to_seats) -> list[dict]:
    anchors = []
    for c in cells:
        nm, sc = match_constituency(c["text"], norm_keys, norm_to_name)
        if nm:
            anchors.append((c, nm, sc))
    money = [c for c in cells if is_money(c)]
    if len(anchors) < 3 or len(money) < 3:  # not a candidate-summary page
        return []
    split_x = column_split(money)
    ays = sorted(yc(a[0]) for a in anchors)
    diffs = [ays[i + 1] - ays[i] for i in range(len(ays) - 1) if ays[i + 1] - ays[i] > 5]
    row_h = statistics.median(diffs) if diffs else 40.0
    tol = row_h * 0.75

    rows = []
    for cell, nm, sc in anchors:
        ay, ax = yc(cell), cell["x0"]
        seats = int(name_to_seats.get(nm, 0))
        limit = STATUTORY_LIMIT.get(seats)
        # money cells right of the constituency, near this anchor by y
        near = sorted(
            ((c, xc(c), is_money(c)) for c in money if xc(c) > ax and abs(yc(c) - ay) <= tol),
            key=lambda t: t[1],
        )
        near = [t for t in near if t[2] and 0 < t[2] <= (limit or STATUTORY_LIMIT[5]) * 1.05]
        assigned = expenditure = None
        a_conf = e_conf = None
        if len(near) >= 2:
            assigned, expenditure = near[0][2], near[-1][2]
            a_conf, e_conf = near[0][0]["score"], near[-1][0]["score"]
        elif len(near) == 1:
            c, x, v = near[0]
            if split_x is not None and x < split_x:
                assigned, a_conf = v, c["score"]  # left band -> assigned (expenditure blank)
            else:
                expenditure, e_conf = v, c["score"]
        # candidate name: non-money, non-numeric cells left of constituency, near y
        name_cells = sorted(
            (c for c in cells
             if c["x0"] < ax and abs(yc(c) - ay) <= tol
             and "€" not in c["text"]
             and not re.fullmatch(r"[\d.,)\s]+", c["text"])
             and not match_constituency(c["text"], norm_keys, norm_to_name)[0]),
            key=lambda c: c["x0"],
        )
        name_raw = re.sub(r"^\W*\d{1,3}[.\-)]?\s*", "",
                          " ".join(c["text"] for c in name_cells)).strip()
        rows.append({
            "name_raw": name_raw, "constituency": nm, "score": sc, "limit": limit,
            "assigned": assigned, "spend": expenditure,
            "spend_conf": round(e_conf, 3) if e_conf else None,
            "row_conf": round(min([sc] + [c["score"] for c in name_cells] +
                                  ([a_conf] if a_conf else []) + ([e_conf] if e_conf else [])), 3),
            "page": pno,
        })
    return rows


def parse_party(key: str, party: str, pdf_name: str, norm_keys, norm_to_name, name_to_seats) -> pl.DataFrame:
    ckpt = CKPT_ROOT / key
    if not ckpt.exists():
        return pl.DataFrame()
    page_rows: dict[int, list[dict]] = {}
    for f in sorted(ckpt.glob("c*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        if d.get("failed") or not d.get("cells"):
            continue
        pno = int(f.stem[1:])
        rows = parse_page(d["cells"], pno, norm_keys, norm_to_name, name_to_seats)
        if rows:
            page_rows[pno] = rows
    # candidate-summary pages: >=3 rows that carry TWO money columns
    summary_pages = sorted(p for p, rs in page_rows.items()
                           if sum(1 for r in rs if r["assigned"] and r["spend"]) >= 3)
    facts = []
    for p in summary_pages:
        for r in page_rows[p]:
            spend, assigned, limit = r["spend"], r["assigned"], r["limit"]
            if spend is None:
                flag = "no_amount"
            elif limit and spend > limit:
                flag = "over_limit_verify"
            elif assigned and spend > assigned * 1.02:
                flag = "spend_gt_assigned_verify"
            elif (r["spend_conf"] or 1) < LOW_CONF or r["row_conf"] < LOW_CONF:
                flag = "low_confidence_verify"
            else:
                flag = "ok"
            facts.append({
                "party": party, "candidate_name_raw": r["name_raw"],
                "constituency": r["constituency"], "constituency_match_score": r["score"],
                "amount_assigned_eur": float(assigned) if assigned else None,
                "expenditure_eur": spend, "expenditure_confidence": r["spend_conf"],
                "row_min_confidence": r["row_conf"],
                "statutory_limit_eur": float(limit) if limit else None,
                "flag": flag, "source_pdf": pdf_name, "source_page": p,
            })
    return pl.DataFrame(facts) if facts else pl.DataFrame()


def rebuild_combined() -> None:
    parts = [pl.read_parquet(p) for p in sorted(BY_PARTY_DIR.glob("*.parquet"))]
    if not parts:
        return
    combined = pl.concat(parts, how="vertical_relaxed").sort(["party", "source_page", "candidate_name_raw"])
    combined.write_parquet(OUT_PARQUET, compression="zstd", compression_level=3, statistics=True)
    hr("COMBINED FACT")
    print(combined.group_by("party").agg(
        pl.len().alias("rows"),
        pl.col("expenditure_eur").is_not_null().sum().alias("with_amt"),
        pl.col("expenditure_eur").sum().alias("total_spend"),
    ).sort("total_spend", descending=True))
    print(f"\nwrote {OUT_PARQUET.relative_to(ROOT)}  ({combined.height} rows, {len(parts)} parties)")


def write_party(key, party, df) -> None:
    if df.height:
        df.sort(["source_page", "candidate_name_raw"]).write_parquet(
            BY_PARTY_DIR / f"{key}.parquet", compression="zstd", compression_level=3, statistics=True)
        wa = df.filter(pl.col("expenditure_eur").is_not_null())
        print(f"  {party}: {df.height} rows, {wa.height} with amount "
              f"({wa.height/max(1,df.height):.0%}), Σspend €{wa['expenditure_eur'].sum():,.2f}", flush=True)
    else:
        print(f"  {party}: NO rows", flush=True)


def main() -> None:
    args = sys.argv[1:]
    parse_only = "--parse-only" in args
    keys = [a for a in args if a in PARTY_JOBS] or list(PARTY_JOBS)

    constit = pl.read_parquet(CONSTIT_PARQUET)
    norm_to_name = {norm(n): n for n in constit["constituency_name"].to_list()}
    norm_keys = list(norm_to_name)
    name_to_seats = dict(zip(constit["constituency_name"], constit["td_seats_2024"]))
    BY_PARTY_DIR.mkdir(parents=True, exist_ok=True)

    ocr = None
    if not parse_only:
        from paddleocr import PaddleOCR

        ocr = PaddleOCR(lang="en", use_doc_orientation_classify=False, use_doc_unwarping=False,
                        use_textline_orientation=False, enable_mkldnn=False,
                        text_det_limit_side_len=1280, text_det_limit_type="max")

    for key in keys:
        pdf_path, party = PARTY_JOBS[key]
        hr(f"{'PARSE' if parse_only else 'OCR+PARSE'} {key} — {party}")
        if not parse_only:
            if not pdf_path.exists():
                print(f"  !! missing PDF: {pdf_path}")
                continue
            ocr_party(ocr, key, pdf_path)
        df = parse_party(key, party, pdf_path.name, norm_keys, norm_to_name, name_to_seats)
        write_party(key, party, df)

    rebuild_combined()


if __name__ == "__main__":
    main()
