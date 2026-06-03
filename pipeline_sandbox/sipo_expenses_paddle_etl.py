"""SIPO GE2024 candidate-expenses extractor — MULTI-PARTY PROTOTYPE (sandbox).

Validated approach (doc/SIPO_OCR_INVESTIGATION.md): the scans are crisp; Tesseract
mangled them. Re-OCR the raster with PaddleOCR (free, local, Apache-2.0) and feed
the cells into the geometry + closed-set-constituency-anchor layer.

GENERALISED off Fianna Fáil: each party's National-Agent expenses return is the
same scanned form, but the 'amount assigned to the party' values differ per party
(FF assigns a flat 40% of the limit; others differ). So we DROP the FF-specific
magic numbers and anchor on:
  - the CLOSED SET of 43 constituencies (party-independent), and
  - the STATUTORY candidate spending LIMIT per seat-count (€38,900/€48,600/€58,350
    for 3/4/5-seat; Electoral Act 1997 as amended) as the universal validity bound.
Columns are read by x-position: of the money cells to the RIGHT of the constituency
the LEFT one is 'amount assigned to the party', the RIGHT one is the national
agent's 'expenditure on the candidate'.

Per-party parquet -> pipeline_sandbox/_sipo_output/by_party/<key>.parquet, then a
combined  pipeline_sandbox/_sipo_output/sipo_expenses_fact.parquet  (party column
distinguishes them). Sandbox only — NOT gold; promotion is a later step.

Run (one or more party keys; default = all):
  ./.venv/Scripts/python.exe pipeline_sandbox/sipo_expenses_paddle_etl.py fg
  ./.venv/Scripts/python.exe pipeline_sandbox/sipo_expenses_paddle_etl.py sf lab green
"""

from __future__ import annotations

import difflib
import re
import sys
import tempfile
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CONSTIT_PARQUET = ROOT / "data/gold/parquet/ec_constituency_pop_2022.parquet"
OUT_DIR = ROOT / "pipeline_sandbox/_sipo_output"
BY_PARTY_DIR = OUT_DIR / "by_party"
OUT_PARQUET = OUT_DIR / "sipo_expenses_fact.parquet"
SCAN_DIR = ROOT / "data/bronze/scan_pdf"

# key -> (pdf path, display party). FF uses its already-rendered OCR pdf; the rest
# are the raw downloaded scans (PaddleOCR re-renders the image either way).
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
# Verified verbatim against the SIPO guidelines PDF; provenance + the per-party
# return URLs are in data/_meta/sipo_ge2024_expenses_sources.md
# (SIPO, Guidelines for the General Election to the 34th Dáil, 29 Nov 2024:
#  https://assets.sipo.ie/media/283883/b6e53676-bb38-4bfd-8773-565b4cd95135.pdf)
STATUTORY_LIMIT = {3: 38900, 4: 48600, 5: 58350}
DPI = 300
LOW_CONF = 0.85
# generic component words that must not match a constituency as a lone cell
DIRECTIONS = {"north", "south", "east", "west", "central", "city", "county", "bay", "mid"}


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def norm(s: str) -> str:
    return re.sub(r"[^a-z]", "", s.lower())


def parse_money(text: str) -> float | None:
    """Parse a PaddleOCR money cell. PaddleOCR keeps decimals, so this is light:
    strip currency/paren/space noise, honour an explicit 2-dp tail."""
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
    if len(cand) < 4 or cand in DIRECTIONS:  # precision guard: lone directional word
        return None, 0.0
    mm = difflib.get_close_matches(cand, norm_keys, n=1, cutoff=0.80)
    if not mm:
        return None, 0.0
    return norm_to_name[mm[0]], round(difflib.SequenceMatcher(None, cand, mm[0]).ratio(), 2)


def ocr_page(ocr, page, tmp_png: Path) -> list[dict]:
    """Render a page to a temp PNG (the stable input path — a numpy array
    segfaults paddle 3.3.1) and return PaddleOCR cells: text, score, x0,y0,x1,y1."""
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
        for t, s, b in zip(texts, scores, boxes):
            cells.append(
                {"text": t, "score": float(s), "x0": b[0], "y0": b[1], "x1": b[2], "y1": b[3]}
            )
    return cells


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


def parse_row(row: list[dict], norm_keys, norm_to_name, name_to_seats) -> dict | None:
    """Return a candidate-summary row dict, or None if no constituency anchor."""
    best_name, best_score, constit_x = None, 0.0, None
    for c in row:
        nm, sc = match_constituency(c["text"], norm_keys, norm_to_name)
        if nm and sc > best_score:
            best_name, best_score, constit_x = nm, sc, c["x0"]
    if not best_name:
        return None
    seats = int(name_to_seats.get(best_name, 0))
    limit = STATUTORY_LIMIT.get(seats)
    # money cells to the RIGHT of the constituency (the assigned + expenditure cols;
    # this also drops the leading row-index integer, which sits left of the name)
    monies = []
    for c in row:
        if constit_x is not None and c["x0"] <= constit_x:
            continue
        if "€" in c["text"] or re.search(r"\d{3}", c["text"]):
            v = parse_money(c["text"])
            if v and 0 < v <= (limit or STATUTORY_LIMIT[5]) * 1.05:
                monies.append({"val": round(v, 2), "x0": c["x0"], "score": c["score"]})
    monies.sort(key=lambda m: m["x0"])
    assigned = monies[0] if len(monies) >= 2 else None  # left money col = assigned
    expenditure = monies[-1] if monies else None  # right money col = expenditure
    # candidate name: cells left of the constituency, minus the row-index and money
    name_cells = [
        c["text"]
        for c in row
        if constit_x is not None
        and c["x0"] < constit_x
        and "€" not in c["text"]
        and not re.fullmatch(r"[\d.,)\s]+", c["text"])
    ]
    name_raw = re.sub(r"^\W*\d{1,3}[.\-)]?\s*", "", " ".join(name_cells)).strip()
    return {
        "name_raw": name_raw,
        "constituency": best_name,
        "score": best_score,
        "seats": seats,
        "limit": limit,
        "n_money": len(monies),
        "assigned": assigned["val"] if assigned else None,
        "spend": expenditure["val"] if expenditure else None,
        "spend_conf": round(expenditure["score"], 3) if expenditure else None,
        "row_conf": round(min(c["score"] for c in row), 3),
    }


def process_party(ocr, key, pdf_path, party, norm_keys, norm_to_name, name_to_seats) -> pl.DataFrame:
    doc = fitz.open(pdf_path)
    y_tol = int(DPI / 72 * 9)
    tmp_png = Path(tempfile.gettempdir()) / f"sipo_etl_{key}.png"
    page_rows: dict[int, list[dict]] = {}
    for pno, page in enumerate(doc, start=1):
        print(f"    [{party}] OCR page {pno}/{doc.page_count}...", flush=True)
        try:
            cells = ocr_page(ocr, page, tmp_png)
        except Exception as e:
            print(f"      !! page {pno} failed: {type(e).__name__}: {str(e)[:80]}", flush=True)
            continue
        rows = [r for row in cluster_rows(cells, y_tol)
                if (r := parse_row(row, norm_keys, norm_to_name, name_to_seats))]
        if rows:
            page_rows[pno] = rows

    # candidate-summary pages: >=3 rows that carry constituency + TWO money columns
    summary_pages = sorted(
        p for p, rs in page_rows.items() if sum(1 for r in rs if r["n_money"] >= 2) >= 3
    )
    print(f"    [{party}] summary pages: {summary_pages}", flush=True)

    facts = []
    for p in summary_pages:
        for r in page_rows[p]:
            spend, assigned, limit = r["spend"], r["assigned"], r["limit"]
            if spend is None:
                flag = "no_amount"
            elif limit and spend > limit:
                flag = "over_limit_verify"  # impossible -> bad read or wrong constituency
            elif assigned and spend > assigned * 1.02:
                flag = "spend_gt_assigned_verify"
            elif (r["spend_conf"] or 1) < LOW_CONF or r["row_conf"] < LOW_CONF:
                flag = "low_confidence_verify"
            else:
                flag = "ok"
            facts.append(
                {
                    "party": party,
                    "candidate_name_raw": r["name_raw"],
                    "constituency": r["constituency"],
                    "constituency_match_score": r["score"],
                    "amount_assigned_eur": float(assigned) if assigned else None,
                    "expenditure_eur": spend,
                    "expenditure_confidence": r["spend_conf"],
                    "row_min_confidence": r["row_conf"],
                    "statutory_limit_eur": float(limit) if limit else None,
                    "flag": flag,
                    "source_pdf": Path(pdf_path).name,
                    "source_page": p,
                }
            )
    return pl.DataFrame(facts) if facts else pl.DataFrame()


def main() -> None:
    keys = [k for k in sys.argv[1:] if k in PARTY_JOBS] or list(PARTY_JOBS)
    constit = pl.read_parquet(CONSTIT_PARQUET)
    norm_to_name = {norm(n): n for n in constit["constituency_name"].to_list()}
    norm_keys = list(norm_to_name)
    name_to_seats = dict(zip(constit["constituency_name"], constit["td_seats_2024"]))

    from paddleocr import PaddleOCR

    ocr = PaddleOCR(
        lang="en",
        use_doc_orientation_classify=False,
        use_doc_unwarping=False,
        use_textline_orientation=False,
        enable_mkldnn=False,  # REQUIRED on Windows (paddle 3.3.1 oneDNN/PIR bug)
        text_det_limit_side_len=1280,  # full A4 @300 DPI segfaults the detector
        text_det_limit_type="max",
    )

    BY_PARTY_DIR.mkdir(parents=True, exist_ok=True)
    for key in keys:
        pdf_path, party = PARTY_JOBS[key]
        hr(f"PROCESSING {key} — {party}")
        if not pdf_path.exists():
            print(f"  !! missing PDF: {pdf_path}")
            continue
        df = process_party(ocr, key, pdf_path, party, norm_keys, norm_to_name, name_to_seats)
        out = BY_PARTY_DIR / f"{key}.parquet"
        if df.height:
            df.sort(["source_page", "candidate_name_raw"]).write_parquet(
                out, compression="zstd", compression_level=3, statistics=True
            )
            wa = df.filter(pl.col("expenditure_eur").is_not_null())
            print(
                f"  {party}: {df.height} rows, {wa.height} with amount "
                f"({wa.height/max(1,df.height):.0%}), Σspend €{wa['expenditure_eur'].sum():,.2f} "
                f"-> {out.relative_to(ROOT)}"
            )
        else:
            print(f"  {party}: NO rows extracted (check layout)")

    # rebuild the combined fact from every per-party parquet present
    parts = [pl.read_parquet(p) for p in sorted(BY_PARTY_DIR.glob("*.parquet"))]
    if parts:
        combined = pl.concat(parts, how="vertical_relaxed").sort(["party", "source_page", "candidate_name_raw"])
        combined.write_parquet(OUT_PARQUET, compression="zstd", compression_level=3, statistics=True)
        hr("COMBINED FACT")
        print(combined.group_by("party").agg(
            pl.len().alias("rows"),
            pl.col("expenditure_eur").is_not_null().sum().alias("with_amount"),
            pl.col("expenditure_eur").sum().alias("total_spend_eur"),
        ).sort("party"))
        print(f"\nwrote {OUT_PARQUET.relative_to(ROOT)}  ({combined.height} rows, {len(parts)} parties)")


if __name__ == "__main__":
    main()
