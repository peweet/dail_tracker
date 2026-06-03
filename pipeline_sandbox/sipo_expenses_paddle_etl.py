"""SIPO GE2024 candidate-expenses extractor — PROTOTYPE (sandbox).

Validated approach (see doc/SIPO_OCR_INVESTIGATION.md): the scan is crisp;
Tesseract mangled it. This re-OCRs the raster with PaddleOCR (free, local,
Apache-2.0) and feeds the cells into the geometry + closed-set-anchor + cap
layer built across probe_sipo_ocr_columns/repair/analysis.

Pipeline per page:
  fitz render @300 DPI -> PaddleOCR (cell text + box + confidence)
    -> cluster cells into rows by y-centre
    -> per row: fuzzy-match constituency vs the CLOSED SET of 43 (with the
       'directional-word' precision guard), read the statutory-cap (assigned)
       and expenditure money cells, keep PaddleOCR's per-cell confidence
    -> isolate the candidate-summary table (pages whose rows carry constituency
       + a statutory cap) and emit one fact row per candidate.

Writes a sandbox parquet (NOT gold — promotion is a later step):
  pipeline_sandbox/_sipo_output/sipo_expenses_fact.parquet

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/sipo_expenses_paddle_etl.py
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

OCR_PDF = ROOT / "data/bronze/scan_pdf/output/ff_sipo_ge_2024_expenses-ocr.pdf"
CONSTIT_PARQUET = ROOT / "data/gold/parquet/ec_constituency_pop_2022.parquet"
OUT_DIR = ROOT / "pipeline_sandbox/_sipo_output"
OUT_PARQUET = OUT_DIR / "sipo_expenses_fact.parquet"

PARTY = "Fianna Fáil"  # this PDF is the FF GE2024 expenses return
SOURCE_PDF = "ff_sipo_ge_2024_expenses.pdf"
SEAT_TO_CAP = {3: 15560, 4: 19440, 5: 23340}
CAPS = set(SEAT_TO_CAP.values())
CAP_MAX = 23340
DPI = 300
LOW_CONF = 0.85
# generic component words that must not match a constituency as a lone cell
DIRECTIONS = {"north", "south", "east", "west", "central", "city", "county", "bay", "mid"}


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def norm(s: str) -> str:
    return re.sub(r"[^a-z]", "", s.lower())


def is_cap(v: float | None) -> bool:
    return v is not None and any(abs(v - c) / c <= 0.03 for c in CAPS)


def snap_cap(v: float | None) -> int | None:
    if v is None:
        return None
    for c in CAPS:
        if abs(v - c) / c <= 0.03:
            return c
    return None


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
    """Render a page to a temp PNG (the validated, stable input path — passing a
    numpy array segfaults paddle 3.3.1) and return PaddleOCR cells."""
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
            x0, y0, x1, y1 = b[0], b[1], b[2], b[3]
            cells.append({"text": t, "score": float(s), "x0": x0, "y0": y0, "x1": x1, "y1": y1})
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


def main() -> None:
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
        # cap the detection input side: a full A4 @300 DPI (2480x3507) SEGFAULTS
        # the detector; 'max' 1280 downscales detection only (recognition still
        # crops from the full-res image, so number legibility is preserved).
        text_det_limit_side_len=1280,
        text_det_limit_type="max",
    )

    doc = fitz.open(OCR_PDF)
    y_tol = int(DPI / 72 * 9)  # ~9 pt row tolerance in rendered pixels
    tmp_png = Path(tempfile.gettempdir()) / "sipo_etl_page.png"

    # --- pass 1: OCR every page, build candidate rows ---
    page_rows: dict[int, list[dict]] = {}
    for pno, page in enumerate(doc, start=1):
        print(f"  OCR page {pno}/{doc.page_count}...", flush=True)
        try:
            cells = ocr_page(ocr, page, tmp_png)
        except Exception as e:  # don't let one bad page kill the whole run
            print(f"    !! page {pno} failed: {type(e).__name__}: {str(e)[:80]}", flush=True)
            continue
        rows = []
        for row in cluster_rows(cells, y_tol):
            # constituency: best whole-cell match across the row's cells
            best_name, best_score, best_x = None, 0.0, None
            for c in row:
                nm, sc = match_constituency(c["text"], norm_keys, norm_to_name)
                if nm and sc > best_score:
                    best_name, best_score, best_x = nm, sc, c["x0"]
            if not best_name:
                continue
            # money cells in the row
            monies = []
            for c in row:
                if "€" in c["text"] or re.search(r"\d{3}", c["text"]):
                    v = parse_money(c["text"])
                    if v:
                        monies.append({"val": v, "x0": c["x0"], "score": c["score"]})
            cap = next((snap_cap(m["val"]) for m in monies if snap_cap(m["val"])), None)
            spends = [m for m in monies if snap_cap(m["val"]) is None and 0 < m["val"] < 60000]
            spend = max(spends, key=lambda m: m["x0"]) if spends else None  # rightmost = expenditure
            # name: text cells left of the constituency, minus row-number/money cells
            name_cells = [
                c["text"]
                for c in row
                if best_x is not None
                and c["x0"] < best_x
                and "€" not in c["text"]
                and not re.fullmatch(r"[\d.,)\s]+", c["text"])
            ]
            name_raw = re.sub(r"^\W*\d{1,3}[.\-)]?\s*", "", " ".join(name_cells)).strip()
            rows.append(
                {
                    "name_raw": name_raw,
                    "constituency": best_name,
                    "score": best_score,
                    "cap": cap,
                    "spend": spend["val"] if spend else None,
                    "spend_conf": round(spend["score"], 3) if spend else None,
                    "row_conf": round(min(c["score"] for c in row), 3),
                    "page": pno,
                }
            )
        if rows:
            page_rows[pno] = rows

    # --- isolate the candidate-summary table: pages with >=3 constituency+cap rows ---
    summary_pages = sorted(
        p for p, rs in page_rows.items() if sum(1 for r in rs if r["cap"]) >= 3
    )
    hr("PAGE SCAN")
    for p, rs in sorted(page_rows.items()):
        tag = "SUMMARY" if p in summary_pages else "detail/other"
        print(f"  p{p:>2}: {len(rs):>2} constituency rows, {sum(1 for r in rs if r['cap'])} with cap  [{tag}]")
    print(f"\nsummary table pages: {summary_pages}")

    # --- build fact rows from summary pages ---
    facts = []
    for p in summary_pages:
        for r in page_rows[p]:
            implied_cap = SEAT_TO_CAP.get(int(name_to_seats.get(r["constituency"], 0)))
            spend = r["spend"]
            # flag logic — PaddleOCR conf + cap bound (engine-independent validation)
            if spend is None:
                flag = "no_amount"
            elif implied_cap and spend > implied_cap:
                flag = "over_cap_verify"
            elif (r["spend_conf"] or 1) < LOW_CONF or r["row_conf"] < LOW_CONF:
                flag = "low_confidence_verify"
            else:
                flag = "ok"
            facts.append(
                {
                    "party": PARTY,
                    "candidate_name_raw": r["name_raw"],
                    "constituency": r["constituency"],
                    "constituency_match_score": r["score"],
                    "amount_assigned_eur": float(r["cap"]) if r["cap"] else None,
                    "expenditure_eur": spend,
                    "expenditure_confidence": r["spend_conf"],
                    "row_min_confidence": r["row_conf"],
                    "flag": flag,
                    "source_pdf": SOURCE_PDF,
                    "source_page": p,
                }
            )

    df = pl.DataFrame(facts).sort(["source_page", "candidate_name_raw"])
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_PARQUET, compression="zstd", compression_level=3, statistics=True)

    hr("RESULT")
    print(f"candidate rows extracted : {df.height}")
    print(f"  flag=ok                : {df.filter(pl.col('flag') == 'ok').height}")
    print(f"  low_confidence_verify  : {df.filter(pl.col('flag') == 'low_confidence_verify').height}")
    print(f"  over_cap_verify        : {df.filter(pl.col('flag') == 'over_cap_verify').height}")
    print(f"  no_amount              : {df.filter(pl.col('flag') == 'no_amount').height}")
    with_spend = df.filter(pl.col("expenditure_eur").is_not_null())
    print(f"  with an amount         : {with_spend.height} ({with_spend.height/max(1,df.height):.0%})")
    print(f"total expenditure (sum)  : €{with_spend['expenditure_eur'].sum():,.2f}")
    print(f"\nwrote {OUT_PARQUET.relative_to(ROOT)}")

    hr("SAMPLE (first 25)")
    with pl.Config(tbl_rows=25, fmt_str_lengths=22, tbl_width_chars=140):
        print(
            df.select(
                "candidate_name_raw", "constituency", "amount_assigned_eur",
                "expenditure_eur", "expenditure_confidence", "flag", "source_page",
            ).head(25)
        )


if __name__ == "__main__":
    main()
