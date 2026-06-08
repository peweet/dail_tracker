"""SIPO GE2024 candidate-expenses extractor — MULTI-PARTY, TWO-STAGE.

Validated approach (doc/SIPO_OCR_INVESTIGATION.md): the scans are crisp; Tesseract
mangled them. Re-OCR the raster with PaddleOCR (free, local, Apache-2.0) and feed
the cells into a geometry + 43-constituency closed-set anchor.

TWO STAGES (the key design point — parsing layout varies per party, so we must be
able to iterate parsing WITHOUT re-OCR'ing):
  1. OCR stage  : render @300 DPI -> PaddleOCR -> cache RAW CELLS per page to
                  _ckpt/<key>/cNNN.json. Expensive (~25 min/party), run via the
                  watchdog (extractors/_sipo_watchdog.py) which bounds
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
  ./.venv/Scripts/python.exe extractors/sipo_expenses_paddle_etl.py fg sf ...
Re-parse cached cells only (fast, iterate freely):
  ./.venv/Scripts/python.exe extractors/sipo_expenses_paddle_etl.py --parse-only
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
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CONSTIT_PARQUET = ROOT / "data/gold/parquet/ec_constituency_pop_2022.parquet"
OUT_DIR = ROOT / "data/silver/sipo"
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
    # Remaining GE2024 national-agent returns (all scanned -> PaddleOCR). Party labels
    # use the SIPO collection spelling. Run via _sipo_watchdog.py, one at a time.
    "centre_party": (SCAN_DIR / "centre_party_sipo_ge_2024_expenses.pdf", "The Centre Party of Ireland"),
    "i4c": (SCAN_DIR / "i4c_sipo_ge_2024_expenses.pdf", "Independents 4 Change"),
    "indep_ireland": (SCAN_DIR / "indep_ireland_sipo_ge_2024_expenses.pdf", "Independent Ireland"),
    "ireland_first": (SCAN_DIR / "ireland_first_sipo_ge_2024_expenses.pdf", "Ireland First"),
    "irish_freedom": (SCAN_DIR / "irish_freedom_sipo_ge_2024_expenses.pdf", "Irish Freedom Party"),
    "irish_people": (SCAN_DIR / "irish_people_sipo_ge_2024_expenses.pdf", "The Irish People"),
    "redress100": (SCAN_DIR / "redress100_sipo_ge_2024_expenses.pdf", "100% Redress Party"),
    "right_to_change": (SCAN_DIR / "right_to_change_sipo_ge_2024_expenses.pdf", "Right to Change"),
}

# Statutory candidate spending limit per constituency seat-count (GE2024).
# Verified verbatim vs the SIPO guidelines; provenance + per-party return URLs in
# data/_meta/sipo_ge2024_expenses_sources.md
STATUTORY_LIMIT = {3: 38900, 4: 48600, 5: 58350}
DPI = 300
LOW_CONF = 0.85
DIRECTIONS = {"north", "south", "east", "west", "central", "city", "county", "bay", "mid"}
# BORN-DIGITAL returns (clean embedded text layer, NOT scans) — read cells straight
# from the text layer (instant, exact); no PaddleOCR. (Census in
# data/_meta/sipo_ge2024_expenses_sources.md.) NB: ff has a text layer too but it's
# the GARBLED Tesseract one, so ff is NOT here — it must be re-OCR'd.
BORN_DIGITAL = {"sf", "aontu"}


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
def text_layer_cells(page) -> list[dict]:
    """Cells straight from a born-digital PDF's embedded text layer (one cell per
    line). Coords are PDF points, not 300-DPI pixels — fine, the parser is
    scale-invariant (x-band split + row-relative tolerances)."""
    cells = []
    for block in page.get_text("dict")["blocks"]:
        for line in block.get("lines", []):
            spans = line["spans"]
            text = "".join(s["text"] for s in spans).strip()
            if not text:
                continue
            xs0 = [s["bbox"][0] for s in spans]
            ys0 = [s["bbox"][1] for s in spans]
            xs1 = [s["bbox"][2] for s in spans]
            ys1 = [s["bbox"][3] for s in spans]
            cells.append(
                {
                    "text": text,
                    "score": 1.0,
                    "x0": int(min(xs0)),
                    "y0": int(min(ys0)),
                    "x1": int(max(xs1)),
                    "y1": int(max(ys1)),
                }
            )
    return cells


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


def ocr_party(ocr, key: str, pdf_path: Path) -> None:
    """Cache raw OCR cells per page (cNNN.json). Crash/​hang-proof via the .attempt
    retry ladder (2x300 -> 1x200 -> skip); the watchdog kills a hung run & resumes."""
    import fitz

    doc = fitz.open(pdf_path)
    ckpt = CKPT_ROOT / key
    ckpt.mkdir(parents=True, exist_ok=True)
    tmp_png = Path(tempfile.gettempdir()) / f"sipo_etl_{key}.png"
    # born-digital: pull cells from the text layer (instant, no PaddleOCR, no segfaults)
    if key in BORN_DIGITAL:
        for pno, page in enumerate(doc, start=1):
            done = ckpt / f"c{pno:03}.json"
            if done.exists():
                continue
            done.write_text(json.dumps({"failed": False, "cells": text_layer_cells(page)}), encoding="utf-8")
        print(f"    [{key}] text-layer cells cached for {doc.page_count} pages", flush=True)
        return
    for pno, page in enumerate(doc, start=1):
        done = ckpt / f"c{pno:03}.json"
        attempt = ckpt / f"a{pno:03}.attempt"
        if done.exists():
            continue
        tries = json.loads(attempt.read_text(encoding="utf-8"))["tries"] if attempt.exists() else []
        if not tries or tries == [300]:
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
    t = cell["text"].strip()
    if re.fullmatch(r"[€\s]*(0|0\.00|nil|Nil|NIL)", t):  # explicit zero expenditure
        return 0.0
    if "€" in t or re.search(r"\d{3}", t):
        return parse_money(t)
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


def find_total_spend(cells: list[dict], split_x: float | None) -> float | None:
    """The candidate-summary TOTAL row: a cell whose label is exactly 'TOTAL'/'TOTAL:'
    (NOT the 'Total Expenditure ...' column header) -> the rightmost money on its y,
    i.e. the printed expenditure total used as a reconciliation checksum."""
    for c in cells:
        if re.sub(r"[^a-z]", "", c["text"].lower()) == "total":
            same = sorted(
                ((m, v) for m in cells if (v := is_money(m)) is not None and abs(yc(m) - yc(c)) <= 25),
                key=lambda t: xc(t[0]),
            )
            if same:
                return same[-1][1]
    return None


def parse_page(cells: list[dict], pno: int, norm_keys, norm_to_name, name_to_seats) -> list[dict]:
    """NEAREST-ANCHOR pairing (proven on aligned/offset/blank layouts): each
    constituency anchor claims its nearest left-band
    money (assigned) and nearest UNCLAIMED right-band money (expenditure). The wide
    expenditure window + claiming tolerates the vertical column offset (PBP/Labour)
    and true blanks; per-pair spend<=assigned is the consistency check."""
    # base constituency matches (per cell)
    base = [
        (c, nm, sc)
        for c in cells
        if (nm := match_constituency(c["text"], norm_keys, norm_to_name)[0])
        for sc in [match_constituency(c["text"], norm_keys, norm_to_name)[1]]
    ]
    money = [(c, v) for c in cells if (v := is_money(c)) is not None]
    if len(base) < 3 or len(money) < 3:  # not a candidate-summary page
        return []
    ays = sorted(yc(a[0]) for a in base)
    diffs = [b - a for a, b in zip(ays, ays[1:]) if b - a > 5]
    row_h = statistics.median(diffs) if diffs else 40.0
    split_x = column_split([c for c, _ in money])

    # CONTINUATION JOIN: some forms split a constituency over two lines
    # ("DUBLIN SOUTH" / "CENTRAL"), so the base cell mis-matches a short name
    # (-> Dublin Bay South). If the cell directly below in the same column makes the
    # joined text match BETTER, adopt the fuller match and consume the continuation
    # (so it can't leak into the candidate name).
    consumed: set[int] = set()
    anchors = []
    for c, nm, sc in base:
        cx, cy = c["x0"], yc(c)
        below = [
            d
            for d in cells
            if id(d) != id(c)
            and abs(d["x0"] - cx) <= 50
            and 0 < (yc(d) - cy) <= row_h * 1.3
            and "€" not in d["text"]
            and not re.fullmatch(r"[\d.,)\s]+", d["text"])
        ]
        below.sort(key=lambda d: yc(d) - cy)
        if below:
            jn, js = match_constituency(c["text"] + " " + below[0]["text"], norm_keys, norm_to_name)
            if jn and js > sc + 0.02:
                nm, sc = jn, js
                consumed.add(id(below[0]))
        anchors.append((c, nm, sc))
    a_tol, e_tol = row_h * 0.7, row_h * 1.5  # assigned aligns tight; expenditure may offset
    name_tol = row_h * 0.45  # names sit ON the constituency row; a tight window avoids
    #                          grabbing the adjacent row's name fragment (e.g. Aontú)
    left = [(c, v) for c, v in money if split_x is None or xc(c) < split_x]
    right = [(c, v) for c, v in money if split_x is not None and xc(c) >= split_x]

    claimed: set[int] = set()
    rows = []
    for cell, nm, sc in sorted(anchors, key=lambda a: yc(a[0])):
        ay, ax = yc(cell), cell["x0"]
        seats = int(name_to_seats.get(nm, 0))
        limit = STATUTORY_LIMIT.get(seats)
        # assigned: nearest left-band money to the right of the constituency
        la = sorted(
            [(c, v) for c, v in left if xc(c) > ax and abs(yc(c) - ay) <= a_tol], key=lambda t: abs(yc(t[0]) - ay)
        )
        assigned = la[0][1] if la else None
        a_conf = la[0][0]["score"] if la else None
        # expenditure: nearest UNCLAIMED right-band money (wide window for offset)
        cand = sorted(
            [(c, v) for c, v in right if id(c) not in claimed and abs(yc(c) - ay) <= e_tol],
            key=lambda t: abs(yc(t[0]) - ay),
        )
        spend = e_conf = None
        if cand:
            c, v = cand[0]
            claimed.add(id(c))
            spend, e_conf = v, c["score"]

        # candidate name: non-money, non-numeric cells left of constituency, on the
        # SAME row (tight name_tol — avoids merging the adjacent row's name fragment)
        def name_in(tol):
            return sorted(
                (
                    c
                    for c in cells
                    if c["x0"] < ax
                    and abs(yc(c) - ay) <= tol
                    and id(c) not in consumed  # not a constituency continuation line
                    and "€" not in c["text"]
                    and not re.fullmatch(r"[\d.,)\s]+", c["text"])
                    and norm(c["text"]) not in DIRECTIONS  # drop lone WEST/CENTRAL etc.
                    and not match_constituency(c["text"], norm_keys, norm_to_name)[0]
                ),
                key=lambda c: c["x0"],
            )

        name_cells = name_in(name_tol)
        raw = " ".join(c["text"] for c in name_cells)
        raw = re.sub(r"^\s*\d{1,3}[.\-)\s]*", "", raw)  # leading row-number "12." / "12 " / "12Name"
        raw = re.sub(r"\s+\d{1,3}[.\-)]\s*", " ", raw)  # embedded "Coppinger 35. Ruth"
        name_raw = re.sub(r"\s+", " ", raw).strip(" ,.")
        rows.append(
            {
                "name_raw": name_raw,
                "constituency": nm,
                "score": sc,
                "limit": limit,
                "assigned": assigned,
                "spend": spend,
                "spend_conf": round(e_conf, 3) if e_conf is not None else None,
                "row_conf": round(
                    min([sc] + ([a_conf] if a_conf else []) + ([e_conf] if e_conf is not None else []) or [sc]), 3
                ),
                "page": pno,
            }
        )
    return rows


def parse_party(key: str, party: str, pdf_name: str, norm_keys, norm_to_name, name_to_seats) -> pl.DataFrame:
    ckpt = CKPT_ROOT / key
    if not ckpt.exists():
        return pl.DataFrame()
    page_rows: dict[int, list[dict]] = {}
    page_total: dict[int, float] = {}
    for f in sorted(ckpt.glob("c*.json")):
        d = json.loads(f.read_text(encoding="utf-8"))
        cells = d if isinstance(d, list) else (None if d.get("failed") else d.get("cells"))
        if not cells:
            continue
        pno = int(f.stem[1:])
        rows = parse_page(cells, pno, norm_keys, norm_to_name, name_to_seats)
        if rows:
            page_rows[pno] = rows
            t = find_total_spend(cells, None)
            if t is not None:
                page_total[pno] = t
    # candidate-summary pages: >=3 rows that carry BOTH money columns (0.0 is valid)
    summary_pages = sorted(
        p
        for p, rs in page_rows.items()
        if sum(1 for r in rs if r["assigned"] is not None and r["spend"] is not None) >= 3
    )
    facts = []
    for p in summary_pages:
        for r in page_rows[p]:
            spend, assigned, limit = r["spend"], r["assigned"], r["limit"]
            # TODO(parser gap, flagged 2026-06-03): the over-limit guard below only
            # checks `spend`, NOT `assigned`. On the FF scan, decimal-loss OCR inflated
            # two ASSIGNED cells x100 past the statutory limit (Jim O'Callaghan
            # €1,944,000 = €19,440; Michael Cahill €1,458,750 = €14,587.50) and they
            # sailed through as "ok" — caught only by test_assigned_within_statutory_limit
            # in test_sipo_data_quality.py. Mirror the limit check on `assigned` (flag
            # e.g. "assigned_over_limit_verify" or null it) so impossible assigned
            # amounts can't ship. The 4 expenditure x100 outliers ARE already caught by
            # over_limit_verify below; this is only the assigned-column blind spot.
            if assigned is not None and limit and assigned > limit * 1.02:
                flag = "assigned_over_limit_verify"  # garbage OCR in the assigned col
            elif spend is None:
                flag = "no_amount"
            elif limit and spend > limit:
                flag = "over_limit_verify"
            elif assigned is not None and spend > assigned * 1.02:
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
                    "amount_assigned_eur": float(assigned) if assigned is not None else None,
                    "expenditure_eur": spend,
                    "expenditure_confidence": r["spend_conf"],
                    "row_min_confidence": r["row_conf"],
                    "statutory_limit_eur": float(limit) if limit else None,
                    "flag": flag,
                    "source_pdf": pdf_name,
                    "source_page": p,
                }
            )
    df = pl.DataFrame(facts) if facts else pl.DataFrame()
    if df.height:
        # DROP nameless rows. On a couple of born-digital returns (Aontú p5, FG p2)
        # the candidate name is split across two text-layer lines; that two-line
        # layout occasionally spawns a phantom constituency anchor with no name cell
        # beside it (the deeper name-pairing limitation is documented in
        # doc/SIPO_OCR_INVESTIGATION.md). A candidate-expenses fact with no candidate
        # isn't usable — drop it transparently rather than ship a blank-name row. We
        # never invent the missing name (faithful-extraction).
        nameless = df.filter(pl.col("candidate_name_raw").str.strip_chars() == "")
        if nameless.height:
            print(
                f"    [{party}] dropped {nameless.height} nameless row(s) "
                f"(two-line-layout phantom): "
                f"{nameless.select(['constituency', 'expenditure_eur', 'source_page']).to_dicts()}",
                flush=True,
            )
            df = df.filter(pl.col("candidate_name_raw").str.strip_chars() != "")
        # SIPO forms print the candidate summary TWICE ("expenses NOT met from public
        # funds" + "expenses MET from public funds"); when both copies carry amounts the
        # summary-page detector grabs both, doubling every candidate (e.g. Labour p3==p29
        # -> 52 rows, total inflated 2x). A candidate+constituency is unique within a
        # party, so dedup keeping the highest-confidence (then earliest-page) row collapses
        # the repeat safely.
        before = df.height
        df = (
            df.sort(["expenditure_confidence", "source_page"], descending=[True, False], nulls_last=True)
            .unique(subset=["candidate_name_raw", "constituency"], keep="first")
            .sort(["source_page", "candidate_name_raw"])
        )
        if df.height < before:
            print(
                f"    [{party}] deduped {before - df.height} repeated summary rows ({before}->{df.height})", flush=True
            )
        # QA: reconcile Σ expenditure against the form's printed TOTAL row (the grand
        # total = the largest TOTAL found; equal duplicates from the twice-printed
        # summary collapse to one). Where no TOTAL is printed, the per-pair
        # spend<=assigned check (flag spend_gt_assigned_verify) is the fallback.
        totals = [page_total[p] for p in summary_pages if p in page_total]
        sum_spend = df["expenditure_eur"].sum() or 0.0
        if totals:
            target = max(totals)
            ok = abs(sum_spend - target) <= max(1.0, target * 0.01)
            print(
                f"    [{party}] RECONCILE: Σspend €{sum_spend:,.2f} vs TOTAL €{target:,.2f} "
                f"-> {'✅ OK' if ok else '❌ MISMATCH'}",
                flush=True,
            )
        else:
            viol = df.filter(pl.col("flag") == "spend_gt_assigned_verify").height
            print(
                f"    [{party}] no printed TOTAL; spend>assigned violations={viol} (Σspend €{sum_spend:,.2f})",
                flush=True,
            )
    return df


def rebuild_combined() -> None:
    # by_party/ is SHARED with the Part-4 extractor (sipo_expense_items_paddle_etl.py
    # writes *_items / *_categories parquets here too). Combine ONLY the Part-3
    # candidate-summary parquets — identified by the candidate-expense schema — so
    # Part-4 line-items/category-totals can't pollute the candidate fact.
    parts = []
    for p in sorted(BY_PARTY_DIR.glob("*.parquet")):
        sch = pl.read_parquet_schema(p)
        if "candidate_name_raw" in sch and "expenditure_eur" in sch:
            parts.append(pl.read_parquet(p))
    if not parts:
        return
    # diagonal_relaxed: align by column NAME, fill missing with null (tolerates a
    # stale parquet with e.g. no statutory_limit_eur).
    combined = pl.concat(parts, how="diagonal_relaxed").sort(["party", "source_page", "candidate_name_raw"])
    save_parquet(combined, OUT_PARQUET)
    hr("COMBINED FACT")
    print(
        combined.group_by("party")
        .agg(
            pl.len().alias("rows"),
            pl.col("expenditure_eur").is_not_null().sum().alias("with_amt"),
            pl.col("expenditure_eur").sum().alias("total_spend"),
        )
        .sort("total_spend", descending=True)
    )
    print(f"\nwrote {OUT_PARQUET.relative_to(ROOT)}  ({combined.height} rows, {len(parts)} parties)")


def write_party(key, party, df) -> None:
    if df.height:
        save_parquet(df.sort(["source_page", "candidate_name_raw"]), BY_PARTY_DIR / f"{key}.parquet")
        wa = df.filter(pl.col("expenditure_eur").is_not_null())
        print(
            f"  {party}: {df.height} rows, {wa.height} with amount "
            f"({wa.height / max(1, df.height):.0%}), Σspend €{wa['expenditure_eur'].sum():,.2f}",
            flush=True,
        )
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
    needs_paddle = not parse_only and any(k not in BORN_DIGITAL for k in keys)
    if needs_paddle:
        from paddleocr import PaddleOCR

        ocr = PaddleOCR(
            lang="en",
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
            enable_mkldnn=False,
            text_det_limit_side_len=1280,
            text_det_limit_type="max",
        )

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
