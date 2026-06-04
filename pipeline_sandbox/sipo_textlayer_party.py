"""Extract a BORN-DIGITAL national-agent expenses return via its text layer — NO OCR.

For the SIPO returns that ship with a real text layer (per the corpus census:
Sinn Féin, the main Aontú, and National Party), the candidate-summary table can be
read directly with fitz get_text('words') — far cheaper and more accurate than
re-OCR. This reconstructs rows by y-clustering + column x-bands and the
43-constituency closed set, exactly like the OCR parser, and writes
by_party/<key>.parquet with the SAME schema as sipo_expenses_paddle_etl.py.

Layout handled: the FF/SF "table" form (single-line "N. Firstname Surname |
Constituency | Amount Assigned | Expenditure"). NOT the Aontú/Labour roster (use
sipo_expenses_roster_fix.py for those).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/sipo_textlayer_party.py national_party
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "pipeline_sandbox"))
from sipo_expenses_roster_fix import (  # reuse the validated helpers
    BY_PARTY, CONSTIT_PARQUET, LOW_CONF, STATUTORY_LIMIT,
    match_constituency, norm, parse_money,
)

try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# key -> (display party, cached pdf path)
JOBS = {
    "national_party": ("National Party", ROOT / "c_tmp_placeholder"),
}
# cached download location
MISSING = Path("c:/tmp/sipo_missing")
PDF_BY_KEY = {"national_party": MISSING / "national_party.pdf"}


def words_to_cells(page) -> list[dict]:
    """fitz words -> the same cell dicts the OCR parser uses (score=1.0, text layer)."""
    cells = []
    for x0, y0, x1, y1, txt, *_ in page.get_text("words"):
        if txt.strip():
            cells.append({"text": txt, "score": 1.0,
                          "x0": int(x0), "y0": int(y0), "x1": int(x1), "y1": int(y1)})
    return cells


def yc(c):
    return (c["y0"] + c["y1"]) / 2


def const_left_edge(cells):
    for c in cells:
        if c["text"].lower().startswith("constituenc"):
            return c["x0"] - 24
    return 185


def parse_table(cells, pno, nk, n2n, n2s):
    """Robust money-by-position parse (mirrors the OCR ETL): name = left of the
    constituency column; money cells right of it, leftmost = assigned, rightmost =
    expenditure. Avoids fragile per-header band detection."""
    c_lo = const_left_edge(cells)
    idx = sorted((c for c in cells
                  if re.match(r"^\d{1,3}\.$", c["text"].strip()) and c["x0"] < c_lo),
                 key=lambda c: c["y0"])
    rows = []
    for i, start in enumerate(idx):
        y0, y1 = start["y0"] - 6, (idx[i + 1]["y0"] - 6 if i + 1 < len(idx) else 10 ** 9)
        blk = [c for c in cells if y0 <= c["y0"] < y1]
        name = re.sub(r"^\d{1,3}\.\s*", "", " ".join(
            c["text"] for c in sorted((c for c in blk if c["x1"] <= c_lo),
                                      key=lambda c: (c["y0"], c["x0"])))).strip()
        if not name:
            continue  # empty roster slot
        money = sorted(((c["x0"], parse_money(c["text"]), c) for c in blk
                        if c["x0"] >= c_lo and parse_money(c["text"]) is not None),
                       key=lambda t: t[0])
        first_money_x = money[0][0] if money else 10 ** 9
        const_txt = " ".join(c["text"] for c in sorted(
            (c for c in blk if c_lo < c["x0"] < first_money_x), key=lambda c: (c["y0"], c["x0"])))
        nm, sc = match_constituency(const_txt, nk, n2n)
        assigned = money[0][1] if len(money) >= 2 else None
        e = money[-1] if money else None
        spend = e[1] if e else None
        seats = int(n2s.get(nm, 0)) if nm else 0
        rows.append({"name": name, "constituency": nm, "score": sc,
                     "limit": STATUTORY_LIMIT.get(seats), "assigned": assigned,
                     "spend": spend, "spend_conf": e[2]["score"] if e else None, "page": pno})
    return rows


def main():
    keys = [k for k in sys.argv[1:] if k in PDF_BY_KEY] or list(PDF_BY_KEY)
    constit = pl.read_parquet(CONSTIT_PARQUET)
    n2n = {norm(n): n for n in constit["constituency_name"].to_list()}
    nk = list(n2n)
    n2s = dict(zip(constit["constituency_name"], constit["td_seats_2024"]))

    for key in keys:
        party = JOBS.get(key, (key, None))[0]
        pdf = PDF_BY_KEY[key]
        if not pdf.exists():
            print(f"  !! missing {pdf}")
            continue
        doc = fitz.open(pdf)
        facts = []
        for pno in range(1, doc.page_count + 1):
            page_text = doc[pno - 1].get_text("text").lower()
            # ONLY the candidate-summary table page (avoids Part-4 A1/A2/4A ref noise)
            if "amount assigned by each candidate" not in page_text:
                continue
            cells = words_to_cells(doc[pno - 1])
            for r in parse_table(cells, pno, nk, n2n, n2s):
                if r["constituency"] is None and r["spend"] is None:
                    continue
                spend, assigned, limit = r["spend"], r["assigned"], r["limit"]
                flag = ("no_amount" if spend is None
                        else "over_limit_verify" if limit and spend > limit
                        else "spend_gt_assigned_verify" if assigned and spend > assigned * 1.02
                        else "low_confidence_verify" if r["score"] and r["score"] < LOW_CONF
                        else "ok")
                facts.append({
                    "party": party, "candidate_name_raw": r["name"],
                    "constituency": r["constituency"], "constituency_match_score": r["score"],
                    "amount_assigned_eur": float(assigned) if assigned else None,
                    "expenditure_eur": spend,
                    "expenditure_confidence": r["spend_conf"],
                    "row_min_confidence": 1.0,
                    "statutory_limit_eur": float(limit) if limit else None,
                    "flag": flag, "source_pdf": pdf.name, "source_page": r["page"],
                })
        df = pl.DataFrame(facts)
        if df.height:
            out = BY_PARTY / f"{key}.parquet"
            df.sort(["source_page", "candidate_name_raw"]).write_parquet(
                out, compression="zstd", compression_level=3, statistics=True)
            wa = df.filter(pl.col("expenditure_eur").is_not_null())
            print(f"  {party}: {df.height} candidates, Σspend €{wa['expenditure_eur'].sum():,.2f} "
                  f"-> {out.name}")
            for r in df.iter_rows(named=True):
                print(f"     {r['candidate_name_raw']:<24} {str(r['constituency']):<20} "
                      f"assigned={r['amount_assigned_eur']} spend={r['expenditure_eur']} [{r['flag']}]")
        else:
            print(f"  {party}: no candidate rows found")


if __name__ == "__main__":
    main()
