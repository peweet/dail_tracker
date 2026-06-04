"""Roster-layout re-parser for SIPO expenses — fixes the name-mangling outlier.

Some parties' National-Agent returns (Aontú, and others — Labour uses a variant)
are NOT the FF/SF candidate-summary table the main ETL's anchor parser targets.
They are a NUMBERED ROSTER:

    6. Considine,        Dublin           24,300        2,935.35
       Aisling           South-Central
    7. Coughlan,         Dublin Rathdown  24,300        2,368.90
       Liam

i.e. the candidate name + constituency WRAP across two lines, in fixed column
bands. The anchor-by-constituency parser merges adjacent wrapped rows, producing
mangled names like "Tóibín,Mairead 39. Tóibín, Peadar". This re-parser reads the
SAME cached OCR cells (pipeline_sandbox/_sipo_output/by_party/_ckpt/<key>/c*.json
— NO re-OCR) and reconstructs rows by the leading "N." index + column x-bands.

It writes corrected by_party/<key>.parquet with the SAME schema as
sipo_expenses_paddle_etl.py, so the combined fact rebuild picks it up. Run ONLY on
roster-layout parties, and ONLY when the main ETL is NOT re-running that key (don't
race its parquet writes).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/sipo_expenses_roster_fix.py aontu
"""

from __future__ import annotations

import difflib
import json
import re
import sys
from pathlib import Path

import polars as pl

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

CONSTIT_PARQUET = ROOT / "data/gold/parquet/ec_constituency_pop_2022.parquet"
BY_PARTY = ROOT / "pipeline_sandbox/_sipo_output/by_party"
CKPT_ROOT = BY_PARTY / "_ckpt"
SCAN = ROOT / "data/bronze/scan_pdf"

PARTY_META = {  # key -> (display party, source pdf name)
    "aontu": ("Aontú", "aontu_sipo_ge_2024_expenses.pdf"),
    "lab": ("Labour", "lab_sipo_ge_2024_expenses.pdf"),
}
STATUTORY_LIMIT = {3: 38900, 4: 48600, 5: 58350}
LOW_CONF = 0.85
DIRECTIONS = {"north", "south", "east", "west", "central", "city", "county", "bay", "mid"}


def norm(s: str) -> str:
    return re.sub(r"[^a-z]", "", s.lower())


def match_constituency(text: str, norm_keys, norm_to_name):
    cand = norm(text)
    if len(cand) < 4 or cand in DIRECTIONS:
        return None, 0.0
    mm = difflib.get_close_matches(cand, norm_keys, n=1, cutoff=0.80)
    if not mm:
        return None, 0.0
    return norm_to_name[mm[0]], round(difflib.SequenceMatcher(None, cand, mm[0]).ratio(), 2)


def parse_money(text: str):
    t = re.sub(r"[^\d.]", "", text.replace(",", ""))
    m = re.match(r"^\d+(?:\.\d{1,2})?$", t)
    return float(m.group(0)) if m else None


def detect_bands(cells):
    """Column x-thresholds from the header row; fall back to observed Aontú bands."""
    name_x = const_x = assign_x = exp_x = None
    for c in cells:
        t = c["text"].lower()
        if "candidate" in t and "name" in t:
            name_x = c["x0"]
        elif t.strip() == "constituency":
            const_x = c["x0"]
        elif "assigned" in t:
            assign_x = c["x0"]
        elif "expenditure" in t:
            exp_x = c["x0"]
    # thresholds between columns
    c_lo = const_x - 8 if const_x else 185
    a_lo = assign_x - 8 if assign_x else 300
    e_lo = exp_x - 8 if exp_x else 410
    return c_lo, a_lo, e_lo


def roster_parse_page(cells, pno, norm_keys, norm_to_name, name_to_seats):
    c_lo, a_lo, e_lo = detect_bands(cells)
    idx = sorted((c for c in cells
                  if re.match(r"^\d{1,3}\.", c["text"].strip()) and c["x0"] < c_lo),
                 key=lambda c: c["y0"])
    rows = []
    for i, start in enumerate(idx):
        y0 = start["y0"] - 6
        y1 = idx[i + 1]["y0"] - 6 if i + 1 < len(idx) else 10 ** 9
        blk = [c for c in cells if y0 <= c["y0"] < y1]

        name = " ".join(c["text"] for c in sorted(
            (c for c in blk if c["x1"] <= c_lo), key=lambda c: (c["y0"], c["x0"])))
        name = re.sub(r"^\d{1,3}\.\s*", "", name).strip(" ,")
        if not name or "candidate" in name.lower():
            continue

        const_cells = sorted((c for c in blk if c_lo < c["x0"] < a_lo),
                             key=lambda c: (c["y0"], c["x0"]))
        const_txt = " ".join(c["text"] for c in const_cells)
        # a money value sometimes bleeds into the constituency cell ("Dublin Bay South 24,300")
        embedded = re.findall(r"\d[\d,]{3,}", const_txt)
        const_clean = re.sub(r"\s*\d[\d, ]{3,}\s*$", "", const_txt).strip()
        const_clean = re.sub(r"\s+\d+\s*$", "", const_clean).strip()  # trailing OCR page digit
        nm, sc = match_constituency(const_clean, norm_keys, norm_to_name)

        a_cells = [c for c in blk if a_lo <= c["x0"] < e_lo and re.search(r"\d", c["text"])]
        e_cells = [c for c in blk if c["x0"] >= e_lo and re.search(r"\d", c["text"])]
        assigned = next((parse_money(c["text"]) for c in a_cells if parse_money(c["text"])), None)
        if assigned is None and embedded:
            assigned = parse_money(embedded[-1])
        e_cell = next((c for c in e_cells if parse_money(c["text"])), None)
        expenditure = parse_money(e_cell["text"]) if e_cell else None

        seats = int(name_to_seats.get(nm, 0)) if nm else 0
        limit = STATUTORY_LIMIT.get(seats)
        confs = [c["score"] for c in blk] or [0.0]
        rows.append({
            "name_raw": name, "constituency": nm, "score": sc, "limit": limit,
            "assigned": assigned, "spend": expenditure,
            "spend_conf": round(e_cell["score"], 3) if e_cell else None,
            "row_conf": round(min(confs), 3), "page": pno,
        })
    return rows


def main():
    keys = [k for k in sys.argv[1:] if k in PARTY_META] or ["aontu"]
    constit = pl.read_parquet(CONSTIT_PARQUET)
    norm_to_name = {norm(n): n for n in constit["constituency_name"].to_list()}
    norm_keys = list(norm_to_name)
    name_to_seats = dict(zip(constit["constituency_name"], constit["td_seats_2024"]))

    for key in keys:
        party, pdf_name = PARTY_META[key]
        ckpt = CKPT_ROOT / key
        if not ckpt.exists():
            print(f"  !! no cache for {key}")
            continue
        facts = []
        for f in sorted(ckpt.glob("c*.json")):
            d = json.loads(f.read_text(encoding="utf-8"))
            if not isinstance(d, dict) or d.get("failed") or not d.get("cells"):
                continue
            pno = int(f.stem[1:])
            for r in roster_parse_page(d["cells"], pno, norm_keys, norm_to_name, name_to_seats):
                if r["constituency"] is None and r["spend"] is None:
                    continue
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
                    "constituency": r["constituency"],
                    "constituency_match_score": r["score"],
                    "amount_assigned_eur": float(assigned) if assigned else None,
                    "expenditure_eur": spend,
                    "expenditure_confidence": r["spend_conf"],
                    "row_min_confidence": r["row_conf"],
                    "statutory_limit_eur": float(limit) if limit else None,
                    "flag": flag, "source_pdf": pdf_name, "source_page": r["page"],
                })
        df = pl.DataFrame(facts)
        if df.height:
            out = BY_PARTY / f"{key}.parquet"
            df.sort(["source_page", "candidate_name_raw"]).write_parquet(
                out, compression="zstd", compression_level=3, statistics=True)
            wa = df.filter(pl.col("expenditure_eur").is_not_null())
            print(f"  {party}: {df.height} candidates, {wa.height} with spend, "
                  f"Σ€{wa['expenditure_eur'].sum():,.2f}, "
                  f"{df['constituency'].n_unique()} constituencies -> {out.name}")
            print("    flags:", dict(zip(*df["flag"].value_counts().sort("flag").to_dict(as_series=False).values())))
        else:
            print(f"  {party}: no rows parsed")


if __name__ == "__main__":
    main()
