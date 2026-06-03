"""PROBE v4 (throwaway): honest recovery rate on the CANDIDATE-SUMMARY table
only, using x-coordinate column bands instead of regex-over-joined-text.

Probe v3 showed two artifacts:
  - denominator pollution: 226 €-rows span the whole doc, but only the candidate
    summary table (the 'amount assigned by each candidate' table) has the
    name|constituency|cap|expenditure shape. Detail/voucher pages dilute it.
  - money bug: regex over joined text grabbed the leading ROW INDEX ('2. Tom..'
    -> 2.0) as the amount. Columns must be split by x-position, not regex.

This probe auto-detects the candidate-summary pages (rows whose tokens fuzzy-hit
a constituency AND carry >=1 money token), bands tokens by x into
[index | name | constituency | assigned-cap | expenditure], and reports the
recovery rate over THAT population — the honest answer to Open Question #2.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_ocr_columns.py
Reads only; writes nothing.
"""

from __future__ import annotations

import difflib
import re
import sys
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
CAPS = {15560, 19440, 23340}
MONEY_TOK = re.compile(r"€|\d{3,}|\d[\d,. ]*\.\d{2}")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def cluster_rows(words, y_tol=4.0):
    rows = []
    for w in sorted(words, key=lambda w: (round(w[1] / y_tol), w[0])):
        if rows and abs(rows[-1][0][1] - w[1]) <= y_tol:
            rows[-1].append(w)
        else:
            rows.append([w])
    for r in rows:
        r.sort(key=lambda w: w[0])
    return rows


def norm(s):
    return re.sub(r"[^a-z]", "", s.lower())


def parse_money(tok):
    t = tok.replace("€", "").replace("(", "").replace(")", "").replace(" ", "")
    t = t.strip("_,.\\")
    m = re.match(r"^(\d[\d,. ]*?)([.,]\d{2})$", t)
    if m:
        whole = re.sub(r"\D", "", m.group(1))
        return float(f"{whole}.{m.group(2)[1:]}") if whole else None
    digits = re.sub(r"\D", "", t)
    return float(digits) if digits else None


def snap_cap(v):
    if v is None:
        return None
    for cap in CAPS:
        if abs(v - cap) / cap <= 0.03:
            return cap
    return None


def best_constit(toks, norm_keys, norm_to_name):
    best_name, best_score = None, 0.0
    for win in (1, 2, 3):
        for i in range(len(toks) - win + 1):
            cand = norm(" ".join(toks[i : i + win]))
            if len(cand) < 4:
                continue
            m = difflib.get_close_matches(cand, norm_keys, n=1, cutoff=0.72)
            if m:
                sc = difflib.SequenceMatcher(None, cand, m[0]).ratio()
                if sc > best_score:
                    best_score, best_name = sc, norm_to_name[m[0]]
    return best_name, round(best_score, 2)


def main():
    constit = pl.read_parquet(CONSTIT_PARQUET)
    names = constit["constituency_name"].to_list()
    norm_to_name = {norm(n): n for n in names}
    norm_keys = list(norm_to_name)

    doc = fitz.open(OCR_PDF)

    candidate_rows = []
    for pno, page in enumerate(doc, start=1):
        for row in cluster_rows(page.get_text("words")):
            toks = [w[4] for w in row]
            money_words = [w for w in row if MONEY_TOK.search(w[4]) and any(c.isdigit() for c in w[4])]
            if not money_words:
                continue
            cname, cscore = best_constit(toks, norm_keys, norm_to_name)
            # a candidate-summary row = has a confident constituency + >=1 money col
            if cname and cscore >= 0.80:
                # rightmost money word = expenditure column; others may be the cap
                money_words.sort(key=lambda w: w[0])
                vals = [(w[0], parse_money(w[4])) for w in money_words]
                vals = [(x, v) for x, v in vals if v]
                cap = next((snap_cap(v) for _x, v in vals if snap_cap(v)), None)
                spends = [v for _x, v in vals if snap_cap(v) is None and 0 < v < 60000]
                # prefer the rightmost non-cap value as the expenditure
                spend = vals[-1][1] if vals and snap_cap(vals[-1][1]) is None else (max(spends) if spends else None)
                candidate_rows.append(
                    {
                        "page": pno,
                        "constituency": cname,
                        "score": cscore,
                        "cap": cap,
                        "spend": spend,
                        "raw": " ".join(toks),
                    }
                )

    n = len(candidate_rows)
    has_spend = sum(1 for x in candidate_rows if x["spend"])
    has_cap = sum(1 for x in candidate_rows if x["cap"])
    full = sum(1 for x in candidate_rows if x["spend"] and x["constituency"])
    pages = sorted({x["page"] for x in candidate_rows})

    hr("CANDIDATE-SUMMARY POPULATION (constituency-anchored rows)")
    print(f"rows identified as candidate-summary : {n}")
    print(f"pages they fall on                   : {pages}")
    print(f"  with confident constituency        : {n}  (100% by construction)")
    print(f"  with statutory cap recovered       : {has_cap}  ({has_cap / max(1, n):.0%})")
    print(f"  with an expenditure amount         : {has_spend}  ({has_spend / max(1, n):.0%})")
    print(f"  FULL (constituency + expenditure)  : {full}  ({full / max(1, n):.0%})")

    hr("ALL recovered candidate rows")
    for x in candidate_rows:
        print(
            f"  p{x['page']:>2} | {x['constituency']:<20} ({x['score']:.2f}) "
            f"cap={x['cap']} spend={x['spend']}"
        )
        print(f"        raw: {x['raw'][:88]}")

    hr("VERDICT")
    print(f"On the constituency-anchored candidate population (n={n}):")
    print(f"  constituency recovery : 100% (closed-set anchor)")
    print(f"  expenditure recovery  : {has_spend / max(1, n):.0%}")
    print("Caveat: candidate NAME column is the weak link (OCR-noisy, no closed")
    print("set) — must fuzzy-match to v_member_registry (Open Q#4). And the €")
    print("amounts still need a 'verify vs official SIPO PDF p.N' caveat.")


if __name__ == "__main__":
    main()
