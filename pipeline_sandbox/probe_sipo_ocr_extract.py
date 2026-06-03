"""PROBE v3 (throwaway): measure the REAL recovery rate of candidate-expense
rows from the OCR'd FF GE2024 expenses PDF, using anchors to correct OCR noise.

Pipeline tested here (geometry + anchors, NOT find_tables):
  1. page.get_text("words") -> cluster into rows by y-coordinate (probe v2 proved
     this reconstructs 'Name Constituency €Amount' rows left-to-right).
  2. Constituency correction: fuzzy-match a sliding token window in each row
     against the CLOSED SET of 43 constituencies (data/gold .../ec_constituency_
     pop_2022.parquet). A scanned form can only contain these 43 strings, so the
     closed set repairs OCR noise ('jublin South' -> 'Dublin South-...').
  3. Amount repair: the repeating round figures are the STATUTORY SPENDING CAPS
     (3-seat=15,560 / 4-seat=19,440 / 5-seat=23,340). We snap a near-cap number
     to the exact cap, and flag the *other* money token as the actual spend.

Reports, per euro-bearing row: did we recover a constituency (confident match)?
an amount? -> the % cleanly recoverable (Open Question #2).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_ocr_extract.py
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

SEAT_TO_CAP = {3: 15560, 4: 19440, 5: 23340}
CAPS = set(SEAT_TO_CAP.values())

EURO_TOKEN = re.compile(r"€")
# extract candidate money numbers from a row's joined text
MONEY_NUM = re.compile(r"€?\s?(\(?\d[\d,. ]*\d|\d)")


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def cluster_rows(words: list[tuple], y_tol: float = 4.0) -> list[list[tuple]]:
    rows: list[list[tuple]] = []
    for w in sorted(words, key=lambda w: (round(w[1] / y_tol), w[0])):
        if rows and abs(rows[-1][0][1] - w[1]) <= y_tol:
            rows[-1].append(w)
        else:
            rows.append([w])
    for r in rows:
        r.sort(key=lambda w: w[0])
    return rows


def norm(s: str) -> str:
    return re.sub(r"[^a-z]", "", s.lower())


def parse_money(tok: str) -> float | None:
    """Turn an OCR money token into a float, repairing common noise."""
    t = tok.replace("€", "").replace("(", "").replace(")", "").replace(" ", "")
    t = t.rstrip("_,.").lstrip("\\")
    if not t or not any(c.isdigit() for c in t):
        return None
    # unify separators: drop thousands sep, keep a single decimal
    # common OCR: '19.440'=19,440 ; '19,440'=19440 ; '7,095.13'=7095.13
    digits = re.sub(r"[^\d]", "", t)
    if not digits:
        return None
    # heuristic: if exactly 2 trailing-decimal pattern present, honour it
    m = re.match(r"^(\d[\d,. ]*?)([.,]\d{2})$", t)
    if m:
        whole = re.sub(r"[^\d]", "", m.group(1))
        cents = m.group(2)[1:]
        try:
            return float(f"{whole}.{cents}")
        except ValueError:
            return None
    try:
        return float(digits)
    except ValueError:
        return None


def snap_to_cap(v: float) -> int | None:
    """If a value is within 3% of a statutory cap, snap it (OCR comma/decimal loss)."""
    for cap in CAPS:
        if abs(v - cap) / cap <= 0.03:
            return cap
    # OCR often drops a digit: 19.440 -> 19440 already handled; try x10/x100? no — too loose
    return None


def main() -> None:
    if not OCR_PDF.exists():
        print("OCR pdf not found:", OCR_PDF)
        return
    constit = pl.read_parquet(CONSTIT_PARQUET)
    names = constit["constituency_name"].to_list()
    norm_to_name = {norm(n): n for n in names}
    norm_keys = list(norm_to_name)

    doc = fitz.open(OCR_PDF)
    rows: list[tuple[int, list[tuple]]] = []
    for pno, page in enumerate(doc, start=1):
        for row in cluster_rows(page.get_text("words")):
            rows.append((pno, row))

    euro_rows = [(p, r) for (p, r) in rows if any(EURO_TOKEN.search(w[4]) for w in r)]

    recovered = []
    for p, r in euro_rows:
        toks = [w[4] for w in r]
        joined = " ".join(toks)
        # --- constituency: best fuzzy match over sliding 1-3 token windows ---
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
        # --- money: all parseable numbers in row ---
        nums = [v for v in (parse_money(t) for t in MONEY_NUM.findall(joined)) if v]
        # separate cap (assigned) from spend (the non-cap money > 0)
        cap = next((snap_to_cap(v) for v in nums if snap_to_cap(v)), None)
        spends = [v for v in nums if snap_to_cap(v) is None and 0 < v < 60000]
        spend = max(spends) if spends else None
        recovered.append(
            {
                "page": p,
                "text": joined,
                "constituency": best_name,
                "constit_score": round(best_score, 2),
                "cap": cap,
                "spend": spend,
            }
        )

    n = len(recovered)
    with_constit = sum(1 for x in recovered if x["constituency"] and x["constit_score"] >= 0.80)
    with_cap = sum(1 for x in recovered if x["cap"])
    with_spend = sum(1 for x in recovered if x["spend"])
    full = sum(
        1
        for x in recovered
        if x["constituency"] and x["constit_score"] >= 0.80 and x["spend"]
    )

    hr("RECOVERY RATE (euro-bearing candidate rows)")
    print(f"euro-bearing rows                 : {n}")
    print(f"  with confident constituency (≥.80): {with_constit}  ({with_constit / max(1, n):.0%})")
    print(f"  with statutory cap snapped       : {with_cap}  ({with_cap / max(1, n):.0%})")
    print(f"  with a parseable spend amount    : {with_spend}  ({with_spend / max(1, n):.0%})")
    print(f"  FULL (constituency + spend)      : {full}  ({full / max(1, n):.0%})")

    hr("SAMPLE: 30 recovered rows (constituency | cap | spend  <= raw)")
    for x in recovered[:30]:
        c = x["constituency"] or "?"
        print(
            f"  p{x['page']:>2} | {c:<20} score={x['constit_score']:.2f} "
            f"cap={x['cap']} spend={x['spend']}"
        )
        print(f"        raw: {x['text'][:90]}")

    hr("VERDICT")
    print(f"full-row recovery (constituency + spend amount): {full}/{n} = {full / max(1, n):.0%}")
    print("anchors used: closed set of 43 constituencies + 3 statutory caps.")
    print("remaining gap is the CANDIDATE NAME column (OCR-noisy, no closed set;")
    print("needs fuzzy match to v_member_registry — Open Question #4).")


if __name__ == "__main__":
    main()
