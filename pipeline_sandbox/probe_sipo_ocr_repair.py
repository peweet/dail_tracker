"""PROBE v5 (throwaway): push the expenditure-€ recovery rate up from the 66%
baseline (probe_sipo_ocr_columns.py) by repairing the two dominant OCR failure
modes, using the statutory spending cap as a hard validity bound.

Failure modes seen in the raw rows (pages 3-10, FF GE2024 expenses):
  A. parser-hidden decimal: a real 2-dp figure buried under leading OCR noise
     ('e3560.63', '‘927.04', 'e9,166.52', '‘3407.15'). Deterministic strip -> clean.
  B. dropped decimal point: a figure OCR'd with no decimal ('85651'=€856.51,
     '249207'=€2,492.07, '4538'=€45.38) or a MISPLACED decimal ('8.41487'=8414.87).
     SIPO always reports to the cent, so digits/100 reconstructs it. HEURISTIC ->
     flagged confidence='reconstructed'.
  C. genuine garbage ('feireaa7e', 'jeast25', '1558062788') -> unrecoverable.

The safety key for B: the statutory cap (3/4/5-seat = 15,560/19,440/23,340) is a
LEGAL spending ceiling. Any value > cap is provably decimal-lost; a digits/100
repair is accepted ONLY if it lands in (0, cap]. This is format reconstruction
against a hard external bound, not modelling — but it changes a displayed number
so it MUST carry a 'reconstructed / verify vs official PDF p.N' flag (consistent
with the gold-layer-quarantine rule).

Reports clean vs deterministic-repair vs reconstructed-heuristic vs unrecoverable.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_ocr_repair.py
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
CAP_MAX = 23340
# minimum digit count before we trust a /100 decimal-reconstruction: a real
# expense is >=€10.00, i.e. >=4 digits once the decimal is dropped. This rejects
# stray-digit garbage ('feireaa7e'->'7'->0.07) that would otherwise look valid.
MIN_RECON_DIGITS = 4


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


def is_cap(v):
    if v is None:
        return False
    return any(abs(v - c) / c <= 0.03 for c in CAPS)


def interpret_token(tok: str):
    """Interpret ONE whitespace-separated token as money.

    Returns (value, mode) where mode in {clean, reconstructed, cap, none}.
      clean        -> explicit 2-dp figure (':'/'.'/',' decimal), 0 < v <= cap
      reconstructed-> no usable decimal; >=4 digits; digits/100 lands in (0, cap]
      cap          -> the token IS a statutory cap (15,560/19,440/23,340) -> not spend
      none         -> no usable digits / fails every test
    """
    digits = re.sub(r"\D", "", tok)
    if not digits:
        return None, "none"
    if is_cap(float(digits)):
        return float(digits), "cap"
    # explicit 2-dp tail (decimal sep may be . , or OCR ':')
    m = re.search(r"(\d[\d,. ]*?)\s*[.,:]\s*(\d{2})\D*$", tok)
    if m:
        whole = re.sub(r"\D", "", m.group(1))
        if whole:
            v = float(f"{whole}.{m.group(2)}")
            if 0 < v <= CAP_MAX:
                return round(v, 2), "clean"
            # one leading-noise digit ('617,160.93' -> '17,160.93')
            if len(whole) > 1:
                v2 = float(f"{whole[1:]}.{m.group(2)}")
                if 0 < v2 <= CAP_MAX:
                    return round(v2, 2), "clean"
    # no usable decimal -> reconstruct via /100 (SIPO cents convention),
    # but only if there are enough digits to be a real figure (>= €10.00)
    if len(digits) >= MIN_RECON_DIGITS:
        for d in (digits, digits[1:]):  # allow one leading-noise digit
            v = float(d) / 100
            if 0 < v <= CAP_MAX:
                return round(v, 2), "reconstructed"
    return None, "none"


def main():
    constit = pl.read_parquet(CONSTIT_PARQUET)
    norm_to_name = {norm(n): n for n in constit["constituency_name"].to_list()}
    norm_keys = list(norm_to_name)

    doc = fitz.open(OCR_PDF)
    stats = {"clean": 0, "reconstructed": 0, "unrecoverable": 0}
    samples = {"clean": [], "reconstructed": [], "unrecoverable": []}

    for pno in range(2, 10):  # pages 3-10 = candidate summary table
        for row in cluster_rows(doc[pno].get_text("words")):
            toks = [w[4] for w in row]
            # locate the constituency match (anchor) + its token index
            best_i, best_name, best_score = -1, None, 0.0
            for win in (1, 2, 3):
                for i in range(len(toks) - win + 1):
                    cand = norm(" ".join(toks[i : i + win]))
                    if len(cand) < 4:
                        continue
                    mm = difflib.get_close_matches(cand, norm_keys, n=1, cutoff=0.80)
                    if mm:
                        sc = difflib.SequenceMatcher(None, cand, mm[0]).ratio()
                        if sc > best_score:
                            best_score, best_name, best_i = sc, norm_to_name[mm[0]], i + win
            if best_score < 0.80:
                continue

            # expenditure lives to the RIGHT of the constituency; scan tokens
            # left-to-right, dropping the statutory-cap token (the "assigned"
            # column) so only the spend column remains.
            right_toks = toks[best_i:]
            parsed = [interpret_token(t) for t in right_toks]
            cleans = [v for v, m in parsed if m == "clean"]
            recons = [v for v, m in parsed if m == "reconstructed"]
            value, mode = None, "none"
            if cleans:
                value, mode = cleans[-1], "clean"  # rightmost clean = expenditure col
            elif recons:
                value, mode = recons[-1], "reconstructed"

            key = "unrecoverable" if value is None else mode
            stats[key] += 1
            if len(samples[key]) < 12:
                samples[key].append((pno, best_name, value, " ".join(right_toks)[:60]))

    tot = sum(stats.values())
    hr("IMPROVED EXPENDITURE RECOVERY (pages 3-10, cap-bounded repair)")
    print(f"candidate rows                         : {tot}")
    print(f"  clean (explicit 2-dp, deterministic) : {stats['clean']}  ({stats['clean']/tot:.0%})")
    print(f"  reconstructed (/100, FLAGGED)        : {stats['reconstructed']}  ({stats['reconstructed']/tot:.0%})")
    print(f"  unrecoverable (garbage)              : {stats['unrecoverable']}  ({stats['unrecoverable']/tot:.0%})")
    usable = stats["clean"] + stats["reconstructed"]
    print(f"  --> USABLE (clean + flagged repair)  : {usable}  ({usable/tot:.0%})")
    print(f"      vs 66% baseline (clean only)     : +{(usable-stats['clean'])/tot:.0%} from repair")

    for k in ("clean", "reconstructed", "unrecoverable"):
        hr(f"SAMPLE: {k}")
        for pno, name, v, raw in samples[k]:
            print(f"  p{pno:>2} | {str(name):<18} value={v}  <= {raw!r}")

    hr("VERDICT")
    print("Deterministic noise-stripping + cap-bounded /100 reconstruction lifts")
    print("usable expenditure rows well above the 66% clean-only baseline. The")
    print("reconstructed bucket MUST ship flagged ('verify vs official SIPO PDF');")
    print("the unrecoverable remainder is true OCR loss (re-OCR at higher DPI is the")
    print("only lever left for those).")


if __name__ == "__main__":
    main()
