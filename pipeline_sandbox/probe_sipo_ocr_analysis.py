"""PROBE v6 (throwaway): (A) sanity-check the recovered SIPO expenditure numbers,
and (B) characterise the ~15% unrecoverable rows in detail.

Builds on probe_sipo_ocr_repair.py. Two jobs:

A. DO THE NUMBERS MAKE SENSE?
   - Hard constraint: every recovered spend must be <= that constituency's
     statutory cap (3/4/5-seat = 15,560/19,440/23,340). Violations = bad reads.
   - Independent cross-check: the cap NUMBER OCR'd on the row should match the
     cap IMPLIED by the matched constituency's seat count (td_seats_2024). Two
     signals agreeing = high confidence the row is correctly assembled.
   - Distribution (min/median/max/sum) of clean vs reconstructed spends — are the
     magnitudes plausible for an election expense (tens to low tens-of-thousands)?
   - Row count vs the number of candidates FF actually ran in GE2024.

B. THE 15%: dump every unrecoverable row in full and bucket WHY it failed
   (no digits at all / digits-but-no-cap-valid-parse / concatenated-multinumber),
   and test whether a looser pass could rescue any (and at what false-positive risk).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_sipo_ocr_analysis.py
Reads only; writes nothing.
"""

from __future__ import annotations

import difflib
import re
import statistics
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
CAP_MAX = 23340
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
    return v is not None and any(abs(v - c) / c <= 0.03 for c in CAPS)


def interpret_token(tok: str):
    digits = re.sub(r"\D", "", tok)
    if not digits:
        return None, "none"
    if is_cap(float(digits)):
        return float(digits), "cap"
    m = re.search(r"(\d[\d,. ]*?)\s*[.,:]\s*(\d{2})\D*$", tok)
    if m:
        whole = re.sub(r"\D", "", m.group(1))
        if whole:
            v = float(f"{whole}.{m.group(2)}")
            if 0 < v <= CAP_MAX:
                return round(v, 2), "clean"
            if len(whole) > 1:
                v2 = float(f"{whole[1:]}.{m.group(2)}")
                if 0 < v2 <= CAP_MAX:
                    return round(v2, 2), "clean"
    if len(digits) >= MIN_RECON_DIGITS:
        for d in (digits, digits[1:]):
            v = float(d) / 100
            if 0 < v <= CAP_MAX:
                return round(v, 2), "reconstructed"
    return None, "none"


def main():
    constit = pl.read_parquet(CONSTIT_PARQUET)
    norm_to_name = {norm(n): n for n in constit["constituency_name"].to_list()}
    norm_keys = list(norm_to_name)
    name_to_seats = dict(zip(constit["constituency_name"], constit["td_seats_2024"]))

    doc = fitz.open(OCR_PDF)
    rows_out = []  # dicts: page, constituency, ocr_cap, implied_cap, spend, mode, raw
    for pno in range(2, 10):
        for row in cluster_rows(doc[pno].get_text("words")):
            toks = [w[4] for w in row]
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
            right = toks[best_i:]
            parsed = [interpret_token(t) for t in right]
            caps_seen = [v for v, m in parsed if m == "cap"]
            cleans = [v for v, m in parsed if m == "clean"]
            recons = [v for v, m in parsed if m == "reconstructed"]
            spend, mode = (cleans[-1], "clean") if cleans else (
                (recons[-1], "reconstructed") if recons else (None, "unrecoverable")
            )
            rows_out.append(
                {
                    "page": pno,
                    "constituency": best_name,
                    "score": round(best_score, 2),
                    "ocr_cap": int(caps_seen[0]) if caps_seen else None,
                    "implied_cap": SEAT_TO_CAP.get(int(name_to_seats.get(best_name, 0))),
                    "spend": spend,
                    "mode": mode,
                    "raw": " ".join(toks),
                    "right": " ".join(right),
                }
            )

    recovered = [r for r in rows_out if r["spend"] is not None]
    unrec = [r for r in rows_out if r["spend"] is None]

    # ---------- A. SANITY ----------
    hr("A1. HARD CONSTRAINT: spend <= constituency cap")
    viol = [r for r in recovered if r["implied_cap"] and r["spend"] > r["implied_cap"]]
    print(f"recovered rows           : {len(recovered)}")
    print(f"spend > implied cap      : {len(viol)}  (should be ~0; any = bad read)")
    for r in viol:
        print(f"  VIOLATION p{r['page']} {r['constituency']}: spend={r['spend']} > cap={r['implied_cap']} | {r['right'][:50]}")

    hr("A2. INDEPENDENT CROSS-CHECK: OCR'd cap vs constituency-implied cap")
    both = [r for r in rows_out if r["ocr_cap"] and r["implied_cap"]]
    agree = [r for r in both if r["ocr_cap"] == r["implied_cap"]]
    print(f"rows with BOTH an OCR cap and an implied cap : {len(both)}")
    print(f"  the two agree                              : {len(agree)}  ({len(agree)/max(1,len(both)):.0%})")
    print("  (agreement = constituency match AND cap read are mutually confirmed)")
    for r in both:
        if r["ocr_cap"] != r["implied_cap"]:
            print(f"  DISAGREE p{r['page']} {r['constituency']}: ocr={r['ocr_cap']} implied={r['implied_cap']} | {r['raw'][:55]}")

    hr("A3. SPEND DISTRIBUTION (plausible for an election expense?)")
    for label, subset in (("clean", [r for r in recovered if r["mode"] == "clean"]),
                          ("reconstructed", [r for r in recovered if r["mode"] == "reconstructed"]),
                          ("all recovered", recovered)):
        vals = sorted(r["spend"] for r in subset)
        if not vals:
            continue
        print(f"{label:<16} n={len(vals):>2} min=€{vals[0]:>9,.2f} median=€{statistics.median(vals):>9,.2f} "
              f"max=€{vals[-1]:>9,.2f} sum=€{sum(vals):>12,.2f}")
    # implausibly tiny reconstructions (< €5) are suspect
    tiny = [r for r in recovered if r["spend"] < 5]
    print(f"suspiciously tiny (<€5)  : {len(tiny)}  {[(r['constituency'], r['spend'], r['mode']) for r in tiny]}")

    hr("A4. ROW COUNT vs reality")
    print(f"candidate rows parsed (pages 3-10) : {len(rows_out)}")
    print("FF ran ~82 candidates in GE2024; this form lists candidates who")
    print("assigned spend to the national party, so a count in the 60s-70s is")
    print("plausible. Duplicate constituencies are EXPECTED (multi-seat = multiple")
    print("FF candidates per constituency).")
    from collections import Counter
    dupes = {k: v for k, v in Counter(r["constituency"] for r in rows_out).items() if v > 1}
    print(f"constituencies with >1 row: {dupes}")

    # ---------- B. THE 15% ----------
    hr(f"B. THE UNRECOVERABLE {len(unrec)} ROWS — full dump + failure bucket")
    def bucket(r):
        digs = re.sub(r"\D", "", r["right"])
        # strip the cap digits to see what's left for the spend column
        non_cap = r["right"]
        if r["ocr_cap"]:
            non_cap = non_cap.replace(str(r["ocr_cap"]), "", 1)
        spend_digits = re.sub(r"\D", "", re.sub(r"\b\d{4,5}\b", "", non_cap))
        if not spend_digits:
            return "no digits in spend column (pure garble)"
        if len(digs) > 10:
            return "concatenated multi-number (ambiguous)"
        return f"has {len(spend_digits)} stray spend-digit(s) but no cap-valid parse"
    from collections import Counter as C
    buckets = C(bucket(r) for r in unrec)
    for b, n in buckets.most_common():
        print(f"  [{n}] {b}")
    print()
    for r in unrec:
        print(f"  p{r['page']:>2} {str(r['constituency']):<18} cap_ocr={r['ocr_cap']} | {r['raw'][:70]}")

    hr("B2. could a LOOSER pass rescue any of the 15%?")
    rescuable = []
    for r in unrec:
        non_cap = r["right"]
        if r["ocr_cap"]:
            non_cap = non_cap.replace(str(r["ocr_cap"]), "", 1)
        # any 2-3 digit run that /100 gives €0.x-€9.xx? too risky. Only flag runs >=3 digits.
        runs = re.findall(r"\d{3,}", re.sub(r"\b1[59]\d{3}\b|\b2[0-3]\d{3}\b", "", non_cap))
        if runs:
            rescuable.append((r["constituency"], runs, r["right"][:45]))
    print(f"rows with a >=3-digit run left after removing the cap: {len(rescuable)}")
    for c, runs, raw in rescuable:
        print(f"  {c}: runs={runs}  <= {raw!r}  (LOW confidence — would need PDF check)")
    print()
    print("VERDICT: the genuine-garble rows have no recoverable spend digits at all")
    print("=> they are a true OCR-quality floor; only a higher-DPI / form-tuned")
    print("re-OCR (Tesseract) can move them. A looser regex would invent numbers.")


if __name__ == "__main__":
    main()
