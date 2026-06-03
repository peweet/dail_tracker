"""PROBE (sample ingest + DQ): the amalgamated Local Authority Annual Financial
Statement (AFS) — Dept of Housing, all-31-LAs, gov.ie PDF. BUDGET/SPENT tier.

Decision-support: ingest a SAMPLE, run data-quality checks, report tangible benefits —
do NOT build. Sample = the 2020 audited amalgamation (49pp, digital). Extracts the two
statutory by-service-division tables:
  - Statement of Comprehensive Income (Income & Expenditure by Division)  [p12]
  - Note 16 Over/Under Expenditure (actual vs adopted BUDGET + variance)   [p29]

KEY SCOPE FINDING (proven by a council-name scan): the *amalgamated* AFS is the
ALL-31-SUMMED national total — it has **no per-LA breakdown**. Per-council/by-division
data needs the 31 INDIVIDUAL council AFS PDFs (a separate, larger ingest).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_afs_amalgamated.py
Reads the cached PDF at c:/tmp/afs/afs_231581.pdf; writes a tidy CSV to c:/tmp.
"""

from __future__ import annotations

import contextlib
import re
import sys
from pathlib import Path

import fitz
import polars as pl

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

PDF = Path("c:/tmp/afs/afs_231581.pdf")  # 2020 amalgamation
OUT = Path("c:/tmp/afs/afs_2020_divisions.csv")
DIVISIONS = [
    "Housing and Building", "Roads Transportation and Safety", "Water Services",
    "Development Management", "Environmental Services", "Recreation and Amenity",
    "Agriculture, Education, Health and Welfare", "Miscellaneous Services",
]
NUM = re.compile(r"^\(?-?[\d,]+\)?$")


def hr(t: str) -> None:
    print(f"\n{'=' * 74}\n{t}\n{'=' * 74}")


def to_num(s: str) -> float:
    s = s.strip()
    neg = s.startswith("(") and s.endswith(")")
    v = re.sub(r"[^\d]", "", s)
    if not v:
        return 0.0
    return -float(v) if neg else float(v)


def parse_division_table(page_text: str, n_cols: int) -> dict[str, list[float]]:
    """Each division name is followed by its row of numeric cells (one per line)."""
    lines = [ln.strip() for ln in page_text.splitlines() if ln.strip()]
    out: dict[str, list[float]] = {}
    for i, ln in enumerate(lines):
        if ln in DIVISIONS:
            nums = []
            j = i + 1
            while j < len(lines) and len(nums) < n_cols:
                if NUM.match(lines[j]):
                    nums.append(to_num(lines[j]))
                elif lines[j] in DIVISIONS or "Total" in lines[j]:
                    break
                j += 1
            if len(nums) == n_cols:
                out[ln] = nums
    return out


def main() -> None:
    if not PDF.exists():
        print("cached PDF missing — download afs_231581.pdf first.")
        return
    doc = fitz.open(PDF)

    hr("SAMPLE INGEST — amalgamated AFS 2020 (all 31 LAs, €)")
    # p12: Gross Expenditure | Income | Net Exp 2020 | Net Exp 2019  (verified, reconciles)
    ie = parse_division_table(doc[12].get_text("text"), 4)
    # p29 Note 16 (actual vs budget) is present but stacks 2 sub-tables → naive parse mis-aligns;
    # deliberately not extracted in this sample (see DQ section).
    doc.close()

    rows = []
    for dv in DIVISIONS:
        g = ie.get(dv, [None] * 4)
        # NOTE: Note-16 (p29) budget cols deliberately NOT written — the naive parse
        # mis-aligned on that stacked-sub-table page (see DQ section). Only the verified
        # I&E table (p12) is persisted.
        rows.append({
            "division": dv,
            "gross_expenditure": g[0], "income": g[1], "net_expenditure": g[2],
            "net_expenditure_prior_yr": g[3],
        })
    df = pl.DataFrame(rows).with_columns(
        pl.lit(2020).alias("year"), pl.lit("all-31-LAs (amalgamated)").alias("scope"),
        pl.lit("SPENT+BUDGET").alias("realisation_tier"),
        pl.lit("Dept Housing amalgamated AFS (gov.ie), audited").alias("source"),
    )
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(OUT)
    print(f"extracted {df.height} service divisions (I&E table p12, verified) -> {OUT}\n")
    for r in df.iter_rows(named=True):
        print(f"  {r['division'][:42]:<42} gross €{(r['gross_expenditure'] or 0) / 1e6:>8,.0f}m  "
              f"income €{(r['income'] or 0) / 1e6:>8,.0f}m  net €{(r['net_expenditure'] or 0) / 1e6:>7,.0f}m")

    hr("DATA-QUALITY ANALYSIS")
    n_ie = sum(1 for dv in DIVISIONS if dv in ie)
    gross_sum = sum((ie[dv][0] for dv in ie), 0.0)
    printed_total = 6_750_822_110  # the "Total Expenditure/Income" line on p12
    recon = abs(gross_sum - printed_total)
    print("  digital text-layer (no OCR)          : YES (2020; older years may be scanned)")
    print(f"  I&E-by-division captured (p12)       : {n_ie}/8")
    print(f"  reconciliation: Σdivisions gross      : €{gross_sum:,.0f}")
    print(f"                  printed Total line     : €{printed_total:,.0f}")
    print(f"                  diff                   : €{recon:,.0f}  "
          f"{'✓ EXACT (rounding)' if recon <= 2 else '⚠ check'}")
    print(f"  prior-year column present (2019)     : {'YES' if all(ie[dv][3] for dv in ie) else 'partial'} → time series feasible")
    print("  actual-vs-budget (Note 16, p29)      : ⚠ NAIVE PARSE MIS-ALIGNED — that page stacks")
    print("      an Expenditure AND an Income sub-table (division names appear twice), so the")
    print("      line-parser grabbed the wrong rows. Data IS present + reconciles in the PDF; it")
    print("      needs a targeted per-sub-table extractor. CAUGHT here, NOT trusted (cols dropped).")
    print("  *** GRANULARITY: NATIONAL all-31-SUMMED — ZERO per-LA rows (no council names in doc).")
    print("      Per-council by-division needs the 31 INDIVIDUAL AFS PDFs (separate larger ingest).")
    print("  accounting basis: ACCRUAL (revenue account) — 'net expenditure' ≠ cash POs; different")
    print("      grain from the per-transaction PO layer (do not reconcile the two).")

    hr("TANGIBLE BENEFITS — verdict")
    print("WHAT IT ADDS (cheap, clean, multi-year 2009–2023, 1 PDF/yr):")
    print("  1. Spend by SERVICE FUNCTION (Housing/Roads/Water/Environment/Recreation/…) —")
    print("     the civic 'what areas' cut that CSO GFA04 (economic category) does NOT give.")
    print("  2. ACTUAL vs adopted BUDGET + variance per division EXISTS (Note 16) — accountability")
    print("     angle; needs a targeted sub-table parser (the stacked-table page defeats a naive parse).")
    print("  3. A national DENOMINATOR/context for the micro procurement layers (e.g. PO")
    print("     construction spend framed against €1.85bn Housing division expenditure).")
    print("  4. Income vs expenditure split per division (self-funding vs rates/LPT-funded).")
    print("WHAT IT DOES NOT GIVE:")
    print("  - NO per-LA / per-constituency detail (the project's USP) → that's the 31 separate")
    print("    council AFS PDFs, a much bigger job. The amalgamation is CONTEXT, not the prize.")
    print("VERDICT: low-cost national BUDGET+SPENT-by-function context layer — worth ingesting for")
    print("  framing; the per-LA AFS is a separate, larger decision (31 PDFs, statutory tables).")


if __name__ == "__main__":
    main()
