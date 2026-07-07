"""NOAC accountability scorecard — gold extractor (Finance/Workforce/Roads/Fire/Litter).

Extends the existing NOAC ingestion (Housing H1-H7 + M2 collection) with the five
citizen-facing accountability indicators surfaced on the "Who Runs Your County" dossier:

  M1  Revenue Account Balance       p185  -> balance as % of income (solvency)
  M3  Public Liability Claims       p189  -> per-capita cost of settled claims (litigation/risk)
  M4  Overheads                     p190  -> central management charge & payroll as % of spend
  C2  Sickness absence              p170  -> % days lost, medically certified (workforce)
  R1  Pavement Surface Condition    p63   -> % local-primary roads rated poor (PSCI 1-4)
  F3  Fire attendance               p134  -> % fires reached within 10 minutes
  E3  Litter pollution              p99   -> moderately / significantly / grossly polluted %

Reads : doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf  (born-digital, PyMuPDF find_tables, no OCR)
Writes: data/gold/parquet/noac_scorecard_wide.parquet  (one row per LA, 2024)

`la` is normalised to the SAME naming as the other noac_*_wide gold tables
('Dun Laoghaire-Rathdown', 'Limerick City and County', …) so v_la_noac_scorecard joins
via the existing la_map crosswalk. Derivation (litter sum, fire service-null, national
medians) lives in the VIEW, not here — gold keeps the raw published columns.
"""

from __future__ import annotations

import re
import sys
import unicodedata
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.parquet_io import save_parquet  # noqa: E402

SRC = ROOT / "doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf"
DEST = ROOT / "data/gold/parquet/noac_scorecard_wide.parquet"

# 31 canonical noac_la names (match existing noac_*_wide gold + the la_map crosswalk).
CANON = {
    "Carlow County",
    "Cavan County",
    "Clare County",
    "Cork City",
    "Cork County",
    "Donegal County",
    "Dublin City",
    "Dun Laoghaire-Rathdown",
    "Fingal County",
    "Galway City",
    "Galway County",
    "Kerry County",
    "Kildare County",
    "Kilkenny County",
    "Laois County",
    "Leitrim County",
    "Limerick City and County",
    "Longford County",
    "Louth County",
    "Mayo County",
    "Meath County",
    "Monaghan County",
    "Offaly County",
    "Roscommon County",
    "Sligo County",
    "South Dublin County",
    "Tipperary County",
    "Waterford City and County",
    "Westmeath County",
    "Wexford County",
    "Wicklow County",
}
_CANON_FOLD = {"".join(c for c in unicodedata.normalize("NFKD", n) if not unicodedata.combining(c)): n for n in CANON}


def _canon_la(raw: str) -> str | None:
    """Map a NOAC row label to the canonical noac_la (accent-fold; 'DLR' alias)."""
    n = re.sub(r"[*†‡\s]+$", "", (raw or "").replace("\n", " ").strip())
    if n == "DLR":
        return "Dun Laoghaire-Rathdown"
    folded = "".join(c for c in unicodedata.normalize("NFKD", n) if not unicodedata.combining(c))
    return _CANON_FOLD.get(folded)


def _num(s: str) -> float | None:
    s = ("" if s is None else str(s)).strip()
    if not s or s in {"-", "n/a", "N/A", "*"}:
        return None
    neg = "(" in s or "-" in s
    digits = re.sub(r"[^\d.]", "", s)
    if not digits or digits == ".":
        return None
    try:
        return -float(digits) if neg else float(digits)
    except ValueError:
        return None


def _table(page_idx: int) -> list[list[str]]:
    rows = fitz.open(SRC)[page_idx].find_tables().tables[0].extract()
    return [[("" if c is None else str(c).replace("\n", " ").strip()) for c in r] for r in rows]


# page_idx, {source_col: out_field}
SOURCES = [
    (184, {6: "revenue_balance_pct"}),  # M1 col F = balance as % of income
    (188, {1: "m3_claims_per_capita_eur"}),  # M3 col A = settled-claims cost per capita
    (189, {1: "m4_central_mgmt_charge_pct", 2: "m4_payroll_pct"}),  # M4 cols A/B = overhead & payroll %
    (169, {1: "sickness_certified_pct"}),  # C2 col A = medically certified
    (62, {1: "roads_poor_pct"}),  # R1 col B(b) = % primary PSCI 1-4
    (133, {1: "fire_within_10min_pct"}),  # F3 col A = within 10 min
    (98, {3: "litter_moderate_pct", 4: "litter_significant_pct", 5: "litter_grossly_pct"}),
]


def main() -> None:
    recs: dict[str, dict] = {}
    for page, colmap in SOURCES:
        for r in _table(page):
            la = _canon_la(r[0])
            if la is None:
                continue
            rec = recs.setdefault(la, {"la": la, "year": 2024})
            for ci, field in colmap.items():
                rec[field] = _num(r[ci]) if ci < len(r) else None

    df = pl.DataFrame(list(recs.values()))
    missing = CANON - set(df["la"])
    if missing:
        raise SystemExit(f"NOAC scorecard: missing councils {sorted(missing)} — aborting (no partial gold)")
    cols = [
        "la",
        "year",
        "revenue_balance_pct",
        "m3_claims_per_capita_eur",
        "m4_central_mgmt_charge_pct",
        "m4_payroll_pct",
        "sickness_certified_pct",
        "roads_poor_pct",
        "fire_within_10min_pct",
        "litter_moderate_pct",
        "litter_significant_pct",
        "litter_grossly_pct",
    ]
    df = df.select(cols).sort("la")
    save_parquet(df, DEST, min_rows=31)
    print(f"wrote {DEST}  ({df.height} LAs)")
    print(df.head(3))


if __name__ == "__main__":
    main()
