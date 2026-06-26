"""EXPERIMENTAL sandbox — extend NOAC ingestion beyond Housing (H1-H7) + M2 collection.

The "who's slacking" uplift for the council accountability page. The live extractor only
parses NOAC Housing + M2; this pulls the rest of the citizen-facing accountability families
that NOAC already publishes in the SAME born-digital PDF
(doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf), proven extractable with PyMuPDF find_tables
(no OCR, lattice tables, 30-31 LAs per table).

NOTHING here touches gold or the pipeline. Writes la/year/value parquets to this sandbox
dir only, matching the existing noac_*_wide schema (la, year, <value cols>). Validates to
~31 local authorities and prints a usefulness summary so we can pick the keepers.

The live H families use camelot; camelot isn't installed on this box, and find_tables gives
clean output on these pages, so this proof uses find_tables. A production graduation can
swap to camelot for parity if desired — the column mapping below is the hard part either way.
"""
from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf"
OUT = Path(__file__).resolve().parent

EXPECTED_N = 31
NOT_LA = re.compile(r"^(authority|average|total|national|overall|all\s|full-time|part-time)", re.I)
# 31 LA name stems (Dublin authorities listed separately; no aggregate row).
_LA_STEMS = (
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal", "Dublin City",
    "DLR", "Dun Laoghaire", "Fingal", "Galway City", "Galway County", "Kerry", "Kildare",
    "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford", "Louth", "Mayo", "Meath",
    "Monaghan", "Offaly", "Roscommon", "Sligo", "South Dublin", "Tipperary", "Waterford",
    "Westmeath", "Wexford", "Wicklow",
)


def _clean_la(name: str) -> str:
    """Strip footnote marks (NOAC tags some rows '*'/'†') so names join cleanly."""
    return re.sub(r"[*†‡\s]+$", "", (name or "").replace("\n", " ").strip())


def _table(page_idx: int) -> list[list[str]]:
    rows = fitz.open(SRC)[page_idx].find_tables().tables[0].extract()
    return [[("" if c is None else str(c).replace("\n", " ").strip()) for c in r] for r in rows]


def _fold(s: str) -> str:
    """NFKD accent-fold so 'Dún Laoghaire' matches the ASCII stem 'Dun Laoghaire'."""
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def _is_la(name: str) -> bool:
    n = _clean_la(name)
    if not n or NOT_LA.match(n):
        return False
    nf = _fold(n)
    return any(nf.startswith(_fold(s)) for s in _LA_STEMS)


def _num(s: str) -> float | None:
    """Generic €/%/plain number → float. Negative if parens or a minus precedes the digits
    (NOAC prints deficits as '€-2,543,531')."""
    s = (s or "").strip()
    if not s or s in {"-", "n/a", "N/A", "*"}:
        return None
    neg = "(" in s or "-" in s
    digits = re.sub(r"[^\d.]", "", s)
    if not digits or digits == ".":
        return None
    try:
        v = float(digits)
    except ValueError:
        return None
    return -v if neg else v


def _mmss(s: str) -> float | None:
    """'06:13' -> 6.22 minutes; '00:00' (no such station) -> None."""
    m = re.match(r"^(\d{1,2}):(\d{2})$", (s or "").strip())
    if not m:
        return None
    mins = int(m.group(1)) + int(m.group(2)) / 60
    return None if mins == 0 else round(mins, 2)


# Single-year indicators: page_idx, out_name, {source_col: out_col}, parser, one-line "why".
# Value cols are read from data rows by index, so it doesn't matter whether the header sits
# in col0 (single-value tables) or its own row.
CONFIG = [
    (62, "noac_r1_pavement_condition", {1: "pct_primary_psci_1_4_poor", 2: "pct_primary_psci_5_6",
        3: "pct_primary_psci_7_8", 4: "pct_primary_psci_9_10_excellent"}, _num,
        "Road condition: % of Local Primary roads rated poor (1-4) vs excellent (9-10)"),
    (86, "noac_w1_water_compliance", {1: "pct_private_scheme_drinking_water_compliant"}, _num,
        "Public health: % private-scheme drinking water meeting statutory standard"),
    (98, "noac_e3_litter_pollution", {1: "pct_area_unpolluted", 2: "pct_area_slightly_polluted",
        3: "pct_area_moderately_polluted", 4: "pct_area_significantly_polluted",
        5: "pct_area_grossly_polluted"}, _num,
        "Litter: % of area unpolluted vs grossly polluted"),
    (120, "noac_p4_planning_cost_per_capita", {1: "planning_cost_per_capita_eur"}, _num,
        "Planning service cost per head"),
    (131, "noac_f1_fire_cost_per_capita", {1: "fire_cost_per_capita_eur"}, _num,
        "Fire service cost per head"),
    (132, "noac_f2_fire_mobilisation", {1: "mobilise_min_fulltime", 2: "mobilise_min_parttime"}, _mmss,
        "Fire response: avg minutes to mobilise (full-time vs part-time station)"),
    (133, "noac_f3_fire_attendance", {1: "pct_fire_attendance_within_10min",
        2: "pct_fire_attendance_10_to_20min"}, _num,
        "Fire response OUTCOME: % of fires reached within 10 min"),
    (169, "noac_c2_sickness_absence", {1: "pct_days_lost_sickness_certified",
        2: "pct_days_lost_sickness_selfcertified"}, _num,
        "Workforce: % paid working days lost to sickness absence"),
    (188, "noac_insurance_claims_per_capita", {1: "settled_claims_cost_per_capita_eur"}, _num,
        "Litigation/risk: per-capita cost of settled insurance claims"),
    (189, "noac_m4_overheads", {1: "central_mgmt_charge_pct_of_expenditure",
        2: "payroll_pct_of_revenue_expenditure"}, _num,
        "Efficiency: management overhead & payroll as % of spend"),
]


def extract_simple(page_idx: int, colmap: dict[int, str], parser, year: int = 2024) -> pl.DataFrame:
    recs = []
    for r in _table(page_idx):
        if not _is_la(r[0]):
            continue
        rec = {"la": _clean_la(r[0]), "year": year}
        for ci, name in colmap.items():
            rec[name] = parser(r[ci]) if ci < len(r) else None
        recs.append(rec)
    return pl.DataFrame(recs)


def m1_revenue_balance() -> pl.DataFrame:
    """5-year cumulative surplus/deficit (€); deficit = negative. Plus 2024 balance as % of
    income and revenue spend per capita (2024-only, on the 2024 row)."""
    years = [2020, 2021, 2022, 2023, 2024]  # cols 1..5 (source mislabels both 2023/24 'E.')
    recs = []
    for r in _table(184):
        if not _is_la(r[0]):
            continue
        la = _clean_la(r[0])
        for j, yr in enumerate(years, start=1):
            recs.append({
                "la": la, "year": yr,
                "cumulative_balance_eur": _num(r[j]) if j < len(r) else None,
                "balance_pct_of_income": _num(r[6]) if yr == 2024 and len(r) > 6 else None,
                "revenue_exp_per_capita_eur": _num(r[7]) if yr == 2024 and len(r) > 7 else None,
            })
    return pl.DataFrame(recs)


def _usefulness(name: str, df: pl.DataFrame, why: str) -> dict:
    val_cols = [c for c in df.columns if c not in ("la", "year")]
    # fill rate across value cells, and the spread of the headline (first) value col
    cells = df.select(val_cols).to_numpy().ravel()
    filled = sum(1 for v in cells if v is not None)
    head_col = val_cols[0]
    s = df.get_column(head_col).drop_nulls()
    rng = (round(float(s.min()), 2), round(float(s.max()), 2)) if s.len() else (None, None)
    return {
        "name": name, "n_la": df["la"].n_unique(), "ok_31": df["la"].n_unique() == EXPECTED_N,
        "fill_pct": round(filled / max(len(cells), 1) * 100, 1),
        "headline_col": head_col, "headline_range": rng, "why": why,
    }


def main() -> None:
    report = []

    m1 = m1_revenue_balance()
    m1.write_parquet(OUT / "noac_m1_revenue_balance_wide.parquet")
    red = m1.filter((pl.col("year") == 2024) & (pl.col("cumulative_balance_eur") < 0))
    info = _usefulness("noac_m1_revenue_balance", m1.filter(pl.col("year") == 2024),
                       "Structural health: cumulative revenue surplus/DEFICIT")
    info["deficit_las_2024"] = sorted(red["la"].to_list())
    report.append(info)

    for page, name, colmap, parser, why in CONFIG:
        df = extract_simple(page, colmap, parser)
        df.write_parquet(OUT / f"{name}_wide.parquet")
        report.append(_usefulness(name, df, why))

    report.sort(key=lambda d: (not d["ok_31"], -d["fill_pct"]))
    print(f"{'indicator':<34}{'LAs':>5}{'fill%':>7}  headline (range)")
    print("-" * 100)
    for d in report:
        flag = "" if d["ok_31"] else "  <-- LA count off"
        print(f"{d['name']:<34}{d['n_la']:>5}{d['fill_pct']:>7}  {d['headline_col']} {d['headline_range']}{flag}")
    print("\nM1 deficit councils 2024:", report[0].get("deficit_las_2024", "(n/a)") if report[0]["name"].startswith("noac_m1") else [d for d in report if d["name"].startswith("noac_m1")][0]["deficit_las_2024"])

    (OUT / "extend_coverage.json").write_text(json.dumps(report, indent=2), encoding="utf-8")
    print("\nwrote", len(report), "sandbox parquets + extend_coverage.json")


if __name__ == "__main__":
    main()
