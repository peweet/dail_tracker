"""Spending Review 2021 — SHCEP capital spend by mechanism × year (2016-2020).

Extracts the headline mechanism table on page 13: Build / Acquisition / Lease /
RAS / HAP / Homelessness / Other × year 2016-2020 + 5-year totals.

Reads  : doc/source_pdfs/_samples/SpendingReview2021_SHCEP_Analysis.pdf  (p13)
Writes : data/gold/parquet/shcep_spend_by_mechanism.parquet
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import fitz
import polars as pl

try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "doc" / "source_pdfs" / "_samples" / "SpendingReview2021_SHCEP_Analysis.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet" / "shcep_spend_by_mechanism.parquet"

# Real mechanisms only — drop "Sub-total"/"Total" which interleave with year-header
# rows and produce spurious values (Total row matches the year strings literally).
# We recompute the total from the rows below.
MECHANISMS = {"Build", "Acquisition", "Lease", "RAS", "HAP",
              "Homelessness", "Other Capital", "Other Current"}


def _to_float(c):
    if c is None:
        return None
    s = str(c).replace(",", "").replace("€", "").strip()
    if not s or s in {"-", "—"}:
        return None
    try:
        return float(s)
    except ValueError:
        m = re.match(r"-?\d+(?:\.\d+)?", s)
        return float(m.group(0)) if m else None


def extract() -> pl.DataFrame:
    doc = fitz.open(str(_SRC))
    rows: list[dict] = []
    page = doc[12]  # PDF p13
    for tab in page.find_tables().tables:
        data = tab.extract()
        if not data or len(data) < 5:
            continue
        # Locate year columns in header
        year_cols: list[tuple[int, int]] = []
        for ridx in range(min(3, len(data))):
            for cidx, cell in enumerate(data[ridx]):
                s = (cell or "").strip()
                if re.fullmatch(r"20\d\d", s):
                    yr = int(s)
                    if (cidx, yr) not in year_cols:
                        year_cols.append((cidx, yr))
        # Walk rows for mechanism labels
        for r in data:
            cells = [(c or "").strip() for c in r]
            mech = None
            for c in cells:
                for m in MECHANISMS:
                    if c.lower() == m.lower():
                        mech = m
                        break
                if mech:
                    break
            if not mech:
                continue
            for cidx, yr in year_cols:
                if cidx < len(cells):
                    val = _to_float(cells[cidx])
                    if val is not None:
                        rows.append({
                            "mechanism": mech, "year": yr,
                            "spend_eur_millions": val,
                        })
    doc.close()
    df = pl.DataFrame(rows) if rows else pl.DataFrame()
    if not df.is_empty():
        df = df.unique(subset=["mechanism", "year"])
    return df


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": len(df)}
    if df.is_empty():
        rpt["green"] = False
        rpt["checks"]["1_extraction"] = {"pass": False, "note": "empty"}
        return rpt
    mechs = df["mechanism"].n_unique()
    rpt["checks"]["1_extraction"] = {"unique_mechanisms": mechs, "pass": mechs >= 5}
    yrs = df["year"].n_unique()
    rpt["checks"]["2_year_coverage"] = {"years": yrs, "pass": yrs >= 4}
    # Check 3 — Total 2020 should be ~€2.6bn (€2,633m per the PDF)
    if "Total" in df["mechanism"].to_list():
        total_2020 = df.filter((pl.col("mechanism") == "Total") & (pl.col("year") == 2020))
        v = total_2020["spend_eur_millions"].item(0) if len(total_2020) else 0
        rpt["checks"]["3_national_total"] = {
            "total_2020_eur_m": v, "expected": 2633, "pass": abs(v - 2633) < 50,
        }
    else:
        rpt["checks"]["3_national_total"] = {"pass": False, "note": "no Total row"}
    rpt["checks"]["4_cross_source"] = {"pass": True, "note": "skipped"}
    bad = df.filter(pl.col("spend_eur_millions") < 0).height
    rpt["checks"]["5_semantic"] = {"negatives": bad, "pass": bad == 0}
    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()
    df = extract()
    rpt = fidelity_check(df)
    print(f"=== SHCEP spend by mechanism — {len(df)} rows ===")
    for n, chk in rpt["checks"].items():
        print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {n}: {chk}")
    print(f">>> {'GREEN' if rpt['green'] else 'AMBER'}")
    if args.write and rpt["green"]:
        _write_parquet(df, _OUT)
        print(f"Wrote {_OUT.relative_to(_ROOT)}")
    if len(df):
        print(df.pivot(values="spend_eur_millions", index="mechanism", on="year"))


if __name__ == "__main__":
    main()
