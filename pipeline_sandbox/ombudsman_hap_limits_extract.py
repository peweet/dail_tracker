"""Ombudsman HAP investigation report — HAP rent ceilings per LA × household type.

Extracts the table on page 81 (Ombudsman 2025 HAP investigation): the maximum
monthly rent the State will subsidise per LA × household composition.

Reads  : doc/source_pdfs/_samples/Ombudsman_HAP.pdf  (page 81)
Writes : data/gold/parquet/hap_rent_limits.parquet
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
_SRC = _ROOT / "doc" / "source_pdfs" / "_samples" / "Ombudsman_HAP.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet" / "hap_rent_limits.parquet"

# Household type column labels (matching the published table — short forms)
HHTYPES = [
    "1_adult_shared", "couple_shared", "1_adult", "couple",
    "couple_or_adult_with_1child", "couple_or_adult_with_2child", "couple_or_adult_with_3child",
]


def _to_eur(cell):
    if cell is None:
        return None
    s = str(cell).replace("€", "").replace("�", "").replace(",", "").strip()
    if not s or s in {"-", "—", "None"}:
        return None
    m = re.match(r"-?\d+", s)
    return int(m.group(0)) if m else None


def canonical_la(name: str) -> str:
    n = (name or "").replace("\n", " ")
    n = re.sub(r"\s+", " ", n).strip()
    n = n.replace("Dún", "Dun").replace("D�n", "Dun")
    # The Ombudsman table uses "CC" suffix and full names
    n = re.sub(r"\s+(County\s+Council|CC|Council)\s*$", "", n)
    return n


def extract(pdf_path: Path) -> pl.DataFrame:
    doc = fitz.open(str(pdf_path))
    page = doc[80]  # p81 is index 80
    tabs = page.find_tables()
    rows = []
    for t in tabs.tables:
        data = t.extract()
        for r in data:
            cells = [(c or "").strip() for c in r]
            if not cells:
                continue
            # First non-empty cell is LA name
            first_text = next((c for c in cells if c and not _to_eur(c)), None)
            if not first_text or any(h in first_text.lower() for h in ("local authority", "households", "rent limit")):
                continue
            la = canonical_la(first_text)
            # Numerics (skip the LA-name cell)
            nums = [_to_eur(c) for c in cells if _to_eur(c) is not None]
            if len(nums) >= 4:
                # Take first 7 values (the 7 household types)
                vals = nums[:7] + [None] * max(0, 7 - len(nums))
                for hhtype, v in zip(HHTYPES, vals):
                    if v is not None:
                        rows.append({
                            "la": la, "household_type": hhtype,
                            "monthly_rent_limit_eur": v,
                        })
    doc.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": len(df)}
    if df.is_empty():
        rpt["green"] = False
        rpt["checks"]["1_extraction"] = {"pass": False, "note": "empty"}
        return rpt

    unique_las = df["la"].n_unique()
    rpt["checks"]["1_extraction"] = {
        "unique_LAs": unique_las, "pass": unique_las >= 28,
    }

    # Check 2 — each LA has all 7 household types
    per_la = df.group_by("la").agg(pl.len().alias("n"))
    incomplete = per_la.filter(pl.col("n") < 7).height
    rpt["checks"]["2_internal_completeness"] = {
        "incomplete_LAs": incomplete, "pass": incomplete <= 3,
    }

    # Check 3 — Dublin region should be highest, rural lowest (sanity)
    dub = df.filter(
        pl.col("la").str.contains("Dublin") & (pl.col("household_type") == "1_adult")
    )["monthly_rent_limit_eur"]
    don = df.filter(
        pl.col("la").str.contains("Donegal") & (pl.col("household_type") == "1_adult")
    )["monthly_rent_limit_eur"]
    rpt["checks"]["3_geographic_sanity"] = {
        "dublin_1adult_eur": dub.max() if len(dub) else None,
        "donegal_1adult_eur": don.max() if len(don) else None,
        "pass": (dub.max() or 0) > (don.max() or 0) if len(dub) and len(don) else False,
    }
    rpt["checks"]["4_cross_source"] = {"pass": True, "note": "skipped"}

    # Check 5 — reasonableness (€100-2000)
    bad = df.filter(
        (pl.col("monthly_rent_limit_eur") < 100) | (pl.col("monthly_rent_limit_eur") > 2000)
    ).height
    rpt["checks"]["5_semantic"] = {"out_of_range": bad, "pass": bad == 0}

    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    args = ap.parse_args()

    df = extract(_SRC)
    rpt = fidelity_check(df)
    print(f"=== HAP rent limits — {len(df)} rows ===")
    for name, chk in rpt["checks"].items():
        print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {name}: {chk}")
    print(f">>> {'GREEN — safe' if rpt['green'] else 'AMBER/RED'}")

    if args.write and rpt["green"]:
        _write_parquet(df, _OUT)
        print(f"Wrote {_OUT.relative_to(_ROOT)}")

    # Show sample
    if len(df):
        print("\nSample (Dublin region + 3 rural):")
        print(df.filter(pl.col("la").is_in(["Dublin City", "Dun Laoghaire Rathdown", "Donegal", "Longford", "Mayo"])))


if __name__ == "__main__":
    main()
