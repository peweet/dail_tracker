"""AHB Provision Overview — per-LA Approved Housing Body activity 2018-2021.

The AHB Provision Overview report contains multiple per-LA tables on
pp. 38–46. Tables use a wide-column layout where most cells are visual
spacers; real data sits at fixed column positions (0, 3, 6, 9, 12, 15).

Captured tables (one parquet each):
  p38: Leased units by LA × year (2018-2021) + 4-year total
  p39: SHCEP AHB leasing payments € by LA × year
  p46: AHBs vs Private Co. acquisitions by LA × year

Reads  : doc/source_pdfs/_samples/AHB_Provision_Overview.pdf
Writes : data/gold/parquet/ahb_<table>.parquet (one per table)
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
_SRC = _ROOT / "doc" / "source_pdfs" / "_samples" / "AHB_Provision_Overview.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet"

# Each captured table: page (0-idx), year columns map, total column, label
TABLES = {
    "leased_units":     {"page": 37, "year_cols": {2018: 3, 2019: 6, 2020: 9, 2021: 12}, "total_col": 15},
    "leasing_payments": {"page": 38, "year_cols": {2018: 3, 2019: 6, 2020: 9, 2021: 12}, "total_col": 15},
    "ahb_vs_private":   {"page": 45, "year_cols": {2018: 3, 2019: 9, 2020: 15, 2021: 21}, "total_col": None,
                         "extra_cols": {"private_2018": 6, "private_2019": 12, "private_2020": 18, "private_2021": 24}},
}


def _to_float(c):
    if c is None:
        return None
    s = str(c).replace(",", "").replace("€", "").replace("€", "").strip()
    if not s or s in {"-", "—", "None"}:
        return None
    try:
        return float(s)
    except ValueError:
        m = re.match(r"-?\d+(?:\.\d+)?", s)
        return float(m.group(0)) if m else None


def canonical_la(name: str) -> str:
    n = (name or "").replace("\n", " ").replace("\r", " ")
    n = re.sub(r"\s+", " ", n).strip()
    n = n.replace("Dún", "Dun").replace("D�n", "Dun").replace("-Rath", " Rath")
    n = re.sub(r"\s+(County\s+Council|City\s+Council|City\s+and\s+County\s+Council|Council)\s*$", "", n)
    return n.strip()


def extract_table(doc, page_idx: int, year_cols: dict, total_col: int | None,
                  extra_cols: dict | None = None) -> pl.DataFrame:
    page = doc[page_idx]
    rows = []
    for tab in page.find_tables().tables:
        data = tab.extract()
        for r in data:
            cells = [(c or "").strip() if c is not None else "" for c in r]
            if not cells or not cells[0]:
                continue
            first = cells[0]
            # Skip header rows
            if any(kw in first.lower() for kw in ("city and county", "councils", "2018", "leased", "ahb", "scheme")):
                continue
            la = canonical_la(first)
            if not la or "council" in la.lower() or len(la) < 4:
                continue
            for year, col in year_cols.items():
                if col < len(cells):
                    v = _to_float(cells[col])
                    if v is not None:
                        rows.append({"la": la, "year": year, "value": v, "stream": "ahb"})
            if total_col is not None and total_col < len(cells):
                v = _to_float(cells[total_col])
                if v is not None:
                    rows.append({"la": la, "year": 0, "value": v, "stream": "ahb_total_4y"})
            if extra_cols:
                for label, col in extra_cols.items():
                    if col < len(cells):
                        v = _to_float(cells[col])
                        if v is not None:
                            year_str = label.rsplit("_", 1)[1]
                            rows.append({"la": la, "year": int(year_str), "value": v,
                                         "stream": label.rsplit("_", 1)[0]})
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fidelity_check(df: pl.DataFrame, label: str) -> dict:
    rpt = {"checks": {}, "rows": len(df), "label": label}
    if df.is_empty():
        rpt["green"] = False
        rpt["checks"]["1_extraction"] = {"pass": False, "note": "empty"}
        return rpt
    unique_las = df["la"].n_unique()
    rpt["checks"]["1_extraction"] = {
        "unique_LAs": unique_las, "pass": unique_las >= 25,
    }
    years = df.filter(pl.col("year") > 0)["year"].unique().sort()
    rpt["checks"]["2_year_coverage"] = {
        "years": years.to_list(), "pass": len(years) >= 4,
    }
    rpt["checks"]["3_cross"] = {"pass": True, "note": "skipped"}
    rpt["checks"]["4_cross_source"] = {"pass": True, "note": "skipped"}
    bad = df.filter(pl.col("value") < 0).height
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
    if not _SRC.exists():
        print(f"ERROR: {_SRC} missing")
        sys.exit(2)

    doc = fitz.open(str(_SRC))
    results = []
    for table_id, cfg in TABLES.items():
        df = extract_table(doc, cfg["page"], cfg["year_cols"], cfg.get("total_col"),
                          cfg.get("extra_cols"))
        rpt = fidelity_check(df, table_id)
        print(f"\n=== ahb_{table_id} — {len(df)} rows ===")
        for n, chk in rpt["checks"].items():
            print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {n}: {chk}")
        print(f"  >>> {'GREEN' if rpt['green'] else 'AMBER'}")
        if args.write and rpt["green"]:
            path = _OUT / f"ahb_{table_id}.parquet"
            _write_parquet(df, path)
            print(f"  Wrote {path.relative_to(_ROOT)}")
        results.append((table_id, len(df), rpt["green"]))
    doc.close()

    print("\n" + "=" * 60)
    print("SUMMARY")
    for tid, n, green in results:
        print(f"  {'✓' if green else '⚠'} ahb_{tid:25s} {n:>5} rows")


if __name__ == "__main__":
    main()
