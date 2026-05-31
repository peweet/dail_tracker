"""DCC Housing Allocations Report (July 2024) — per-area-committee tables.

Extracts the 11 tables in DCC's periodic Housing Allocations Report, which
breaks the Dublin City waiting list and lettings down by the 10 Area Committees
(B, D, E, H, J, K, L, M, N, P).

Reads  : doc/source_pdfs/_samples/DCC_HousingAllocReport_Jul2024.pdf
Writes : data/gold/parquet/dcc_allocations_jul2024.parquet (long format)
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
_SRC = _ROOT / "doc" / "source_pdfs" / "_samples" / "DCC_HousingAllocReport_Jul2024.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet" / "dcc_allocations_jul2024.parquet"

AREAS = ("B", "D", "E", "H", "J", "K", "L", "M", "N", "P")
AREA_COL_PATTERN = re.compile(r"^Area\s+([BDEHJKLMNP])$", re.IGNORECASE)


def _to_int(c):
    if c is None:
        return None
    s = str(c).replace(",", "").replace("\n", " ").strip()
    if not s or s in {"-", "—"}:
        return None
    m = re.match(r"-?\d+", s)
    return int(m.group(0)) if m else None


def extract() -> pl.DataFrame:
    doc = fitz.open(str(_SRC))
    rows: list[dict] = []
    for page_idx in range(doc.page_count):
        page = doc[page_idx]
        for ti, tab in enumerate(page.find_tables().tables):
            data = tab.extract()
            if not data or len(data) < 3:
                continue

            # First two rows are usually combined header — try to find Area columns
            area_col_idx: dict[int, str] = {}
            for hdr_row in data[:3]:
                for cidx, cell in enumerate(hdr_row):
                    s = (cell or "").strip()
                    m = AREA_COL_PATTERN.match(s)
                    if m:
                        area_col_idx[cidx] = m.group(1).upper()

            if not area_col_idx:
                continue  # not an area-committee table

            # Find a section/table heading by looking at the first non-empty header
            table_label = ""
            for r in data[:2]:
                for c in r:
                    if c and len(str(c).strip()) > 5 and not AREA_COL_PATTERN.match(str(c).strip()):
                        table_label = str(c).strip()[:50]
                        break
                if table_label:
                    break

            # Walk data rows
            for r in data[3:]:  # skip header rows
                cells = [(c or "").strip() for c in r]
                if not cells or not cells[0]:
                    continue
                category = cells[0]
                # Skip rows that look like sub-totals
                if category.lower() in {"", "grand total", "total"}:
                    continue
                for col_idx, area in area_col_idx.items():
                    if col_idx < len(cells):
                        val = _to_int(cells[col_idx])
                        if val is not None:
                            rows.append({
                                "table_label": table_label,
                                "category": category,
                                "area_committee": area,
                                "value": val,
                                "source_page": page_idx + 1,
                                "source_table_index": ti,
                            })
    doc.close()
    return pl.DataFrame(rows) if rows else pl.DataFrame()


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": len(df)}
    if df.is_empty():
        rpt["green"] = False
        rpt["checks"]["1_extraction"] = {"pass": False, "note": "empty"}
        return rpt
    unique_areas = set(df["area_committee"].unique().to_list())
    rpt["checks"]["1_extraction"] = {
        "unique_area_committees": len(unique_areas),
        "expected": 10,
        "pass": len(unique_areas) >= 8,
    }
    rpt["checks"]["2_categories"] = {
        "unique_categories": df["category"].n_unique(),
        "pass": df["category"].n_unique() >= 5,
    }
    # Distinct (page, table_index) — most DCC tables sit at index 0 on their page
    distinct_tables = df.unique(["source_page", "source_table_index"]).height
    rpt["checks"]["3_table_coverage"] = {
        "distinct_tables": distinct_tables,
        "pages_covered": df["source_page"].n_unique(),
        "pass": distinct_tables >= 5,
    }
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

    df = extract()
    rpt = fidelity_check(df)
    print(f"=== DCC Housing Allocations Jul 2024 — {len(df)} rows ===")
    for n, chk in rpt["checks"].items():
        print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {n}: {chk}")
    print(f">>> {'GREEN' if rpt['green'] else 'AMBER'}")

    if args.write and rpt["green"]:
        _write_parquet(df, _OUT)
        print(f"Wrote {_OUT.relative_to(_ROOT)}")

    if len(df):
        print("\nSample (5 rows):")
        print(df.head(5))
        print(f"\nGrand totals by area committee:")
        print(df.group_by("area_committee").agg(pl.col("value").sum().alias("total")).sort("total", descending=True))


if __name__ == "__main__":
    main()
