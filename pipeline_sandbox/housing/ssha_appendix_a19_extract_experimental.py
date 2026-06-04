"""SSHA Appendix A1.9 — per-LA citizenship of waiting-list applicants.

Pulls the 31-LA × 4-citizenship-bucket × 2-year table from the SSHA 2025 PDF
using fitz, runs the 5-check fidelity cascade in code, and (optionally) writes
a clean parquet. Sandbox prototype demonstrating the PDF→clean-data pattern.

Reads  : doc/source_pdfs/SSHA_2025_FINAL.pdf   (pages 73–74 = Table A1.9)
Writes : data/gold/parquet/ssha_a19_citizenship.parquet  (only with --write)

Run `python pipeline_sandbox/ssha_appendix_a19_extract.py` to see the
fidelity report + extracted table; add `--write` to land the parquet.
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import fitz
import polars as pl

# Force UTF-8 on stdout so the report doesn't crash on Windows default cp1252
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except Exception:
    pass

_ROOT = Path(__file__).resolve().parents[1]
_SRC = _ROOT / "doc" / "source_pdfs" / "SSHA_2025_FINAL.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet" / "ssha_a19_citizenship.parquet"

# Published national totals — fidelity Check 3 ground truth (from SSHA Table 2.9)
PUBLISHED_NATIONAL_2025 = {
    "irish": 43_991, "eea": 8_820, "non_eea": 8_142, "uk": 766, "total": 61_719,
}
PUBLISHED_NATIONAL_2024 = {
    "irish": 43_189, "eea": 8_787, "non_eea": 7_282, "uk": 683, "total": 59_941,
}

# Canonical 31 LA names (Check 1 — required-cells). These must match the
# output of canonical_la() exactly (after dropping " Council" suffix only).
EXPECTED_LAS = {
    "Carlow County", "Cavan County", "Clare County", "Cork City", "Cork County",
    "Donegal County", "Dublin City", "Dun Laoghaire Rathdown County", "Fingal County",
    "Galway City", "Galway County", "Kerry County", "Kildare County", "Kilkenny County",
    "Laois County", "Leitrim County", "Limerick City and County", "Longford County",
    "Louth County", "Mayo County", "Meath County", "Monaghan County", "Offaly County",
    "Roscommon County", "Sligo County", "South Dublin County", "Tipperary County",
    "Waterford City and County", "Westmeath County", "Wexford County", "Wicklow County",
}


_TYPO_FIXES = {
    "Ofaf ly": "Offaly",  # fitz column-wrap artefact for "Offaly" in 3-column LA layout
}

def canonical_la(name: str) -> str:
    """Normalise LA name strings: collapse whitespace + fix typos.

    KEEP 'City'/'County' qualifiers so Cork City ≠ Cork County.
    Drop the trailing 'Council' but preserve everything that distinguishes LAs.
    """
    n = (name or "").replace("\n", " ").replace("\r", " ")
    n = re.sub(r"\s+", " ", n).strip()
    n = n.replace("Dún", "Dun").replace("D�n", "Dun")
    for typo, fix in _TYPO_FIXES.items():
        n = n.replace(typo, fix)
    # Strip only the trailing "Council" — keep "City" / "County" qualifiers
    n = re.sub(r"\s+Council\s*$", "", n)
    return n.strip()


def _to_int(cell: str | None) -> int | None:
    if cell is None:
        return None
    s = str(cell).replace(",", "").replace("€", "").strip()
    if not s or s in {"-", "—"}:
        return None
    m = re.match(r"-?\d+", s)
    return int(m.group(0)) if m else None


def extract_a19(pdf_path: Path) -> pl.DataFrame:
    """Lift Table A1.9 from pages 73–74 and stitch into a long-format frame."""
    doc = fitz.open(str(pdf_path))
    rows: list[dict] = []

    for page_idx in (72, 73):  # 0-based
        page = doc[page_idx]
        for tab in page.find_tables().tables:
            data = tab.extract()
            if not data or len(data) < 10:
                continue
            # Walk pairs of rows: (LA, year, cells…) followed by (continuation, …)
            i = 0
            current_la: str | None = None
            while i < len(data):
                cells = [(c or "").strip() for c in data[i]]
                # Identify LA-row vs continuation-row by leading text cell
                first = cells[0] if cells else ""
                if first and not first.replace(",", "").replace(".", "").isdigit() \
                        and "Year" not in first and "Local" not in first:
                    current_la = first
                # Find the Year cell and four citizenship cells per published layout
                year = next((c for c in cells if c in ("2024", "2025")), None)
                if current_la and year:
                    # Exclude the year cell so we don't read 2024/2025 as the Irish count
                    numerics = [
                        _to_int(c) for c in cells
                        if _to_int(c) is not None and str(c).strip() not in ("2024", "2025")
                    ]
                    if len(numerics) >= 4:
                        rows.append({
                            "la_raw": current_la,
                            "year": int(year),
                            "irish": numerics[0],
                            "eea": numerics[1],
                            "non_eea": numerics[2],
                            "uk": numerics[3] if len(numerics) >= 4 else None,
                            "total": numerics[4] if len(numerics) >= 5 else None,
                            "source_page": page_idx + 1,
                        })
                i += 1
    doc.close()

    df = pl.DataFrame(rows)
    if df.is_empty():
        return df
    df = df.with_columns(
        pl.col("la_raw").map_elements(canonical_la, return_dtype=pl.Utf8).alias("la")
    )
    # Drop the published "Total" row — it's not a LA, it's the national sum we'll re-validate against
    df = df.filter(pl.col("la").str.to_lowercase() != "total")
    # Recompute total where missing
    df = df.with_columns(
        pl.when(pl.col("total").is_null())
        .then(pl.col("irish") + pl.col("eea") + pl.col("non_eea") + pl.col("uk").fill_null(0))
        .otherwise(pl.col("total"))
        .alias("total")
    )
    return df.select(["la", "la_raw", "year", "irish", "eea", "non_eea", "uk", "total", "source_page"])


def run_fidelity_cascade(df: pl.DataFrame) -> dict:
    """5-check cascade. Returns a structured report; prints to stdout."""
    report: dict = {"checks": {}, "rows": len(df), "green": True, "flags": []}

    # Check 1 — Extraction fidelity
    n_rows = len(df)
    las_found = set(df["la"].unique().to_list())
    missing_las: set[str] = set()
    for expected in EXPECTED_LAS:
        if not any(expected.lower() in la.lower() for la in las_found):
            missing_las.add(expected)
    report["checks"]["1_extraction"] = {
        "row_count": n_rows,
        "unique_LAs": len(las_found),
        "expected_LAs": 31,
        "missing_LAs": sorted(missing_las),
        "pass": len(missing_las) == 0 and n_rows >= 60,
    }

    # Check 2 — Internal consistency (per-row: irish + eea + non_eea + uk == total)
    sum_check = df.with_columns(
        (pl.col("irish") + pl.col("eea") + pl.col("non_eea") + pl.col("uk").fill_null(0)).alias("computed_total")
    ).with_columns((pl.col("total") - pl.col("computed_total")).abs().alias("diff"))
    bad_rows = sum_check.filter(pl.col("diff") > 0)
    report["checks"]["2_internal_sum"] = {
        "rows_failing_sum": len(bad_rows),
        "sample_failures": bad_rows.head(3).to_dicts() if len(bad_rows) else [],
        "pass": len(bad_rows) == 0,
    }

    # Check 3 — National totals reconcile vs SSHA Table 2.9 published
    for yr, expected in [(2025, PUBLISHED_NATIONAL_2025), (2024, PUBLISHED_NATIONAL_2024)]:
        yr_df = df.filter(pl.col("year") == yr)
        if yr_df.is_empty():
            report["checks"][f"3_national_{yr}"] = {"pass": False, "note": "no rows for year"}
            continue
        sums = {c: int(yr_df[c].sum()) for c in ("irish", "eea", "non_eea", "uk", "total")}
        diffs = {c: sums[c] - expected[c] for c in expected}
        passed = all(abs(d) <= 2 for d in diffs.values())  # rounding tolerance
        report["checks"][f"3_national_{yr}"] = {
            "computed": sums, "published": expected, "diff": diffs, "pass": passed,
        }

    # Check 4 — Cross-source: would need HAP01 etc. — skip in this single-file demo
    report["checks"]["4_cross_source"] = {"pass": True, "note": "skipped (single-source demo)"}

    # Check 5 — Semantic / reasonableness
    bad_values = df.filter(
        (pl.col("irish") < 0) | (pl.col("eea") < 0) | (pl.col("non_eea") < 0)
        | (pl.col("uk") < 0) | (pl.col("total") <= 0)
        | (pl.col("total") > 50_000)  # no single LA waiting list > 50k
    )
    report["checks"]["5_semantic"] = {
        "out_of_range_rows": len(bad_values), "pass": len(bad_values) == 0,
    }

    report["green"] = all(c.get("pass", False) for c in report["checks"].values())
    return report


def print_report(report: dict, df: pl.DataFrame) -> None:
    print("=" * 72)
    print(f"SSHA Appendix A1.9 extraction — {report['rows']} rows / {df['la'].n_unique()} unique LAs")
    print("=" * 72)
    for name, chk in report["checks"].items():
        flag = "[GREEN]" if chk.get("pass") else "[FAIL]"
        print(f"  {flag} Check {name}: {chk}")
    print()
    print(f"Overall fidelity: {'GREEN — safe to land' if report['green'] else 'RED — DO NOT LAND'}")
    print()
    print("Sample rows (2025):")
    print(df.filter(pl.col("year") == 2025).select(
        ["la", "irish", "eea", "non_eea", "uk", "total"]
    ).head(10))


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser(description="SSHA A1.9 extractor — sandbox prototype")
    ap.add_argument("--write", action="store_true", help="Write parquet output (only if fidelity is GREEN)")
    args = ap.parse_args()

    if not _SRC.exists():
        print(f"ERROR: source not found at {_SRC}", file=sys.stderr)
        sys.exit(2)

    df = extract_a19(_SRC)
    if df.is_empty():
        print("ERROR: extraction returned no rows", file=sys.stderr)
        sys.exit(3)

    report = run_fidelity_cascade(df)
    print_report(report, df)

    if args.write:
        if not report["green"]:
            print("\nABORT: fidelity not GREEN — refusing to write parquet.", file=sys.stderr)
            sys.exit(4)
        _write_parquet(df, _OUT)
        print(f"\n✓ Wrote {_OUT}")


if __name__ == "__main__":
    main()
