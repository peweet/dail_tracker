"""NOAC M2 — Revenue Collection Rates (council enforcement performance).

Extracts the three per-LA M2 collection tables from the NOAC 2024 report:
  M2(A) Commercial Rates    — % of commercial rates collected
  M2(B) Rent & Annuities    — % of (social-housing) rent + annuities collected
  M2(C) Housing Loans       — % of housing-loan repayments collected
Each is a 31-LA x 5-year matrix (2020-2024). Output: one tidy wide parquet
  noac_m2_collection_wide.parquet : la, year, commercial_rates_collection_pct,
                                    rent_annuities_collection_pct, housing_loans_collection_pct

Tables are located by their "M2 (X)" caption (robust to page drift); the five
value columns are positional 2020->2024 (the report renders them left-to-right
oldest-first, headers are clipped to "Collection level o…"). Collection can exceed
100% in a year (arrears recovery), so the sanity gate allows 0-130.

Reads  : doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf  (repo root via parents[2])
Writes : data/gold/parquet/noac_m2_collection_wide.parquet  (with --write [--gold])

NOTE: sandbox/experimental — sandbox->vet->promote. Default run is dry (no write).
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

_ROOT = Path(__file__).resolve().parents[2]
_SRC = _ROOT / "doc" / "source_pdfs" / "NOAC_LA_PerfInd_2024.pdf"
_OUT = _ROOT / "data" / "gold" / "parquet"

_YEARS = [2020, 2021, 2022, 2023, 2024]  # positional column -> year (oldest first)

# Each M2 sub-table: caption token + the output metric column it feeds.
_TABLES = {
    "A": {"metric": "commercial_rates_collection_pct", "needle": "Commercial Rates"},
    "B": {"metric": "rent_annuities_collection_pct", "needle": "Rent"},
    "C": {"metric": "housing_loans_collection_pct", "needle": "Housing Loans"},
}

EXPECTED_LAS = {
    "Carlow", "Cavan", "Clare", "Cork City", "Cork County", "Donegal",
    "Dublin City", "DLR", "Dun Laoghaire", "Fingal", "Galway City",
    "Galway County", "Kerry", "Kildare", "Kilkenny", "Laois", "Leitrim",
    "Limerick", "Longford", "Louth", "Mayo", "Meath", "Monaghan", "Offaly",
    "Roscommon", "Sligo", "South Dublin", "Tipperary", "Waterford",
    "Westmeath", "Wexford", "Wicklow",
}


def canonical_la(name: str) -> str:
    n = re.sub(r"\s+", " ", (name or "").replace("\n", " ")).strip()
    n = n.replace("Dún", "Dun").replace("D�n", "Dun").replace("DLR", "Dun Laoghaire-Rathdown")
    n = re.sub(r"\s+(County\s+Council|City\s+Council|City\s+and\s+County\s+Council|Council)\s*$", "", n)
    return n.strip()


def _is_la_row(text: str) -> bool:
    t = (text or "").replace("\n", " ").strip().lower()
    t = t.replace("ú", "u").replace("�", "u")  # fada/mojibake -> plain (Dún Laoghaire)
    return any(la.lower() in t for la in EXPECTED_LAS)


def _to_float(c):
    s = str(c or "").replace(",", "").replace("%", "").strip()
    if not s or s in {"-", "—", "n/a", "N/A"}:
        return None
    m = re.match(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


def _find_table(doc, needle: str):
    """Locate the M2 sub-table whose page caption matches the needle; return its
    per-LA rows as {canonical_la: [5 floats]}."""
    cap = re.compile(r"M2\s*\(([ABC])\)", re.I)
    for pi in range(doc.page_count):
        txt = doc[pi].get_text()
        if not cap.search(txt) or needle.lower() not in txt.lower() or "ollection" not in txt:
            continue
        for tab in doc[pi].find_tables().tables:
            data = tab.extract()
            if not data or len(data) < 10:
                continue
            first_col = [(r[0] or "").strip() for r in data]
            if sum(1 for c in first_col if _is_la_row(c)) < 25:
                continue
            rows: dict[str, list] = {}
            for r in data:
                cells = [(c or "").strip() for c in r]
                if not _is_la_row(cells[0]):
                    continue
                vals = [_to_float(c) for c in cells[1:6]]
                rows[canonical_la(cells[0])] = vals
            if len(rows) >= 25:
                return rows
    return {}


def extract() -> pl.DataFrame:
    doc = fitz.open(str(_SRC))
    per_metric = {k: _find_table(doc, v["needle"]) for k, v in _TABLES.items()}
    doc.close()
    las = sorted(set().union(*[set(d) for d in per_metric.values() if d]))
    records = []
    for la in las:
        for ci, yr in enumerate(_YEARS):
            rec = {"la": la, "year": yr}
            for k, cfg in _TABLES.items():
                vals = per_metric.get(k, {}).get(la)
                rec[cfg["metric"]] = vals[ci] if vals and ci < len(vals) else None
            records.append(rec)
    return pl.DataFrame(records) if records else pl.DataFrame()


def fidelity_check(df: pl.DataFrame) -> dict:
    rpt = {"checks": {}, "rows": df.height}
    if df.is_empty():
        rpt["green"] = False
        return rpt
    n_la = df["la"].n_unique()
    rpt["checks"]["1_la_coverage"] = {"unique_LAs": n_la, "pass": n_la >= 30}
    years = sorted(df["year"].unique().to_list())
    rpt["checks"]["2_years"] = {"years": years, "pass": years == _YEARS}
    metric_cols = [c["metric"] for c in _TABLES.values()]
    bad = 0
    for c in metric_cols:
        bad += df.filter((pl.col(c) < 0) | (pl.col(c) > 130)).height
    rpt["checks"]["3_range"] = {"out_of_range": bad, "pass": bad == 0}
    # each metric should be populated for the latest year across the LAs
    latest = df.filter(pl.col("year") == 2024)
    cov = {c: latest.filter(pl.col(c).is_not_null()).height for c in metric_cols}
    rpt["checks"]["4_latest_coverage"] = {"2024_non_null": cov, "pass": all(v >= 30 for v in cov.values())}
    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def _write_parquet(df: pl.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path, compression="zstd", compression_level=3, statistics=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--gold", action="store_true", help="write to data/gold/parquet (else _noac_eval)")
    args = ap.parse_args()
    if not _SRC.exists():
        print(f"ERROR: source missing: {_SRC}")
        sys.exit(1)

    df = extract()
    rpt = fidelity_check(df)
    print(f"=== noac_m2_collection_wide — {df.height} rows ===")
    for n, chk in rpt["checks"].items():
        print(f"  [{'GREEN' if chk.get('pass') else 'FAIL'}] {n}: {chk}")
    print(f"  >>> {'GREEN' if rpt['green'] else 'AMBER'}")
    if not df.is_empty():
        with pl.Config(tbl_rows=4, tbl_width_chars=140):
            print(df.filter(pl.col("year") == 2024).head(4))
    if args.write and rpt["green"]:
        out = (_ROOT / "data" / "gold" / "parquet") if args.gold else (_OUT / "_noac_eval")
        path = out / "noac_m2_collection_wide.parquet"
        _write_parquet(df, path)
        print(f"  Wrote {path.relative_to(_ROOT)}")
    elif not args.write:
        print("\n(dry-run — pass --write [--gold] to persist)")


if __name__ == "__main__":
    main()
