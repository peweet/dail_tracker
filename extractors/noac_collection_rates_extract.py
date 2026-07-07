"""NOAC M2 — Revenue Collection Rates per local authority (gold).

Extracts the three per-LA M2 collection tables from the NOAC Performance Indicator
Report 2024 PDF:
  M2(A) Commercial Rates · M2(B) Rent & Annuities · M2(C) Housing Loans
Each is a 31-LA × 5-year matrix (2020–2024). Output: one tidy wide parquet powering
v_la_collection_rates ("Who runs your county") + v_constituency_council_housing_performance.

Tables are located by their "M2 (X)" caption (robust to page drift); the five value
columns are positional 2020→2024 (rendered left-to-right oldest-first; headers are
clipped). Collection can exceed 100% in a year (arrears recovery), so the range gate
allows 0–130.

Reads  : doc/source_pdfs/NOAC_LA_PerfInd_2024.pdf
Writes : data/gold/parquet/noac_m2_collection_wide.parquet  (atomic, via services.parquet_io)

Promoted from pipeline_sandbox/housing/ (2026-06-20): routes through save_parquet,
writes by default, gated on the fidelity check. Annual source (NOAC publishes ~Sept);
when a new year's report lands, drop the PDF in and re-run. The freshness watch
(tools/check_freshness.py → noac_council_performance) flags staleness.

⚠️ NOAC ships PDF-only and SOME indicators are chart images, but the M2 tables ARE
real tables that Camelot extracts cleanly — this is the proof. (See doc/LOCAL_AUTHORITY_ACCOUNTABILITY.md.)

Run:
  python extractors/noac_collection_rates_extract.py            # parse + write gold
  python extractors/noac_collection_rates_extract.py --dry-run  # parse + report, no write
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.logging_setup import setup_standalone_logging  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

LOG = logging.getLogger("noac_collection_rates")
_SRC = ROOT / "doc" / "source_pdfs" / "NOAC_LA_PerfInd_2024.pdf"
_OUT = ROOT / "data" / "gold" / "parquet" / "noac_m2_collection_wide.parquet"

_YEARS = [2020, 2021, 2022, 2023, 2024]  # positional column -> year (oldest first)

# Each M2 sub-table: caption token + the output metric column it feeds.
_TABLES = {
    "A": {"metric": "commercial_rates_collection_pct", "needle": "Commercial Rates"},
    "B": {"metric": "rent_annuities_collection_pct", "needle": "Rent"},
    "C": {"metric": "housing_loans_collection_pct", "needle": "Housing Loans"},
}

EXPECTED_LAS = {
    "Carlow",
    "Cavan",
    "Clare",
    "Cork City",
    "Cork County",
    "Donegal",
    "Dublin City",
    "DLR",
    "Dun Laoghaire",
    "Fingal",
    "Galway City",
    "Galway County",
    "Kerry",
    "Kildare",
    "Kilkenny",
    "Laois",
    "Leitrim",
    "Limerick",
    "Longford",
    "Louth",
    "Mayo",
    "Meath",
    "Monaghan",
    "Offaly",
    "Roscommon",
    "Sligo",
    "South Dublin",
    "Tipperary",
    "Waterford",
    "Westmeath",
    "Wexford",
    "Wicklow",
}


def canonical_la(name: str) -> str:
    n = re.sub(r"\s+", " ", (name or "").replace("\n", " ")).strip()
    n = n.replace("Dún", "Dun").replace("D�n", "Dun").replace("DLR", "Dun Laoghaire-Rathdown")
    n = re.sub(r"\s+(County\s+Council|City\s+Council|City\s+and\s+County\s+Council|Council)\s*$", "", n)
    return n.strip()


def _is_la_row(text: str) -> bool:
    t = (text or "").replace("\n", " ").strip().lower()
    t = t.replace("ú", "u").replace("�", "u")  # fada/mojibake -> plain
    return any(la.lower() in t for la in EXPECTED_LAS)


def _to_float(c):
    s = str(c or "").replace(",", "").replace("%", "").strip()
    if not s or s in {"-", "—", "n/a", "N/A"}:
        return None
    m = re.match(r"-?\d+(?:\.\d+)?", s)
    return float(m.group(0)) if m else None


def _find_table(doc, needle: str) -> dict:
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
                rows[canonical_la(cells[0])] = [_to_float(c) for c in cells[1:6]]
            if len(rows) >= 25:
                return rows
    return {}


def extract() -> pl.DataFrame:
    doc = fitz.open(str(_SRC))
    per_metric = {k: _find_table(doc, v["needle"]) for k, v in _TABLES.items()}
    doc.close()
    las = sorted(set().union(*[set(d) for d in per_metric.values() if d]) or set())
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
    bad = sum(df.filter((pl.col(c) < 0) | (pl.col(c) > 130)).height for c in metric_cols)
    rpt["checks"]["3_range"] = {"out_of_range": bad, "pass": bad == 0}
    latest = df.filter(pl.col("year") == 2024)
    cov = {c: latest.filter(pl.col(c).is_not_null()).height for c in metric_cols}
    rpt["checks"]["4_latest_coverage"] = {"2024_non_null": cov, "pass": all(v >= 30 for v in cov.values())}
    rpt["green"] = all(c.get("pass", False) for c in rpt["checks"].values())
    return rpt


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="parse + report, do NOT write")
    args = ap.parse_args()
    setup_standalone_logging("noac_collection_rates")

    if not _SRC.exists():
        LOG.error("source missing: %s", _SRC)
        sys.exit(1)

    df = extract()
    rpt = fidelity_check(df)
    LOG.info("noac_m2_collection_wide — %d rows", df.height)
    for n, chk in rpt["checks"].items():
        LOG.info("  [%s] %s: %s", "GREEN" if chk.get("pass") else "FAIL", n, chk)

    if args.dry_run:
        LOG.info("dry-run: not writing (parse %s)", "GREEN" if rpt["green"] else "AMBER")
        return
    if not rpt["green"]:
        LOG.error("fidelity AMBER — refusing to overwrite %s", _OUT.name)
        sys.exit(2)
    save_parquet(df, _OUT, min_rows=150)
    LOG.info("wrote %s (%d rows)", _OUT.relative_to(ROOT), df.height)


if __name__ == "__main__":
    main()
