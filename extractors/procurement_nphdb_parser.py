"""PHASE 4 (PRE-ETL, sandbox): bespoke parser for the National Paediatric Hospital
Development Board (NPHDB) quarterly PO listing -> public_payments_fact schema.

WHY BESPOKE (not the generic config-driven reader): the NPHDB PDF is published with a
90-degree page rotation and a 3-column (Supplier / Net Amount / Description) layout whose
description cells wrap over many lines. The generic header-anchored, word-geometry reader in
procurement_public_body_extract.py finds NO header on the rotated page (cols=[] -> 0 rows).
PyMuPDF get_text("text") DErotates correctly and yields a clean reading-order stream of
  <supplier> / <net amount> / <description...>   triples
so this parser keys on the money line as the record anchor: the line immediately BEFORE a
money line is the supplier, and the line(s) AFTER it (up to the next record's supplier) are
the description. Same family as procurement_hse_tusla_parser.py — a per-publisher reader the
generic one can't handle; it emits THIS repo's public_payments_fact schema so the layers
union at promotion time.

NPHDB is the publisher that holds the New Children's Hospital construction spend (incl. the
BAM Building conciliator/adjudicator award rows that were absent from the HSE PO listing).
The largest single row (BAM ~€107.6m, Conciliator's Recommendation No. 25) is a REAL figure
that matches public reporting (RTE/Irish Examiner, 2024) but dominates >50% of the file —
outlier_warning fires so no raw total is ever headlined.

NOT wired into pipeline.py. Writes a GOLD-CANDIDATE to data/sandbox/parquet/ (LA precedent:
promote only on a separate go-ahead).

Run:
  ./.venv/Scripts/python.exe extractors/procurement_nphdb_parser.py
  ./.venv/Scripts/python.exe extractors/procurement_nphdb_parser.py --pdf c:/tmp/.../file.pdf
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import importlib.util
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import fitz  # PyMuPDF
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

# Reuse, don't rebuild: the gold-schema classification + safe-to-sum + coverage caveat all
# live in the generic extractor (graduated to extractors/). Import it by path.
_spec = importlib.util.spec_from_file_location("pbe", str(ROOT / "extractors/procurement_public_body_extract.py"))
pbe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pbe)

TMP = Path("c:/tmp/procurement_publishers")
OUT_FACT = ROOT / "data/silver/parquet/nphdb_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/nphdb_payments_coverage.json"
PARSER_VERSION = "0.1.0"

LISTING_URL = "https://newchildrenshospital.ie/freedom-of-information/procurement/"

# Caveat carried by the file that holds the BAM conciliator/adjudicator AWARD rows (the
# ~€107.6m Conciliator's Recommendation row that dominates the early listing).
_BAM_CAVEAT = (
    "PO listing for the New Children's Hospital. Contains BAM Building "
    "conciliator/adjudicator AWARD rows that are disputed/under Notice of "
    "Dissatisfaction, not ordinary purchase orders — the ~€107.6m row is a REAL "
    "figure matching public reporting but never headline a raw sum without the outlier flag."
)
_GENERIC_CAVEAT = (
    "NPHDB quarterly PO listing (po_committed). May contain BAM Building "
    "conciliator/adjudicator award rows — see the outlier flag before summing."
)

# Every published NPHDB listing we ingest. The board publishes some quarters as standalone
# files and others as a multi-quarter span; rows carry no per-line quarter, so we tag each
# row with the file's period and (where the file is a single quarter) its year+quarter.
# Spans are contiguous and non-overlapping, so a plain union is the full corpus.
# Discovered via tools/procurement_source_poller.py (NPHDB == FRESH, 2026-06-20).
FILES = [
    {
        "url": "https://newchildrenshospital.ie/wp-content/uploads/2025/10/"
        "NPHDB-Quarterly-PO-Listing-Q1-2024-to-Q2-2025-ID-182108.pdf",
        "period": "2024-Q1..2025-Q2",
        "year": None,
        "quarter": None,
        "caveat": _BAM_CAVEAT,
    },
    {
        "url": "https://newchildrenshospital.ie/wp-content/uploads/2026/01/"
        "Quarterly-PO-Listing-Q3-2025-ID-183186.pdf",
        "period": "2025-Q3",
        "year": 2025,
        "quarter": 3,
        "caveat": _GENERIC_CAVEAT,
    },
    {
        "url": "https://newchildrenshospital.ie/wp-content/uploads/2026/06/"
        "Quarterly-PO-Listing-Q4-2025-ID-195073.pdf",
        "period": "2025-Q4",
        "year": 2025,
        "quarter": 4,
        "caveat": _GENERIC_CAVEAT,
    },
]

# The overall span the unioned fact now covers (min..max across FILES above).
OVERALL_SPAN = "2024-Q1..2025-Q4"

MONEY_LINE = re.compile(r"^\s*\d{1,3}(?:,\d{3})*\.\d{2}\s*$")
HEADER_LINES = {"supplier", "net amount", "description", "net", "amount"}


def _tokens_with_pages(doc) -> list[tuple[str, int]]:
    """Non-blank, non-header text lines across all pages, in reading order, tagged by page."""
    toks: list[tuple[str, int]] = []
    for i in range(doc.page_count):
        for ln in doc[i].get_text().splitlines():
            s = ln.strip()
            if not s or s.lower() in HEADER_LINES:
                continue
            toks.append((s, i + 1))
    return toks


def parse_records(doc) -> list[dict]:
    """Reading-order parse: each money line is a record anchor. Supplier = preceding token;
    description = tokens between the money line and the next record's supplier."""
    toks = _tokens_with_pages(doc)
    lines = [t for t, _ in toks]
    money_idx = [i for i, t in enumerate(lines) if MONEY_LINE.match(t)]
    recs: list[dict] = []
    for k, j in enumerate(money_idx):
        if j == 0:
            continue  # money with no preceding supplier — skip defensively
        supplier = lines[j - 1]
        if k + 1 < len(money_idx):
            next_supplier_idx = money_idx[k + 1] - 1  # token right before next money = its supplier
            desc = " ".join(lines[j + 1 : next_supplier_idx])
        else:
            desc = " ".join(lines[j + 1 :])  # last record runs to EOF
        amount = float(lines[j].replace(",", ""))
        recs.append(
            {
                "supplier_raw": supplier,
                "amount_eur": amount,
                "description": desc or None,
                "source_row_number": k,
                "source_page_number": toks[j][1],
            }
        )
    return recs


def build_rows(recs: list[dict], spec: dict, fhash: str) -> list[dict]:
    conf = "high" if len(recs) > 20 else ("medium" if len(recs) > 3 else "low")
    caveat = spec["caveat"]
    out = []
    for r in recs:
        out.append(
            {
                "publisher_id": "ie_nphdb",
                "publisher_name": "National Paediatric Hospital Development Board",
                "publisher_type": "state_body",
                "sector": "health",
                "source_landing_url": LISTING_URL,
                "source_file_url": spec["url"],
                "source_file_hash": fhash,
                "period": spec["period"],
                "year": spec["year"],
                "quarter": spec["quarter"],
                "supplier_raw": r["supplier_raw"],
                "amount_eur": r["amount_eur"],
                "amount_semantics": "po_committed",
                "description": r["description"],
                "po_number": None,
                "paid_flag": None,
                "source_row_number": r["source_row_number"],
                "source_page_number": r["source_page_number"],
                "parser_name": "nphdb_reading_order",
                "parser_version": PARSER_VERSION,
                "extraction_status": "extracted",
                "extraction_confidence": conf,
                "caveat_text_detected": True,
                "source_caveat": caveat,
            }
        )
    return out


def _load_pdf_bytes(url: str) -> bytes | None:
    """Fetch a listing PDF, caching to TMP so re-runs don't re-download (quarterly
    files are immutable). Returns None on a failed download."""
    cache = TMP / re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:90]
    if cache.exists() and cache.stat().st_size > 1500:
        return cache.read_bytes()
    b = pbe.fetch_bytes(url)
    if not b:
        return None
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_bytes(b)
    return b


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", default=None, help="parse a single local PDF instead of the FILES registry")
    ap.add_argument("--url", default=None, help="provenance source_file_url for --pdf (single-file mode)")
    args = ap.parse_args()

    # Single-file override (legacy / ad-hoc): parse one local PDF as a span-tagged file.
    if args.pdf:
        specs = [{"url": args.url or f"file://{args.pdf}", "period": OVERALL_SPAN, "year": None, "quarter": None, "caveat": _BAM_CAVEAT}]
        local = {specs[0]["url"]: Path(args.pdf).read_bytes()}
    else:
        specs = FILES
        local = {}

    print(f"{'=' * 80}\nNPHDB PO LISTING — {len(specs)} file(s)\n{'=' * 80}")
    all_rows: list[dict] = []
    per_file: list[dict] = []
    for spec in specs:
        b = local.get(spec["url"]) or _load_pdf_bytes(spec["url"])
        if not b:
            print(f"  download FAILED, skipped: {spec['url']}")
            per_file.append({"period": spec["period"], "url": spec["url"], "rows": 0, "status": "download_failed"})
            continue
        fhash = hashlib.sha256(b).hexdigest()[:16]
        doc = fitz.open(stream=b, filetype="pdf")
        recs = parse_records(doc)
        pages = doc.page_count
        doc.close()
        rows = build_rows(recs, spec, fhash)
        all_rows.extend(rows)
        sub = float(sum(r["amount_eur"] for r in rows) or 0)
        print(f"  {spec['period']:<18} {pages:>3} pages  {len(recs):>5,} records  €{sub:>15,.2f}")
        per_file.append(
            {"period": spec["period"], "url": spec["url"], "hash": fhash, "rows": len(rows), "sum_eur": sub, "status": "ok"}
        )

    if not all_rows:
        print("no rows parsed from any file — refusing to overwrite the fact")
        return

    df = pl.DataFrame(all_rows, infer_schema_length=None)
    df = pbe.classify_and_flag(df)

    SCHEMA_COLS = [
        "publisher_id",
        "publisher_name",
        "publisher_type",
        "sector",
        "source_landing_url",
        "source_file_url",
        "source_file_hash",
        "period",
        "year",
        "quarter",
        "supplier_raw",
        "supplier_normalised",
        "amount_eur",
        "amount_semantics",
        "value_safe_to_sum",
        "description",
        "po_number",
        "paid_flag",
        "source_row_number",
        "source_page_number",
        "parser_name",
        "parser_version",
        "extraction_status",
        "extraction_confidence",
        "caveat_text_detected",
        "supplier_class",
        "privacy_status",
        "public_display",
        "source_caveat",
    ]
    df = df.select([c for c in SCHEMA_COLS if c in df.columns])

    OUT_FACT.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_FACT, compression="zstd", compression_level=3, statistics=True)

    total = float(df["amount_eur"].sum() or 0)
    mx = float(df["amount_eur"].max() or 0)
    outlier_share = mx / total if total else 0.0
    print(f"\nrows: {df.height:,}  ->  {OUT_FACT}")
    print(f"sum=€{total:,.2f}  max=€{mx:,.2f}  largest_share={outlier_share * 100:.1f}%")
    print(df.group_by("supplier_class").len().sort("len", descending=True))
    top = df.sort("amount_eur", descending=True).select(["supplier_raw", "amount_eur", "description"]).head(8)
    print("\nTop rows:")
    for s, a, d in top.iter_rows():
        print(f"  {s[:34]:<34} €{a:>15,.2f}  {(d or '')[:48]}")

    quarters = sorted({f"{s['year']}-Q{s['quarter']}" for s in specs if s.get("year") and s.get("quarter")})
    cov = {
        "publisher_id": "ie_nphdb",
        "publisher_name": "National Paediatric Hospital Development Board",
        "source_landing_url": LISTING_URL,
        "files_parsed": per_file,
        "period_span": OVERALL_SPAN,
        "single_quarters_covered": quarters,
        "rows_extracted": df.height,
        "supplier_class_counts": {
            r["supplier_class"]: r["len"] for r in df.group_by("supplier_class").len().iter_rows(named=True)
        },
        "amount_total_eur": total,
        "largest_amount_eur": mx,
        "largest_amount_share_of_total": round(outlier_share, 4),
        "outlier_warning": outlier_share > 0.5,
        "value_safe_to_sum_rows": int(df["value_safe_to_sum"].sum()),
        "privacy_quarantine_applied": False,
        "schema_version": 1,
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "GOLD-CANDIDATE (sandbox, pre-promotion). NPHDB quarterly PO listings for the "
        "New Children's Hospital, unioned across published files (2024-Q1..2025-Q4). One row per "
        "source line. amount_semantics=po_committed. BAM Building rows are disputed "
        "conciliator/adjudicator awards (Notice of Dissatisfaction) — the ~€107.6m row is ~49% of "
        "the corpus: never headline a raw sum. Unions with public_payments_fact at promotion. "
        "PRIVACY QUARANTINE DEFERRED (public_display=True for all rows).",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"\nwrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
