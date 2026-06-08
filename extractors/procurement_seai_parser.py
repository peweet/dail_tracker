"""PHASE 4 (PRE-ETL, sandbox): bespoke parser for the Sustainable Energy Authority of
Ireland (SEAI) quarterly PO-over-€20k report -> public_payments_fact schema.

WHY BESPOKE (not the generic config-driven reader): SEAI's PO report is a reading-order PDF
laid out as fixed 5-line records, NOT a column table:
    400026637            <- PO number (9 digits)  ← record anchor
    01 Apr 2025          <- date
    Version 1 Software   <- supplier
    €        57,951.45 P <- amount + payment flag (P=paid, Y=?)
    IT Systems Development<- description
The generic header-anchored, word-geometry reader has no header to lock onto and reads the
9-digit PO number (400026637) as the AMOUNT — producing nonsense €400m rows with null
suppliers. This parser anchors on the PO# line (record start) and uses the €-line to split
[date, supplier] from [description], so it is robust to a supplier/description that wraps.

Same family as procurement_nphdb_parser.py / procurement_hse_tusla_parser.py — a per-publisher
reader the generic one can't handle; emits THIS repo's public_payments_fact schema so the
layers union at promotion. amount_semantics=po_committed; the P/Y suffix → paid_flag.

NOT wired into pipeline.py. Writes a GOLD-CANDIDATE to data/sandbox/parquet/.

Run:
  ./.venv/Scripts/python.exe extractors/procurement_seai_parser.py
  ./.venv/Scripts/python.exe extractors/procurement_seai_parser.py --url <other-quarter.pdf>
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
from services.parquet_io import save_parquet  # noqa: E402

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

# Reuse, don't rebuild: gold-schema classification + safe-to-sum live in the generic extractor.
_spec = importlib.util.spec_from_file_location("pbe", str(ROOT / "extractors/procurement_public_body_extract.py"))
pbe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pbe)

OUT_FACT = ROOT / "data/sandbox/parquet/seai_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/seai_payments_coverage.json"
PARSER_VERSION = "0.1.0"

LISTING_URL = "https://www.seai.ie/publications"
DEFAULT_FILE_URL = "https://www.seai.ie/sites/default/files/2025-08/Q2-2025-PO-Report-over-20K.pdf"

PO_LINE = re.compile(r"^\d{9}$")  # 9-digit purchase-order number = record anchor
EUR_LINE = re.compile(r"^€")  # amount line leads with €
DATE_LINE = re.compile(r"^\d{1,2} [A-Za-z]{3,9} 20\d\d$")
AMOUNT_NUM = re.compile(r"\d{1,3}(?:,\d{3})*(?:\.\d{2})?")
PAID_FLAG = re.compile(r"([A-Za-z])\s*$")  # trailing P / Y after the amount


def _lines_with_pages(doc) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for i in range(doc.page_count):
        for ln in doc[i].get_text().splitlines():
            s = ln.strip()
            if s:
                out.append((s, i + 1))
    return out


def parse_records(doc) -> list[dict]:
    toks = _lines_with_pages(doc)
    lines = [t for t, _ in toks]
    po_idx = [i for i, l in enumerate(lines) if PO_LINE.match(l)]
    recs: list[dict] = []
    for k, start in enumerate(po_idx):
        end = po_idx[k + 1] if k + 1 < len(po_idx) else len(lines)
        block = lines[start:end]
        eur_pos = next((bi for bi, l in enumerate(block) if EUR_LINE.match(l)), None)
        if eur_pos is None:
            continue  # a PO# with no amount in its block — skip defensively
        amt_m = AMOUNT_NUM.search(block[eur_pos])
        if not amt_m:
            continue
        amount = float(amt_m.group().replace(",", ""))
        flag_m = PAID_FLAG.search(block[eur_pos])
        paid_flag = flag_m.group(1).upper() if flag_m else None
        # between PO# and €-line: [date, supplier...]; after €-line: [description...]
        mid = block[1:eur_pos]
        if mid and DATE_LINE.match(mid[0]):
            date, supplier = mid[0], " ".join(mid[1:]).strip()
        else:
            date, supplier = None, " ".join(mid).strip()
        description = " ".join(block[eur_pos + 1 : end]).strip() or None
        recs.append(
            {
                "po_number": block[0],
                "date": date,
                "supplier_raw": supplier or None,
                "amount_eur": amount,
                "paid_flag": paid_flag,
                "description": description,
                "source_row_number": k,
                "source_page_number": toks[start][1],
            }
        )
    return recs


def build_rows(recs: list[dict], file_url: str, fhash: str) -> list[dict]:
    period, year, quarter = pbe.period_from_url(file_url)
    conf = "high" if len(recs) > 20 else ("medium" if len(recs) > 3 else "low")
    out = []
    for r in recs:
        out.append(
            {
                "publisher_id": "ie_seai",
                "publisher_name": "Sustainable Energy Authority of Ireland (SEAI)",
                "publisher_type": "agency",
                "sector": "energy_utilities",
                "source_landing_url": LISTING_URL,
                "source_file_url": file_url,
                "source_file_hash": fhash,
                "period": period,
                "year": year,
                "quarter": quarter,
                "supplier_raw": r["supplier_raw"],
                "amount_eur": r["amount_eur"],
                "amount_semantics": "po_committed",
                "description": r["description"],
                "po_number": r["po_number"],
                "paid_flag": r["paid_flag"],
                "source_row_number": r["source_row_number"],
                "source_page_number": r["source_page_number"],
                "parser_name": "seai_reading_order",
                "parser_version": PARSER_VERSION,
                "extraction_status": "extracted",
                "extraction_confidence": conf,
                "caveat_text_detected": False,
                "source_caveat": "PO-over-€20k report; 5-line reading-order records; paid_flag P=paid/Y=?. "
                "Grant-heavy body (privacy=medium) but PO suppliers are goods/services firms.",
            }
        )
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=DEFAULT_FILE_URL, help="SEAI PO-report PDF URL")
    args = ap.parse_args()

    b = pbe.fetch_bytes(args.url)
    if not b or b[:4] != b"%PDF":
        print(f"download failed or not a PDF: {args.url}")
        return
    fhash = hashlib.sha256(b).hexdigest()[:16]
    doc = fitz.open(stream=b, filetype="pdf")
    recs = parse_records(doc)
    print(f"{'=' * 80}\nSEAI PO REPORT — {doc.page_count} pages\n{'=' * 80}")
    print(f"records parsed: {len(recs)}")
    doc.close()

    rows = build_rows(recs, args.url, fhash)
    df = pl.DataFrame(rows, infer_schema_length=None)
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
    save_parquet(df, OUT_FACT)

    total = float(df["amount_eur"].sum() or 0)
    mx = float(df["amount_eur"].max() or 0)
    share = mx / total if total else 0.0
    print(f"\nrows: {df.height:,}  ->  {OUT_FACT}")
    print(f"sum=€{total:,.2f}  max=€{mx:,.2f}  largest_share={share * 100:.1f}%")
    print(df.group_by("supplier_class").len().sort("len", descending=True))
    print("\nTop rows:")
    for s, a, d in (
        df.sort("amount_eur", descending=True).select(["supplier_raw", "amount_eur", "description"]).head(6).iter_rows()
    ):
        print(f"  {str(s)[:32]:<32} €{a:>12,.2f}  {str(d)[:40]}")

    cov = {
        "publisher_id": "ie_seai",
        "source_file_url": args.url,
        "source_file_hash": fhash,
        "rows_extracted": df.height,
        "amount_total_eur": total,
        "largest_amount_share_of_total": round(share, 4),
        "outlier_warning": share > 0.5,
        "value_safe_to_sum_rows": int(df["value_safe_to_sum"].sum()),
        "supplier_class_counts": {
            r["supplier_class"]: r["len"] for r in df.group_by("supplier_class").len().iter_rows(named=True)
        },
        "privacy_quarantine_applied": False,
        "schema_version": 1,
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "GOLD-CANDIDATE (sandbox, pre-promotion). SEAI quarterly PO-over-€20k report, "
        "5-line reading-order records. amount_semantics=po_committed, paid_flag P=paid. "
        "Unions with public_payments_fact at promotion. PRIVACY QUARANTINE DEFERRED.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"\nwrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
