"""PHASE 4 (PRE-ETL, sandbox): bespoke parser for the National Transport Authority (NTA)
quarterly "Purchase Orders €20,000 and over" PDFs -> public_payments_fact schema.

WHY BESPOKE (not the generic config-driven reader): every NTA PO PDF is published with a
90-degree page rotation (the page is landscape, rect ~1191x842, /Rotate 90). The generic
header-anchored, word-geometry reader in procurement_public_body_extract.py clusters an
ENTIRE rotated column of €-values into a single "row" and never assigns an amount column,
so it returns 0 rows. Same failure family as the rotated NPHDB listing.

PyMuPDF get_text("text") DErotates correctly and yields a clean reading-order stream, one
record per visual row as exactly:
    <DD/MM/YYYY> <PO-number> <supplier>      e.g. "02/01/2026 PO0000490 Eyecue"
    <services or goods relating to>          e.g. "Graphic design and creative"
    <€ value>                                e.g. "€30,321.64"
so this parser anchors on the DATE+PO line (record start), reads the value off the money
line inside that record's window, and treats the line(s) in between as the description.
Header/"Data Classification" noise sits OUTSIDE any record window and is dropped for free.

It emits THIS repo's public_payments_fact schema (reusing pbe.classify_and_flag for
supplier_class / privacy_status / value_safe_to_sum) so the layer unions with
public_payments_fact.parquet at promotion. ie_nta is therefore DE-SCOPED from the generic
extractor's PUBLISHERS list (it owned a 0-row entry there) — this file owns NTA.

History: NTA publishes one PDF per quarter under per-year listing pages
(/publications/<year>-purchase-orders-e20000-and-over/). 2024 Q1-Q4, 2025 Q1-Q4, 2026 Q1
are live (9 files); 2019-2023 pages return nothing.

NOT wired into pipeline.py. Writes a GOLD-CANDIDATE to data/sandbox/parquet/ (LA precedent:
promote only on a separate go-ahead).

Run:
  ./.venv/Scripts/python.exe extractors/procurement_nta_parser.py
  ./.venv/Scripts/python.exe extractors/procurement_nta_parser.py --pdf c:/tmp/some.pdf
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

# Reuse, don't rebuild: gold-schema classification + safe-to-sum + fetch all live in the
# generic extractor (graduated to extractors/). Import it by path (same idiom as NPHDB).
_spec = importlib.util.spec_from_file_location(
    "pbe", str(ROOT / "extractors/procurement_public_body_extract.py"))
pbe = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pbe)

OUT_FACT = ROOT / "data/silver/parquet/nta_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/nta_payments_coverage.json"
PARSER_VERSION = "0.1.0"

LISTING_URLS = [
    "https://www.nationaltransport.ie/publications/2024-purchase-orders-e20000-and-over/",
    "https://www.nationaltransport.ie/publications/2025-purchase-orders-e20000-and-over/",
    "https://www.nationaltransport.ie/publications/2026-purchase-orders-e20000-and-over/",
]

# NTA publishes two layouts across years, both handled by anchoring on the DATE line:
#   - 2026 (rotated PDF): "DD/MM/YYYY PO<n> <supplier>" on one line, then <services>, then €value
#   - 2024-25 (upright):  "DD/MM/YYYY" / "<order-no>" / "<supplier>" / "<services>" / €value
# DATE_ANCHOR captures the date and, IF present on the same line, the order ref + supplier.
# The date is either DD/MM/YYYY (2024-Q1..2025-Q3, 2026) or YYYYMMDD as one token (2025-Q4);
# an 8-digit 20###### can't be confused with a 7-digit order ref (e.g. "2025229").
DATE_ANCHOR = re.compile(r"^(\d{2}/\d{2}/\d{4}|20\d{6})(?:\s+(\S+)\s+(.+))?$")
# Value cell on its own line: € is the reliable marker; decimals are optional (2024-25 omit
# the cents). Requiring the € sign keeps it from matching an order number or bare year.
MONEY_LINE = re.compile(r"^\s*€\s*(\d{1,3}(?:,\d{3})*(?:\.\d{2})?)\s*$")


def harvest_pdfs() -> list[str]:
    """Collect every quarterly PO PDF across the per-year NTA listing pages (basename-dedup)."""
    seen: set[str] = set()
    out: list[str] = []
    for listing in LISTING_URLS:
        html = pbe.fetch_text(listing)
        if not html:
            continue
        for href in pbe.HREF_RE.findall(html):
            low = href.lower().split("?")[0]
            if not low.endswith(".pdf") or not re.search(r"purchase|order|20k|e20000", href, re.I):
                continue
            url = href if href.startswith("http") else \
                "https://www.nationaltransport.ie" + (href if href.startswith("/") else "/" + href)
            key = url.rsplit("/", 1)[-1].lower()
            if key not in seen:
                seen.add(key)
                out.append(url)
    return out


def parse_records(doc) -> list[dict]:
    """Reading-order parse anchored on the DATE+PO line. The money line inside each record's
    window is the value; the line(s) between the date line and the money line are the
    description. Header/classification noise falls outside every window and is ignored."""
    lines: list[str] = []
    pages: list[int] = []
    for i in range(doc.page_count):
        for ln in doc[i].get_text("text").splitlines():
            s = ln.strip()
            if not s:
                continue
            lines.append(s)
            pages.append(i + 1)

    starts = [i for i, ln in enumerate(lines) if DATE_ANCHOR.match(ln)]
    recs: list[dict] = []
    for k, s in enumerate(starts):
        end = starts[k + 1] if k + 1 < len(starts) else len(lines)
        # value = the (last) € line inside this record's window
        money_i = next((j for j in range(end - 1, s, -1) if MONEY_LINE.match(lines[j])), None)
        if money_i is None:
            continue  # no value in this window -> not a complete record, skip defensively
        amount = float(MONEY_LINE.match(lines[money_i]).group(1).replace(",", ""))
        m = DATE_ANCHOR.match(lines[s])
        date = m.group(1)
        if m.group(2) and m.group(3):                       # 2026: date + order + supplier inline
            po, supplier = m.group(2), m.group(3).strip()
            desc = " ".join(lines[s + 1:money_i]).strip() or None
        else:                                               # 2024-25: order / supplier / services
            body = lines[s + 1:money_i]
            po = body[0] if body else None
            supplier = body[1] if len(body) > 1 else None
            desc = " ".join(body[2:]).strip() or None
        recs.append({
            "supplier_raw": supplier, "amount_eur": amount, "description": desc,
            "po_number": po, "order_date": date,
            "source_row_number": k, "source_page_number": pages[s],
        })
    return recs


def build_rows(recs: list[dict], file_url: str, fhash: str) -> list[dict]:
    period, year, quarter = pbe.period_from_url(file_url)
    conf = "high" if len(recs) > 20 else ("medium" if len(recs) > 3 else "low")
    out = []
    for r in recs:
        out.append({
            "publisher_id": "ie_nta",
            "publisher_name": "National Transport Authority",
            "publisher_type": "agency",
            "sector": "transport",
            "source_landing_url": LISTING_URLS[-1],
            "source_file_url": file_url,
            "source_file_hash": fhash,
            "period": period, "year": year, "quarter": quarter,
            "supplier_raw": r["supplier_raw"],
            "amount_eur": r["amount_eur"],
            "amount_semantics": "po_committed",
            "description": r["description"],
            "po_number": r["po_number"],
            "paid_flag": None,
            "source_row_number": r["source_row_number"],
            "source_page_number": r["source_page_number"],
            "parser_name": "nta_reading_order",
            "parser_version": PARSER_VERSION,
            "extraction_status": "extracted",
            "extraction_confidence": conf,
            "caveat_text_detected": False,
            "source_caveat": "Rotated-PDF PO listing read in reading order; amount is the "
                             "order value (po_committed), not a payment.",
        })
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pdf", default="", help="parse a single local PDF instead of harvesting")
    ap.add_argument("--url", default="", help="provenance source_file_url for --pdf")
    args = ap.parse_args()

    print(f"{'=' * 80}\nNTA PURCHASE-ORDER LISTINGS (bespoke rotated-PDF reader)\n{'=' * 80}")

    jobs: list[tuple[str, bytes]] = []
    if args.pdf:
        b = Path(args.pdf).read_bytes()
        jobs.append((args.url or args.pdf, b))
    else:
        urls = harvest_pdfs()
        print(f"harvested {len(urls)} quarterly PDFs")
        for u in urls:
            b = pbe.fetch_bytes(u)
            if not b or b[:4] != b"%PDF":
                print(f"  ! download/format failed: {u.rsplit('/', 1)[-1]}")
                continue
            jobs.append((u, b))

    all_rows: list[dict] = []
    per_file: list[dict] = []
    for url, b in jobs:
        fhash = hashlib.sha256(b).hexdigest()[:16]
        doc = fitz.open(stream=b, filetype="pdf")
        recs = parse_records(doc)
        npages = doc.page_count
        doc.close()
        rows = build_rows(recs, url, fhash)
        all_rows.extend(rows)
        fsum = sum(r["amount_eur"] for r in rows)
        per_file.append({"file": url.rsplit("/", 1)[-1], "pages": npages,
                         "rows": len(rows), "sum_eur": fsum})
        print(f"  -> {url.rsplit('/', 1)[-1][:52]:<52} pages={npages:>2} rows={len(rows):>4} €{fsum:>14,.0f}")

    if not all_rows:
        print("\nno rows extracted")
        return

    df = pl.DataFrame(all_rows, infer_schema_length=None)
    df = pbe.classify_and_flag(df)
    SCHEMA_COLS = [
        "publisher_id", "publisher_name", "publisher_type", "sector",
        "source_landing_url", "source_file_url", "source_file_hash",
        "period", "year", "quarter", "supplier_raw", "supplier_normalised",
        "amount_eur", "amount_semantics", "value_safe_to_sum", "description",
        "po_number", "paid_flag", "source_row_number", "source_page_number",
        "parser_name", "parser_version", "extraction_status", "extraction_confidence",
        "caveat_text_detected", "supplier_class", "privacy_status", "public_display",
        "source_caveat",
    ]
    df = df.select([c for c in SCHEMA_COLS if c in df.columns])

    OUT_FACT.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(OUT_FACT, compression="zstd", compression_level=3, statistics=True)

    total = float(df["amount_eur"].sum() or 0)
    mx = float(df["amount_eur"].max() or 0)
    outlier_share = mx / total if total else 0.0
    print(f"\n{'=' * 80}\nGOLD-CANDIDATE WRITTEN\n{'=' * 80}")
    print(f"rows: {df.height:,}  ->  {OUT_FACT}")
    print(f"sum=€{total:,.0f}  max=€{mx:,.0f}  largest_share={outlier_share * 100:.1f}%  "
          f"quarters={df['period'].n_unique()}")
    print(df.group_by("supplier_class").len().sort("len", descending=True))

    cov = {
        "publisher_id": "ie_nta",
        "publisher_name": "National Transport Authority",
        "listing_urls": LISTING_URLS,
        "files_parsed": len(per_file),
        "by_file": per_file,
        "rows_extracted": df.height,
        "quarters_covered": sorted(df["period"].unique().to_list()),
        "supplier_class_counts": {r["supplier_class"]: r["len"]
                                  for r in df.group_by("supplier_class").len().iter_rows(named=True)},
        "amount_total_eur": total,
        "largest_amount_eur": mx,
        "largest_amount_share_of_total": round(outlier_share, 4),
        "outlier_warning": outlier_share > 0.5,
        "value_safe_to_sum_rows": int(df["value_safe_to_sum"].sum()),
        "rows_review_personal_data": int((df["privacy_status"] == "review_personal_data").sum()),
        "privacy_quarantine_applied": False,
        "schema_version": 1,
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "GOLD-CANDIDATE (sandbox, pre-promotion). NTA quarterly purchase orders "
                  "€20,000+. Rotated PDFs read in reading order. One row per PO line. "
                  "amount_semantics=po_committed (orders, not payments). Unions with "
                  "public_payments_fact at promotion. PRIVACY QUARANTINE DEFERRED "
                  "(public_display=True for all rows).",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
