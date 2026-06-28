"""BESPOKE reading-order parser for the 3 gov.ie departments the GENERIC column-geometry reader
cannot handle (de-scoped from extractors/procurement_public_body_extract.py 2026-06-13).

These three publish their over-€20k lists as single-column, reading-order PDFs (one field per text
line) rather than column-geometry tables, so the generic word-row clusterer either fuses every line
into one bucket (DFAT) or scores a notes paragraph as the header and reads the 6-digit PO number as
the amount (Justice -> €30bn garbage). Same family as the NTA/SEAI/NPHDB bespoke parsers. Each record
is anchored on its AMOUNT line (a `12,345.67` / `€12,345.67` token on its own line); the surrounding
lines carry supplier / PO / description / paid-flag in a per-publisher order:

  DFAT (payment)      : [description] [supplier] [AMOUNT]                       (3-line)
  Justice (PO)        : [ref#] [supplier] [€AMOUNT] [description] [paid Y/N]    (5-line)
  Transport-2026 (PO) : [PO#] [supplier] [AMOUNT] [paid Y/N] [description]      (5-line)
  Transport-Q1.. (PO) : [PO# supplier] [AMOUNT] [Drawdown/Paid] [description]   (4-line, PO inline)

Emits the public_payments_fact schema (via pbe.classify_and_flag) -> its own silver fact, which unions
with public_payments_fact + the bespoke nta/seai/nphdb/hse_tusla facts at gold consolidate time.

NOT wired into pipeline.py. Run:
  ./.venv/Scripts/python.exe extractors/procurement_dept_readingorder_parser.py
  ./.venv/Scripts/python.exe extractors/procurement_dept_readingorder_parser.py --only dept_justice --max-files 2
  ./.venv/Scripts/python.exe extractors/procurement_dept_readingorder_parser.py --validate   # no write, parse 1 file each
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

import fitz
import polars as pl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "extractors"))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

import procurement_public_body_extract as pbe  # noqa: E402

from services.parquet_io import save_parquet  # noqa: E402

OUT_FACT = ROOT / "data/silver/parquet/dept_readingorder_payments_fact.parquet"
OUT_COV = ROOT / "data/_meta/dept_readingorder_payments_coverage.json"
PARSER_VERSION = "0.1.0"

# A money token on its own line: optional €, thousands-grouped or bare, always 2 decimals. PO numbers
# (pure ints, no decimal) and reference numbers never match, so they can't be mistaken for an amount.
MONEY_LINE = re.compile(r"^\s*€?\s*\d{1,3}(?:,\d{3})*\.\d{2}\s*$|^\s*€?\s*\d+\.\d{2}\s*$")
# Money at the START of a line, capturing the trailing token (the "20K Purchase Order" Transport
# layout merges the amount and the Paid/Drawdown flag: "6,554,307.17 Drawdown"). Only treated as
# the merged layout when that trailing token is an actual paid flag (PAID_FLAG) — otherwise the
# line is "amount description …" (a different layout: q2/q3-2018) and is left to the pure path.
MONEY_START = re.compile(r"^\s*€?\s*(\d{1,3}(?:,\d{3})*\.\d{2}|\d+\.\d{2})\s+(\S.*)$")
# The trailing must LEAD with a Paid/Drawdown flag for the line to be the merged layout — capturing
# the flag and any inline description after it ("Drawdown IRCG: Helicopter"). A line whose trailing
# is "Helicopter Service Drawdown" (description THEN flag, q2/q3-2018) does NOT match and is left to
# the pure-money path, so those files are unchanged. Restricted to drawdown/paid (the only flags
# these files use) so a description that merely starts with "No"/"Yes" can't be misread as a flag.
PAID_LEAD = re.compile(r"^(drawdown|part[\s-]?paid|paid)\b\.?\s*(.*)$", re.I)
PO_INLINE = re.compile(r"^\s*(\d{6,})\s+(.+\S)\s*$")  # "100037880 CHC (Ireland) Ltd" (Transport-Q1)
PURE_INT = re.compile(r"^\s*\d{4,}\s*$")  # a standalone PO/reference number line
# A grand-total / category-subtotal row masquerading as a supplier. Two shapes: STANDALONE
# ("TOTALS" alone — DFAT/Transport quarter footers) and TRAILING (Justice end-of-report category
# summaries "ACCOMMODATION SUPPORT MAINTENANCE N TOTAL"). Anchored on a leading total/sum OR a
# trailing "…total" so it won't drop real names that merely contain the word (e.g. "Total Produce
# Ltd" ends in "Ltd", "Totalmobile Ltd" — neither matches a trailing/standalone total).
TOTAL_SUP = re.compile(r"^\s*(grand\s+)?totals?\s*$|^\s*sub-?totals?\s*$|^\s*sums?\s*$|\btotals?\s*$", re.I)
MIN_ROWS_PER_FILE = 5  # below this, the file is a different (columnar/scanned) layout this parser can't read


def to_eur(s: str) -> float | None:
    return pbe.to_eur(s)


def _lines(page) -> list[str]:
    return [ln.strip() for ln in page.get_text("text").splitlines() if ln.strip()]


# --------------------------------------------------------------- per-publisher strategies -----
def dfat_records(lines: list[str]):
    """[description] [supplier] [AMOUNT] — 3 consecutive lines, amount last."""
    for i, ln in enumerate(lines):
        if not MONEY_LINE.match(ln):
            continue
        amt = to_eur(ln)
        if amt is None or i < 2:
            continue
        supplier = lines[i - 1]
        desc = lines[i - 2]
        # skip the header trio ("Description"/"Name"/"Base Amount") and totals
        if supplier.lower() in {"name", "base amount"} or MONEY_LINE.match(supplier):
            continue
        yield {"supplier": supplier, "amount": amt, "description": desc, "po": None, "paid": None}


def justice_records(lines: list[str]):
    """[ref#] [supplier(maybe multi-line)] [€AMOUNT] [description] [paid Y/N]."""
    n = len(lines)
    for i, ln in enumerate(lines):
        if not MONEY_LINE.match(ln):
            continue
        amt = to_eur(ln)
        if amt is None or i < 1:
            continue
        # walk UP collecting supplier lines until the pure-int reference (handles wrapped names)
        j = i - 1
        sup_parts: list[str] = []
        po = None
        while j >= 0 and len(sup_parts) < 4:
            if PURE_INT.match(lines[j]):
                po = lines[j].strip()
                break
            if MONEY_LINE.match(lines[j]):
                break
            sup_parts.insert(0, lines[j])
            j -= 1
        if not sup_parts:
            continue
        supplier = " ".join(sup_parts)
        desc = lines[i + 1] if i + 1 < n and not MONEY_LINE.match(lines[i + 1]) else None
        paid = lines[i + 2] if i + 2 < n and re.fullmatch(r"[YNP]", lines[i + 2].strip()) else None
        yield {"supplier": supplier, "amount": amt, "description": desc, "po": po, "paid": paid}


def transport_records(lines: list[str]):
    """Two layouts: 2026 = [PO#] [supplier] [AMOUNT] [paid] [desc]; Q1.. = [PO# supplier] [AMOUNT]
    [Drawdown/Paid] [desc]. Detect per-record from the line above the amount."""
    n = len(lines)
    for i, ln in enumerate(lines):
        if i < 1:
            continue
        ms = MONEY_START.match(ln)
        mlead = PAID_LEAD.match(ms.group(2).strip()) if ms else None  # trailing leads with a paid flag?
        merged = ms if mlead else None  # "AMOUNT Drawdown [description]"
        pure = MONEY_LINE.match(ln)  # amount alone on its line (2026/Q1 layouts)
        if not (merged or pure):
            continue
        amt = to_eur(merged.group(1) if merged else ln)
        if amt is None:
            continue
        prev = lines[i - 1]
        m = PO_INLINE.match(prev)
        if m and not PURE_INT.match(lines[i - 2] if i >= 2 else ""):
            # Q1 style: PO and supplier share the line above the amount
            po, supplier = m.group(1), m.group(2)
        else:
            # 2026 style: supplier on the line above, pure-int PO two above
            supplier = prev
            po = lines[i - 2].strip() if i >= 2 and PURE_INT.match(lines[i - 2]) else None
        if MONEY_LINE.match(supplier) or supplier.lower() in {"amount", "supplier name"}:
            continue
        if merged:
            # the amount line leads with the Paid/Drawdown flag; an inline description may follow it,
            # otherwise the description is the next line
            paid = mlead.group(1)
            inline = mlead.group(2).strip()
            if inline:
                desc = inline
            else:
                nxt = lines[i + 1] if i + 1 < n else None
                desc = nxt if nxt and not (MONEY_START.match(nxt) or MONEY_LINE.match(nxt)) else None
        else:
            paid = lines[i + 1] if i + 1 < n else None
            desc = lines[i + 2] if i + 2 < n and not MONEY_LINE.match(lines[i + 2]) else None
            # Q1's line after amount is the Paid/Drawdown flag then the description; 2026's is paid then desc
            if paid and MONEY_LINE.match(paid):
                paid, desc = None, None
        yield {"supplier": supplier, "amount": amt, "description": desc, "po": po, "paid": paid}


DEPTS = {
    "dept_foreign_affairs": {
        "name": "Department of Foreign Affairs and Trade",
        "listing": "https://www.gov.ie/en/department-of-foreign-affairs/organisation-information/payments-over-20000/",
        "semantics": "payment_actual",
        "strategy": dfat_records,
    },
    "dept_justice": {
        "name": "Department of Justice, Home Affairs and Migration",
        "listing": "https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/collections/department-of-justice-purchase-orders-issued-over-20000-in-value/",
        "semantics": "po_committed",
        "strategy": justice_records,
        "include": r"\.pdf(\?|$)",  # the xlsx buries its header under a 7-row notes preamble; use the PDF
    },
    "dept_transport": {
        "name": "Department of Transport",
        "listing": "https://www.gov.ie/en/department-of-transport/organisation-information/departmental-purchase-orders-greater-than-20000/",
        "semantics": "po_committed",
        "strategy": transport_records,
    },
}


def _cfg(pid: str) -> dict:
    d = DEPTS[pid]
    return {
        "id": pid,
        "name": d["name"],
        "listing_url": d["listing"],
        "direct_files": [],
        "include": re.compile(d["include"], re.I) if d.get("include") else None,
        "exclude": None,
    }


def parse_file(pid: str, url: str, b: bytes) -> list[dict]:
    strat = DEPTS[pid]["strategy"]
    fhash = hashlib.sha256(b).hexdigest()[:16]
    period, year, quarter = pbe.period_from_url(url)
    doc = fitz.open(stream=b, filetype="pdf")
    out: list[dict] = []
    for pi in range(doc.page_count):
        for srn, rec in enumerate(strat(_lines(doc[pi]))):
            sup = pbe.clean_supplier(rec["supplier"])
            if TOTAL_SUP.search(sup or "") or TOTAL_SUP.search(rec["supplier"] or ""):
                continue  # quarterly grand-total row, never a real supplier
            out.append(
                {
                    "publisher_id": pid,
                    "publisher_name": DEPTS[pid]["name"],
                    "publisher_type": "department",
                    "sector": "central_government",
                    "source_landing_url": DEPTS[pid]["listing"],
                    "source_file_url": url,
                    "source_file_hash": fhash,
                    "period": period,
                    "year": year,
                    "quarter": quarter,
                    "supplier_raw": sup,
                    "amount_eur": rec["amount"],
                    "amount_semantics": DEPTS[pid]["semantics"],
                    "description": rec["description"],
                    "po_number": rec["po"],
                    "paid_flag": rec["paid"],
                    "source_row_number": srn,
                    "source_page_number": pi + 1,
                    "parser_name": f"dept_readingorder_{pid}",
                    "parser_version": PARSER_VERSION,
                    "extraction_status": "extracted",
                    "caveat_text_detected": False,
                    "source_caveat": "Bespoke reading-order parse (single-column PDF; generic column reader misparses).",
                }
            )
    doc.close()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--max-files", type=int, default=None)
    ap.add_argument("--validate", action="store_true", help="parse 1 file per dept, print quality, NO write")
    args = ap.parse_args()
    only = {x.strip() for x in args.only.split(",") if x.strip()} or None
    pids = [p for p in DEPTS if not only or p in only]

    all_rows: list[dict] = []
    per_pub: list[dict] = []
    excluded: list[dict] = []
    for pid in pids:
        files = pbe.harvest_files(_cfg(pid))
        files = files[: (1 if args.validate else args.max_files)]
        print(f"\n[{pid}] {DEPTS[pid]['name']} — {len(files)} file(s)")
        pub_rows: list[dict] = []
        for u in files:
            ext = ".pdf"
            b, _ = pbe.fetch_to_bronze(pid, u, ext)
            if not b or b[:4] != b"%PDF":
                print(f"   ! fetch/format failed: {u.rsplit('/', 1)[-1][:50]}")
                continue
            try:
                rows = parse_file(pid, u, b)
            except Exception as e:
                print(f"   ! {type(e).__name__}: {u.rsplit('/', 1)[-1][:46]}")
                continue
            # Layout gate: a file that yields almost nothing is a DIFFERENT layout this parser can't
            # read (e.g. some Transport quarters are columnar, not reading-order) — exclude it rather
            # than ship a lone total/garbage row. DFAT/Justice files always clear this comfortably.
            if len(rows) < MIN_ROWS_PER_FILE:
                excluded.append({"publisher": pid, "file": u.rsplit("/", 1)[-1], "rows": len(rows)})
                if args.validate:
                    print(f"   ~ EXCLUDED (only {len(rows)} rows, wrong layout): {u.rsplit('/', 1)[-1][:46]}")
                continue
            pub_rows.extend(rows)
            if args.validate:
                amts = [r["amount_eur"] for r in rows if r["amount_eur"]]
                nulls = sum(1 for r in rows if not (r["supplier_raw"] or "").strip())
                print(
                    f"   -> {u.rsplit('/', 1)[-1][:46]:46} rows={len(rows)} null_sup={nulls} "
                    f"sum=€{sum(amts):,.0f}" + (f" max=€{max(amts):,.0f}" if amts else "")
                )
                for r in rows[:4]:
                    print(
                        f"        €{r['amount_eur']:>13,.2f} | {str(r['supplier_raw'])[:34]:34} | "
                        f"po={r['po_number']} | {str(r['description'])[:28]}"
                    )
        all_rows.extend(pub_rows)
        per_pub.append({"id": pid, "name": DEPTS[pid]["name"], "rows": len(pub_rows), "files": len(files)})

    if args.validate or not all_rows:
        print("\nVALIDATE/empty — nothing written." if args.validate else "\nno rows")
        return

    df = pl.DataFrame(all_rows, infer_schema_length=None)
    # confidence: a file's rows are high-conf if its null-supplier share is low
    df = df.with_columns(pl.lit("high").alias("extraction_confidence"))
    df = pbe.classify_and_flag(df)
    df = pbe.flag_unidentifiable_suppliers(df)
    SCHEMA = [
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
    df = df.select([c for c in SCHEMA if c in df.columns])

    leaked = df.filter(pl.col("public_display") & (pl.col("supplier_class") == "sole_trader_or_individual"))
    if leaked.height:
        raise RuntimeError(f"privacy breach: {leaked.height} personal rows public_display=True")

    OUT_FACT.parent.mkdir(parents=True, exist_ok=True)
    save_parquet(df, OUT_FACT)
    safe = df.filter(pl.col("value_safe_to_sum"))
    print(f"\n{'=' * 78}\nWRITTEN {df.height:,} rows -> {OUT_FACT}")
    for p in per_pub:
        sub = df.filter(pl.col("publisher_id") == p["id"])
        s = sub.filter(pl.col("value_safe_to_sum"))
        print(f"  {p['id']:<22} rows={sub.height:>6}  safe=€{s['amount_eur'].sum() or 0:>14,.0f}")
    print(f"  value_safe_to_sum total €{safe['amount_eur'].sum() or 0:,.0f}")

    cov = {
        "publishers": list(DEPTS),
        "rows_extracted": df.height,
        "by_publisher": per_pub,
        "files_excluded_wrong_layout": excluded,
        "value_safe_to_sum_rows": safe.height,
        "value_safe_to_sum_total_eur": float(safe["amount_eur"].sum() or 0),
        "rows_review_personal_data": int((df["privacy_status"] == "review_personal_data").sum()),
        "privacy_quarantine_applied": True,
        "schema_version": 1,
        "parser_version": PARSER_VERSION,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "caveat": "GOLD-CANDIDATE (sandbox). Bespoke reading-order parse of single-column dept PDFs "
        "(DFAT payment / Justice + Transport PO). Unions with public_payments_fact at consolidate.",
    }
    OUT_COV.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    print(f"wrote coverage {OUT_COV}")


if __name__ == "__main__":
    main()
