"""SANDBOX: reading-order reader for dept_defence PO PDFs.

5-column NUMBER / CATEGORY / SUPPLIER / CURRENCY / AMOUNT, with two header orderings
(Number,Category,Supplier vs Number,Supplier,Category). The generic word-geometry parser scrambles
these — category→po_number, PO→description, supplier suffix split — leaving 1,191 empty-supplier
rows whose real name sits in po_number (DQ audit 2026-06). Each record:
    NUMBER (PO) | CATEGORY (unit) | SUPPLIER | CURRENCY (EUR/USD…) | AMOUNT
Anchor on the CURRENCY line (single 3-letter code) + AMOUNT after it; PO is the leading digit line;
the 1-2 lines between are CATEGORY + SUPPLIER assigned by the file's header order. Validation only.
"""
from __future__ import annotations
import re
import sys
from pathlib import Path
import fitz

ROOT = Path(__file__).resolve().parents[2]
DD = ROOT / "data/bronze/pdfs/public_body_procurement/dept_defence"

CUR = re.compile(r"^(EUR|USD|GBP|CHF|SEK|NOK|DKK|JPY|CAD|AUD|PLN|CZK|HUF|ZAR|SGD|HKD|NZD)$")
AMT = re.compile(r"^[€$£]?\s*([\d,]+\.\d{2})$")
PO = re.compile(r"^\d{4,7}$")
_BOILER = {"number", "category", "supplier", "vendor", "currency", "amount", "po number",
           "po no", "po no.", "po", "po no"}


def _lines(doc, limit):
    out = []
    for pi in range(limit):
        for raw in doc[pi].get_text().splitlines():
            s = re.sub(r"\s+", " ", raw.replace("\xa0", " ").replace("€", "")).strip()
            if not s or s.lower() in _BOILER or "purchase order" in s.lower() or "department of defence" in s.lower():
                continue
            out.append(s)
    return out


def _supplier_first(doc) -> bool:
    """Header order: does SUPPLIER/VENDOR come before CATEGORY?"""
    head = " ".join(l.strip().lower() for l in doc[0].get_text().splitlines()[:8])
    si = min([head.find(k) for k in ("supplier", "vendor") if head.find(k) >= 0] or [9999])
    ci = head.find("category")
    return si < ci if (si < 9999 and ci >= 0) else False


def read_defence(b: bytes, max_pages=None) -> list[dict]:
    doc = fitz.open(stream=b, filetype="pdf")
    limit = min(doc.page_count, max_pages) if max_pages else doc.page_count
    sup_first = _supplier_first(doc)
    lines = _lines(doc, limit)
    doc.close()
    n = len(lines)
    recs = []
    i = 0
    while i < n:
        # anchor on currency line with an amount right after
        if CUR.match(lines[i]) and i + 1 < n and AMT.match(lines[i + 1]):
            amount = float(AMT.match(lines[i + 1]).group(1).replace(",", ""))
            # walk back: PO is the nearest preceding pure-digit line; the 1-2 lines between PO and
            # currency are CATEGORY + SUPPLIER.
            k = i - 1
            mid = []
            po = None
            while k >= 0:
                if PO.match(lines[k]):  # pure-digit PO line (5-field layout)
                    po = lines[k]
                    break
                m = re.match(r"^(\d{4,7})\s+(.+)$", lines[k])  # merged 'PO NAME' line (4-field layout)
                if m:
                    po = m.group(1)
                    mid.insert(0, m.group(2).strip())
                    break
                if CUR.match(lines[k]) or AMT.match(lines[k]):  # ran into previous record
                    break
                mid.insert(0, lines[k])
                k -= 1
            # assign supplier vs category from header order (category is one line)
            category = supplier = None
            if len(mid) == 1:
                supplier = mid[0]
            elif len(mid) >= 2:
                if sup_first:
                    category, supplier = mid[-1], " ".join(mid[:-1])
                else:
                    category, supplier = mid[0], " ".join(mid[1:])
            if supplier and re.search(r"[A-Za-z]", supplier):
                recs.append({"ref": po, "supplier": supplier.strip(), "category": category,
                             "amount": amount})
            i += 2
            continue
        i += 1
    return recs


def main():
    pdfs = sorted(DD.glob("*.pdf"))
    allr, empty = [], 0
    for p in pdfs:
        r = read_defence(p.read_bytes())
        allr += r
    empty = sum(1 for r in allr if not r["supplier"] or not re.search(r"[A-Za-z]", r["supplier"]))
    print(f"{len(pdfs)} PDFs -> {len(allr)} records, empty-supplier={empty}, sum=EUR {sum(r['amount'] for r in allr)/1e6:.1f}m")
    print("top recovered suppliers:")
    from collections import defaultdict
    agg = defaultdict(lambda: [0, 0.0])
    for r in allr:
        agg[r["supplier"]][0] += 1
        agg[r["supplier"]][1] += r["amount"]
    for s, (nr, tot) in sorted(agg.items(), key=lambda x: -x[1][1])[:14]:
        print(f"  EUR {tot/1e6:>7.2f}m x{nr:<4} {ascii(s)[:42]}")


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
