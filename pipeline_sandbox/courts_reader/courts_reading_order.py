"""SANDBOX: bespoke reading-order reader for the Courts Service 'PO analysis report' PDFs.

WHY: the generic word-geometry parser mis-columns these files. Their layout merges the PO number
and supplier name onto one text line for some quarters (2016+), and the x-coordinate column
bucketing then splits the supplier name — the body lands in the PO column, the trailing legal
suffix in the supplier column. Result: 3,690 scrambled rows + 37 blank/boilerplate rows = the
€693m 'no payee' problem (audit P1).

The files are actually a clean 5-field reading-order record:
    <PO> <SUPPLIER NAME>        (PO may be on its own line in older quarters)
    <AMOUNT>
    <blank / € placeholder>
    <DESCRIPTION>               (may wrap)
    <PAID Y/N>
Anchoring on the AMOUNT line (pure money) and terminating each record on its PAID (Y/N) line makes
the read robust to the merged-vs-separate PO/name variation and recovers PO, supplier, amount,
description AND paid correctly — and naturally skips header/footer boilerplate.

This is a VALIDATION harness only: it parses the cached bronze and reports recovery vs the current
(scrambled) gold. No writes. If validated, the reader graduates into
extractors/procurement_public_body_extract.py with `reader="reading_order_courts"` on the ie_courts cfg.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import fitz

ROOT = Path(__file__).resolve().parents[2]
COURTS = ROOT / "data/bronze/pdfs/public_body_procurement/ie_courts"

# Amount anchors the record. Two published layouts: the amount alone on its line, OR the amount
# followed by the description on the same line ('1,875,200.68 PPP - Unitary Payments'). Match the
# leading money token and capture any trailing description. A PO/name line ('58132 IPP CCC') has no
# decimal so it never matches this.
AMT = re.compile(r"^([\d,]+\.\d{2})\b\s*(.*)$")
PAID = re.compile(r"^(Y|N|Yes|No|Paid|Not Paid)$", re.I)
PO_NAME = re.compile(r"^(\d{4,7})\s+(.+)$")
PO_ONLY = re.compile(r"^\d{4,7}$")
_BOILER = {
    "po no.", "po no", "supplier name", "amount", "description", "paid yes/no", "paid",
    "the courts services", "the courts service", "yes/no",
}


def _lines(b: bytes, max_pages: int | None = None) -> list[str]:
    doc = fitz.open(stream=b, filetype="pdf")
    limit = min(doc.page_count, max_pages) if max_pages else doc.page_count
    out: list[str] = []
    for pi in range(limit):
        for raw in doc[pi].get_text().splitlines():
            # normalise unicode spaces (newer quarters use NBSP \xa0 between words) + drop € placeholder
            s = raw.replace("\xa0", " ").replace(" ", " ").replace(" ", " ").replace("�", " ")
            s = re.sub(r"\s+", " ", s).strip()
            if not s:
                continue
            low = s.lower()
            if low in _BOILER or low.startswith("purchase orders for") or "analysis report" in low:
                continue
            if re.fullmatch(r"page \d+( of \d+)?", low):
                continue
            out.append(s)
    doc.close()
    return out


def read_courts(b: bytes, max_pages: int | None = None) -> list[dict]:
    lines = _lines(b, max_pages)
    n = len(lines)
    recs: list[dict] = []
    rec_start = 0
    i = 0
    while i < n:
        am = AMT.match(lines[i])
        if not am:
            i += 1
            continue
        amount = float(am.group(1).replace(",", ""))
        head = lines[rec_start:i]  # PO + supplier line(s)
        # forward: description until PAID (record terminator) or next amount
        j = i + 1
        desc: list[str] = []
        if am.group(2).strip():  # description shared the amount line
            desc.append(am.group(2).strip())
        paid = None
        while j < n and not AMT.match(lines[j]):
            if PAID.match(lines[j]):
                paid = lines[j]
                j += 1
                break
            # next record's head (PO+name or PO-only) terminates this description — prevents
            # the next supplier bleeding into desc when a record has no explicit Paid line.
            if PO_NAME.match(lines[j]) or PO_ONLY.match(lines[j]):
                break
            desc.append(lines[j])
            j += 1
        # parse head -> po + supplier
        po = None
        supplier = ""
        headtext = " ".join(head).strip()
        m = PO_NAME.match(headtext)
        if m:
            po, supplier = m.group(1), m.group(2).strip()
        elif head and PO_ONLY.match(head[0]):
            po, supplier = head[0], " ".join(head[1:]).strip()
        else:
            supplier = headtext
        # a record must have a supplier with letters (skip stray totals/boilerplate)
        if supplier and re.search(r"[A-Za-z]", supplier):
            recs.append({"po": po, "supplier": supplier, "amount": amount,
                         "desc": " ".join(desc).strip(), "paid": paid})
        rec_start = j
        i = j
    return recs


def main():
    pdfs = sorted(COURTS.glob("*.pdf"))
    all_recs = []
    empty_sup = 0
    per_file = []
    for p in pdfs:
        recs = read_courts(p.read_bytes())
        e = sum(1 for r in recs if not r["supplier"] or not re.search(r"[A-Za-z]", r["supplier"]))
        empty_sup += e
        tot = sum(r["amount"] for r in recs)
        per_file.append((p.name[:46], len(recs), e, tot / 1e6))
        all_recs += recs
    print(f"{'file':48} {'recs':>5} {'empty':>6} {'eur_m':>9}")
    for nm, nr, e, em in per_file:
        print(f"{nm:48} {nr:>5} {e:>6} {em:>9.2f}")
    print(f"\nTOTAL: {len(all_recs)} records across {len(pdfs)} PDFs; "
          f"empty-supplier={empty_sup}; sum=€{sum(r['amount'] for r in all_recs)/1e6:.1f}m")
    # show a sample of recovered names that were scrambled before
    print("\nsample recovered suppliers (with PO + amount):")
    seen = set()
    for r in all_recs:
        if r["supplier"] not in seen and r["po"]:
            seen.add(r["supplier"])
            print(f"  PO {r['po']:>8}  €{r['amount']:>12,.2f}  {r['supplier'][:48]!r}  desc={r['desc'][:24]!r} paid={r['paid']}")
        if len(seen) >= 16:
            break


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")
    main()
