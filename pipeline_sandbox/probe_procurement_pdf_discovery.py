"""PROBE (throwaway): WIDE discovery of the actual-spend procurement corpus on
data.gov.ie — size the PDF debt (and the format spread overall) before any build.

probe_procurement_pdf.py confirmed the *handful* of PDFs it found are digital
(fitz, no OCR). But that used one narrow query and saw ~8 PDFs from one council.
This probe casts a wide net: many query phrasings ("Purchase Orders over",
"Payments over 20", "Prompt Payment", "spend over 20000", "supplier payments"…),
dedups packages by id, and produces a CENSUS:
  - how many distinct datasets / publishers in the spend corpus
  - resource-format histogram (csv / xlsx / pdf / other) across ALL of them
  - the PDF subset broken out by publisher (the actual OCR-or-not risk surface)
  - a HEAD byte-sniff on a sample of PDFs (is it %PDF, digital or image?) so the
    "all digital" claim is tested on more than Kildare.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_pdf_discovery.py
Network-only discovery; sniffs a few PDFs to c:/tmp; writes no repo data.
"""

from __future__ import annotations

import re
import sys
from collections import Counter, defaultdict
from pathlib import Path

import fitz  # PyMuPDF
import requests

ROOT = Path(__file__).resolve().parents[1]
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

TMP = Path("c:/tmp/procurement_pdf")
H = {"User-Agent": "dail-tracker research probe"}

QUERIES = [
    "Purchase Orders over 20000",
    "Purchase Orders over 20,000",
    "Purchase Orders over 20",
    "Procurement Related Payments over 20000",
    "Payments over 20000",
    "Prompt Payment",
    "supplier payments",
    "spend over 20000",
    "expenditure over 20000",
    "purchase order report",
]
# only keep packages whose title looks like a spend/PO/payment listing
KEEP_RE = re.compile(r"purchase order|payments? over|over 20|prompt payment|spend|procurement", re.I)


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def search(q: str, rows: int = 300) -> list[dict]:
    try:
        r = requests.get(
            "https://data.gov.ie/api/3/action/package_search",
            params={"q": q, "rows": rows},
            headers=H,
            timeout=60,
        )
        return r.json()["result"]["results"]
    except Exception as e:
        print(f"  search ERR {q!r}: {e!r}")
        return []


def fmt_of(res: dict, url: str) -> str:
    f = (res.get("format", "") or "").strip().lower()
    if not f and "." in url.rsplit("/", 1)[-1]:
        f = url.rsplit(".", 1)[-1].lower()
    f = f.split(";")[0].strip()
    return f or "unknown"


def org_of(pkg: dict) -> str:
    org = pkg.get("organization") or {}
    return (org.get("title") or org.get("name") or "unknown")[:55]


def sniff_pdf(url: str) -> str:
    """Download first ~600KB, classify digital vs scanned (or dead)."""
    name = re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:70] or "doc"
    if not name.lower().endswith(".pdf"):
        name += ".pdf"
    dest = TMP / name
    try:
        if not (dest.exists() and dest.stat().st_size > 2000):
            b = requests.get(url, headers=H, timeout=60).content
            if b[:4] != b"%PDF":
                return "not-pdf"
            TMP.mkdir(parents=True, exist_ok=True)
            dest.write_bytes(b)
        doc = fitz.open(dest)
        chars = sum(len(doc[i].get_text("text").strip()) for i in range(min(doc.page_count, 4)))
        npages = doc.page_count
        doc.close()
        return f"digital({npages}p,{chars}c)" if chars > 200 else f"scanned({npages}p)"
    except Exception as e:
        return f"err:{type(e).__name__}"


def main() -> None:
    hr("WIDE CKAN DISCOVERY — spend / PO / payment corpus")
    packages: dict[str, dict] = {}
    for q in QUERIES:
        hits = search(q)
        kept = 0
        for p in hits:
            if KEEP_RE.search(p.get("title", "")):
                packages.setdefault(p["id"], p)
                kept += 1
        print(f"  q={q!r:<42} hits={len(hits):>3}  kept={kept}")
    print(f"\ndistinct spend/PO/payment datasets: {len(packages)}")

    # census across all kept packages
    fmt_counter: Counter = Counter()
    pdf_by_org: dict[str, list[str]] = defaultdict(list)
    orgs: Counter = Counter()
    total_res = 0
    for p in packages.values():
        orgs[org_of(p)] += 1
        for x in p.get("resources", []):
            u = x.get("url", "") or ""
            if not u:
                continue
            total_res += 1
            f = fmt_of(x, u)
            fmt_counter[f] += 1
            if f == "pdf" or u.lower().endswith(".pdf"):
                pdf_by_org[org_of(p)].append(u)

    hr("RESOURCE FORMAT CENSUS (all spend datasets)")
    print(f"total resources: {total_res:,}")
    for f, n in fmt_counter.most_common():
        print(f"  {f:<10} {n:>5}  ({n / max(1, total_res):.0%})")

    hr("PUBLISHERS (datasets per organisation, top 15)")
    for o, n in orgs.most_common(15):
        print(f"  {n:>3}x  {o}")

    npdf = sum(len(v) for v in pdf_by_org.values())
    hr(f"PDF DEBT BY PUBLISHER  (total {npdf} PDF resources, {len(pdf_by_org)} bodies)")
    for o, urls in sorted(pdf_by_org.items(), key=lambda kv: -len(kv[1])):
        print(f"  {len(urls):>3} PDFs  {o}")

    # sniff PDFs (up to 6 per publisher) to test the 'all digital' claim corpus-wide
    hr("PDF SNIFF — digital vs scanned")
    results: Counter = Counter()
    sniffed = 0
    for o, urls in sorted(pdf_by_org.items(), key=lambda kv: -len(kv[1])):
        for u in urls[:6]:
            verdict = sniff_pdf(u)
            results[verdict.split("(")[0]] += 1
            print(f"  [{o[:30]:<30}] {verdict:<18} {u[-55:]}")
            sniffed += 1
        if sniffed >= 18:
            break

    hr("VERDICT")
    print(f"spend datasets: {len(packages)} | resources: {total_res:,} | PDFs: {npdf} "
          f"({npdf / max(1, total_res):.0%}) across {len(pdf_by_org)} bodies")
    digital = sum(v for k, v in results.items() if k == "digital")
    scanned = sum(v for k, v in results.items() if k == "scanned")
    print(f"PDF sniff: {digital} digital / {scanned} scanned / "
          f"{sum(results.values()) - digital - scanned} dead/other (n={sum(results.values())})")
    bulk = fmt_counter.get("csv", 0) + fmt_counter.get("xlsx", 0) + fmt_counter.get("xls", 0)
    print(f"tabular bulk (csv+xls/x): {bulk:,} ({bulk / max(1, total_res):.0%}) -> the main build;")
    print("PDFs are the long tail. If sniff shows all/mostly digital, no OCR is needed —")
    print("just per-publisher fitz column normalisation. Scanned ones -> PaddleOCR scaffold.")


if __name__ == "__main__":
    main()
