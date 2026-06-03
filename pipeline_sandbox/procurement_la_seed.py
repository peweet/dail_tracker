"""PROBE/SEED (throwaway-ish): a per-council SEED REGISTRY for the off-portal
local-authority Purchase-Orders-over-€20k harvest, plus a generic scrape-tester.

Context: the data.gov.ie / smartdublin portals catalogue almost no council spend
(see probe_procurement_pdf.py). The Circular Fin 07/2012 obligation is met on each
council's OWN website — in inconsistent formats (XLSX template / scattered digital
PDFs / aggregate CSV / nothing). This file:

  1. SEEDS   — a hand-curated registry of council -> finance landing page (the only
               thing that can't be auto-discovered reliably).
  2. harvest — fetch each landing page, extract links that look like PO-over-20k
               files (pdf/xlsx/xls/csv), tally formats, count files (≈ quarters).
  3. classify— download ONE sample per council and report extractability:
               PDF -> digital(fitz)/scanned ; XLSX/CSV -> header + first row.

Goal: turn "31 LAs = 31 bespoke scrapers" into a concrete, testable registry so the
eventual harvester has a seed list and we know up front what each council yields.
Merge target for the parallel Galway work (probe_procurement_pdf_galway.py).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/procurement_la_seed.py
Writes a summary to c:/tmp/procurement_la/seed_report.json; samples to same dir.
"""

from __future__ import annotations

import contextlib
import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

H = {"User-Agent": "Mozilla/5.0 (dail-tracker research probe)"}
TMP = Path("c:/tmp/procurement_la")

# council -> landing page that LISTS the quarterly PO files. region tags let us
# fan out by area. "kind" is the expected publication shape (confirmed where noted).
SEEDS: list[dict] = [
    # --- Dublin (confirmed in probe_procurement_dublin_la.py) ---
    {"council": "South Dublin", "region": "Dublin",
     "url": "https://www.sdcc.ie/en/services/business/payments/", "kind": "xlsx-template"},
    # Fingal: the procurement page only carries POLICY pdfs; the actual quarterly PO
    # files live elsewhere (…/sites/default/files/…) with no clean listing page → scrape-hard.
    {"council": "Fingal", "region": "Dublin",
     "url": "https://www.fingal.ie/council/service/procurement", "kind": "pdf-subpage(scattered)"},
    {"council": "Dublin City", "region": "Dublin",
     "url": "https://www.dublincity.ie/business/doing-business-council/public-procurement", "kind": "stale-aggregate(CKAN<=2014)"},
    {"council": "Dun Laoghaire-Rathdown", "region": "Dublin",
     "url": "https://www.dlrcoco.ie/governance/procurement", "kind": "none-found"},
    # --- the rest of the country (confirmed by the scrape test below) ---
    {"council": "Cork County", "region": "Munster",
     "url": "https://www.corkcoco.ie/en/council/accessibility-maps-and-publications/purchase-orders-in-excess-of-eu20000", "kind": "pdf-digital(107 files; Supplier·Total·Description·Paid)"},
    {"council": "Cork City", "region": "Munster",
     "url": "https://www.corkcity.ie/en/council-services/public-info/spending-and-revenue/", "kind": "xlsx(Supplier·Gross·Description)"},
    {"council": "Waterford", "region": "Munster",
     "url": "https://waterfordcouncil.ie/openness-transparency/governance-related-financial-information/procurement/purchase-orders-e20000/", "kind": "pdf-digital(OrderNo·Supplier·…)"},
    {"council": "Limerick", "region": "Munster",
     "url": "https://www.limerick.ie/council/services/business-and-economy/revenue-collection/accounts-payable", "kind": "pdf-digital(Supplier·Paid·Description)"},
    {"council": "Meath", "region": "Leinster",
     "url": "https://www.meath.ie/council/your-council/finance-and-procurement/tenders-and-contracts/payments-over-eu20000", "kind": "tabular+pdf(curl fallback; TLS was our stack, not a block)"},
    {"council": "Wicklow", "region": "Leinster",
     "url": "https://www.wicklow.ie/Living/Your-Council/Finance/Procurement/Purchase-Orders-Over-20-000", "kind": "xlsx+csv+pdf(Supplier·EURO·Description)"},
    {"council": "Westmeath", "region": "Leinster",
     "url": "https://www.westmeathcoco.ie/en/ourservices/finance/procurement/purchaseorders/", "kind": "pdf-digital(55 files)"},
    {"council": "Monaghan", "region": "Ulster",
     "url": "https://monaghan.ie/finance/publication-of-purchase-orders/", "kind": "xlsx+pdf(Supplier·Amount·Description)"},
    # --- remaining LAs to complete all 31 (kind = scrape-test result 2026-06-03) ---
    {"council": "Carlow", "region": "Leinster",
     "url": "https://carlow.ie/information-technology/statistics-and-reports/financial-statistical-reports", "kind": "PLAYWRIGHT(JS-rendered SPA)"},
    {"council": "Cavan", "region": "Ulster",
     "url": "https://www.cavancoco.ie/file-library/business/procurement/over-20k/", "kind": "PLAYWRIGHT(JS-rendered file list)"},
    {"council": "Clare", "region": "Munster",
     "url": "https://www.clarecoco.ie/business-licensing-and-economy/procurement-and-tenders", "kind": "pdf-digital(via one-hop crawl; sampler may grab a neighbour doc)"},
    {"council": "Donegal", "region": "Ulster",
     "url": "https://www.donegalcoco.ie/en/services/other-services/finance/procurement", "kind": "PUBLISHER(yearly PDFs at /media/{code}/YYYY.pdf; 1,221 rows digital per counties probe; this landing only shows >=10m)"},
    {"council": "Galway City", "region": "Connacht",
     "url": "https://www.galwaycity.ie/services/finance-services/budgets-and-financial-publications", "kind": "pdf-digital(PO PDFs; budgets page mixes prompt-pay)"},
    {"council": "Galway County", "region": "Connacht",
     "url": "https://www.gaillimh.ie/en/finance/financial-publications/purchase-orders", "kind": "pdf-digital(SUPPLIER·PRODUCT·€; gaillimh.ie UNBLOCKS galwaycoco WAF)"},
    {"council": "Kerry", "region": "Munster",
     "url": "https://www.kerrycoco.ie/finance/financial-documents/", "kind": "pdf-digital(16 files; transient fetch err)"},
    {"council": "Kildare", "region": "Leinster",
     "url": "https://kildarecoco.ie/YourCouncil/Publications/Finance/PurchaseOrdersover20000/", "kind": "pdf-digital(Supplier·Total·Description; 52 files)"},
    {"council": "Kilkenny", "region": "Leinster",
     "url": "https://kilkennycoco.ie/eng/services/finance/purchase-orders-over-%E2%82%AC20-000/", "kind": "xlsx(Ap/Ar·Period·EURO·DESCRIPTION; 67; also CKAN)"},
    {"council": "Laois", "region": "Leinster",
     "url": "https://laois.ie/finance/business-and-enterprise-support/procurement-information-and-advice", "kind": "pdf(Procurement Report; via crawl)"},
    {"council": "Leitrim", "region": "Connacht",
     "url": "https://www.leitrim.ie/council/services/finance/procurement/purchase-orders-greater-thank-20k/", "kind": "pdf(via one-hop crawl; 33 files)"},
    {"council": "Longford", "region": "Leinster",
     "url": "https://www.longfordcoco.ie/services/finance/finance-documents/large-purchase-orders/", "kind": "pdf-digital(SUPPLIER·EURO·DESCRIPTION)"},
    {"council": "Louth", "region": "Leinster",
     "url": "https://www.louthcoco.ie/en/publications/finance_reports/", "kind": "crawl reaches finance sub-pages (target the right PO file)"},
    {"council": "Mayo", "region": "Connacht",
     "url": "https://www.mayo.ie/financial-documents/purchase-orders", "kind": "PUBLISHER(837 rows digital per counties probe); files = getattachment/{guid} behind JS list → render to ENUMERATE"},
    {"council": "Offaly", "region": "Leinster",
     "url": "https://www.offaly.ie/financial-reports/", "kind": "pdf-digital(GL30 Payments>€20k; 19pp)"},
    {"council": "Roscommon", "region": "Connacht",
     "url": "https://www.roscommoncoco.ie/en/Download-It/Finance-Publications/", "kind": "PLAYWRIGHT(JS-rendered file list)"},
    {"council": "Sligo", "region": "Connacht",
     "url": "https://www.sligococo.ie/YourCouncil/Finance/ProcurementPurchasing/PurchasingActivity/", "kind": "pdf(curl fallback; TLS was our stack, not a block)"},
    {"council": "Tipperary", "region": "Munster",
     "url": "https://www.tipperarycoco.ie/finance/financial-reports", "kind": "UNCERTAIN(sample=scanned contracts doc; check PO file)"},
    {"council": "Wexford", "region": "Leinster",
     "url": "https://www.wexfordcoco.ie/council-and-democracy/procurement-finance-and-credit-control/council-spend", "kind": "xls+xlsx(tabular; old .xls needs xlrd)"},
]

HREF_RE = re.compile(r"""href\s*=\s*["']([^"']+)["']""", re.I)
DATA_EXT = (".pdf", ".xlsx", ".xls", ".csv")
# link text/url must look procurement-ish to avoid harvesting nav junk
PO_HINT = re.compile(r"purchase|p\.?o\.?s?\b|20[,]?0?00|20k|payment|supplier|procure|quarter|q[1-4]", re.I)
# an ACTUAL quarterly PO data file (vs a policy/guidance/aggregate doc) — picks the sample
DATA_FILE_RE = re.compile(r"q[1-4]\b|qtr|quarter|20[12]\d|q[1-4]\s*['’]?\d{2}", re.I)
# exclude policy/guidance docs AND aggregate returns / non-PO publications that share the page
POLICY_RE = re.compile(
    r"guide|guidelin|\bplan\b|policy|circular|strategy|manual|terms|fin.?07|"
    r"prompt.?payment|\bcontract|10.?m\b|10.?million|over.?10|appendix|procedure",
    re.I,
)


def hr(t: str) -> None:
    print(f"\n{'=' * 72}\n{t}\n{'=' * 72}")


# nav links worth following on a one-hop crawl when the landing page has no direct files
NAV_HINT = re.compile(r"purchase|procure|over.?20|20k|payment|quarter|qtr|finance|publication|spend|supplier", re.I)


def _curl(url: str) -> bytes | None:
    """Fallback fetch via curl — some council hosts (Meath/Sligo) fail Python's TLS
    stack but answer curl fine (NOT a server block). -k tolerates their cert quirks."""
    try:
        p = subprocess.run(
            ["curl", "-sS", "-k", "-L", "--max-time", "40", "-A", H["User-Agent"], url],
            capture_output=True, timeout=60,
        )
        return p.stdout if p.returncode == 0 and p.stdout else None
    except Exception:
        return None


def fetch_bytes(url: str) -> bytes | None:
    try:
        r = requests.get(url, headers=H, timeout=40)
        r.raise_for_status()
        return r.content
    except Exception:
        return _curl(url)


def fetch_text(url: str) -> str | None:
    b = fetch_bytes(url)
    return b.decode("utf-8", "ignore") if b else None


def extract_data_links(html: str, base: str) -> dict[str, list[str]]:
    hits: dict[str, list[str]] = {}
    for href in HREF_RE.findall(html):
        low = href.lower().split("?")[0]
        ext = next((e for e in DATA_EXT if low.endswith(e)), None)
        if ext and PO_HINT.search(href):
            hits.setdefault(ext, []).append(urljoin(base, href))
    return hits


def pick_sample(hits: dict[str, list[str]]) -> str | None:
    def best(urls: list[str]) -> str | None:
        good = [u for u in urls if DATA_FILE_RE.search(u) and not POLICY_RE.search(u)]
        return (good or urls)[0] if urls else None
    for e in (".xlsx", ".csv", ".xls", ".pdf"):  # prefer a tabular sample, else pdf
        if hits.get(e):
            return best(hits[e])
    return None


def harvest_links(landing: str) -> dict:
    """Harvest PO data links from the landing page; if none, one-hop crawl same-host
    nav links. Returns {ok, formats, sample, error, via}."""
    html = fetch_text(landing)
    if html is None:
        return {"ok": False, "error": "fetch failed (requests+curl)", "formats": {}, "sample": None, "via": None}
    hits = extract_data_links(html, landing)
    via = "landing"
    if not hits:
        host = urlparse(landing).netloc
        subs, seen = [], set()
        for href in HREF_RE.findall(html):
            full = urljoin(landing, href)
            low = full.lower().split("?")[0]
            if urlparse(full).netloc != host or full == landing:
                continue
            if any(low.endswith(e) for e in DATA_EXT):
                continue
            if NAV_HINT.search(href) and full not in seen:
                seen.add(full)
                subs.append(full)
        for s in subs[:6]:
            sub_html = fetch_text(s)
            if not sub_html:
                continue
            for e, v in extract_data_links(sub_html, s).items():
                hits.setdefault(e, []).extend(v)
            if hits and via == "landing":
                via = f"crawl:{s.rsplit('/', 1)[-1][:28] or urlparse(s).path[:28]}"
    return {"ok": True, "error": None, "formats": {e: len(v) for e, v in hits.items()},
            "sample": pick_sample(hits), "via": via}


def classify(url: str) -> str:
    """Download one file and report what's inside (schema / digital-vs-scanned)."""
    ext = next((e for e in DATA_EXT if url.lower().split("?")[0].endswith(e)), "")
    TMP.mkdir(parents=True, exist_ok=True)
    dest = TMP / (re.sub(r"[^A-Za-z0-9._-]", "_", url.rsplit("/", 1)[-1])[:60] or "s")
    if not dest.suffix:
        dest = dest.with_suffix(ext or ".bin")
    b = fetch_bytes(url)
    if not b:
        return "download ERR (requests+curl)"
    dest.write_bytes(b)
    try:
        if ext == ".pdf":
            import fitz
            d = fitz.open(dest)
            npages = d.page_count
            txt = d[0].get_text("text")
            d.close()
            kind = "DIGITAL(fitz)" if len(txt.strip()) > 200 else "SCANNED(OCR)"
            first = " ".join(txt.split()[:8])
            return f"PDF {npages}pp {kind} | {first[:60]}"
        if ext in (".xlsx", ".xls"):
            import openpyxl
            ws = openpyxl.load_workbook(dest, read_only=True, data_only=True).active
            rows = [r for r in ws.iter_rows(values_only=True)][:10]
            # header = the early row with the most non-empty string cells
            hdr = max(rows[:6], default=(),
                      key=lambda r: sum(isinstance(c, str) and c.strip() != "" for c in (r or ())))
            return f"XLSX header≈ {tuple(str(c)[:18] for c in hdr if c is not None)}"
        if ext == ".csv":
            import io

            import polars as pl
            df = pl.read_csv(io.BytesIO(b), infer_schema_length=0, truncate_ragged_lines=True,
                             ignore_errors=True, encoding="utf8-lossy")
            return f"CSV {df.height}rows cols={df.columns[:6]}"
    except Exception as e:
        return f"parse ERR {e!r}"[:90]
    return "unknown ext"


def main() -> None:
    hr("LOCAL-AUTHORITY PO-over-€20k SEED REGISTRY — harvest + scrape test")
    report = []
    for s in SEEDS:
        h = harvest_links(s["url"])
        line = {"council": s["council"], "region": s["region"], "url": s["url"],
                "kind": s["kind"], **h, "sample_classify": None}
        tag = "OK " if h["ok"] else "ERR"
        fmt = h["formats"] or {}
        print(f"\n[{tag}] {s['council']} ({s['region']})  kind={s['kind']}")
        print(f"     {s['url']}")
        if not h["ok"]:
            print(f"     fetch failed: {h['error']}")
        elif not fmt:
            print("     no PO-looking data links on landing page (may be a sub-page or JS-rendered)")
        else:
            print(f"     data links: {fmt}  [via {h.get('via')}]")
            if h["sample"]:
                c = classify(h["sample"])
                line["sample_classify"] = c
                print(f"     sample: {h['sample'].rsplit('/', 1)[-1][:50]}")
                print(f"       -> {c}")
        report.append(line)

    TMP.mkdir(parents=True, exist_ok=True)
    (TMP / "seed_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    hr("SUMMARY")
    ok = [r for r in report if r["ok"] and r["formats"]]
    pdf = [r for r in ok if ".pdf" in r["formats"]]
    tab = [r for r in ok if {".xlsx", ".csv", ".xls"} & set(r["formats"])]
    dead = [r for r in report if not (r["ok"] and r["formats"])]
    print(f"councils probed          : {len(report)}")
    print(f"  yielded data links     : {len(ok)}  {[r['council'] for r in ok]}")
    print(f"  have tabular (csv/xlsx): {len(tab)}  {[r['council'] for r in tab]}")
    print(f"  have PDF               : {len(pdf)}  {[r['council'] for r in pdf]}")
    print(f"  no links on landing    : {len(dead)}  {[r['council'] for r in dead]}")
    print("\nseed_report.json written to c:/tmp/procurement_la/")
    print("NOTE: 'no links on landing' usually = the files are on a sub-page or rendered")
    print("by JS; those councils need a per-site crawl, not a single-page harvest.")


if __name__ == "__main__":
    main()
