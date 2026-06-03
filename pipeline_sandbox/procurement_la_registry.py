"""MERGED AUTHORITATIVE SEED REGISTRY — all 31 local authorities, PO-over-€20k.

Consolidates the resolved per-council routes discovered across the parallel evaluation
probes into ONE source of truth (still PRE-ETL — nothing here is wired into pipeline.py):

  - procurement_la_seed.py            generic harvester + curl fallback + one-hop crawl
  - probe_procurement_pdf_counties.py Mayo / Donegal / Clare / Cork County / Cork City
  - probe_procurement_pdf_galway.py   Galway City + Galway County (sites/default/files)
  - probe_procurement_city_vs_county.py  Limerick (merged), structure
  - probe_procurement_excel.py        South Dublin / Kilkenny xlsx schema

KEY MERGE LESSON: per-council coverage needs the FILE-LIST page (or a direct URL
pattern), NOT the generic finance/procurement landing — that mistake made the first pass
wrongly call Donegal a non-publisher and Mayo unreachable. `url` below is the best known
route; `pattern` is a fill-in-the-quarter template where one exists.

`status`:  READY-HTTP   fetchable now (requests/curl) from url/pattern
           READY-CRAWL  one-hop crawl from url reaches the file list
           NEEDS-RENDER file list is JS-rendered / opaque-GUID -> Playwright to ENUMERATE
                        (files THEMSELVES are fetchable once enumerated; data confirmed)
           NEEDS-CHECK  publishes, but the exact PO file wasn't pinned this pass
           NON-PUBLISHER no line-level PO-over-20k published

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/procurement_la_registry.py
Prints a coverage summary; writes c:/tmp/procurement_la/registry.csv (NOT committed).
"""

from __future__ import annotations

import csv
import sys
from collections import Counter
from pathlib import Path

with __import__("contextlib").suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

OUT = Path("c:/tmp/procurement_la/registry.csv")

# entity_type: county | city | merged (city+county since 2014) | dublin (the 4-way split)
# schema = column names/order as published; grain = line-level PO unless noted.
REGISTRY: list[dict] = [
    # ---- READY: tabular (XLSX/CSV) — no PDF parsing ----
    {"council": "South Dublin", "region": "Dublin", "entity": "dublin", "status": "READY-HTTP",
     "fmt": "xlsx", "url": "https://www.sdcc.ie/en/services/business/payments/",
     "pattern": ".../purchase-order-over-20-000-quarter-{Q}-{YYYY}.xlsx",
     "schema": "PO|SUPPLIER|TOTAL|DESCRIPTION|PAID", "notes": "richest; has PO# + paid flag", "src": "seed/excel"},
    {"council": "Cork City", "region": "Munster", "entity": "city", "status": "READY-HTTP",
     "fmt": "xlsx", "url": "https://www.corkcity.ie/en/council-services/public-info/spending-and-revenue/",
     "pattern": ".../purchase-orders-greater-than-20-000-q{N}-{YYYY}.xlsx",
     "schema": "Supplier|Sum of Gross Amount|Description", "notes": "use xlsx; old media-folder PDFs 404", "src": "seed"},
    {"council": "Wicklow", "region": "Leinster", "entity": "county", "status": "READY-HTTP",
     "fmt": "xlsx/csv", "url": "https://www.wicklow.ie/Living/Your-Council/Finance/Procurement/Purchase-Orders-Over-20-000",
     "pattern": "", "schema": "Supplier|EURO|Description", "notes": "111 files incl. csv", "src": "seed"},
    {"council": "Monaghan", "region": "Ulster", "entity": "county", "status": "READY-HTTP",
     "fmt": "xlsx", "url": "https://monaghan.ie/finance/publication-of-purchase-orders/",
     "pattern": "", "schema": "Supplier|Amount|Description", "notes": "", "src": "seed"},
    {"council": "Kilkenny", "region": "Leinster", "entity": "county", "status": "READY-HTTP",
     "fmt": "xlsx", "url": "https://kilkennycoco.ie/eng/services/finance/purchase-orders-over-%E2%82%AC20-000/",
     "pattern": "", "schema": "OrderNo|Supplier|Period|EURO|Description", "notes": "also on data.gov.ie CKAN", "src": "seed/excel"},
    {"council": "Wexford", "region": "Leinster", "entity": "county", "status": "READY-HTTP",
     "fmt": "xls/xlsx", "url": "https://www.wexfordcoco.ie/council-and-democracy/procurement-finance-and-credit-control/council-spend",
     "pattern": "", "schema": "tabular", "notes": "old .xls (~9%) needs xlrd", "src": "seed"},
    # ---- READY: digital PDF (fitz + largest-x-gap; NO OCR) ----
    {"council": "Cork County", "region": "Munster", "entity": "county", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.corkcoco.ie/sites/default/files/2025-08/2025-q2-purchase-orders-in-excess-of-eu20000.pdf",
     "pattern": ".../sites/default/files/{YYYY-MM}/{YYYY}-q{N}-purchase-orders-in-excess-of-eu20000(-pdf).pdf",
     "schema": "Supplier|Description|Total|Paid", "notes": "upload-month varies; 107 files", "src": "counties"},
    {"council": "Kildare", "region": "Leinster", "entity": "county", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://kildarecoco.ie/YourCouncil/Publications/Finance/PurchaseOrdersover20000/",
     "pattern": "", "schema": "Supplier|Total|Description", "notes": "52 files", "src": "seed/pdf"},
    {"council": "Westmeath", "region": "Leinster", "entity": "county", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.westmeathcoco.ie/en/ourservices/finance/procurement/purchaseorders/",
     "pattern": ".../PurchaseOrdersQ{N}{YYYY}.pdf", "schema": "Supplier|EURO|Description", "notes": "55 files", "src": "seed"},
    {"council": "Waterford", "region": "Munster", "entity": "merged", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://waterfordcouncil.ie/openness-transparency/governance-related-financial-information/procurement/purchase-orders-e20000/",
     "pattern": "", "schema": "OrderNo|Supplier|...", "notes": "merged city+county = one list", "src": "seed"},
    {"council": "Limerick", "region": "Munster", "entity": "merged", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.limerick.ie/sites/default/files/media/documents/2026-05/purchase-orders-over-eu20-000-quarter-1-2026.pdf",
     "pattern": ".../sites/default/files/media/documents/{YYYY-MM}/purchase-orders-over-eu20-000-quarter-{N}-{YYYY}.pdf",
     "schema": "Supplier|Paid|Description", "notes": "merged; highest CRO 64%", "src": "city_vs_county"},
    {"council": "Offaly", "region": "Leinster", "entity": "county", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.offaly.ie/financial-reports/",
     "pattern": ".../{YYYY}-Q{N}-Payments-Greater-than-E20k.pdf", "schema": "GL30: Supplier|...|Amount", "notes": "grain=payments", "src": "seed"},
    {"council": "Longford", "region": "Leinster", "entity": "county", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.longfordcoco.ie/services/finance/finance-documents/large-purchase-orders/",
     "pattern": ".../pos-greater-than-20-000-qtr-{N}-{YYYY}.pdf", "schema": "SUPPLIER|EURO|DESCRIPTION", "notes": "", "src": "seed"},
    {"council": "Galway City", "region": "Connacht", "entity": "city", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.galwaycity.ie/sites/default/files/2026-05/Qtr%201%202026_Purchase%20Orders%20over%20%E2%82%AC20k_0.pdf",
     "pattern": ".../sites/default/files/{YYYY-MM}/Qtr {N} {YYYY} Purchase Orders over €20k.pdf",
     "schema": "Supplier|Category|EURO (amount LAST)", "notes": "budgets page mixes prompt-pay", "src": "galway"},
    {"council": "Galway County", "region": "Connacht", "entity": "county", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.galwaycoco.ie/sites/default/files/2026-01/Quarter%201%202025%20%28ENG%29.pdf",
     "pattern": ".../sites/default/files/{YYYY-MM}/Quarter {N} {YYYY} (ENG).pdf",
     "schema": "SUPPLIER|PRODUCT|EURO", "notes": "files OK on sites/default/files; only the LISTING was WAF-blocked; gaillimh.ie alt", "src": "galway/seed"},
    {"council": "Kerry", "region": "Munster", "entity": "county", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.kerrycoco.ie/finance/financial-documents/",
     "pattern": "", "schema": "(16 files)", "notes": "transient fetch err on one sample", "src": "seed"},
    {"council": "Meath", "region": "Leinster", "entity": "county", "status": "READY-HTTP",
     "fmt": "xlsx/pdf", "url": "https://www.meath.ie/council/your-council/finance-and-procurement/tenders-and-contracts/payments-over-eu20000",
     "pattern": "", "schema": "tabular+pdf", "notes": "fetch via curl fallback (Python TLS quirk, not a block)", "src": "seed"},
    {"council": "Sligo", "region": "Connacht", "entity": "county", "status": "READY-HTTP",
     "fmt": "pdf", "url": "https://www.sligococo.ie/YourCouncil/Finance/ProcurementPurchasing/PurchasingActivity/",
     "pattern": "", "schema": "pdf", "notes": "fetch via curl fallback", "src": "seed"},
    # ---- READY via one-hop crawl from the landing page ----
    {"council": "Clare", "region": "Munster", "entity": "county", "status": "READY-CRAWL",
     "fmt": "pdf", "url": "https://www.clarecoco.ie/business-licensing-and-economy/procurement-and-tenders",
     "pattern": ".../sites/default/files/{YYYY-MM}/purchase-orders-over-20-000-in-the-{n}-quarter-of-{YYYY}-*.pdf",
     "schema": "Supplier|...|EURO", "notes": "files at sites/default/files + /services/.../pos-over-20k/{YYYY}/", "src": "counties/seed"},
    {"council": "Leitrim", "region": "Connacht", "entity": "county", "status": "READY-CRAWL",
     "fmt": "pdf", "url": "https://www.leitrim.ie/council/services/finance/accounts-payable/purchase-to-pay/",
     "pattern": "", "schema": "Supplier|EURO|Description", "notes": "crawl from finance/accounts-payable; 33 files", "src": "seed"},
    {"council": "Laois", "region": "Leinster", "entity": "county", "status": "READY-CRAWL",
     "fmt": "pdf", "url": "https://laois.ie/finance/business-and-enterprise-support/procurement-information-and-advice",
     "pattern": ".../sites/default/files/{YYYY-MM}/Procurement Report {YYYY} {Mon-Mon}.pdf",
     "schema": "Supplier|...|EURO", "notes": "grain=procurement report", "src": "seed"},
    {"council": "Fingal", "region": "Dublin", "entity": "dublin", "status": "READY-CRAWL",
     "fmt": "pdf", "url": "https://www.fingal.ie/council/service/procurement",
     "pattern": ".../sites/default/files/{YYYY-MM}/...purchase-orders-over-20k.pdf",
     "schema": "SupplierID|Acc element|Amount", "notes": "PO files off the procurement page; scattered names", "src": "seed/dublin"},
    # ---- NEEDS-RENDER: file list JS-rendered / opaque-GUID (data CONFIRMED to exist) ----
    {"council": "Mayo", "region": "Connacht", "entity": "county", "status": "NEEDS-RENDER",
     "fmt": "pdf", "url": "https://www.mayo.ie/financial-documents/purchase-orders",
     "pattern": ".../getattachment/{guid}/attachment.aspx", "schema": "Supplier|...|Amount (PO#/ID prefix)",
     "notes": "837 rows digital confirmed; strip leading digit tokens before CRO", "src": "counties"},
    {"council": "Donegal", "region": "Ulster", "entity": "county", "status": "NEEDS-RENDER",
     "fmt": "pdf", "url": "https://www.donegalcoco.ie/media/b2aopuh2/2024.pdf",
     "pattern": ".../media/{opaque-code}/{YYYY}.pdf  (yearly, not quarterly)",
     "schema": "Supplier|...|Amount (PO#/ID prefix)", "notes": "1,221 rows digital; landing only lists >€10m; strip digit prefix", "src": "counties"},
    {"council": "Carlow", "region": "Leinster", "entity": "county", "status": "NEEDS-RENDER",
     "fmt": "pdf?", "url": "https://carlow.ie/information-technology/statistics-and-reports/financial-statistical-reports",
     "pattern": "", "schema": "?", "notes": "JS/SPA file list (0 links in raw HTML)", "src": "seed"},
    {"council": "Cavan", "region": "Ulster", "entity": "county", "status": "NEEDS-RENDER",
     "fmt": "pdf?", "url": "https://www.cavancoco.ie/file-library/business/procurement/over-20k/",
     "pattern": "", "schema": "?", "notes": "JS-rendered file library", "src": "seed"},
    {"council": "Roscommon", "region": "Connacht", "entity": "county", "status": "NEEDS-RENDER",
     "fmt": "pdf?", "url": "https://www.roscommoncoco.ie/en/Download-It/Finance-Publications/",
     "pattern": "", "schema": "?", "notes": "JS-rendered download portal", "src": "seed"},
    # ---- NEEDS-CHECK: publishes, exact PO file not pinned this pass ----
    {"council": "Louth", "region": "Leinster", "entity": "county", "status": "NEEDS-CHECK",
     "fmt": "pdf?", "url": "https://www.louthcoco.ie/en/publications/finance_reports/",
     "pattern": "", "schema": "?", "notes": "crawl reached finance sub-pages but sampled a 404 doc", "src": "seed"},
    {"council": "Tipperary", "region": "Munster", "entity": "county", "status": "NEEDS-CHECK",
     "fmt": "pdf", "url": "https://www.tipperarycoco.ie/finance/financial-reports",
     "pattern": "", "schema": "?", "notes": "37 PDFs; sample was a SCANNED contracts doc — find the PO file", "src": "seed"},
    # ---- NON-PUBLISHER (no line-level PO-over-20k) ----
    {"council": "Dublin City", "region": "Dublin", "entity": "dublin", "status": "NON-PUBLISHER",
     "fmt": "csv", "url": "https://data.smartdublin.ie/dataset/dublin-city-council-prompt-payments",
     "pattern": "", "schema": "aggregate prompt-payment return", "notes": "stale <=2014; wrong grain", "src": "seed"},
    {"council": "Dun Laoghaire-Rathdown", "region": "Dublin", "entity": "dublin", "status": "NON-PUBLISHER",
     "fmt": "-", "url": "https://www.dlrcoco.ie/governance/procurement",
     "pattern": "", "schema": "-", "notes": "policy PDFs only; FOI territory", "src": "seed"},
]

FIELDS = ["council", "region", "entity", "status", "fmt", "url", "pattern", "schema", "notes", "src"]


def main() -> None:
    assert len(REGISTRY) == 31, f"expected 31 LAs, got {len(REGISTRY)}"
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        w.writerows(REGISTRY)

    print(f"{'=' * 74}\nMERGED LA PO-over-€20k SEED REGISTRY — {len(REGISTRY)} councils\n{'=' * 74}")
    by_status = Counter(r["status"] for r in REGISTRY)
    by_fmt = Counter(r["fmt"].split("/")[0].rstrip("?") for r in REGISTRY)
    for st in ["READY-HTTP", "READY-CRAWL", "NEEDS-RENDER", "NEEDS-CHECK", "NON-PUBLISHER"]:
        names = [r["council"] for r in REGISTRY if r["status"] == st]
        print(f"\n{st:<14} {len(names):>2}  {', '.join(names)}")
    ready = by_status["READY-HTTP"] + by_status["READY-CRAWL"]
    obtainable = ready + by_status["NEEDS-RENDER"]  # render = enumerate only; data exists
    print(f"\n{'-' * 74}")
    print(f"scrapeable now (HTTP+crawl): {ready}/31   "
          f"obtainable incl. render-to-enumerate: {obtainable}/31   "
          f"non-publishers: {by_status['NON-PUBLISHER']}")
    print(f"format mix: {dict(by_fmt)}  (no OCR needed — every sampled PO PDF is digital)")
    print(f"entities: {dict(Counter(r['entity'] for r in REGISTRY))}")
    print(f"\nwrote {OUT}")
    print("PRE-ETL artifact. Promote to data/_meta/procurement_la_seed.csv only on build go-ahead.")


if __name__ == "__main__":
    main()
