"""PROBE (throwaway): can the OFF-PORTAL local-authority spend data be harvested?

The data.gov.ie / data.smartdublin.ie portals carry almost no council Purchase-Order
spend (see probe_procurement_pdf.py census: 3 publishers nationally). The Circular
05/2023 obligation is met on each council's OWN website instead. This probe tests the
four Dublin local authorities to see whether those off-portal sources are enumerable,
what format/schema/grain each uses, and how uniform (or not) they are across one region.

Findings are printed; no repo data written (samples cached to c:/tmp/dublin_la).

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/probe_procurement_dublin_la.py
"""

from __future__ import annotations

import contextlib
import sys
from pathlib import Path

import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

H = {"User-Agent": "dail-tracker research probe"}
TMP = Path("c:/tmp/dublin_la")
SMART = "https://data.smartdublin.ie/api/3/action"


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def head(url: str) -> tuple[int, int]:
    """(status, bytes) without downloading the body where possible."""
    try:
        r = requests.get(url, headers=H, timeout=40, stream=True, allow_redirects=True)
        n = int(r.headers.get("Content-Length") or 0)
        r.close()
        return r.status_code, n
    except Exception as e:
        print(f"    ERR {e!r}")
        return 0, 0


def portal_spend_coverage() -> None:
    """What spend data do the 4 Dublin LAs publish to the SmartDublin CKAN portal?"""
    hr("1. SmartDublin CKAN portal — spend coverage per Dublin LA")
    orgs = [
        "dublin-city-council",
        "fingal-county-council",
        "south-dublin-county-council",
        "dun-laoghaire-rathdown-county-council",
    ]
    kw = ("payment", "purchase", "procure", "spend", "invoice", "supplier")
    for org in orgs:
        r = requests.get(
            f"{SMART}/package_search",
            params={"fq": f"organization:{org}", "rows": 1000},
            headers=H, timeout=60,
        ).json()["result"]
        spend = [d["title"] for d in r["results"] if any(w in d["title"].lower() for w in kw)]
        print(f"  {org:<40} {r['count']:>3} datasets | spend: {spend or '—'}")
    print("  => portal is NOT the route: only DCC 'Prompt Payments' exists, and it is")
    print("     an AGGREGATE return that stops in 2014 (see section 4).")


def sdcc_template() -> None:
    """South Dublin — clean enumerable XLSX URL template, line-level POs."""
    hr("2. South Dublin CoCo — XLSX template (BEST: enumerable + richest schema)")
    base = "https://www.sdcc.ie/en/services/business/payments"
    ok = 0
    for yr in (2025, 2024, 2023, 2022, 2021):
        line = []
        for q in (1, 2, 3, 4):
            url = f"{base}/purchase-order-over-20-000-quarter-{q}-{yr}.xlsx"
            code, n = head(url)
            line.append(f"Q{q}:{code}")
            ok += code == 200
        print(f"  {yr}: {'  '.join(line)}")
    print(f"  template resolves for ~{ok} quarter-files (some years use a name variant).")
    TMP.mkdir(parents=True, exist_ok=True)
    sample = TMP / "sdcc_q1_2025.xlsx"
    if not sample.exists():
        sample.write_bytes(requests.get(
            f"{base}/purchase-order-over-20-000-quarter-1-2025.xlsx", headers=H, timeout=60).content)
    try:
        import openpyxl
        ws = openpyxl.load_workbook(sample, read_only=True, data_only=True).active
        rows = list(ws.iter_rows(values_only=True))
        print(f"  schema (row 2): {rows[1]}")
        print(f"  sample row    : {rows[2]}")
        print(f"  ~{len(rows) - 2} PO rows/quarter | grain = ONE purchase order; has PO#, € TOTAL, PAID flag")
    except Exception as e:
        print(f"  xlsx read ERR {e!r}")


def fingal_pdfs() -> None:
    """Fingal — digital PDFs, scattered URLs (needs a listing scrape, not a template)."""
    hr("3. Fingal CoCo — digital PDFs (fitz-extractable, but URLs are scattered)")
    known = [
        "https://www.fingal.ie/sites/default/files/2024-09/q2-pos-over-20k-2023.pdf",
        "https://www.fingal.ie/sites/default/files/2024-08/q3-2022-purchase-orders-over-20k.pdf",
        "https://www.fingal.ie/sites/default/files/2019-03/2017%20Q4%20Purchase%20Orders%20over%2020k.pdf",
    ]
    for u in known:
        code, n = head(u)
        print(f"  {code}  {u.rsplit('/', 1)[-1]}")
    print("  naming is INCONSISTENT (q2-pos-over-20k-2023 / 2017 Q4 Purchase Orders / 2015q4_…)")
    print("  and the upload-month folder varies → no URL template; must scrape a listing page.")
    TMP.mkdir(parents=True, exist_ok=True)
    p = TMP / "fingal_q2_2023.pdf"
    if not p.exists():
        p.write_bytes(requests.get(known[0], headers=H, timeout=60).content)
    try:
        import fitz
        d = fitz.open(p)
        txt = d[0].get_text("text")
        d.close()
        kind = "DIGITAL (no OCR)" if len(txt.strip()) > 200 else "SCANNED (needs OCR)"
        print(f"  {kind} | cols: SupplierID(T) · Acc element(T) · Amount(C); € rendered as �")
    except Exception as e:
        print(f"  pdf read ERR {e!r}")


def dcc_and_dlr() -> None:
    hr("4. Dublin City (stale) & Dún Laoghaire-Rathdown (absent)")
    d = requests.get(f"{SMART}/package_show",
                     params={"id": "dublin-city-council-prompt-payments"},
                     headers=H, timeout=60).json()["result"]
    names = sorted(x.get("name", "") for x in d["resources"])
    print(f"  DCC 'Prompt Payments': {len(d['resources'])} resources, range "
          f"{names[0]} … {names[-1]}")
    print("    => ABANDONED after 2014 + it's an AGGREGATE prompt-payment return,")
    print("       NOT line-level POs. Wrong grain and stale; open route is a dead end.")
    print("  DLR: procurement page carries only policy PDFs (Procurement Plan/guide);")
    print("       no PO/payment listing surfaced → FOI territory, not open data.")


def main() -> None:
    portal_spend_coverage()
    sdcc_template()
    fingal_pdfs()
    dcc_and_dlr()
    hr("VERDICT — four councils, four different realities (no uniformity)")
    print("  SDCC  : XLSX, clean URL template, 2020+, richest schema (PO#/€/PAID)  ✅")
    print("  Fingal: digital PDF, 2013–2024, scattered URLs (listing scrape + fitz) ✅")
    print("  DCC   : only a stale (≤2014) AGGREGATE prompt-payment CSV on portal     ⚠️")
    print("  DLR   : nothing published openly                                        ❌")
    print("  => off-portal harvest is BESPOKE PER COUNCIL: different host, format,")
    print("     URL pattern, schema and grain even within ONE region. 31 LAs = 31")
    print("     mini-scrapers, ~half usable, no shared schema. Portal captures ~none.")


if __name__ == "__main__":
    main()
