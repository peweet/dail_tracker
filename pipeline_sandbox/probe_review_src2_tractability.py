"""READ-ONLY review probe (src2 tangible sources).

Tests source tractability for the second-pass tangible-source review. Fetches
HEAD/small GETs only; writes NOTHING to gold/silver. Purpose: confirm format,
schema shape, and machine-readability of the sources flagged 'additive' or
'high-tractability' so the review's Build/Defer/Reject verdicts are grounded,
not assumed.

Run: ./.venv/Scripts/python.exe pipeline_sandbox/probe_review_src2_tractability.py
"""

from __future__ import annotations

import contextlib
import sys

import requests

with contextlib.suppress(Exception):
    sys.stdout.reconfigure(encoding="utf-8")

H = {"User-Agent": "Mozilla/5.0 (dail-tracker review probe)"}
CKAN = "https://data.gov.ie/api/3/action"


def hr(t: str) -> None:
    print(f"\n{'=' * 70}\n{t}\n{'=' * 70}")


def head(url: str) -> str:
    try:
        r = requests.head(url, headers=H, timeout=30, allow_redirects=True)
        ct = r.headers.get("content-type", "?")
        cl = r.headers.get("content-length", "?")
        return f"HTTP {r.status_code} | {ct} | {cl} bytes"
    except Exception as e:  # noqa: BLE001
        return f"ERR {type(e).__name__}: {e}"


def ckan_package(pkg: str) -> None:
    """CKAN package_show: list resources (format + url) without downloading them."""
    try:
        r = requests.get(f"{CKAN}/package_show", params={"id": pkg}, headers=H, timeout=30)
        if r.status_code != 200:
            print(f"  package_show {pkg}: HTTP {r.status_code}")
            return
        res = r.json()["result"]
        print(f"  package: {res.get('title', pkg)}  (org={res.get('organization', {}).get('name')})")
        for rs in res.get("resources", [])[:12]:
            print(f"    - {rs.get('format', '?'):<8} {rs.get('name', '')[:48]:<48} {rs.get('url', '')[:70]}")
    except Exception as e:  # noqa: BLE001
        print(f"  package_show {pkg}: ERR {e}")


def main() -> None:
    hr("1. eTenders open-data CSV ALREADY IN USE (confirm it's the same source)")
    print("  current extractor URL (procurement_etenders_extract.py:39):")
    print("  ", head("https://assets.gov.ie/static/documents/7ba65f1b/Public_Procurement_Opendata_Dataset.csv"))

    hr("2. Mini-comp / standalone award CSVs (data.gov.ie CKAN)")
    # the doc cites resource IDs under these packages
    for pkg in (
        "contracts-for-mini-competitions-and-standalone-awards-2023-q1-q4",
        "contracts-for-mini-competitions-and-standalone-awards-2024-q2",
    ):
        ckan_package(pkg)

    hr("3. Social Housing Construction Status Report (CSR) — XLSX/CSV")
    ckan_package("social-housing-construction-status-report-q4-2025")

    hr("4. DPC fines table (HTML page — is it a real table?)")
    try:
        r = requests.get("https://www.dataprotection.ie/en/dpc-guidance/decisions/fines", headers=H, timeout=30)
        txt = r.text
        print(f"  HTTP {r.status_code} | {len(txt):,} chars | <table> count={txt.lower().count('<table')}")
        print(f"  '€' occurrences={txt.count('€')} | 'inquiry' occurrences={txt.lower().count('inquiry')}")
    except Exception as e:  # noqa: BLE001
        print(f"  ERR {e}")

    hr("5. CSO Register of Public Sector Bodies")
    print("  release page:", head("https://www.cso.ie/en/releasesandpublications/ep/p-rpbi/registerofpublicsectorbodies2024-final/centralgovernment/"))

    hr("6. NWRA ERDF beneficiaries XLSX")
    for u in (
        "https://www.nwra.ie/wp-content/uploads/2025/11/2021-2027-beneficiaries-Oct-2025.xlsx",
        "https://www.nwra.ie/wp-content/uploads/2024/03/march-2024-listing.xlsx",
    ):
        print(f"  {u[-40:]}: {head(u)}")

    hr("7. DHLGH planning open data (ArcGIS REST / GeoJSON)")
    print("  opendata.housing.gov.ie planning tag (HTML index):")
    print("  ", head("https://opendata.housing.gov.ie/dataset/?tags=planning"))

    hr("8. NOAC Performance Indicator Report (PDF — is it tabular/digital?)")
    print("  PI 2024 PDF:", head("https://cdn.noac.ie/wp-content/uploads/2025/11/NOAC-Local-Authority-Performance-Indicator-Report-2024_FINAL.pdf"))

    print("\nDONE (read-only).")


if __name__ == "__main__":
    main()
