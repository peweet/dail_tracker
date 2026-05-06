"""
payments_2019_backfill_probe.py — discover & download 2019 PSA PDFs

STATUS: SANDBOX. Discovery-only by default. Set DOWNLOAD = True to fetch.

WHY
---
Our bronze coverage starts at January 2020 (PDF published 2020-03-01). gript's
"since 2019" PSA total (€28.68M) implies they have monthly disclosures for
January-December 2019 that we don't.

A HEAD probe against data.oireachtas.ie confirms the December 2019 PSA PDF
exists at:

    https://data.oireachtas.ie/ie/oireachtas/members/parliamentaryAllowances/
    psa/2020/2020-02-01_parliamentary-standard-allowance-payments-to-deputies-
    for-december-2019_en.pdf

i.e. the 2019-period PDFs were *published* in early 2020 and live under the
2020 folder. This script enumerates the 12 months of 2019 and HEAD-checks each
candidate URL using the same LAG_MIN / LAG_MAX heuristic as
payment_pdf_url_probe.py.

WHAT THIS DOES NOT DO
---------------------
- Does not run on import or pipeline.py — invoke directly.
- Does not write into bronze unless DOWNLOAD = True.
- Does not parse the PDFs — that's payments_full_psa_etl.py's job. After
  download, re-run payments_full_psa_etl.py to fold the new PDFs into the
  gold parquet.

USAGE
-----
    # Discover only (default)
    python pipeline_sandbox/payments_2019_backfill_probe.py

    # Download missing PDFs into data/bronze/pdfs/payments/
    python pipeline_sandbox/payments_2019_backfill_probe.py --download
"""
from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import requests

PROJECT_ROOT = Path(__file__).resolve().parents[1]
BRONZE_PDF_DIR = PROJECT_ROOT / "data" / "bronze" / "pdfs" / "payments"

# Re-use the discovery primitives from the existing probe so we share the
# lag-window logic. This file's contribution is the year-range enumeration.
sys.path.insert(0, str(PROJECT_ROOT))
from pipeline_sandbox.payment_pdf_url_probe import (  # noqa: E402
    construct_candidates,
    head_check,
    _session,
)

MONTHS_2019 = list(range(1, 13))


def existing_bronze_pdf_for(year: int, month: int) -> Path | None:
    """Return the bronze PDF path for the given period if already downloaded."""
    month_name = [
        "january","february","march","april","may","june",
        "july","august","september","october","november","december",
    ][month - 1]
    matches = sorted(BRONZE_PDF_DIR.glob(
        f"*_parliamentary-standard-allowance-payments-to-deputies-for-{month_name}-{year}_en.pdf"
    ))
    return matches[0] if matches else None


def discover_2019(session: requests.Session, max_attempts: int = 80) -> dict[int, str | None]:
    """For each month in 2019, return the first URL that 200s, or None.

    Skips months whose PDF is already in bronze.
    """
    found: dict[int, str | None] = {}
    for month in MONTHS_2019:
        already = existing_bronze_pdf_for(2019, month)
        if already is not None:
            print(f"  {month:02d}/2019: already in bronze ({already.name})")
            found[month] = None
            continue

        print(f"  {month:02d}/2019: probing...")
        attempts = 0
        hit: str | None = None
        for candidate in construct_candidates(2019, month):
            if attempts >= max_attempts:
                break
            status = head_check(session, candidate.url)
            attempts += 1
            if status == 200:
                hit = candidate.url
                break
        if hit:
            print(f"    found ({attempts} HEADs): {hit}")
        else:
            print(f"    no candidate URL returned 200 after {attempts} HEADs")
        found[month] = hit
    return found


def download_pdf(session: requests.Session, url: str) -> Path | None:
    """Download `url` into BRONZE_PDF_DIR. Returns the saved path or None."""
    BRONZE_PDF_DIR.mkdir(parents=True, exist_ok=True)
    filename = url.rsplit("/", 1)[-1]
    target = BRONZE_PDF_DIR / filename
    if target.exists():
        return target
    try:
        resp = session.get(url, timeout=(10, 60))
    except requests.RequestException as e:
        print(f"    download failed: {e}")
        return None
    if resp.status_code != 200:
        print(f"    download returned {resp.status_code}")
        return None
    target.write_bytes(resp.content)
    return target


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--download", action="store_true",
                    help="Download discovered URLs into data/bronze/pdfs/payments/")
    args = ap.parse_args()

    session = _session()

    print("Probing for 2019 PSA monthly PDFs on data.oireachtas.ie")
    print(f"Bronze target: {BRONZE_PDF_DIR}")
    print()

    found = discover_2019(session)
    print()

    hits = {m: u for m, u in found.items() if u}
    print(f"Discovered {len(hits)}/12 months for 2019")

    if args.download and hits:
        print()
        print("Downloading...")
        for month, url in sorted(hits.items()):
            saved = download_pdf(session, url)
            if saved:
                print(f"  {month:02d}/2019 -> {saved.name}")
            time.sleep(0.5)  # gentle on the host
    elif hits:
        print()
        print("Re-run with --download to fetch.")
    return 0 if hits else 1


if __name__ == "__main__":
    sys.exit(main())
