"""cro_poller.py — fetch the CRO bulk company register, repeatably.

Replaces the manual "drop a companies_*.csv into data/bronze/cro/" step. The CRO
open-data portal (opendata.cro.ie, a CKAN instance) republishes the full company
register as a zipped CSV **daily**. This poller:

    1. resolves the current CSV resource via the CKAN package API (the stable
       slug `companies`; resource IDs rotate, the slug does not);
    2. skips the download when the portal's ``last_modified`` is not newer than
       the snapshot we already hold (idempotent — safe to run every pipeline);
    3. downloads the zip (the CKAN URL 302-redirects to a presigned object-store
       URL that is GET-only, so we follow redirects and never HEAD), extracts the
       CSV, and validates its header + row count;
    4. writes data/bronze/cro/companies_YYYYMMDD.csv (date = the portal's
       last_modified) for cro_normalise.py to consume.

Source: https://opendata.cro.ie/dataset/companies

Exit codes (poller convention, cf. lobbying_poller / iris_oifigiuil_poller):
    0  ok — downloaded a new snapshot, or already current
    1  transient failure (network / IO) — safe to retry
    2  source drift / human needed — CKAN structure changed, no CSV resource,
       header schema mismatch, or row floor not met (do NOT overwrite bronze)

CLI:
    python cro_poller.py                # poll + download if newer
    python cro_poller.py --force        # download even if we already hold it
    python cro_poller.py --check-only   # report whether an update is available; no download
"""

from __future__ import annotations

import argparse
import contextlib
import datetime as dt
import io
import logging
import os
import re
import sys
import tempfile
import zipfile
from pathlib import Path

import requests

from config import BRONZE_DIR

logger = logging.getLogger("cro_poller")

# ── API alternative (kept as bulk for now; see note) ─────────────────────────
# The same portal exposes a CKAN DataStore over this resource (datastore_active=
# True), so the full register is queryable live WITHOUT the bulk download:
#   search: GET  {CKAN_BASE}/api/3/action/datastore_search?resource_id=<id>&q=...
#   SQL:    POST {CKAN_BASE}/api/3/action/datastore_search_sql   (arbitrary SQL,
#           e.g. SELECT ... FROM "<resource_id>" WHERE company_name ILIKE 'X%')
# The DataStore is the cleaner path for one-off / interactive TARGETED lookups
# (resolve a single company by num/name). It is NOT better for the batch
# supplier->CRO match: that join is thousands of names against the register and
# relies on the local `name_norm` key (the server has no normalisation), so the
# bulk-zip + Polars join stays the right tool there. We keep the bulk zip for
# the FULL-register silver join: one ~46MB fetch beats ~26 paginated API pages of
# 815k rows. Sibling dataset `financial-statements` explored 2026-06-04 — a
# filing INDEX (PDF pointer + dates), not financial figures; see
# doc/CRO_FINANCIAL_STATEMENTS_EXPLORATION.md.

# ── source config (read by tools/build_source_registry.py) ───────────────────
CKAN_BASE = "https://opendata.cro.ie"
PACKAGE_ID = "companies"
DEST_DIR = BRONZE_DIR / "cro"
USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"

# validation floors — the register is ~815k rows / ~190MB unzipped. Floors reject
# error pages / truncated transfers without being brittle to organic growth.
MIN_ROWS = 700_000
MIN_ZIP_BYTES = 5_000_000

# the 21 columns cro_normalise.py's schema check depends on (exact set).
EXPECTED_COLUMNS = {
    "company_num", "company_name", "company_status_code", "company_status",
    "company_type_code", "company_type", "company_reg_date", "last_ar_date",
    "company_address_1", "company_address_2", "company_address_3", "company_address_4",
    "comp_dissolved_date", "nard", "last_accounts_date", "company_status_date",
    "nace_v2_code", "eircode", "company_name_eff_date", "company_type_eff_date",
    "princ_object_code",
}

# what the source-health registry needs to know about this source.
SOURCE_META = {
    "source_id": "cro_companies",
    "name": "CRO bulk company register",
    "owner_module": "cro_poller",
    "ckan_base": CKAN_BASE,
    "package_id": PACKAGE_ID,
    "input_pattern": "data/bronze/cro/companies_*.csv",
    # automated daily upstream → a snapshot older than a week means the poller
    # (not the operator) has stopped running.
    "stale_after_days": 7,
}

_DATE_RE = re.compile(r"companies_(\d{8})\.csv$", re.I)


class SourceDrift(Exception):
    """Raised when the CKAN structure or file schema no longer matches; exit 2."""


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def resolve_resource(session: requests.Session) -> dict:
    """Return {url, last_modified (date), resource_id} for the current CSV
    resource. Raises SourceDrift if the package has no CSV resource."""
    api = f"{CKAN_BASE}/api/3/action/package_show"
    r = session.get(api, params={"id": PACKAGE_ID}, timeout=60)
    r.raise_for_status()
    body = r.json()
    if not body.get("success"):
        raise SourceDrift(f"CKAN package_show(id={PACKAGE_ID}) returned success=false")
    resources = body.get("result", {}).get("resources", [])
    csv_res = next((res for res in resources
                    if (res.get("format") or "").upper() == "CSV"), None)
    if csv_res is None:
        fmts = sorted({(res.get("format") or "?") for res in resources})
        raise SourceDrift(f"no CSV resource in package {PACKAGE_ID!r}; formats present: {fmts}")
    url = csv_res.get("url")
    if not url:
        raise SourceDrift("CSV resource has no download url")
    lm_raw = csv_res.get("last_modified") or csv_res.get("created")
    last_modified = _parse_ckan_date(lm_raw)
    return {"url": url, "last_modified": last_modified, "resource_id": csv_res.get("id")}


def _parse_ckan_date(raw: str | None) -> dt.date:
    """CKAN timestamps look like '2026-06-04T04:01:43.446029'. Fall back to today
    if absent so a missing field never blocks a refresh."""
    if not raw:
        return dt.date.today()
    try:
        return dt.datetime.fromisoformat(raw).date()
    except ValueError:
        return dt.datetime.strptime(raw[:10], "%Y-%m-%d").date()


def latest_local_date() -> dt.date | None:
    """Newest companies_YYYYMMDD.csv date already in bronze, or None."""
    dates = []
    for p in DEST_DIR.glob("companies_*.csv"):
        m = _DATE_RE.search(p.name)
        if m:
            with contextlib.suppress(ValueError):
                dates.append(dt.datetime.strptime(m.group(1), "%Y%m%d").date())
    return max(dates) if dates else None


def download_zip(session: requests.Session, url: str, dest: Path) -> int:
    """Stream the (redirecting) zip URL to ``dest``. Returns bytes written."""
    written = 0
    with session.get(url, stream=True, allow_redirects=True, timeout=(15, 600)) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                if chunk:
                    f.write(chunk)
                    written += len(chunk)
    return written


def _count_rows(path: Path) -> int:
    """Fast newline count minus the header row."""
    n = 0
    with open(path, "rb") as f:
        while block := f.read(1 << 20):
            n += block.count(b"\n")
    return max(n - 1, 0)


def extract_and_validate(zip_path: Path, out_csv: Path) -> int:
    """Extract the CSV member, validate header + row floor, write to ``out_csv``.
    Returns the data row count. Raises SourceDrift on schema/row problems."""
    if zip_path.stat().st_size < MIN_ZIP_BYTES:
        raise SourceDrift(f"zip too small ({zip_path.stat().st_size} bytes < {MIN_ZIP_BYTES})")
    with zipfile.ZipFile(zip_path) as zf:
        members = [n for n in zf.namelist() if n.lower().endswith(".csv")]
        if not members:
            raise SourceDrift(f"no .csv inside zip; members={zf.namelist()}")
        member = members[0]
        # header check before committing to a full extract
        with zf.open(member) as fh:
            header = io.TextIOWrapper(fh, encoding="utf-8").readline().strip()
        cols = {c.strip() for c in header.split(",")}
        missing = EXPECTED_COLUMNS - cols
        if missing:
            raise SourceDrift(f"header missing expected columns: {sorted(missing)}")
        # extract to a temp sibling, validate row count, then atomic rename in
        tmp = out_csv.with_suffix(".csv.partial")
        with zf.open(member) as fh, open(tmp, "wb") as out:
            while block := fh.read(1 << 20):
                out.write(block)
    rows = _count_rows(tmp)
    if rows < MIN_ROWS:
        tmp.unlink(missing_ok=True)
        raise SourceDrift(f"row floor not met: {rows} < {MIN_ROWS}")
    os.replace(tmp, out_csv)
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--force", action="store_true",
                    help="download even if the held snapshot is already current")
    ap.add_argument("--check-only", action="store_true",
                    help="report whether a newer snapshot exists; do not download")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    DEST_DIR.mkdir(parents=True, exist_ok=True)
    session = _session()

    try:
        res = resolve_resource(session)
    except SourceDrift as e:
        logger.error("source drift: %s", e)
        return 2
    except requests.RequestException as e:
        logger.error("CKAN resolve failed (transient): %s", e)
        return 1

    upstream = res["last_modified"]
    have = latest_local_date()
    logger.info("upstream last_modified=%s  held=%s  resource=%s",
                upstream, have, res["resource_id"])

    up_to_date = have is not None and have >= upstream
    if args.check_only:
        print(f"cro_poller: {'CURRENT' if up_to_date else 'UPDATE AVAILABLE'} "
              f"(upstream={upstream}, held={have})")
        return 0
    if up_to_date and not args.force:
        print(f"cro_poller: already current (held {have} >= upstream {upstream})")
        return 0

    out_csv = DEST_DIR / f"companies_{upstream:%Y%m%d}.csv"
    with tempfile.TemporaryDirectory() as td:
        zip_tmp = Path(td) / "companies.csv.zip"
        try:
            nbytes = download_zip(session, res["url"], zip_tmp)
            logger.info("downloaded %s (%.1f MB)", zip_tmp.name, nbytes / 1e6)
            rows = extract_and_validate(zip_tmp, out_csv)
        except SourceDrift as e:
            logger.error("validation failed (bronze NOT updated): %s", e)
            return 2
        except (requests.RequestException, OSError) as e:
            logger.error("download/extract failed (transient): %s", e)
            return 1

    print(f"cro_poller: wrote {out_csv.relative_to(BRONZE_DIR.parent.parent)}  "
          f"rows={rows:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
