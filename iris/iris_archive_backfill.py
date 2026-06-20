"""
iris_archive_backfill.py — backfill old Iris Oifigiúil PDFs by scraping the
per-month archive index pages instead of guessing filenames.

WHY THIS EXISTS (vs iris_oifigiuil_poller.py):
    The poller fills the recent gap by GUESSING the slug for each Tue/Fri date
    (IR/Ir prefix, currentissues vs archive). That works from ~2016 on, where
    filenames are the tidy `IR{DDMMYY}.pdf`. The older archive is chaotic:

        ir310114.pdf  Ir280114.pdf  IR061015.pdf  IO170114.pdf  Ir030114.PDF

    — mixed prefixes (ir/Ir/IR/IO), mixed extensions (.pdf/.PDF), and the
    occasional non-Tue/Fri issue. Guessing misses most of these. So for cold
    backfill of historic years we instead read each month's directory index
    (e.g. /archive/2014/january/) and download every PDF it links.

NORMALISATION:
    Every download is saved as `IR{DDMMYY}.pdf` (uppercase) so it matches the
    existing bronze convention and the downstream ETL's filename filters
    (future-stub regex, poller lookback glob). The date is taken from the 6
    trailing digits of the source filename.

SAFETY:
    Reuses the poller's hardened `download()` (atomic .part → size + %PDF-
    signature check → rename) and `already_on_disk()` (tolerates IR/Ir casing).
    Never overwrites an existing issue.

USAGE:
    python iris/iris_archive_backfill.py --year 2012 2013 2014 2015
    python iris/iris_archive_backfill.py --year 2014 --dry-run
"""

from __future__ import annotations

import argparse
import calendar
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urljoin

import requests

from config import BRONZE_DIR
from iris.iris_oifigiuil_poller import (
    DEFAULT_TIMEOUT,
    USER_AGENT,
    already_on_disk,
    download,
)

ARCHIVE_BASE = "https://www.irisoifigiuil.ie/archive"
DEFAULT_DEST = BRONZE_DIR / "iris_oifigiuil"

# Match an archive PDF href and pull out the trailing DDMMYY date stamp.
# Tolerates any 2-letter prefix (ir/Ir/IR/IO), any case, .pdf or .PDF.
_HREF_RE = re.compile(r'href="([^"]*?([A-Za-z]{2})(\d{2})(\d{2})(\d{2})\.(?:pdf|PDF))"', re.IGNORECASE)


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def month_index_url(year: int, month: int) -> str:
    return f"{ARCHIVE_BASE}/{year}/{calendar.month_name[month].lower()}/"


def issues_in_month(session: requests.Session, year: int, month: int) -> list[tuple[date, str]]:
    """Return (issue_date, absolute_pdf_url) for every PDF the month index links.

    Filenames whose embedded DDMMYY don't parse to a real date in `year` are
    skipped (defends against stray links / typo'd stamps).
    """
    url = month_index_url(year, month)
    try:
        resp = session.get(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
    except requests.RequestException as exc:
        print(f"  [warn] {year}-{month:02d} index fetch failed: {exc}", file=sys.stderr)
        return []
    if resp.status_code != 200:
        return []

    found: dict[date, str] = {}
    for m in _HREF_RE.finditer(resp.text):
        href, _prefix, dd, mm, yy = m.group(1), m.group(2), m.group(3), m.group(4), m.group(5)
        try:
            d = date(2000 + int(yy), int(mm), int(dd))
        except ValueError:
            continue
        if d.year != year:
            continue  # stamp must agree with the folder year
        found[d] = urljoin(url, href)  # dedupe on date; last href wins
    return sorted(found.items())


def filename_for(d: date) -> str:
    return f"IR{d.strftime('%d%m%y')}.pdf"


def backfill(dest_dir: Path, years: list[int], dry_run: bool) -> dict:
    dest_dir.mkdir(parents=True, exist_ok=True)
    session = _session()
    new_count = 0
    skip_count = 0
    fail_count = 0

    for year in sorted(set(years)):
        year_new = 0
        for month in range(1, 13):
            for d, url in issues_in_month(session, year, month):
                if already_on_disk(dest_dir, d):
                    skip_count += 1
                    continue
                target = dest_dir / filename_for(d)
                if dry_run:
                    print(f"NEW (dry-run): {d.isoformat()} -> {url}")
                    new_count += 1
                    year_new += 1
                    continue
                try:
                    download(session, url, target)
                    print(f"NEW: {target.name}  ({url})")
                    new_count += 1
                    year_new += 1
                except Exception as exc:  # noqa: BLE001 — log and keep going
                    fail_count += 1
                    print(f"  download FAILED  {d.isoformat()}  {url}  {exc}", file=sys.stderr)
        print(f"[iris-backfill] {year}: {year_new} new")

    print(f"[iris-backfill] done — new={new_count} already_on_disk={skip_count} failures={fail_count}")
    return {"new": new_count, "skipped": skip_count, "failures": fail_count}


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[1],
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST, help="destination dir (default: bronze/iris_oifigiuil)")
    parser.add_argument("--year", type=int, nargs="+", required=True, metavar="YYYY", help="calendar year(s) to backfill")
    parser.add_argument("--dry-run", action="store_true", help="list what would be downloaded without fetching")
    args = parser.parse_args(argv)

    summary = backfill(dest_dir=args.dest, years=args.year, dry_run=args.dry_run)
    return 1 if summary["failures"] > 0 else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
