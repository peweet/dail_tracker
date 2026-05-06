"""
lobbying_fetch.py -- pull historical lobbying.ie return CSVs from the public
api endpoint and save them into data/bronze/lobbying_csv_data/ matching the
existing manual-export naming convention.

STATUS: SANDBOX. Companion to lobbying_bootstrap.py. Idempotent -- already-
present files for a given window are not refetched unless --force is passed.

ENDPOINT:
    https://api.lobbying.ie/api/ExportReturns/Csv

    Date format is dd-mm-yyyy (with hyphens). Server has a hard ~1-year cap
    per request and is slow (response can take 1-3 min for a year of data),
    so we set a generous read timeout.

WINDOWS:
    1-Feb-aligned 1-year windows, matching the existing manual exports so
    bootstrap inventory keeps a single primary_year per file. Default range
    pulls 2016-02-01..2019-02-01 (three windows), stopping right before the
    existing 2019 manual file's window starts.

USAGE:
    python pipeline_sandbox/lobbying_fetch.py
    python pipeline_sandbox/lobbying_fetch.py --start 2016-02-01 --end 2019-02-01
    python pipeline_sandbox/lobbying_fetch.py --force      # refetch even if file exists
    python pipeline_sandbox/lobbying_fetch.py --dry-run    # show URLs only

EXIT CODE:
    0 = success, all windows fetched (or already present, or dry-run).
    1 = at least one window failed to fetch.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = REPO_ROOT / "data" / "bronze" / "lobbying_csv_data"

API_URL = "https://api.lobbying.ie/api/ExportReturns/Csv"

DEFAULT_START = date(2016, 2, 1)
DEFAULT_END = date(2019, 2, 1)  # exclusive upper bound; existing manual file starts here

# Page size: server returns the whole window in one response if pageSize is
# larger than the row count. Existing manual files are 5k-13k rows/year, so
# 50000 is a safe ceiling for the historical pre-2020 era.
PAGE_SIZE = 50000

# Server is slow (1-3 min for a year of data). Set a generous read timeout.
READ_TIMEOUT_SECONDS = 600


def filename_for(start: date, end: date) -> str:
    """Match the existing manual-export naming convention (zero-padded)."""
    return (
        f"Lobbying_ie_returns_results_"
        f"{start.day:02d}_{start.month:02d}_{start.year}"
        f"_to_"
        f"{end.day:02d}_{end.month:02d}_{end.year}.csv"
    )


def url_for(start: date, end: date) -> str:
    params = {
        "currentPage": 0,
        "pageSize": PAGE_SIZE,
        "queryText": "",
        "subjectMatters": "",
        "subjectMatterAreas": "",
        "publicBodys": "",
        "jobTitles": "",
        "returnDateFrom": start.strftime("%d-%m-%Y"),
        "returnDateTo": end.strftime("%d-%m-%Y"),
        "period": "",
        "dpo": "",
        "client": "",
        "responsible": "",
        "lobbyist": "",
        "lobbyistId": "",
    }
    return f"{API_URL}?{urlencode(params)}"


def windows_between(start: date, end: date) -> list[tuple[date, date]]:
    """Yield 1-Feb-aligned 1-year windows from start (inclusive) to end (exclusive)."""
    out: list[tuple[date, date]] = []
    cursor = start
    while cursor < end:
        try:
            next_cursor = cursor.replace(year=cursor.year + 1)
        except ValueError:  # 29-Feb edge case; not relevant for 01-02 starts
            next_cursor = cursor.replace(year=cursor.year + 1, day=28)
        if next_cursor > end:
            next_cursor = end
        out.append((cursor, next_cursor))
        cursor = next_cursor
    return out


def fetch_window(start: date, end: date, dest: Path) -> tuple[bool, int, str]:
    """Download one window to dest. Return (ok, bytes_written, error_or_status)."""
    url = url_for(start, end)
    tmp = dest.with_suffix(".csv.partial")
    req = Request(url, headers={"User-Agent": "dail-tracker-lobbying-fetcher/0.1"})
    try:
        with urlopen(req, timeout=READ_TIMEOUT_SECONDS) as resp:
            status = resp.status
            content_type = resp.headers.get("Content-Type", "")
            if status != 200:
                return False, 0, f"HTTP {status}"
            if "csv" not in content_type.lower():
                return False, 0, f"unexpected content-type: {content_type}"
            written = 0
            with tmp.open("wb") as f:
                while True:
                    chunk = resp.read(64 * 1024)
                    if not chunk:
                        break
                    f.write(chunk)
                    written += len(chunk)
        if written == 0:
            tmp.unlink(missing_ok=True)
            return False, 0, "empty response"
        tmp.replace(dest)
        return True, written, "ok"
    except Exception as e:
        tmp.unlink(missing_ok=True)
        return False, 0, f"{type(e).__name__}: {e}"


def fmt_bytes(n: int) -> str:
    n_f = float(n)
    for unit in ("B", "KB", "MB", "GB"):
        if n_f < 1024:
            return f"{n_f:.1f} {unit}" if unit != "B" else f"{int(n_f)} B"
        n_f /= 1024
    return f"{n_f:.1f} TB"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    p.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START,
                   help="ISO start date (inclusive). Default 2016-02-01.")
    p.add_argument("--end", type=date.fromisoformat, default=DEFAULT_END,
                   help="ISO end date (exclusive upper bound). Default 2019-02-01.")
    p.add_argument("--force", action="store_true",
                   help="refetch even if destination file already exists")
    p.add_argument("--dry-run", action="store_true",
                   help="print URLs only, do not download")
    args = p.parse_args(argv)

    if args.end <= args.start:
        print(f"ERROR: --end ({args.end}) must be after --start ({args.start})", file=sys.stderr)
        return 1

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    windows = windows_between(args.start, args.end)

    print(f"Lobbying fetch -- {args.start} -> {args.end} ({len(windows)} window(s))")
    print(f"Dest: {BRONZE_DIR.relative_to(REPO_ROOT)}")
    print("=" * 60)

    failures = 0
    for ws, we in windows:
        name = filename_for(ws, we)
        dest = BRONZE_DIR / name
        url = url_for(ws, we)

        if args.dry_run:
            print(f"  [dry-run] {ws} -> {we}")
            print(f"            {url}")
            print(f"            -> {dest.relative_to(REPO_ROOT)}")
            continue

        if dest.exists() and not args.force:
            size = dest.stat().st_size
            print(f"  [skip]    {ws} -> {we}  exists ({fmt_bytes(size)})")
            continue

        print(f"  [fetch]   {ws} -> {we}  ...", flush=True)
        t0 = time.monotonic()
        ok, written, status = fetch_window(ws, we, dest)
        elapsed = time.monotonic() - t0
        if ok:
            print(f"            OK  {fmt_bytes(written)} in {elapsed:.1f}s -> {name}")
        else:
            print(f"            FAIL  {status} (after {elapsed:.1f}s)")
            failures += 1

    print("=" * 60)
    if failures:
        print(f"Done with {failures} failure(s).")
        return 1
    print("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
