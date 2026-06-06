"""
iris_oifigiuil_poller.py — detect & download newly published Iris Oifigiúil PDFs.

Iris is published Tuesdays and Fridays. By default this script walks from the
most-recent on-disk issue up to today and fetches any Tue/Fri PDFs missing
from bronze. So scheduled re-runs always close the gap since the last
successful fetch, with no need to guess a lookback window.

URL PATTERNS (lifted from iris_oifiiguil_downloader.py):
    current:  https://www.irisoifigiuil.ie/currentissues/IR{DDMMYY}.pdf
    archive:  https://irisoifigiuil.ie/archive/{year}/{month_name}/IR{DDMMYY}.pdf
    pre-2024 archive sometimes uses prefix "Ir" instead of "IR" — try both.

STRATEGY:
    1. Determine the gap to fill:
        - Default: from (latest past-dated issue on disk + 1 day) to today.
        - Override: --lookback-days N forces a fixed N-day window for backfill.
        - Empty bronze: falls back to --fallback-days (default 10).
    2. Enumerate Tue/Fri dates in the gap.
    3. Skip dates whose PDF already lives in --dest (case-insensitive match,
       to tolerate the legacy 'Ir…' files in bronze).
    4. HEAD-check currentissues first (cheapest), then archive (IR + Ir).
    5. On 200, stream-download to dest with atomic .part → final rename.
    6. Defensive size + PDF-signature check before rename — never expose a
       partial or non-PDF file to downstream globs.

EXIT CODE:
    0 = run finished cleanly (whether or not anything new was found).
    1 = a download was attempted but failed (network / signature / size).
    2 = a past Tue/Fri returned 4xx on every URL variant — the slug pattern
        may have changed; investigate before trusting future runs.

USAGE:
    python iris_oifigiuil_poller.py
    python iris_oifigiuil_poller.py --dry-run
    python iris_oifigiuil_poller.py --year 2020              # backfill a calendar year
    python iris_oifigiuil_poller.py --year 2018 2019 2020    # several years at once
    python iris_oifigiuil_poller.py --lookback-days 365      # last N days ending today
    python iris_oifigiuil_poller.py --dest /tmp/iris_test --lookback-days 30
"""

from __future__ import annotations

import argparse
import calendar
import re
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import requests

from config import BRONZE_DIR

USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
DEFAULT_TIMEOUT = (10, 30)
CURRENT_BASE = "https://www.irisoifigiuil.ie/currentissues"
ARCHIVE_BASE = "https://irisoifigiuil.ie/archive"
DEFAULT_DEST = BRONZE_DIR / "iris_oifigiuil"
# Used only when bronze is empty (no prior issues to anchor the lookback).
DEFAULT_FALLBACK_DAYS = 10
MIN_PDF_BYTES = 10_000
_FILENAME_DATE_RE = re.compile(r"^[Ii][Rr](\d{2})(\d{2})(\d{2})\.pdf$")


@dataclass
class Candidate:
    issue_date: date
    url: str
    label: str  # "current" | "archive-IR" | "archive-Ir"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def latest_past_issue_on_disk(dest_dir: Path, today: date) -> date | None:
    """Most-recent past-or-today Iris filename date in dest_dir.

    Returns None if there's nothing recognisable. Future-dated filenames are
    ignored — they're either placeholders (per repair_future_iris_placeholders.py)
    or real PDFs the operator added manually; either way they shouldn't drive
    the lookback floor.
    """
    latest: date | None = None
    for f in dest_dir.glob("[Ii][Rr]*.pdf"):
        m = _FILENAME_DATE_RE.match(f.name)
        if not m:
            continue
        dd, mm, yy = m.groups()
        try:
            d = date(2000 + int(yy), int(mm), int(dd))
        except ValueError:
            continue
        if d <= today and (latest is None or d > latest):
            latest = d
    return latest


def expected_dates(today: date, lookback_days: int) -> list[date]:
    """Tue/Fri dates within the lookback window, oldest first."""
    out: list[date] = []
    for offset in range(lookback_days, -1, -1):
        d = today - timedelta(days=offset)
        if d.weekday() in (calendar.TUESDAY, calendar.FRIDAY):
            out.append(d)
    return out


def expected_dates_since(start_after: date, today: date) -> list[date]:
    """Tue/Fri dates strictly after `start_after` up to and including `today`."""
    out: list[date] = []
    d = start_after + timedelta(days=1)
    while d <= today:
        if d.weekday() in (calendar.TUESDAY, calendar.FRIDAY):
            out.append(d)
        d += timedelta(days=1)
    return out


def expected_dates_for_years(years: list[int], today: date) -> list[date]:
    """Tue/Fri dates within each calendar year, capped at today.

    Years are deduped and processed in order. A future year produces no dates.
    A partially-complete current year is correctly truncated at today.
    """
    out: list[date] = []
    for year in sorted(set(years)):
        d = date(year, 1, 1)
        end = min(date(year, 12, 31), today)
        while d <= end:
            if d.weekday() in (calendar.TUESDAY, calendar.FRIDAY):
                out.append(d)
            d += timedelta(days=1)
    return out


def candidates_for(d: date) -> list[Candidate]:
    ddmmyy = d.strftime("%d%m%y")
    month_name = calendar.month_name[d.month].lower()
    year_str = str(d.year)
    return [
        Candidate(d, f"{CURRENT_BASE}/IR{ddmmyy}.pdf", "current"),
        Candidate(d, f"{ARCHIVE_BASE}/{year_str}/{month_name}/IR{ddmmyy}.pdf", "archive-IR"),
        Candidate(d, f"{ARCHIVE_BASE}/{year_str}/{month_name}/Ir{ddmmyy}.pdf", "archive-Ir"),
    ]


def filename_for(d: date) -> str:
    return f"IR{d.strftime('%d%m%y')}.pdf"


def already_on_disk(dest_dir: Path, d: date) -> bool:
    """Tolerate both IR* and Ir* casings on disk — legacy bronze uses Ir."""
    ddmmyy = d.strftime("%d%m%y")
    return (dest_dir / f"IR{ddmmyy}.pdf").exists() or (dest_dir / f"Ir{ddmmyy}.pdf").exists()


def head_status(session: requests.Session, url: str) -> int:
    try:
        resp = session.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        return resp.status_code
    except requests.RequestException:
        return -1


def download(session: requests.Session, url: str, dest: Path) -> Path:
    """Stream → .part → size+signature check → atomic rename. Raises on failure.

    On any error the .part is unlinked so a corrupt/partial file never becomes
    visible to downstream ETL globs.
    """
    tmp = dest.with_suffix(dest.suffix + ".part")
    try:
        with session.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as resp:
            resp.raise_for_status()
            with tmp.open("wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        size = tmp.stat().st_size
        if size < MIN_PDF_BYTES:
            raise ValueError(f"suspiciously small download ({size} bytes < {MIN_PDF_BYTES}): {url}")
        with tmp.open("rb") as f:
            head = f.read(5)
        if head != b"%PDF-":
            raise ValueError(f"not a PDF (first 5 bytes = {head!r}): {url}")
        tmp.replace(dest)
        return dest
    except Exception:
        tmp.unlink(missing_ok=True)
        raise


def poll(
    dest_dir: Path,
    today: date,
    lookback_days: int | None,
    years: list[int] | None,
    fallback_days: int,
    dry_run: bool,
) -> dict:
    """Returns summary dict with new_count, slug_miss_count, download_fail_count.

    Date-selection precedence (highest first):
        1. years     — fetch Tue/Fri in each named calendar year (truncated to today)
        2. lookback  — fetch Tue/Fri in a fixed N-day window ending today
        3. default   — fetch from latest past-dated file on disk up to today
        4. empty bronze fallback — fixed `fallback_days`
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    session = _session()

    if years:
        dates = expected_dates_for_years(years, today)
        window_descr = "year(s) " + ", ".join(str(y) for y in sorted(set(years)))
    elif lookback_days is not None:
        dates = expected_dates(today, lookback_days)
        window_descr = f"fixed lookback={lookback_days}d"
    else:
        last = latest_past_issue_on_disk(dest_dir, today)
        if last is None:
            dates = expected_dates(today, fallback_days)
            window_descr = f"empty bronze; fallback={fallback_days}d"
        else:
            dates = expected_dates_since(last, today)
            window_descr = f"since latest on-disk {last.isoformat()}"

    new_count = 0
    slug_miss_count = 0
    download_fail_count = 0

    print(
        f"[iris] poll start — dest={dest_dir}  window={window_descr}  "
        f"today={today.isoformat()}  candidate_dates={len(dates)}"
    )

    for d in dates:
        if d > today:
            continue  # belt-and-braces; expected_dates shouldn't yield future dates
        if already_on_disk(dest_dir, d):
            continue

        found_url: str | None = None
        last_statuses: list[tuple[str, int]] = []
        for cand in candidates_for(d):
            status = head_status(session, cand.url)
            last_statuses.append((cand.label, status))
            if status == 200:
                found_url = cand.url
                break

        if found_url is None:
            if d == today:
                # Publication day itself — PDF often appears later in the day.
                continue
            print(f"  miss  {d.isoformat()}  " + "  ".join(f"{lbl}={st}" for lbl, st in last_statuses))
            # Only count as a slug change if every variant returned a clean
            # 4xx (not a transport error / -1).
            if all(400 <= st < 500 for _, st in last_statuses):
                slug_miss_count += 1
            continue

        target = dest_dir / filename_for(d)
        if dry_run:
            print(f"NEW (dry-run): {d.isoformat()} -> {found_url}")
            new_count += 1
        else:
            try:
                download(session, found_url, target)
                print(f"NEW: {target.name}  ({found_url})")
                new_count += 1
            except Exception as exc:
                download_fail_count += 1
                print(f"  download FAILED  {d.isoformat()}  {found_url}  {exc}")

    print(f"[iris] poll done — new={new_count} slug_misses={slug_miss_count} download_failures={download_fail_count}")
    return {
        "new": new_count,
        "slug_misses": slug_miss_count,
        "download_failures": download_fail_count,
    }


def _exit_code(summary: dict) -> int:
    if summary["slug_misses"] > 0:
        return 2
    if summary["download_failures"] > 0:
        return 1
    return 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[1],
        epilog=(
            "Examples:\n"
            "  python iris_oifigiuil_poller.py                  # fill the gap since last on-disk\n"
            "  python iris_oifigiuil_poller.py --dry-run\n"
            "  python iris_oifigiuil_poller.py --year 2020      # backfill all of 2020\n"
            "  python iris_oifigiuil_poller.py --year 2018 2019 2020   # multiple years\n"
            "  python iris_oifigiuil_poller.py --lookback-days 365     # last year ending today\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--dest", type=Path, default=DEFAULT_DEST, help="destination directory (default: data/bronze/iris_oifigiuil)"
    )
    window = parser.add_mutually_exclusive_group()
    window.add_argument(
        "--lookback-days",
        type=int,
        default=None,
        help="explicit lookback window in days, ending today (forces a fixed window)",
    )
    window.add_argument(
        "--year",
        type=int,
        nargs="+",
        metavar="YYYY",
        default=None,
        help="backfill one or more calendar years (Tue/Fri only; current year truncated at today)",
    )
    parser.add_argument(
        "--fallback-days",
        type=int,
        default=DEFAULT_FALLBACK_DAYS,
        help=f"lookback used only when bronze is empty (default: {DEFAULT_FALLBACK_DAYS})",
    )
    parser.add_argument("--dry-run", action="store_true", help="list what would be downloaded without fetching")
    args = parser.parse_args(argv)

    summary = poll(
        dest_dir=args.dest,
        today=date.today(),
        lookback_days=args.lookback_days,
        years=args.year,
        fallback_days=args.fallback_days,
        dry_run=args.dry_run,
    )
    if summary["slug_misses"] > 0:
        print(
            "WARNING: past Tue/Fri dates 404'd on every URL variant — slug pattern may have changed.",
            file=sys.stderr,
        )
    return _exit_code(summary)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
