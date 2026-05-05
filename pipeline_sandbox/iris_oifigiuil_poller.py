"""
iris_oifigiuil_poller.py — detect & download newly published Iris Oifigiúil PDFs.

STATUS: SANDBOX. Designed to run on a schedule (e.g. /schedule daily) and
print a NEW: line for each freshly downloaded issue so the schedule output
acts as the alert.

URL PATTERNS (lifted from iris_oifiiguil_downloader.py):
    current:  https://www.irisoifigiuil.ie/currentissues/IR{DDMMYY}.pdf
    archive:  https://irisoifigiuil.ie/archive/{year}/{month_name}/IR{DDMMYY}.pdf
    pre-2024 archive sometimes uses prefix "Ir" instead of "IR" — try both.

CADENCE: Iris is published Tuesdays and Fridays.

STRATEGY:
    1. Build the candidate Tue/Fri dates within LOOKBACK_DAYS up to today.
    2. Skip dates whose PDF already lives in the bronze dir.
    3. HEAD-check currentissues first (cheapest), then archive (IR + Ir).
    4. On 200, stream-download to bronze and emit `NEW: <path>` to stdout.

EXIT CODE:
    0 = run finished cleanly (whether or not anything new was found).
    2 = a date was expected to exist but every candidate URL 4xx'd (the URL
        slug may have changed — investigate before trusting future runs).

USAGE:
    python pipeline_sandbox/iris_oifigiuil_poller.py
    python pipeline_sandbox/iris_oifigiuil_poller.py --dry-run
    python pipeline_sandbox/iris_oifigiuil_poller.py --lookback-days 14
"""

from __future__ import annotations

import argparse
import calendar
import sys
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import requests


USER_AGENT = (
    "dail-tracker-bot/0.1 (+https://github.com/<owner>/dail-extractor; "
    "mailto:<contact>)"
)
DEFAULT_TIMEOUT = (10, 30)

CURRENT_BASE = "https://www.irisoifigiuil.ie/currentissues"
ARCHIVE_BASE = "https://irisoifigiuil.ie/archive"

# Default destination — mirrors the path used by iris_oifiiguil_downloader.py
# and pipeline_sandbox/iris_oifigiuil_etl_polars.py.
DEFAULT_DEST = (
    Path(__file__).resolve().parents[1] / "data" / "bronze" / "iris_oifigiuil"
)

DEFAULT_LOOKBACK_DAYS = 10


@dataclass
class Candidate:
    issue_date: date
    url: str
    label: str  # "current" | "archive-IR" | "archive-Ir"


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update({"User-Agent": USER_AGENT})
    return s


def expected_dates(today: date, lookback_days: int) -> list[date]:
    """Tue/Fri dates within the lookback window, oldest first."""
    out: list[date] = []
    for offset in range(lookback_days, -1, -1):
        d = today - timedelta(days=offset)
        if d.weekday() in (calendar.TUESDAY, calendar.FRIDAY):
            out.append(d)
    return out


def candidates_for(d: date) -> list[Candidate]:
    ddmmyy = d.strftime("%d%m%y")
    month_name = calendar.month_name[d.month].lower()
    year_str = str(d.year)
    return [
        Candidate(d, f"{CURRENT_BASE}/IR{ddmmyy}.pdf", "current"),
        Candidate(
            d,
            f"{ARCHIVE_BASE}/{year_str}/{month_name}/IR{ddmmyy}.pdf",
            "archive-IR",
        ),
        Candidate(
            d,
            f"{ARCHIVE_BASE}/{year_str}/{month_name}/Ir{ddmmyy}.pdf",
            "archive-Ir",
        ),
    ]


def filename_for(d: date) -> str:
    return f"IR{d.strftime('%d%m%y')}.pdf"


def head_status(session: requests.Session, url: str) -> int:
    try:
        resp = session.head(url, timeout=DEFAULT_TIMEOUT, allow_redirects=True)
        return resp.status_code
    except requests.RequestException:
        return -1


def download(session: requests.Session, url: str, dest: Path) -> None:
    with session.get(url, stream=True, timeout=DEFAULT_TIMEOUT) as resp:
        resp.raise_for_status()
        tmp = dest.with_suffix(dest.suffix + ".part")
        with tmp.open("wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        tmp.replace(dest)


def poll(
    dest_dir: Path,
    today: date,
    lookback_days: int,
    dry_run: bool,
) -> tuple[int, int]:
    """Returns (new_count, slug_miss_count)."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    session = _session()

    new_count = 0
    slug_miss_count = 0

    for d in expected_dates(today, lookback_days):
        target = dest_dir / filename_for(d)
        if target.exists():
            continue

        # Skip future dates inside the window — `today` may include a
        # publication day before the PDF goes up.
        if d > today:
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
            # If today is the publication day itself, a miss is normal — the
            # PDF often appears later in the day. Don't flag as a slug change.
            if d == today:
                continue
            print(
                f"  miss  {d.isoformat()}  "
                + "  ".join(f"{lbl}={st}" for lbl, st in last_statuses)
            )
            # Treat repeated 404s on a past Tue/Fri as a slug-change signal
            # only when *every* candidate variant returned 4xx (not a transport
            # error). One transient -1 should not trip the flag.
            if all(400 <= st < 500 for _lbl, st in last_statuses):
                slug_miss_count += 1
            continue

        if dry_run:
            print(f"NEW (dry-run): {d.isoformat()} -> {found_url}")
        else:
            download(session, found_url, target)
            print(f"NEW: {target}  ({found_url})")
        new_count += 1

    return new_count, slug_miss_count


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument("--dest", type=Path, default=DEFAULT_DEST)
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    today = date.today()
    new_count, slug_miss_count = poll(
        dest_dir=args.dest,
        today=today,
        lookback_days=args.lookback_days,
        dry_run=args.dry_run,
    )

    print(
        f"\nsummary: {new_count} new, "
        f"{slug_miss_count} expected-but-missing "
        f"(window {args.lookback_days}d ending {today.isoformat()})"
    )

    if slug_miss_count > 0:
        print(
            "WARNING: past Tue/Fri dates 404'd on every URL variant — "
            "slug pattern may have changed.",
            file=sys.stderr,
        )
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
