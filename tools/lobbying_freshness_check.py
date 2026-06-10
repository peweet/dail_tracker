"""tools/lobbying_freshness_check.py — is our shipped lobbying data behind upstream?

Lobbying is the one chain that is NOT auto-fetched in the pipeline — lobbying.ie
returns are pulled as a manual/wave CSV (see project_lobbying_automation). The
risk is therefore operator-shaped: a new SIPO reporting period opens upstream and
nobody re-pulls it, so the shipped gold silently lags. This scheduled canary
catches exactly that.

What it compares — and why period-START, not publish-date
---------------------------------------------------------
Our committed gold ``lobbyist_persistence.parquet`` carries ``last_return_date``
= ``MAX(lobbying_period_start_date)`` (see sql_queries/lobbyist_persistence.sql) —
the start of the most recent reporting period we hold. The upstream export's
``Period`` column is a human label like ``"1 May, 2026 to 31 Aug, 2026"``; its
left side is that same period start. So the honest, apples-to-apples question is:

    does upstream have returns whose PERIOD START is later than the latest period
    start in our shipped data?

If yes, a whole new reporting period is published that we have not ingested →
stale. Comparing publish dates instead would false-alarm constantly (a return for
an old period can be published any day), and comparing period *labels* would need
fuzzy matching. Period start vs period start is the clean signal.

SIPO periods (period starts): P1 Jan–Apr (start 1 Jan), P2 May–Aug (1 May),
P3 Sep–Dec (1 Sep); each due ~21 May / 21 Sep / 21 Jan and bulk-published within
~7 days of the deadline. A small ``--slack-days`` guards the day the new window
opens but before anything is filed.

Exit code:
    0  current — upstream's latest period start is not ahead of ours
    1  STALE — a newer reporting period exists upstream than we ship
    2  could-not-check — network/parse/gold-read error (no verdict)
A scheduled workflow turns a nonzero exit into a GitHub issue
(.github/workflows/lobbying_freshness.yml).

Usage:
    python tools/lobbying_freshness_check.py                 # default 60-day probe window
    python tools/lobbying_freshness_check.py --window-days 90 --slack-days 0
"""

from __future__ import annotations

import argparse
import csv
import io
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_GOLD_PARQUET = _PROJECT_ROOT / "data" / "gold" / "parquet" / "lobbyist_persistence.parquet"

ENDPOINT = "https://api.lobbying.ie/api/ExportReturns/Csv"
USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
# Connect short, read generous: the export server can take minutes to assemble.
TIMEOUT = (10, 600)
PAGE_SIZE = 50_000  # safe ceiling; a probe window holds far fewer returns


def parse_period_start(period: str) -> date | None:
    """Left side of ``"1 May, 2026 to 31 Aug, 2026"`` -> date(2026, 5, 1).

    Returns None for an empty/unparseable label (a format drift surfaces as a
    None we count, not a crash)."""
    if not period:
        return None
    left = period.split(" to ", 1)[0].strip()
    try:
        return datetime.strptime(left, "%d %b, %Y").date()
    except ValueError:
        return None


def latest_period_start(periods: list[str]) -> date | None:
    """Max parseable period start across a list of ``Period`` labels, or None."""
    starts = [d for d in (parse_period_start(p) for p in periods) if d is not None]
    return max(starts) if starts else None


def verdict(upstream_start: date | None, held_start: date | None, slack_days: int) -> tuple[int, str]:
    """Pure comparison. Returns (exit_code, message).

    STALE only when upstream's latest period start is strictly later than the
    held one by more than ``slack_days`` (slack covers the gap between a new
    window opening and its first returns being filed)."""
    if held_start is None:
        return 2, "could not read held period start from gold lobbyist_persistence.parquet"
    if upstream_start is None:
        return 2, "no parseable Period found in the upstream sample (possible format drift)"
    gap = (upstream_start - held_start).days
    if gap > slack_days:
        return 1, (
            f"STALE: upstream has a newer reporting period (starts {upstream_start.isoformat()}) "
            f"than the latest we ship (starts {held_start.isoformat()}); {gap}d ahead "
            f"(> {slack_days}d slack). Re-pull lobbying.ie and re-run the lobbying chain."
        )
    return 0, (
        f"OK: shipped lobbying data is current — latest period start {held_start.isoformat()} "
        f"vs upstream {upstream_start.isoformat()} ({gap}d; within {slack_days}d slack)."
    )


def fetch_recent_periods(window_days: int) -> list[str]:
    """Fetch a narrow recent window from the export API and return its ``Period``
    column values. Raises on network/HTTP error (caller maps to exit 2)."""
    import requests  # lazy: keeps the pure functions importable without the dep

    today = date.today()
    start = today - timedelta(days=window_days)
    params = {
        "currentPage": 0,
        "pageSize": PAGE_SIZE,
        "queryText": "",
        "subjectMatters": "",
        "subjectMatterAreas": "",
        "publicBodys": "",
        "jobTitles": "",
        "returnDateFrom": start.strftime("%d-%m-%Y"),
        "returnDateTo": today.strftime("%d-%m-%Y"),
        "period": "",
        "dpo": "",
        "client": "",
        "responsible": "",
        "lobbyist": "",
        "lobbyistId": "",
    }
    resp = requests.get(ENDPOINT, headers={"User-Agent": USER_AGENT}, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    text = resp.content.lstrip(b"\xef\xbb\xbf").decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames or "Period" not in reader.fieldnames:
        raise ValueError("upstream CSV has no 'Period' column (schema drift)")
    return [row.get("Period", "") for row in reader]


def held_period_start(parquet_path: Path = _GOLD_PARQUET) -> date | None:
    """Max ``last_return_date`` in the committed gold (the latest period start we
    ship). Reads via duckdb (a core dep — no polars/WMI import on Windows)."""
    if not parquet_path.exists():
        return None
    import duckdb

    row = (
        duckdb.connect()
        .execute(f"SELECT max(last_return_date) FROM read_parquet('{parquet_path.as_posix()}')")
        .fetchone()
    )
    if not row or row[0] is None:
        return None
    val = row[0]
    return val.date() if isinstance(val, datetime) else val


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Lobbying freshness canary: is shipped gold behind upstream?")
    parser.add_argument(
        "--window-days", type=int, default=60, help="how many recent days of returns to sample upstream (default 60)"
    )
    parser.add_argument(
        "--slack-days",
        type=int,
        default=14,
        help="allow upstream to be this many days ahead before flagging (covers a window opening before filings land)",
    )
    args = parser.parse_args(argv)

    today = datetime.now().date()
    print(f"LOBBYING FRESHNESS CHECK  (as of {today.isoformat()})")

    try:
        periods = fetch_recent_periods(args.window_days)
    except Exception as exc:  # noqa: BLE001 — any fetch error is a "could not check", not "stale"
        print(f"  could-not-check: upstream fetch failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2
    upstream = latest_period_start(periods)
    held = held_period_start()

    print(
        f"  upstream latest period start: {upstream.isoformat() if upstream else '--'}  "
        f"(from {len(periods)} returns in last {args.window_days}d)"
    )
    print(f"  shipped latest period start : {held.isoformat() if held else '--'}  ({_GOLD_PARQUET.name})")

    code, message = verdict(upstream, held, args.slack_days)
    print(("" if code == 0 else "\n") + message)
    return code


if __name__ == "__main__":
    sys.exit(main())
