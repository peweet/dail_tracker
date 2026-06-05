"""
lobbying_poller.py — single-file lobbying.ie returns poller.

Fetches year-to-date returns and writes them to
data/bronze/lobbying_csv_data/lobbying_ytd.csv. Auto-selects mode by today's
date — 'trickle' on most days, 'wave' on or after NEXT_WAVE_DATE — and only
overwrites the on-disk file when the content has actually changed.

Run:
    python lobbying_poller.py

Exit codes:
    0 — clean (no change OR successful write)
    1 — infra failure (network, 5xx, response too small for the mode)
    2 — needs human (schema drift, or wave-mode response below expected size)

ENDPOINT BEHAVIOUR:
   The lobbying.ie API requires a specific param schema (all empty filter
   params must be present even when unused) and date format DD-MM-YYYY.
   Server is SLOW — expect 1-3 min for a year of data; READ_TIMEOUT is
   tuned accordingly. Don't shorten it without checking response times.

⚠ NEXT_WAVE_DATE: update after each successful wave run. Wave deadlines fall
   ~10 days after each regulatory return deadline (21 May, 21 Sep, 21 Jan).
"""

from __future__ import annotations

import hashlib
import sys
import time
from datetime import date, datetime
from pathlib import Path

import requests

from config import BRONZE_DIR

# ── Operator-edited constants ───────────────────────────────────────────────
ENDPOINT = "https://api.lobbying.ie/api/ExportReturns/Csv"
NEXT_WAVE_DATE = date(2026, 5, 31)

# ── Fixed configuration ─────────────────────────────────────────────────────
USER_AGENT = "dail-tracker-bot/0.1 (+https://github.com/peweet/dail_tracker; mailto:p.glynn18@gmail.com)"
DEST_FILE = BRONZE_DIR / "lobbying_csv_data" / "lobbying_ytd.csv"
# Connect timeout is short (10s); read timeout is generous because the
# server can take 1-3 min to assemble a year of data.
TIMEOUT = (10, 600)
PAGE_SIZE = 50_000  # safe ceiling — annual return counts are 5k-15k

# Mode-specific size floors. Trickle floor just rejects HTML error pages;
# wave floor sets a real expectation for a 4-month bulk filing window.
TRICKLE_MIN_BYTES = 1_000
WAVE_MIN_BYTES = 500_000

# Set-inclusion check: any of these missing is a hard fail. Extras are OK.
EXPECTED_COLUMNS = {
    "Id",
    "Url",
    "Lobbyist Name",
    "Date Published",
    "Period",
    "Relevant Matter",
    "Public Policy Area",
    "Specific Details",
    "DPOs Lobbied",
    "Intended Results",
    "Lobbying Activities",
    "Person primarily responsible for lobbying on this activity",
    "Any DPOs or Former DPOs who carried out lobbying activities",
    "Current or Former DPOs",
    "Was this a grassroots campaign?",
    "Grassroots directive",
    "Was this lobbying done on behalf of a client?",
    "Client(s)",
}


def fetch_ytd() -> bytes:
    """Single GET for Jan 1 of current year → today. One retry on 429.

    Requires the full filter-param schema even when filters are empty —
    omitting any of these returns 404 from the upstream API.
    """
    today = date.today()
    start = date(today.year, 1, 1)
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
    if resp.status_code == 429:
        retry_after = int(resp.headers.get("Retry-After", "30"))
        print(f"[lobbying] 429 rate-limited, sleeping {retry_after}s and retrying once")
        time.sleep(retry_after)
        resp = requests.get(ENDPOINT, headers={"User-Agent": USER_AGENT}, params=params, timeout=TIMEOUT)
    resp.raise_for_status()
    return resp.content


def validate(csv_bytes: bytes, mode: str) -> tuple[bool, list[str]]:
    """Returns (ok, problems). Strips BOM, checks size + header columns."""
    problems: list[str] = []
    min_size = WAVE_MIN_BYTES if mode == "wave" else TRICKLE_MIN_BYTES
    if len(csv_bytes) < min_size:
        problems.append(f"size {len(csv_bytes):,} below {mode}-mode minimum {min_size:,}")

    text = csv_bytes.lstrip(b"\xef\xbb\xbf").decode("utf-8", errors="replace")
    first_line = text.splitlines()[0] if text else ""
    if not first_line:
        problems.append("response has no first line")
        return False, problems

    if first_line.lstrip().lower().startswith("<"):
        problems.append("response looks like HTML, not CSV")
        return False, problems

    found = {c.strip() for c in first_line.split(",")}
    missing = EXPECTED_COLUMNS - found
    if missing:
        problems.append(f"missing required columns: {sorted(missing)}")
    extras = found - EXPECTED_COLUMNS
    if extras:
        print(f"[lobbying] new columns appeared (accepting): {sorted(extras)}")
    return not problems, problems


def write_if_changed(csv_bytes: bytes) -> str:
    """sha256-compare vs on-disk; atomic .tmp → rename on write."""
    DEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    new_hash = hashlib.sha256(csv_bytes).hexdigest()
    if DEST_FILE.exists():
        old_hash = hashlib.sha256(DEST_FILE.read_bytes()).hexdigest()
        if old_hash == new_hash:
            return "unchanged"
    tmp = DEST_FILE.with_suffix(DEST_FILE.suffix + ".tmp")
    tmp.write_bytes(csv_bytes)
    tmp.replace(DEST_FILE)
    return "written"


def quarantine(csv_bytes: bytes, prefix: str) -> Path:
    """Save a problematic response with a timestamped name (does not overwrite bronze)."""
    DEST_FILE.parent.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%dT%H-%M-%S")
    path = DEST_FILE.parent / f"_{prefix}_{stamp}.csv"
    path.write_bytes(csv_bytes)
    return path


def main() -> int:
    today = date.today()
    mode = "wave" if today >= NEXT_WAVE_DATE else "trickle"
    print(f"[lobbying] start — mode={mode} today={today.isoformat()} endpoint={ENDPOINT}")

    try:
        csv_bytes = fetch_ytd()
    except requests.RequestException as exc:
        print(f"[lobbying] ERROR fetching: {exc}", file=sys.stderr)
        return 1
    print(f"[lobbying] fetched {len(csv_bytes):,} bytes")

    ok, problems = validate(csv_bytes, mode)
    if not ok:
        for p in problems:
            print(f"[lobbying] PROBLEM: {p}", file=sys.stderr)
        if any("missing required columns" in p for p in problems):
            print(f"[lobbying] saved drift sample: {quarantine(csv_bytes, 'SCHEMA_DRIFT')}", file=sys.stderr)
            return 2
        if mode == "wave" and any("below wave-mode minimum" in p for p in problems):
            print(
                f"[lobbying] saved small-wave sample: {quarantine(csv_bytes, 'SUSPICIOUSLY_SMALL_WAVE')}",
                file=sys.stderr,
            )
            return 2
        return 1

    action = write_if_changed(csv_bytes)
    if action == "unchanged":
        print(f"[lobbying] no change vs on-disk {DEST_FILE.name}")
    else:
        print(f"[lobbying] wrote {DEST_FILE.name} ({len(csv_bytes):,} bytes)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
