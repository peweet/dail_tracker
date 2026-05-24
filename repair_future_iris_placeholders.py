"""
repair_future_iris_placeholders.py — one-shot cleanup.

An older Iris downloader saved 404/error responses as ~146-byte HTML files
labelled .pdf in data/bronze/iris_oifigiuil/. Because iris_oifigiuil_poller
treats any file matching the IR{DDMMYY}.pdf name as 'already on disk', these
stubs are sticky — the new poller won't replace them.

This script removes the FUTURE-DATED ones only: their issue date is past
today, so no upstream PDF exists yet to fetch. Once deleted, the next poll
run after the actual publication date will download the real PDF via the
poller's normal flow (size + %PDF- signature checks ensure no new stub).

Past-dated placeholder repair is a separate, larger piece of work; this
script doesn't touch them.

Deletion criteria — must satisfy ALL of:
  1. filename parses as IR{DD}{MM}{YY}.pdf (case insensitive)
  2. parsed issue date is strictly AFTER today
  3. file is < 10,000 bytes
  4. file does NOT start with '%PDF-'

A real Iris PDF in bronze (signature-valid, full-size) is therefore never
deleted, even if its filename date is in the future.

Usage:
    python repair_future_iris_placeholders.py
"""

from __future__ import annotations

import re
import sys
from datetime import date
from pathlib import Path

from config import BRONZE_DIR

IRIS_DIR = BRONZE_DIR / "iris_oifigiuil"
PLACEHOLDER_MAX_BYTES = 10_000
FILENAME_RE = re.compile(r"^[Ii][Rr](\d{2})(\d{2})(\d{2})\.pdf$")


def parse_issue_date(filename: str) -> date | None:
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    dd, mm, yy = m.groups()
    try:
        return date(2000 + int(yy), int(mm), int(dd))
    except ValueError:
        return None


def looks_like_placeholder(path: Path) -> bool:
    """Conservative: small AND not a real PDF."""
    try:
        size = path.stat().st_size
    except OSError:
        return False
    if size >= PLACEHOLDER_MAX_BYTES:
        return False
    try:
        with path.open("rb") as f:
            head = f.read(5)
    except OSError:
        return False
    return head != b"%PDF-"


def main() -> int:
    if not IRIS_DIR.exists():
        print(f"bronze dir not found: {IRIS_DIR}")
        return 1

    today = date.today()
    all_pdfs = sorted(IRIS_DIR.glob("[Ii][Rr]*.pdf"))

    to_delete: list[tuple[date, Path, int]] = []
    skipped = {"unparseable": 0, "past_dated": 0, "future_real_pdf": 0}

    for p in all_pdfs:
        d = parse_issue_date(p.name)
        if d is None:
            skipped["unparseable"] += 1
            continue
        if d <= today:
            skipped["past_dated"] += 1
            continue
        if not looks_like_placeholder(p):
            skipped["future_real_pdf"] += 1
            continue
        to_delete.append((d, p, p.stat().st_size))

    print(f"Today                          : {today.isoformat()}")
    print(f"Iris bronze files scanned      : {len(all_pdfs):,}")
    print(f"  past-dated (untouched)       : {skipped['past_dated']:,}")
    print(f"  unparseable filenames        : {skipped['unparseable']:,}")
    print(f"  future-dated real PDFs       : {skipped['future_real_pdf']:,}  (preserved)")
    print(f"  future-dated placeholders    : {len(to_delete):,}  (to delete)")
    print()

    if not to_delete:
        print("Nothing to delete.")
        return 0

    failures = 0
    for d, p, size in to_delete:
        try:
            p.unlink()
            print(f"DELETED  {d.isoformat()} ({d.strftime('%a')})  {p.name:>16}  ({size:,} bytes)")
        except OSError as exc:
            failures += 1
            print(f"FAILED   {d.isoformat()} ({d.strftime('%a')})  {p.name:>16}  ({exc})")

    print(f"\nDone. Removed {len(to_delete) - failures} of {len(to_delete)} future-dated placeholder(s).")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
