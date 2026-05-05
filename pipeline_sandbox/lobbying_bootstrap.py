"""
lobbying_bootstrap.py -- one-shot inventory of manually-ingested lobbying.ie
CSV exports, ahead of the lobbying auto-fetcher.

STATUS: SANDBOX. One-shot. Designed to be run once before the auto-fetcher
(https://api.lobbying.ie/api/ExportReturns/Csv) goes live, then never again.
Idempotent -- re-running produces the same state file content.

CONTEXT:
    Lobbying.ie ingestion is currently manual CSV. The DevTools probe found a
    public CSV endpoint with a hard 1-year cap per request. Before automating,
    we need a state file that records what's already on disk so the auto-fetcher
    knows what's been seen and what to refresh.

    Existing manual exports use a 1-Feb-to-1-Feb window (whatever the UI default
    happened to be), NOT calendar years. The auto-fetcher will use calendar-year
    windows (01-Jan to 31-Dec) for clean semantics. This script records the
    truth -- the actual window of each manual file -- and defers freeze/refetch
    decisions to the auto-fetcher.

WHAT IT DOES:
    1. Walks data/bronze/lobbying_csv_data/ and finds files matching the known
       manual-export naming pattern:
         Lobbying_ie_returns_results_DD_MM_YYYY_to_DD_MM_YYYY.csv
       (Tolerates the missing-leading-zero typo in 1_02_2026.)
    2. For each match: parses the date window, reads row count, sha256, and
       first-line header.
    3. Writes data/_meta/lobbying_state.json -- the state file that the
       auto-fetcher will consume.
    4. Skips non-returns exports (e.g. organisation results) and derived
       artefacts (cleaned*.csv) with logged reasons.
    5. With --archive, moves matched files into
       data/bronze/lobbying_csv_data/manual_bootstrap/ so it's obvious which
       files are the manual seed vs API-refreshed.

WHAT IT DOES NOT DO:
    - Fetch anything from api.lobbying.ie.
    - Delete files.
    - Mark anything frozen -- manual export windows are 1-Feb aligned, not
      calendar-year aligned. Freezing decisions are deferred to the auto-fetcher.

EXIT CODE:
    0 = state file written (or, in --dry-run, would be written) successfully.
    1 = no matching files found.
    2 = state file already exists with different content; re-run with --force.

USAGE:
    python pipeline_sandbox/lobbying_bootstrap.py
    python pipeline_sandbox/lobbying_bootstrap.py --dry-run
    python pipeline_sandbox/lobbying_bootstrap.py --archive
    python pipeline_sandbox/lobbying_bootstrap.py --force
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parents[1]
BRONZE_DIR = REPO_ROOT / "data" / "bronze" / "lobbying_csv_data"
STATE_PATH = REPO_ROOT / "data" / "_meta" / "lobbying_state.json"
ARCHIVE_DIR = BRONZE_DIR / "manual_bootstrap"

# DD_MM_YYYY_to_DD_MM_YYYY  -- day/month tolerated as 1- or 2-digit to handle
# the typo'd 1_02_2026 file. Year is strict 4-digit.
RETURNS_PATTERN = re.compile(
    r"^Lobbying_ie_returns_results_"
    r"(?P<d1>\d{1,2})_(?P<m1>\d{1,2})_(?P<y1>\d{4})"
    r"_to_"
    r"(?P<d2>\d{1,2})_(?P<m2>\d{1,2})_(?P<y2>\d{4})"
    r"\.csv$",
    re.IGNORECASE,
)

# Files we recognise but deliberately skip, with the reason.
SKIP_REASONS: dict[str, str] = {
    "Lobbying_ie_organisation_results.csv": "organisation export, not returns",
    "Lobbying_ie_returns_results.csv":      "no date window in filename",
    "Lobbying_ie_returns_results_1.csv":    "no date window in filename",
    "cleaned.csv":                          "derived artefact",
    "cleaned_output.csv":                   "derived artefact",
    ".gitkeep":                             "directory placeholder",
}


@dataclass
class FileEntry:
    """One manual export file, fully described."""
    filename: str
    window_start: str          # ISO date
    window_end:   str          # ISO date
    primary_year: int          # the calendar year covered by most of the window
    rows: int
    bytes: int
    sha256: str
    header: str
    source: str = "manual"
    frozen: bool = False       # always False at bootstrap; auto-fetcher decides
    notes: str = "1-Feb-aligned export window; does not align to calendar year"


@dataclass
class BootstrapResult:
    matched: list[FileEntry] = field(default_factory=list)
    skipped: list[tuple[str, str]] = field(default_factory=list)  # (filename, reason)
    unrecognised: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# File classification + parsing
# ---------------------------------------------------------------------------


def parse_window(filename: str) -> tuple[date, date] | None:
    m = RETURNS_PATTERN.match(filename)
    if not m:
        return None
    try:
        start = date(int(m["y1"]), int(m["m1"]), int(m["d1"]))
        end   = date(int(m["y2"]), int(m["m2"]), int(m["d2"]))
    except ValueError:
        return None
    if end <= start:
        return None
    return start, end


def primary_year(start: date, end: date) -> int:
    """The calendar year that contains the most days of the window.

    For 1-Feb-2019 to 1-Feb-2020: 334 days in 2019, 31 in 2020 → 2019.
    """
    if start.year == end.year:
        return start.year
    boundary = date(start.year + 1, 1, 1)
    days_in_start_year = (boundary - start).days
    days_in_end_year   = (end - boundary).days
    return start.year if days_in_start_year >= days_in_end_year else end.year


def hash_and_count(path: Path) -> tuple[str, int, int, str]:
    """Return (sha256_hex, row_count_excluding_header, byte_size, header_line)."""
    sha = hashlib.sha256()
    byte_size = 0
    newlines = 0
    header_bytes = b""

    with path.open("rb") as f:
        # First line for the header. Read up to 64KB -- CSV headers are never
        # larger than that in practice.
        header_bytes = f.readline()
        sha.update(header_bytes)
        byte_size += len(header_bytes)
        newlines += header_bytes.count(b"\n")

        chunk_size = 8 * 1024 * 1024
        while chunk := f.read(chunk_size):
            sha.update(chunk)
            byte_size += len(chunk)
            newlines += chunk.count(b"\n")

    # Files without a trailing newline still have one row past the last \n.
    # Count rows excluding the header. Underflow guard: never negative.
    rows = max(newlines - 1, 0)

    # Strip BOM and decode header tolerantly. We display the header for the
    # human reading the summary, not for downstream consumers.
    if header_bytes.startswith(b"\xef\xbb\xbf"):
        header_bytes = header_bytes[3:]
    header = header_bytes.decode("utf-8", errors="replace").rstrip("\r\n")

    return sha.hexdigest(), rows, byte_size, header


# ---------------------------------------------------------------------------
# Walk + classify
# ---------------------------------------------------------------------------


def inventory(bronze_dir: Path) -> BootstrapResult:
    result = BootstrapResult()
    if not bronze_dir.is_dir():
        print(f"ERROR: bronze dir does not exist: {bronze_dir}", file=sys.stderr)
        return result

    for path in sorted(bronze_dir.iterdir()):
        if not path.is_file():
            continue
        name = path.name

        if name in SKIP_REASONS:
            result.skipped.append((name, SKIP_REASONS[name]))
            continue

        window = parse_window(name)
        if window is None:
            result.unrecognised.append(name)
            continue

        start, end = window
        sha, rows, byte_size, header = hash_and_count(path)
        result.matched.append(
            FileEntry(
                filename=name,
                window_start=start.isoformat(),
                window_end=end.isoformat(),
                primary_year=primary_year(start, end),
                rows=rows,
                bytes=byte_size,
                sha256=sha,
                header=header,
            )
        )

    return result


# ---------------------------------------------------------------------------
# State file shape
# ---------------------------------------------------------------------------


def build_state(result: BootstrapResult) -> dict:
    years: dict[str, dict] = {}
    bootstrapped_at = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for entry in result.matched:
        # Multiple files could in principle map to the same primary_year. The
        # last writer wins; the file inventory below preserves all of them so
        # nothing is silently lost.
        years[str(entry.primary_year)] = {
            "rows": entry.rows,
            "bytes": entry.bytes,
            "sha256": entry.sha256,
            "fetched_at": bootstrapped_at,
            "source": "manual",
            "frozen": False,
            "window_start": entry.window_start,
            "window_end": entry.window_end,
            "filename": entry.filename,
            "notes": entry.notes,
        }

    return {
        "schema_version": 1,
        "bootstrapped_at": bootstrapped_at,
        "bronze_dir": str(BRONZE_DIR.relative_to(REPO_ROOT)).replace("\\", "/"),
        "years": years,
        "manual_files": [asdict(e) for e in result.matched],
        "skipped": [{"filename": n, "reason": r} for n, r in result.skipped],
        "unrecognised": result.unrecognised,
    }


def state_differs(existing: dict, new: dict) -> bool:
    """Compare ignoring the timestamp fields that change every run."""
    def strip(s: dict) -> dict:
        s = dict(s)
        s.pop("bootstrapped_at", None)
        years = s.get("years", {})
        s["years"] = {
            y: {k: v for k, v in d.items() if k != "fetched_at"}
            for y, d in years.items()
        }
        return s
    return strip(existing) != strip(new)


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------


def fmt_bytes(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024:
            return f"{n:.1f} {unit}" if unit != "B" else f"{n} B"
        n = n / 1024  # type: ignore[assignment]
    return f"{n:.1f} TB"


def print_summary(result: BootstrapResult, state_path: Path, dry_run: bool, archive: bool) -> None:
    print("Lobbying bootstrap -- inventory of manual exports")
    print("=" * 50)
    print(f"Bronze dir : {BRONZE_DIR.relative_to(REPO_ROOT)}")
    print(f"State file : {state_path.relative_to(REPO_ROOT)}")
    print()

    if result.matched:
        print("Matched (returns exports with parseable windows):")
        for e in sorted(result.matched, key=lambda x: x.primary_year):
            print(
                f"  {e.primary_year}  {e.filename}"
                f"  {e.rows:>7,} rows  {fmt_bytes(e.bytes):>9}"
                f"  sha256:{e.sha256[:8]}..."
            )
            print(f"         window {e.window_start} -> {e.window_end}")
        print()
    else:
        print("Matched: NONE")
        print()

    if result.skipped:
        print("Skipped (with reasons):")
        for name, reason in result.skipped:
            print(f"  {name:<55} -- {reason}")
        print()

    if result.unrecognised:
        print("Unrecognised filenames (review and add to SKIP_REASONS or pattern):")
        for name in result.unrecognised:
            print(f"  {name}")
        print()

    note = "[DRY RUN -- no files written or moved]" if dry_run else ""
    if archive and not dry_run:
        print(f"Archived matched files to: {ARCHIVE_DIR.relative_to(REPO_ROOT)}")
    print(f"State file: {len(result.matched)} entries {'would be' if dry_run else ''} written. {note}".strip())

    print()
    print("Note: manual exports use 1-Feb-to-1-Feb windows, not calendar years.")
    print("The auto-fetcher will use calendar-year windows (01-Jan to 31-Dec) and")
    print("treat these manual files as baselines for comparison, not as authoritative.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__.strip().splitlines()[0])
    p.add_argument("--dry-run", action="store_true", help="show what would happen, write nothing")
    p.add_argument("--archive", action="store_true", help="move matched files to manual_bootstrap/ subfolder")
    p.add_argument("--force", action="store_true", help="overwrite state file even if existing differs")
    args = p.parse_args(argv)

    result = inventory(BRONZE_DIR)

    if not result.matched:
        print_summary(result, STATE_PATH, args.dry_run, args.archive)
        print("\nNo matching returns files found. Nothing to bootstrap.", file=sys.stderr)
        return 1

    new_state = build_state(result)

    if STATE_PATH.exists() and not args.force:
        try:
            existing = json.loads(STATE_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            existing = {}
        if state_differs(existing, new_state):
            print_summary(result, STATE_PATH, args.dry_run, args.archive)
            print(
                f"\nState file already exists at {STATE_PATH.relative_to(REPO_ROOT)} and would change.",
                file=sys.stderr,
            )
            print("Re-run with --force to overwrite.", file=sys.stderr)
            return 2

    if args.dry_run:
        print_summary(result, STATE_PATH, args.dry_run, args.archive)
        return 0

    # Write state atomically: write to .tmp, rename.
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(new_state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(STATE_PATH)

    if args.archive:
        ARCHIVE_DIR.mkdir(parents=True, exist_ok=True)
        for e in result.matched:
            src = BRONZE_DIR / e.filename
            dst = ARCHIVE_DIR / e.filename
            if src.exists() and not dst.exists():
                shutil.move(str(src), str(dst))

    print_summary(result, STATE_PATH, args.dry_run, args.archive)
    return 0


if __name__ == "__main__":
    sys.exit(main())
