"""Report how stale each backup lane is, and fail when one has gone quiet.

WHY THIS EXISTS (2026-08-21): both lanes stopped after 2026-08-19 and nothing noticed
for two days. The box was kernel-crashing (GPU driver, bugcheck 0x113), so the scheduled
tasks kept dying mid-run. Neither lane writes a FAIL line in that case -- it simply never
writes anything -- so a checker that reads logs looking for errors sees a clean file and
reports success. The only honest signal is the AGE of the newest success.

Two independent sources are read per lane, because each can lie on its own:
  * the lane's own log line (`copy=ok` / `result=ok`) -- says a run finished
  * the restic repository's newest snapshot time -- says data actually landed
A log line with no matching snapshot means the run claimed success without storing
anything, which is the failure this tool exists to surface.

Exit codes: 0 fresh, 1 stale (older than the threshold), 2 unreadable.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

BACKUP_ROOT = Path(r"C:\Users\pglyn\dail_tracker_backup")
LOG_DIR = Path(__file__).resolve().parent.parent / "logs" / "standalone"

#: A daily lane is stale once it has missed two runs -- one miss is a laptop asleep at
#: 02:00, two in a row is a lane that has stopped.
DEFAULT_MAX_AGE_HOURS = 48

#: `<timestamp>  ... copy=ok` (rclone lane) or `... result=ok` (restic lane).
_OK_LINE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s+.*\b(?:copy|result)=ok\b")


def _newest_ok_log(path: Path) -> datetime | None:
    if not path.exists():
        return None
    newest: datetime | None = None
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        match = _OK_LINE.match(line.strip())
        if not match:
            continue
        stamp = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M:%S")
        if newest is None or stamp > newest:
            newest = stamp
    return newest


def _restic_binary() -> str | None:
    from shutil import which

    found = which("restic")
    if found:
        return found
    packages = Path(os.environ.get("LOCALAPPDATA", "")) / "Microsoft" / "WinGet" / "Packages"
    if not packages.is_dir():
        return None
    return next((str(p) for p in packages.rglob("restic*.exe")), None)


def _repo_password(repo_name: str) -> str | None:
    """Passwords live one line BELOW their `<repo_name>` header line."""
    password_file = BACKUP_ROOT / "restic_passwords.txt"
    if not password_file.exists():
        return None
    lines = password_file.read_text(encoding="utf-8", errors="replace").splitlines()
    for index, line in enumerate(lines):
        if line.strip().startswith(repo_name) and index + 1 < len(lines):
            return lines[index + 1].strip()
    return None


def _newest_snapshot(repo_name: str) -> datetime | None:
    repo = BACKUP_ROOT / repo_name
    binary = _restic_binary()
    password = _repo_password(repo_name)
    if not repo.is_dir() or binary is None or password is None:
        return None

    environment = {**os.environ, "RESTIC_REPOSITORY": str(repo), "RESTIC_PASSWORD": password}
    try:
        result = subprocess.run(
            [binary, "snapshots", "--json", "--latest", "1"],
            capture_output=True,
            text=True,
            timeout=180,
            env=environment,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0 or not result.stdout.strip():
        return None

    try:
        snapshots = json.loads(result.stdout)
    except json.JSONDecodeError:
        return None
    stamps = []
    for snapshot in snapshots:
        raw = str(snapshot.get("time", ""))
        # restic emits RFC3339 with nanoseconds, which datetime cannot parse; trim to
        # microseconds and normalise a trailing Z.
        raw = re.sub(r"(\.\d{6})\d+", r"\1", raw).replace("Z", "+00:00")
        try:
            stamps.append(datetime.fromisoformat(raw).astimezone().replace(tzinfo=None))
        except ValueError:
            continue
    return max(stamps) if stamps else None


def _age_text(stamp: datetime | None, now: datetime) -> str:
    if stamp is None:
        return "never / unreadable"
    hours = (now - stamp).total_seconds() / 3600
    return f"{stamp:%Y-%m-%d %H:%M} ({hours:.0f}h ago)"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-age-hours", type=float, default=DEFAULT_MAX_AGE_HOURS)
    args = parser.parse_args()

    now = datetime.now()
    limit = timedelta(hours=args.max_age_hours)

    lanes = (
        ("data (rclone: bronze/silver/raw_bq)", LOG_DIR / "backup_to_r2.log", None),
        ("private (restic: planning, apps)", LOG_DIR / "backup_restic_to_r2.log", "restic_private"),
        ("sandbox (restic: whole repo)", LOG_DIR / "backup_restic_to_r2.log", "restic_sandbox"),
    )

    stale = 0
    print(f"Backup freshness (threshold {args.max_age_hours:.0f}h)\n")
    for label, log_path, repo_name in lanes:
        log_stamp = _newest_ok_log(log_path)
        snapshot_stamp = _newest_snapshot(repo_name) if repo_name else None
        # The lane is only as fresh as its weakest evidence: a log line claiming success
        # with no snapshot behind it is not a backup.
        candidates = [s for s in (log_stamp, snapshot_stamp) if s is not None]
        effective = min(candidates) if candidates else None

        print(f"  {label}")
        print(f"      last ok log : {_age_text(log_stamp, now)}")
        if repo_name:
            print(f"      last snapshot: {_age_text(snapshot_stamp, now)}")

        if effective is None or now - effective > limit:
            stale += 1
            print("      [!!] STALE")
        print()

    if stale:
        print(f"{stale} lane(s) stale. Run tools/backup_to_r2.ps1 and tools/backup_restic_to_r2.ps1.")
        return 1
    print("All lanes fresh.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
