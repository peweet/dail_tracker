"""tools/freshness_heartbeat.py — record a per-lane "I ran successfully" beat.

The problem this solves
-----------------------
Data freshness is produced by several INDEPENDENT lanes on different clocks and
different runners:

* the local Windows scheduled task (the legal-diary + year-round bundle) — daily,
  refreshes LOCAL gold but does NOT publish;
* the cloud GitHub Actions (legal-diary OpenView, live-tenders, money-flow) —
  daily, refresh AND publish;
* a full local ``pipeline.py`` run — ad hoc;
* the occasional manual ``git add gold && push``.

``data/_meta/freshness.json`` only stamps the END of a local pipeline run, so it
is blind to whether each of those lanes actually fired. There was no single place
to answer "did lane X run on time?". This heartbeat is that place.

How it works
------------
Each lane calls this on SUCCESS (CLI or ``record()``), writing ONE small file:
``data/_meta/heartbeats/<lane>.json``. One file per lane => two runners (local +
cloud) never touch the same file, so there are no merge conflicts when both commit
their beats. ``tools/freshness_status.py`` reads the whole directory and flags any
lane whose newest beat is older than its declared cadence (or missing entirely).

Deliberately stdlib-only: it has to run inside the minimal cloud Action env and
inside a PowerShell scheduled task without importing polars/config.

Usage:
    python tools/freshness_heartbeat.py legal_diary_openview --runner cloud --cadence-hours 24
    python tools/freshness_heartbeat.py live_tenders --runner cloud --cadence-hours 24 --note "42 live notices"
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

# Repo root = parent of tools/. No config import (must stay stdlib-only / cloud-safe).
PROJECT_ROOT = Path(__file__).resolve().parents[1]
HEARTBEAT_DIR = PROJECT_ROOT / "data" / "_meta" / "heartbeats"

# A lane name becomes a filename, so keep it to a safe slug.
_LANE_RE = re.compile(r"^[a-z0-9][a-z0-9_]*$")


def _now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def record(
    lane: str,
    *,
    runner: str = "local",
    cadence_hours: float | None = 24,
    note: str | None = None,
    when: str | None = None,
) -> Path:
    """Write/overwrite the heartbeat for ``lane``. Returns the file path.

    ``runner``        — "local" | "cloud" | "manual"; informational, shown in the report.
    ``cadence_hours`` — how often this lane is expected to beat; the report flags it
                        OVERDUE once the beat is older than cadence * grace. None => no
                        staleness threshold (informational only).
    ``note``          — short free-text shown in the report (e.g. a row count).
    ``when``          — override the timestamp (ISO-8601 Z); defaults to now.
    """
    if not _LANE_RE.match(lane):
        raise ValueError(f"invalid lane slug {lane!r} — use [a-z0-9_], starting alphanumeric")
    HEARTBEAT_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "lane": lane,
        "last_success_utc": when or _now_iso(),
        "runner": runner,
        "cadence_hours": cadence_hours,
        "note": note,
    }
    path = HEARTBEAT_DIR / f"{lane}.json"
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    return path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Record a per-lane freshness heartbeat.")
    parser.add_argument("lane", help="lane slug, e.g. legal_diary_openview (chars: a-z 0-9 _)")
    parser.add_argument("--runner", default="local", choices=["local", "cloud", "manual"])
    parser.add_argument(
        "--cadence-hours",
        type=float,
        default=24.0,
        help="expected beat interval in hours (default 24); 0 => informational, no staleness flag",
    )
    parser.add_argument("--note", default=None, help="short free-text (e.g. a row count)")
    parser.add_argument("--when", default=None, help="override timestamp (ISO-8601 Z); defaults to now")
    args = parser.parse_args(argv)

    cadence = None if args.cadence_hours == 0 else args.cadence_hours
    try:
        path = record(
            args.lane,
            runner=args.runner,
            cadence_hours=cadence,
            note=args.note,
            when=args.when,
        )
    except ValueError as e:
        sys.stderr.write(f"heartbeat: {e}\n")
        return 2
    rel = path.relative_to(PROJECT_ROOT).as_posix()
    print(f"heartbeat: {args.lane} -> {rel} ({args.runner}, cadence {cadence}h)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
