"""tools/freshness_status.py — one view of every refresh lane's freshness.

Reads the per-lane heartbeats written by ``tools/freshness_heartbeat.py`` and
answers the question that was previously scattered across six places (freshness
.json, coverage JSONs, source-health canary, poller manifests, scheduled-task
logs, GitHub Actions history): **did each lane run on time?**

A lane is one independently-scheduled refresh job. The registry below declares
every lane we EXPECT to beat, so a lane that has never run is reported MISSING
rather than silently absent — the most dangerous failure (a job that quietly
never fires) is the one this is built to catch.

Status per lane:
    OK       — newest beat is within cadence * grace
    LATE     — beat exists but is older than cadence * grace
    MISSING  — expected lane has no heartbeat file at all
    INFO     — lane has no cadence (informational; never flagged)

``--strict`` exits non-zero if any expected lane is LATE or MISSING (for CI /
the scheduled freshness canary). Stdlib-only so it runs anywhere.

Usage:
    python tools/freshness_status.py            # print the table
    python tools/freshness_status.py --strict   # exit 1 if any lane LATE/MISSING
    python tools/freshness_status.py --json      # machine-readable rollup
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
HEARTBEAT_DIR = PROJECT_ROOT / "data" / "_meta" / "heartbeats"

# Every refresh lane we expect to beat. ``cadence_hours`` is the expected interval;
# ``runner`` is where it runs; ``publishes`` flags whether a successful run also
# pushes data to the deployed app (cloud Actions) or only refreshes LOCAL gold
# (the Windows task) — the latter still needs a manual/cloud publish to go live.
LANES: dict[str, dict] = {
    "legal_diary_docx": {
        "cadence_hours": 24,
        "runner": "local",
        "publishes": False,
        "desc": "Legal Diary .docx daily bundle (Windows task) -> local gold",
    },
    "legal_diary_openview": {
        "cadence_hours": 24,
        "runner": "cloud",
        "publishes": True,
        "desc": "Legal Diary OpenView (Circuit + higher courts) cloud refresh + publish",
    },
    "live_tenders": {
        "cadence_hours": 24,
        "runner": "cloud",
        "publishes": True,
        "desc": "Live national tender snapshot cloud refresh + publish",
    },
    "money_flow": {
        "cadence_hours": 24,
        "runner": "cloud",
        "publishes": True,
        "desc": "Money-flow (payments/procurement) cloud refresh + publish",
    },
    "pipeline": {
        "cadence_hours": None,  # ad hoc full local run — informational
        "runner": "local",
        "publishes": False,
        "desc": "Full local pipeline.py run -> local gold + freshness.json",
    },
}

# A beat is only LATE once it exceeds cadence * GRACE — absorbs a run that fires a
# few hours behind schedule (laptop asleep, Actions queue) without false alarms.
GRACE = 2.0


def _parse_iso(s: str) -> datetime:
    """Parse an ISO-8601 timestamp (accepts a trailing Z) as tz-aware UTC."""
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _read_beats() -> dict[str, dict]:
    """lane -> heartbeat payload, for every *.json under the heartbeats dir."""
    beats: dict[str, dict] = {}
    if not HEARTBEAT_DIR.exists():
        return beats
    for fp in sorted(HEARTBEAT_DIR.glob("*.json")):
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        lane = data.get("lane") or fp.stem
        beats[lane] = data
    return beats


def _evaluate(now: datetime) -> dict:
    """Merge the registry with the on-disk beats into a status rollup."""
    beats = _read_beats()
    rows: list[dict] = []
    # Known lanes first (registry order), then any unexpected beat we find.
    lane_names = list(LANES) + [name for name in beats if name not in LANES]

    for lane in lane_names:
        spec = LANES.get(lane, {})
        beat = beats.get(lane)
        # Registered lanes use the authoritative registry cadence; an unregistered beat
        # (a typo'd or newly-added lane) is judged against its own self-declared cadence.
        cadence = spec["cadence_hours"] if lane in LANES else (beat or {}).get("cadence_hours")
        row: dict = {
            "lane": lane,
            "runner": (beat or {}).get("runner") or spec.get("runner", "?"),
            "cadence_hours": cadence,
            "publishes": spec.get("publishes"),
            "registered": lane in LANES,
            "last_success_utc": (beat or {}).get("last_success_utc"),
            "note": (beat or {}).get("note"),
        }

        ts = (beat or {}).get("last_success_utc")
        try:
            age_h = (now - _parse_iso(ts)).total_seconds() / 3600 if ts else None
        except (ValueError, TypeError):
            age_h = None
        row["age_hours"] = round(age_h, 1) if age_h is not None else None

        # A lane with no cadence is purely informational and is NEVER a problem,
        # even when it has never beat (ad-hoc full pipeline runs).
        if cadence is None:
            row["status"] = "INFO"
        elif age_h is None:
            row["status"] = "MISSING"
        elif age_h > cadence * GRACE:
            row["status"] = "LATE"
        else:
            row["status"] = "OK"
        rows.append(row)

    problems = [r["lane"] for r in rows if r["status"] in ("LATE", "MISSING")]
    return {
        "generated_at": now.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "grace_factor": GRACE,
        "problem_lanes": problems,
        "lanes": rows,
    }


def _fmt_age(age_hours: float | None) -> str:
    if age_hours is None:
        return "--"
    if age_hours < 48:
        return f"{age_hours:.0f}h"
    return f"{age_hours / 24:.1f}d"


def _print_table(rollup: dict) -> None:
    print(f"freshness lanes @ {rollup['generated_at']}  (grace x{rollup['grace_factor']})")
    print(f"  {'lane':<22} {'status':<8} {'runner':<7} {'age':<7} {'cadence':<9} note")
    for r in rollup["lanes"]:
        cad = "--" if r["cadence_hours"] is None else f"{r['cadence_hours']:g}h"
        note = r.get("note") or ""
        pub = "" if r.get("publishes") else " (local-only)"
        print(f"  {r['lane']:<22} {r['status']:<8} {r['runner']:<7} {_fmt_age(r['age_hours']):<7} {cad:<9} {note}{pub}")
    problems = rollup["problem_lanes"]
    if problems:
        print(f"\n[ATTENTION] {len(problems)} lane(s) LATE/MISSING: {', '.join(problems)}")
    else:
        print("\nAll registered lanes are fresh.")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Show every refresh lane's freshness.")
    parser.add_argument("--strict", action="store_true", help="exit 1 if any lane is LATE/MISSING")
    parser.add_argument("--json", action="store_true", help="emit the machine-readable rollup")
    args = parser.parse_args(argv)

    rollup = _evaluate(datetime.now(UTC))

    if args.json:
        sys.stdout.write(json.dumps(rollup, indent=2) + "\n")
    else:
        _print_table(rollup)

    if args.strict and rollup["problem_lanes"]:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
