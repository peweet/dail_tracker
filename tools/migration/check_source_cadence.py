"""Cadence checker — which sources are DUE, OVERDUE, or TAKEN DOWN?

Joins the curated ledger (data/_meta/source_cadence.csv) with the last-known
freshness per source (data/_meta/source_health.json's ``days_old`` + ``status``)
and answers the two questions a human cannot hold across 129 sources: **what needs
pulling now**, and **which sources look broken or gone?**

Reuses the status model of tools/freshness_status.py (cadence * GRACE, non-fatal
by default, ``--strict`` for CI) — the same OK/LATE logic, applied per source
instead of per automation lane.

Two axes, one table
-------------------
* CADENCE axis (needs a curated cadence): STATIC / OK / DUE / OVERDUE.
* HEALTH axis (works with no curation): BROKEN / TAKEN_DOWN — folded in from the
  former source_health_ledger. A source whose health check fails is BROKEN; if it
  has been failing past the "it's gone" horizon (default 14 days) it is TAKEN_DOWN.
  Telling those apart needs memory, so a tiny state file records when each source
  first went bad (data/_meta/source_cadence_state.json).

Status per source (worst first):
    TAKEN_DOWN — health failing continuously past TAKEN_DOWN_AFTER_DAYS — presumed gone.
    BROKEN     — health check reports failed (first/recent) — a real problem now.
    OVERDUE    — newest pull older than cadence * grace.
    DUE        — a known release window (next_expected) passed and the newest pull
                 is older than one cadence — new data is expected upstream.
    REVIEW     — cadence not yet human-curated (curated!=yes); not yet judged.
    UNKNOWN    — no freshness signal (health check skipped / never run against bronze).
    OK         — pulled within cadence * grace.
    STATIC     — one-off historical file (cadence_days=0); never due.

``--strict`` exits non-zero if any source is OVERDUE, BROKEN, or TAKEN_DOWN (for the
scheduled canary). Stdlib-only.

Usage
-----
    python tools/migration/check_source_cadence.py            # table
    python tools/migration/check_source_cadence.py --strict   # exit 1 on problems
    python tools/migration/check_source_cadence.py --json
    python tools/migration/check_source_cadence.py --due-only # only DUE/OVERDUE/BROKEN/TAKEN_DOWN
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
from tools.cadence_constants import GRACE, TAKEN_DOWN_AFTER_DAYS  # noqa: E402 — one home for both

LEDGER = PROJECT_ROOT / "data" / "_meta" / "source_cadence.csv"
HEALTH = PROJECT_ROOT / "data" / "_meta" / "source_health.json"
STATE = PROJECT_ROOT / "data" / "_meta" / "source_cadence_state.json"

# Worst first — drives sort order, the counts row, and which statuses are "problems".
STATUS_ORDER = ["TAKEN_DOWN", "BROKEN", "OVERDUE", "DUE", "REVIEW", "UNKNOWN", "OK", "STATIC"]
PROBLEM_STATUSES = {"TAKEN_DOWN", "BROKEN", "OVERDUE"}


def load_ledger() -> list[dict]:
    if not LEDGER.exists():
        print(f"ledger not found: {LEDGER} — run tools/migration/build_source_cadence.py", file=sys.stderr)
        return []
    with LEDGER.open(encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def load_health() -> dict[str, dict]:
    if not HEALTH.exists():
        return {}
    h = json.loads(HEALTH.read_text(encoding="utf-8"))
    return {r["source_id"]: r for r in h.get("sources", [])}


def load_state() -> dict[str, dict]:
    if not STATE.exists():
        return {}
    try:
        return json.loads(STATE.read_text(encoding="utf-8")).get("sources", {})
    except (json.JSONDecodeError, OSError):
        return {}


def _int(v, default=None):
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return default


def _parse_date(s: str) -> date | None:
    s = (s or "").strip()
    for fmt in ("%Y-%m-%d", "%Y-%m", "%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def evaluate(
    today: date,
    ledger: list[dict] | None = None,
    health: dict[str, dict] | None = None,
    prior_state: dict[str, dict] | None = None,
) -> dict:
    """Classify every ledger source. Pure w.r.t. its inputs (inject for tests).

    Returns the rollup PLUS the ``state`` to persist — for each source that is
    currently failing, when it first went bad — so the next run can tell a fresh
    BROKEN from a long-dead TAKEN_DOWN.
    """
    ledger = load_ledger() if ledger is None else ledger
    health = load_health() if health is None else health
    prior_state = load_state() if prior_state is None else prior_state

    rows: list[dict] = []
    new_state: dict[str, dict] = {}

    for r in ledger:
        sid = r["source_id"]
        cadence_days = _int(r.get("cadence_days"), 0) or 0
        hrec = health.get(sid, {})
        days_old = _int(hrec.get("days_old"))
        hstatus = hrec.get("status")

        nxt = _parse_date(r.get("next_expected", ""))
        window_open = nxt is not None and today >= nxt

        days_broken: int | None = None

        if hstatus == "failed":
            # Health axis: carry the first-failure date forward to measure duration.
            first_bad = _parse_date((prior_state.get(sid) or {}).get("first_broken_at", "")) or today
            days_broken = (today - first_bad).days
            new_state[sid] = {"first_broken_at": first_bad.isoformat()}
            status = "TAKEN_DOWN" if days_broken >= TAKEN_DOWN_AFTER_DAYS else "BROKEN"
        elif (r.get("curated") or "").strip() != "yes":
            # cadence not yet human-set — don't pretend to judge it
            status = "REVIEW"
        elif cadence_days == 0:
            status = "STATIC"
        elif days_old is None:
            status = "UNKNOWN"
        elif days_old > cadence_days * GRACE:
            status = "OVERDUE"
        elif window_open and days_old > cadence_days:
            status = "DUE"
        else:
            status = "OK"

        rows.append({
            "source_id": sid,
            "cadence": r.get("cadence", ""),
            "cadence_days": cadence_days,
            "days_old": days_old,
            "days_broken": days_broken,
            "next_expected": r.get("next_expected", ""),
            "poller": r.get("poller", ""),
            "runner": r.get("runner", ""),
            "curated": r.get("curated", ""),
            "status": status,
        })

    order = {s: i for i, s in enumerate(STATUS_ORDER)}
    rows.sort(key=lambda x: (order.get(x["status"], 9), x["source_id"]))
    problems = [r["source_id"] for r in rows if r["status"] in PROBLEM_STATUSES]
    return {
        "generated_at": today.isoformat(),
        "grace_factor": GRACE,
        "taken_down_after_days": TAKEN_DOWN_AFTER_DAYS,
        "counts": {s: sum(1 for r in rows if r["status"] == s) for s in STATUS_ORDER},
        "problem_sources": problems,
        "sources": rows,
        "state": {"generated_at": today.isoformat(), "sources": new_state},
    }


def print_table(rollup: dict, due_only: bool) -> None:
    counts = rollup["counts"]
    summary = "  ".join(f"{k}={v}" for k, v in counts.items() if v)
    print(f"source cadence @ {rollup['generated_at']}  (grace x{rollup['grace_factor']})")
    print(f"  {summary}\n")
    shown = 0
    print(f"  {'source_id':<34} {'status':<10} {'cadence':<10} {'age':<7} {'runner':<12} poller")
    for r in rollup["sources"]:
        if due_only and r["status"] not in PROBLEM_STATUSES and r["status"] != "DUE":
            continue
        age = "--" if r["days_old"] is None else f"{r['days_old']}d"
        if r["status"] in ("BROKEN", "TAKEN_DOWN") and r.get("days_broken") is not None:
            age = f"{r['days_broken']}d bad"
        print(f"  {r['source_id']:<34} {r['status']:<10} {r['cadence']:<10} {age:<7} {r['runner']:<12} {r['poller']}")
        shown += 1
    if due_only and shown == 0:
        print("  nothing due, overdue, broken, or taken down.")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--strict", action="store_true", help="exit 1 if any source is OVERDUE/BROKEN/TAKEN_DOWN")
    ap.add_argument("--json", action="store_true", help="emit machine-readable rollup")
    ap.add_argument("--due-only", action="store_true", help="show only DUE/OVERDUE/BROKEN/TAKEN_DOWN")
    ap.add_argument("--no-state", action="store_true", help="don't read/write the broken-since state file")
    args = ap.parse_args()

    prior_state = {} if args.no_state else load_state()
    rollup = evaluate(datetime.now(UTC).date(), prior_state=prior_state)
    if not rollup["sources"]:
        return 1

    if not args.no_state:
        STATE.write_text(json.dumps(rollup["state"], indent=2) + "\n", encoding="utf-8")

    published = {k: v for k, v in rollup.items() if k != "state"}
    if args.json:
        sys.stdout.write(json.dumps(published, indent=2) + "\n")
    else:
        print_table(rollup, args.due_only)

    if args.strict and rollup["problem_sources"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
