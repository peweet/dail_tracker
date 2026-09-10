#!/usr/bin/env python
"""Append-only status registry for long-running builds, transfers, and gates.

The registry is deliberately small and measured.  A running process, a recent log
line, and an artifact on disk are different facts; this tool records the owner who
observed one job, the exact artifact, its last measured progress, and the evidence
behind that observation.  It never polls or infers completion.

Examples::

    python tools/job_status.py start nlc-point-sibling \
        --owner task-01a046f5 --kind build \
        --artifact data/layers/_point_scoped/national_land_cover.parquet \
        --source-snapshot git:bda499d --phase external-sort \
        --total 10114816 --unit rows --evidence "DuckDB query count at 2026-08-28T09:10Z"

    python tools/job_status.py update nlc-point-sibling \
        --owner task-01a046f5 --current 5000000 \
        --phase bbox-proof --eta unknown --evidence "COUNT(*) from .part file"

    python tools/job_status.py finish nlc-point-sibling \
        --owner task-01a046f5 --state succeeded \
        --evidence "final row count and SHA-256 verified"

    python tools/job_status.py show --active

The machine-local JSONL file is ignored by Git.  Every update is a complete event,
so the latest valid row for a job is its current status and prior observations remain
available for diagnosis.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LEDGER = ROOT / "logs" / "job_status.jsonl"
SCHEMA_VERSION = 1
JOB_ID_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{2,79}$")
KINDS = ("build", "transfer", "test", "release", "ingest", "other")
ACTIVE_STATES = frozenset({"running"})
TERMINAL_STATES = ("succeeded", "failed", "blocked", "cancelled", "superseded")


class StatusError(ValueError):
    """User-actionable registry contract failure."""


def _now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    rows: list[dict] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError as exc:
            raise StatusError(f"{path}:{number} is not valid JSON: {exc.msg}") from exc
        if not isinstance(row, dict) or row.get("schema_version") != SCHEMA_VERSION:
            raise StatusError(f"{path}:{number} has an unsupported status schema")
        rows.append(row)
    return rows


def latest_by_job(path: Path) -> dict[str, dict]:
    latest: dict[str, dict] = {}
    for row in _read_rows(path):
        job_id = str(row.get("job_id") or "")
        if job_id:
            latest[job_id] = row
    return latest


def _append(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o600)
    try:
        written = os.write(descriptor, encoded)
        if written != len(encoded):
            raise OSError(f"short status-ledger write: {written}/{len(encoded)} bytes")
    finally:
        os.close(descriptor)


def _claim_path(path: Path, job_id: str) -> Path:
    return path.parent / f"{path.stem}_claims" / f"{job_id}.claim"


def _claim_job(path: Path, job_id: str, owner: str) -> Path:
    """Atomically reserve a stable id so two sessions cannot start one job."""
    claim = _claim_path(path, job_id)
    claim.parent.mkdir(parents=True, exist_ok=True)
    try:
        descriptor = os.open(claim, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise StatusError(f"job {job_id!r} already has an ownership claim; inspect before retrying") from exc
    try:
        os.write(descriptor, (owner.strip() + "\n").encode("utf-8"))
    finally:
        os.close(descriptor)
    return claim


def _text(value: str, label: str) -> str:
    cleaned = value.strip()
    if not cleaned:
        raise StatusError(f"{label} must not be empty")
    return cleaned


def _number(value: float | None, label: str) -> float | None:
    if value is None:
        return None
    if not math.isfinite(value) or value < 0:
        raise StatusError(f"{label} must be a finite non-negative number")
    return value


def _validate_progress(current: float | None, total: float | None, unit: str | None) -> None:
    current = _number(current, "current")
    total = _number(total, "total")
    if current is not None and total is None:
        raise StatusError("current requires total")
    if total is not None and not unit:
        raise StatusError("total requires --unit")
    if current is not None and total is not None and current > total:
        raise StatusError("current must not exceed total")


def start_job(
    path: Path,
    *,
    job_id: str,
    owner: str,
    kind: str,
    artifact: str,
    source_snapshot: str,
    phase: str,
    evidence: str,
    current: float | None = None,
    total: float | None = None,
    unit: str | None = None,
    eta: str = "unknown",
    observed_at: str | None = None,
) -> dict:
    if not JOB_ID_RE.fullmatch(job_id):
        raise StatusError("job id must be 3-80 lowercase letters, digits, dots, underscores, or hyphens")
    if job_id in latest_by_job(path):
        raise StatusError(f"job {job_id!r} already exists; use update or choose a new stable id")
    if kind not in KINDS:
        raise StatusError(f"kind must be one of {KINDS}")
    if total is not None and current is None:
        current = 0.0
    _validate_progress(current, total, unit)
    clean_owner = _text(owner, "owner")
    row = {
        "schema_version": SCHEMA_VERSION,
        "job_id": job_id,
        "sequence": 1,
        "state": "running",
        "owner": clean_owner,
        "kind": kind,
        "artifact": _text(artifact, "artifact"),
        "source_snapshot": _text(source_snapshot, "source snapshot"),
        "phase": _text(phase, "phase"),
        "evidence": _text(evidence, "evidence"),
        "current": current,
        "total": total,
        "unit": _text(unit, "unit") if unit else None,
        "eta": _text(eta, "eta"),
        "observed_at": observed_at or _now(),
    }
    claim = _claim_job(path, job_id, clean_owner)
    try:
        _append(path, row)
    except BaseException:
        claim.unlink(missing_ok=True)
        raise
    return row


def update_job(
    path: Path,
    *,
    job_id: str,
    owner: str,
    evidence: str,
    phase: str | None = None,
    current: float | None = None,
    total: float | None = None,
    unit: str | None = None,
    eta: str | None = None,
    state: str = "running",
    observed_at: str | None = None,
) -> dict:
    previous = latest_by_job(path).get(job_id)
    if previous is None:
        raise StatusError(f"unknown job {job_id!r}; start it first")
    if previous.get("state") not in ACTIVE_STATES:
        raise StatusError(f"job {job_id!r} is already {previous.get('state')}")
    if owner.strip() != previous.get("owner"):
        raise StatusError(f"job {job_id!r} is owned by {previous.get('owner')!r}, not {owner.strip()!r}")
    if state != "running" and state not in TERMINAL_STATES:
        raise StatusError(f"state must be running or one of {TERMINAL_STATES}")

    next_total = previous.get("total") if total is None else total
    next_current = previous.get("current") if current is None else current
    next_unit = previous.get("unit") if unit is None else unit.strip()
    _validate_progress(next_current, next_total, next_unit)
    if current is not None and previous.get("current") is not None and current < previous["current"]:
        raise StatusError("current progress must not move backwards; supersede the job if measurement changed")
    if total is not None and previous.get("total") is not None and total != previous["total"]:
        raise StatusError("total is immutable; supersede the job if its scope changed")

    row = dict(previous)
    row.update(
        sequence=int(previous.get("sequence", 0)) + 1,
        state=state,
        phase=_text(phase, "phase") if phase is not None else previous["phase"],
        evidence=_text(evidence, "evidence"),
        current=next_current,
        total=next_total,
        unit=next_unit,
        eta=_text(eta, "eta") if eta is not None else previous["eta"],
        observed_at=observed_at or _now(),
    )
    _append(path, row)
    return row


def _progress(row: dict) -> str:
    current, total, unit = row.get("current"), row.get("total"), row.get("unit")
    if total is None:
        return "unmeasured"
    percent = 0.0 if not total else 100.0 * float(current or 0) / float(total)
    return f"{current:g}/{total:g} {unit} ({percent:.1f}%)"


def _print_row(row: dict) -> None:
    print(f"[{row['state']}] {row['job_id']} owner={row['owner']} kind={row['kind']} artifact={row['artifact']}")
    print(f"  observed={row['observed_at']} phase={row['phase']} progress={_progress(row)} eta={row['eta']}")
    print(f"  snapshot={row['source_snapshot']} evidence={row['evidence']}")


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER, help=argparse.SUPPRESS)
    sub = parser.add_subparsers(dest="command", required=True)

    start = sub.add_parser("start", help="register one new long-running job")
    start.add_argument("job_id")
    start.add_argument("--owner", required=True)
    start.add_argument("--kind", required=True, choices=KINDS)
    start.add_argument("--artifact", required=True)
    start.add_argument("--source-snapshot", required=True)
    start.add_argument("--phase", required=True)
    start.add_argument("--evidence", required=True)
    start.add_argument("--current", type=float)
    start.add_argument("--total", type=float)
    start.add_argument("--unit")
    start.add_argument("--eta", default="unknown")

    update = sub.add_parser("update", help="append a measured observation for a running job")
    update.add_argument("job_id")
    update.add_argument("--owner", required=True)
    update.add_argument("--evidence", required=True)
    update.add_argument("--phase")
    update.add_argument("--current", type=float)
    update.add_argument("--total", type=float)
    update.add_argument("--unit")
    update.add_argument("--eta")

    finish = sub.add_parser("finish", help="record a terminal state with evidence")
    finish.add_argument("job_id")
    finish.add_argument("--owner", required=True)
    finish.add_argument("--state", required=True, choices=TERMINAL_STATES)
    finish.add_argument("--evidence", required=True)
    finish.add_argument("--phase")
    finish.add_argument("--current", type=float)

    show = sub.add_parser("show", help="show latest observations")
    show.add_argument("job_id", nargs="?")
    show.add_argument("--active", action="store_true")
    show.add_argument("--json", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "start":
            row = start_job(
                args.ledger,
                job_id=args.job_id,
                owner=args.owner,
                kind=args.kind,
                artifact=args.artifact,
                source_snapshot=args.source_snapshot,
                phase=args.phase,
                evidence=args.evidence,
                current=args.current,
                total=args.total,
                unit=args.unit,
                eta=args.eta,
            )
            _print_row(row)
            return 0
        if args.command == "update":
            row = update_job(
                args.ledger,
                job_id=args.job_id,
                owner=args.owner,
                evidence=args.evidence,
                phase=args.phase,
                current=args.current,
                total=args.total,
                unit=args.unit,
                eta=args.eta,
            )
            _print_row(row)
            return 0
        if args.command == "finish":
            row = update_job(
                args.ledger,
                job_id=args.job_id,
                owner=args.owner,
                evidence=args.evidence,
                phase=args.phase,
                current=args.current,
                state=args.state,
            )
            _print_row(row)
            return 0

        rows = latest_by_job(args.ledger)
        selected = [rows[args.job_id]] if args.job_id in rows else list(rows.values())
        if args.job_id and args.job_id not in rows:
            raise StatusError(f"unknown job {args.job_id!r}")
        if args.active:
            selected = [row for row in selected if row.get("state") in ACTIVE_STATES]
        selected.sort(key=lambda row: (row.get("observed_at", ""), row.get("job_id", "")), reverse=True)
        if args.json:
            for row in selected:
                print(json.dumps(row, ensure_ascii=False, sort_keys=True))
        elif not selected:
            print("no matching jobs")
        else:
            for row in selected:
                _print_row(row)
        return 0
    except StatusError as exc:
        parser.error(str(exc))
    return 2


if __name__ == "__main__":
    sys.exit(main())
