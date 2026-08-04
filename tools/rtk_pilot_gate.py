"""Advisory adoption trigger for the project-local RTK pytest pilot.

RTK's SQLite history can measure how much command output it filtered, but cannot
tell whether an agent had to rerun raw pytest, whether a summary omitted a needed
fact, or whether a filtered exit code matched the raw one. This tool deliberately
combines both sources of evidence:

* ``.cache/rtk/history.db`` -- objective, project-scoped RTK pytest metrics.
* ``.cache/rtk/pilot-events.jsonl`` -- wrapper run receipts plus small, explicit
  human review records.

The result is an advisory state only:

* ``collecting`` -- more representative evidence is needed.
* ``blocked`` -- a safety, parity, or data-integrity condition failed.
* ``eligible_for_review`` -- the evidence clears the thresholds; a human may
  choose to widen the *explicit* allowlist.

It never installs a hook, edits instructions, or changes RTK usage automatically.

Usage:
    python tools/rtk_pilot_gate.py
    python tools/rtk_pilot_gate.py --record sufficient --scenario failure \
        --run-id <receipt-id> \
        --note "Compared the compact failure with its tee; node and assertion were complete."
    python tools/rtk_pilot_gate.py --record paired --scenario pass \
        --run-id <receipt-id> \
        --raw-exit 0 --rtk-exit 0 --raw-ms 2200 --rtk-ms 2350 \
        --note "Paired raw and RTK run selected the same test and preserved exit zero."
    python tools/rtk_pilot_gate.py --require-eligible

``--require-eligible`` is the automation trigger: it exits 0 only when the state
is ``eligible_for_review``. It still performs no adoption action.
"""

from __future__ import annotations

import argparse
import json
import math
import sqlite3
import sys
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any

REPO = Path(__file__).resolve().parents[1]
CACHE = REPO / ".cache" / "rtk"
DB = CACHE / "history.db"
EVENTS = CACHE / "pilot-events.jsonl"
STATUS = CACHE / "adoption_status.json"

STATUS_SCHEMA = "rtk-pilot-gate/v1"
EVENT_SCHEMA = "rtk-pilot-event/v1"
POLICY_VERSION = 1
RTK_VERSION = "0.44.2"
NOTE_MIN_CHARS = 20
RECORD_KINDS = ("sufficient", "raw-rerun", "semantic-incident", "paired")
SCENARIOS = ("pass", "failure", "no-tests")
EVENT_KINDS = ("rtk_run", *RECORD_KINDS)


@dataclass(frozen=True)
class Policy:
    """Fixed, conservative thresholds for widening the pytest pilot."""

    window_days: int = 14
    min_runs: int = 30
    min_active_days: int = 5
    min_distinct_selectors: int = 10
    min_input_tokens: int = 10_000
    min_weighted_savings_pct: float = 50.0
    min_p10_savings_pct: float = 30.0
    min_sufficient_reviews: int = 5
    min_failure_reviews: int = 3
    min_paired_checks: int = 5
    max_fallback_rate_pct: float = 5.0
    max_p50_added_ms: int = 1_000
    max_p50_overhead_pct: float = 10.0


DEFAULT_POLICY = Policy()


def _now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _normalise_path(value: object) -> str:
    path = str(value or "").replace("\\", "/").rstrip("/")
    # RTK on Windows can record extended paths such as \\?\C:\repo.
    while path.startswith("//?/"):
        path = path[4:]
    return path.casefold()


def _same_project(value: object, repo: Path) -> bool:
    # The wrapper owns this database, so an empty legacy project field is still
    # safe to include. Non-empty fields must identify this repository exactly.
    return not value or _normalise_path(value) == _normalise_path(repo.resolve())


def _read_events(path: Path, cutoff: datetime, current: datetime) -> tuple[list[dict[str, Any]], int]:
    """Read in-window event rows, rejecting malformed or future-dated evidence."""
    if not path.exists():
        return [], 0
    rows: list[dict[str, Any]] = []
    malformed = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            malformed += 1
            continue
        if not isinstance(row, dict):
            malformed += 1
            continue
        if row.get("schema") != EVENT_SCHEMA or row.get("kind") not in EVENT_KINDS:
            malformed += 1
            continue
        at = _parse_time(row.get("at_utc"))
        if at is None:
            malformed += 1
            continue
        if at < cutoff:
            continue
        if at > current:
            malformed += 1
            continue
        row["_at"] = at
        rows.append(row)
    return rows, malformed


def _load_commands(
    db_path: Path, repo: Path, cutoff: datetime, current: datetime
) -> tuple[list[dict[str, Any]], str | None, int]:
    """Read RTK's pinned-v0.44.2 command rows without creating or mutating its DB."""
    if not db_path.exists():
        return [], None, 0
    required = {
        "timestamp",
        "original_cmd",
        "rtk_cmd",
        "input_tokens",
        "output_tokens",
        "saved_tokens",
        "savings_pct",
        "exec_time_ms",
        "project_path",
    }
    connection: sqlite3.Connection | None = None
    try:
        connection = sqlite3.connect(f"{db_path.resolve().as_uri()}?mode=ro", uri=True, timeout=5)
        connection.row_factory = sqlite3.Row
        columns = {row["name"] for row in connection.execute("PRAGMA table_info(commands)")}
        missing = sorted(required - columns)
        if missing:
            return [], f"history schema is missing: {', '.join(missing)}", 0
        result = connection.execute(
            """
            SELECT timestamp, original_cmd, rtk_cmd, input_tokens, output_tokens,
                   saved_tokens, savings_pct, exec_time_ms, project_path
            FROM commands
            WHERE rtk_cmd GLOB 'rtk pytest*'
            """
        )
        rows: list[dict[str, Any]] = []
        invalid_rows = 0
        for raw in result:
            row = dict(raw)
            if not _same_project(row["project_path"], repo):
                continue
            at = _parse_time(row["timestamp"])
            if at is None or at > current:
                invalid_rows += 1
                continue
            if at < cutoff:
                continue
            row["_at"] = at
            rows.append(row)
        return rows, None, invalid_rows
    except sqlite3.Error as exc:
        return [], f"cannot read RTK history: {exc}", 0
    finally:
        if connection is not None:
            connection.close()


def _number(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _integer(value: object) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _nonempty_text(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _scenario_for_exit(exit_code: int) -> str:
    if exit_code == 0:
        return "pass"
    if exit_code == 5:
        return "no-tests"
    return "failure"


def _valid_run_event(event: dict[str, Any], current: datetime) -> bool:
    started = _parse_time(event.get("started_at_utc"))
    exit_code = _integer(event.get("exit_code"))
    elapsed_ms = _integer(event.get("elapsed_ms"))
    return (
        _nonempty_text(event.get("run_id"))
        and event.get("command") == "pytest"
        and _nonempty_text(event.get("rtk_version"))
        and started is not None
        and started <= current
        and exit_code is not None
        and elapsed_ms is not None
        and elapsed_ms >= 0
    )


def _valid_manual_event(event: dict[str, Any], runs_by_id: dict[str, dict[str, Any]]) -> bool:
    run_id = event.get("run_id")
    if (
        not _nonempty_text(run_id)
        or event.get("scenario") not in SCENARIOS
        or not _nonempty_text(event.get("note"))
        or len(event["note"].strip()) < NOTE_MIN_CHARS
        or run_id not in runs_by_id
    ):
        return False

    run = runs_by_id[run_id]
    run_exit = _integer(run.get("exit_code"))
    if run_exit is None or event.get("scenario") != _scenario_for_exit(run_exit):
        return False
    if event["_at"] < run["_at"]:
        return False

    if event.get("kind") != "paired":
        return True

    raw_exit = _integer(event.get("raw_exit_code"))
    rtk_exit = _integer(event.get("rtk_exit_code"))
    raw_ms = _integer(event.get("raw_elapsed_ms"))
    rtk_ms = _integer(event.get("rtk_elapsed_ms"))
    return (
        raw_exit is not None
        and rtk_exit == run_exit
        and raw_ms is not None
        and rtk_ms is not None
        and raw_ms > 0
        and rtk_ms >= 0
    )


def _validated_events(
    path: Path, cutoff: datetime, current: datetime
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int]:
    """Return valid wrapper/manual evidence and count rejected current-window rows."""
    events, malformed = _read_events(path, cutoff, current)
    run_candidates: list[dict[str, Any]] = []
    for event in events:
        if event.get("kind") != "rtk_run":
            continue
        if _valid_run_event(event, current):
            run_candidates.append(event)
        else:
            malformed += 1

    run_id_counts: dict[str, int] = {}
    for event in run_candidates:
        run_id = str(event["run_id"])
        run_id_counts[run_id] = run_id_counts.get(run_id, 0) + 1
    run_events = []
    for event in run_candidates:
        if run_id_counts[str(event["run_id"])] == 1:
            run_events.append(event)
        else:
            malformed += 1
    runs_by_id = {str(event["run_id"]): event for event in run_events}

    manual_events: list[dict[str, Any]] = []
    seen_records: set[tuple[str, str]] = set()
    for event in events:
        if event.get("kind") not in RECORD_KINDS:
            continue
        key = (str(event.get("kind")), str(event.get("run_id")))
        if key in seen_records or not _valid_manual_event(event, runs_by_id):
            malformed += 1
            continue
        seen_records.add(key)
        manual_events.append(event)
    return run_events, manual_events, malformed


def _p10(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * 0.10) - 1)]


def _criterion(name: str, actual: object, threshold: object, met: bool, category: str) -> dict[str, Any]:
    return {
        "name": name,
        "actual": actual,
        "threshold": threshold,
        "met": met,
        "category": category,
    }


def build_report(
    *,
    db_path: Path = DB,
    events_path: Path = EVENTS,
    repo: Path = REPO,
    now: datetime | None = None,
    policy: Policy = DEFAULT_POLICY,
) -> dict[str, Any]:
    """Build a serialisable advisory report without writing a status file."""
    current = (now or _now()).astimezone(UTC)
    window_start = current - timedelta(days=policy.window_days)
    run_events, manual_events, malformed_events = _validated_events(events_path, window_start, current)

    # Start database evidence only once wrapper receipts exist. This makes old rows
    # from before instrumentation harmless rather than pretending they were audited.
    db_cutoff = window_start
    starts = [_parse_time(event.get("started_at_utc")) or event["_at"] for event in run_events]
    if starts:
        db_cutoff = max(window_start, min(starts) - timedelta(seconds=2))
    commands, db_error, invalid_history_rows = _load_commands(db_path, repo, db_cutoff, current)

    sufficient = [event for event in manual_events if event.get("kind") == "sufficient"]
    fallbacks = [event for event in manual_events if event.get("kind") == "raw-rerun"]
    incidents = [event for event in manual_events if event.get("kind") == "semantic-incident"]
    paired = [event for event in manual_events if event.get("kind") == "paired"]
    version_mismatches = [event for event in run_events if event.get("rtk_version") != RTK_VERSION]

    input_tokens = sum(_integer(row.get("input_tokens")) or 0 for row in commands)
    output_tokens = sum(_integer(row.get("output_tokens")) or 0 for row in commands)
    saved_tokens = sum(_integer(row.get("saved_tokens")) or 0 for row in commands)
    weighted_savings = 100 * saved_tokens / input_tokens if input_tokens else 0.0
    savings_values = [_number(row.get("savings_pct")) for row in commands]
    exec_values = [_integer(row.get("exec_time_ms")) or 0 for row in commands]
    p10_savings = _p10(savings_values)

    paired_overheads: list[float] = []
    paired_added_ms: list[int] = []
    exit_mismatches = 0
    malformed_pairs = 0
    for event in paired:
        raw_exit = _integer(event.get("raw_exit_code"))
        rtk_exit = _integer(event.get("rtk_exit_code"))
        raw_ms = _integer(event.get("raw_elapsed_ms"))
        rtk_ms = _integer(event.get("rtk_elapsed_ms"))
        if raw_exit is None or rtk_exit is None or raw_ms is None or rtk_ms is None or raw_ms <= 0 or rtk_ms < 0:
            malformed_pairs += 1
            continue
        exit_mismatches += raw_exit != rtk_exit
        paired_added_ms.append(rtk_ms - raw_ms)
        paired_overheads.append(100 * (rtk_ms - raw_ms) / raw_ms)

    nonzero_runs = sum((_integer(event.get("exit_code")) or 0) != 0 for event in run_events)
    sufficient_scenarios = {str(event.get("scenario", "")) for event in sufficient}
    failure_reviews = sum(event.get("scenario") == "failure" for event in sufficient)
    fallback_rate = 100 * len(fallbacks) / len(commands) if commands else 0.0
    active_days = len({row["_at"].date().isoformat() for row in commands})

    observed = {
        "runs": len(commands),
        "wrapper_runs": len(run_events),
        "active_days": active_days,
        "distinct_selectors": len({str(row["original_cmd"]) for row in commands}),
        "input_est_tokens": input_tokens,
        "output_est_tokens": output_tokens,
        "saved_est_tokens": saved_tokens,
        "weighted_savings_pct": round(weighted_savings, 1),
        "p10_savings_pct": None if p10_savings is None else round(p10_savings, 1),
        "avg_rtk_exec_ms": round(sum(exec_values) / len(exec_values)) if exec_values else None,
        "nonzero_wrapper_runs": nonzero_runs,
        "sufficient_reviews": len(sufficient),
        "reviewed_scenarios": sorted(sufficient_scenarios),
        "failure_reviews": failure_reviews,
        "fallbacks": len(fallbacks),
        "fallback_rate_pct": round(fallback_rate, 1),
        "semantic_incidents": len(incidents),
        "paired_checks": len(paired),
        "exit_code_mismatches": exit_mismatches,
        "malformed_paired_checks": malformed_pairs,
        "paired_timing_p50_added_ms": round(median(paired_added_ms)) if paired_added_ms else None,
        "paired_timing_p50_overhead_pct": round(median(paired_overheads), 1) if paired_overheads else None,
        "malformed_event_lines": malformed_events,
        "invalid_history_rows": invalid_history_rows,
    }

    criteria: list[dict[str, Any]] = []
    blockers: list[str] = []
    awaiting: list[str] = []

    def evidence(name: str, actual: object, threshold: object, met: bool, message: str) -> None:
        criteria.append(_criterion(name, actual, threshold, met, "evidence"))
        if not met:
            awaiting.append(message)

    def safety(name: str, actual: object, threshold: object, met: bool, message: str) -> None:
        criteria.append(_criterion(name, actual, threshold, met, "safety"))
        if not met:
            blockers.append(message)

    agreement_actual = {"wrapper_runs": len(run_events), "database_runs": len(commands)}
    agreement_message = "wrapper receipts and RTK database rows disagree after instrumentation starts"
    if run_events:
        safety(
            "wrapper_database_agreement",
            agreement_actual,
            "equal after instrumentation starts",
            len(run_events) == len(commands) and db_error is None,
            agreement_message,
        )
    else:
        evidence(
            "wrapper_database_agreement",
            agreement_actual,
            "first wrapper receipt and matching database row",
            False,
            "run the wrapper once so database evidence can be tied to local pilot receipts",
        )
    evidence(
        "sample_size",
        len(commands),
        policy.min_runs,
        len(commands) >= policy.min_runs,
        f"need {policy.min_runs - len(commands)} more RTK pytest runs",
    )
    evidence(
        "active_days",
        active_days,
        policy.min_active_days,
        active_days >= policy.min_active_days,
        f"need {max(0, policy.min_active_days - active_days)} more active pilot day(s)",
    )
    evidence(
        "selector_diversity",
        observed["distinct_selectors"],
        policy.min_distinct_selectors,
        observed["distinct_selectors"] >= policy.min_distinct_selectors,
        f"need {max(0, policy.min_distinct_selectors - observed['distinct_selectors'])} more distinct pytest selectors",
    )
    evidence(
        "material_output_sample",
        input_tokens,
        policy.min_input_tokens,
        input_tokens >= policy.min_input_tokens,
        f"need {max(0, policy.min_input_tokens - input_tokens):,} more estimated raw-output tokens",
    )
    evidence(
        "sufficient_reviews",
        len(sufficient),
        policy.min_sufficient_reviews,
        len(sufficient) >= policy.min_sufficient_reviews,
        f"need {max(0, policy.min_sufficient_reviews - len(sufficient))} more explicit sufficient-output review(s)",
    )
    required_scenarios = set(SCENARIOS)
    evidence(
        "review_scenarios",
        sorted(sufficient_scenarios),
        sorted(required_scenarios),
        required_scenarios <= sufficient_scenarios,
        "record sufficient reviews for pass, failure, and no-tests cases",
    )
    evidence(
        "failure_reviews",
        failure_reviews,
        policy.min_failure_reviews,
        failure_reviews >= policy.min_failure_reviews,
        f"need {max(0, policy.min_failure_reviews - failure_reviews)} more reviewed failure case(s)",
    )
    evidence(
        "paired_checks",
        len(paired),
        policy.min_paired_checks,
        len(paired) >= policy.min_paired_checks,
        f"need {max(0, policy.min_paired_checks - len(paired))} more raw/RTK paired check(s)",
    )

    enough_savings_evidence = len(commands) >= policy.min_runs and input_tokens >= policy.min_input_tokens
    savings_met = weighted_savings >= policy.min_weighted_savings_pct
    criteria.append(
        _criterion(
            "weighted_savings",
            round(weighted_savings, 1),
            policy.min_weighted_savings_pct,
            savings_met,
            "safety" if enough_savings_evidence else "evidence",
        )
    )
    if not savings_met:
        target = "blocks" if enough_savings_evidence else "is not yet meaningful"
        (blockers if enough_savings_evidence else awaiting).append(
            f"weighted output reduction {weighted_savings:.1f}% {target}; threshold is {policy.min_weighted_savings_pct:.1f}%"
        )

    p10_met = p10_savings is not None and p10_savings >= policy.min_p10_savings_pct
    criteria.append(
        _criterion(
            "p10_savings",
            None if p10_savings is None else round(p10_savings, 1),
            policy.min_p10_savings_pct,
            p10_met,
            "safety" if enough_savings_evidence else "evidence",
        )
    )
    if not p10_met:
        target = "blocks" if enough_savings_evidence else "needs a larger sample"
        (blockers if enough_savings_evidence else awaiting).append(
            f"low-end output reduction {target}; P10 must be at least {policy.min_p10_savings_pct:.1f}%"
        )

    fallback_met = not commands or fallback_rate <= policy.max_fallback_rate_pct
    criteria.append(
        _criterion("raw_rerun_rate", round(fallback_rate, 1), policy.max_fallback_rate_pct, fallback_met, "safety")
    )
    if commands and not fallback_met:
        blockers.append(f"raw-rerun rate {fallback_rate:.1f}% exceeds the {policy.max_fallback_rate_pct:.1f}% limit")
    safety("semantic_incidents", len(incidents), 0, not incidents, "semantic incident recorded; do not widen the pilot")
    safety(
        "exit_code_parity",
        exit_mismatches,
        0,
        exit_mismatches == 0 and malformed_pairs == 0,
        "a paired raw/RTK exit-code mismatch or malformed parity record exists",
    )
    safety(
        "rtk_version_consistency",
        len(version_mismatches),
        0,
        not version_mismatches,
        f"evidence includes an RTK version other than pinned v{RTK_VERSION}",
    )
    safety(
        "event_log_integrity",
        malformed_events,
        0,
        malformed_events == 0,
        "pilot event log has malformed rows; repair or wait for a clean window",
    )
    safety(
        "history_timestamp_integrity",
        invalid_history_rows,
        0,
        invalid_history_rows == 0,
        "RTK history has an invalid or future-dated pytest row; correct the local clock or wait for a clean window",
    )
    if db_error:
        safety("history_database_readable", db_error, "readable v0.44.2 schema", False, db_error)

    timing_ready = len(paired_added_ms) >= policy.min_paired_checks
    timing_met = timing_ready and (
        median(paired_added_ms) <= policy.max_p50_added_ms or median(paired_overheads) <= policy.max_p50_overhead_pct
    )
    criteria.append(
        _criterion(
            "paired_timing",
            {
                "p50_added_ms": observed["paired_timing_p50_added_ms"],
                "p50_overhead_pct": observed["paired_timing_p50_overhead_pct"],
            },
            {"max_p50_added_ms": policy.max_p50_added_ms, "max_p50_overhead_pct": policy.max_p50_overhead_pct},
            timing_met,
            "safety" if timing_ready else "evidence",
        )
    )
    if not timing_met:
        if timing_ready:
            blockers.append("paired RTK latency exceeds both permitted median limits")
        else:
            awaiting.append(
                f"need {max(0, policy.min_paired_checks - len(paired_added_ms))} valid paired timing sample(s)"
            )

    if blockers:
        state = "blocked"
        next_action = "Resolve the listed safety or data-integrity finding; do not widen RTK usage."
    elif awaiting:
        state = "collecting"
        next_action = "Continue the pytest-only pilot and record representative reviews; no adoption action occurs yet."
    else:
        state = "eligible_for_review"
        next_action = (
            "Human review required before widening the explicit pytest allowlist; no automatic action will occur."
        )

    review_kinds_by_run: dict[str, list[str]] = {}
    for event in manual_events:
        review_kinds_by_run.setdefault(str(event["run_id"]), []).append(str(event["kind"]))
    recent_receipts = [
        {
            "run_id": str(event["run_id"]),
            "scenario": _scenario_for_exit(_integer(event["exit_code"]) or 0),
            "exit_code": _integer(event["exit_code"]),
            "at_utc": _iso(event["_at"]),
            "reviews": sorted(review_kinds_by_run.get(str(event["run_id"]), [])),
        }
        for event in sorted(run_events, key=lambda item: item["_at"], reverse=True)[:10]
    ]

    return {
        "schema": STATUS_SCHEMA,
        "policy_version": POLICY_VERSION,
        "generated_at_utc": _iso(current),
        "status": state,
        "automatic_action": "none",
        "window": {
            "days": policy.window_days,
            "start_utc": _iso(window_start),
            "database_start_utc": _iso(db_cutoff),
        },
        "policy": asdict(policy),
        "observed": observed,
        "criteria": criteria,
        "recent_receipts": recent_receipts,
        "blocking_reasons": blockers,
        "awaiting_evidence": awaiting,
        "next_action": next_action,
    }


def write_status(report: dict[str, Any], path: Path = STATUS) -> None:
    """Atomically replace the local status snapshot for a cheap external trigger."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _record_event(args: argparse.Namespace, path: Path = EVENTS, now: datetime | None = None) -> dict[str, Any]:
    if len(args.note.strip()) < NOTE_MIN_CHARS:
        raise ValueError(f"--note is required and must be at least {NOTE_MIN_CHARS} characters")
    if args.scenario is None:
        raise ValueError("--scenario is required for every manual pilot record")
    if args.record == "paired":
        required = (args.raw_exit, args.rtk_exit, args.raw_ms, args.rtk_ms)
        if any(value is None for value in required):
            raise ValueError("paired records require --raw-exit, --rtk-exit, --raw-ms, and --rtk-ms")
        if args.raw_ms <= 0 or args.rtk_ms < 0:
            raise ValueError("paired elapsed times must be raw-ms > 0 and rtk-ms >= 0")

    run_id = getattr(args, "run_id", None)
    if not _nonempty_text(run_id):
        raise ValueError("--run-id is required; use `adoption --json` to find a recent wrapper receipt")
    recorded_at = (now or _now()).astimezone(UTC)
    run_events, manual_events, _ = _validated_events(
        path,
        recorded_at - timedelta(days=DEFAULT_POLICY.window_days),
        recorded_at,
    )
    runs_by_id = {str(event["run_id"]): event for event in run_events}
    run = runs_by_id.get(run_id)
    if run is None:
        raise ValueError("--run-id must reference an in-window pytest receipt written by tools/rtk.ps1")
    expected_scenario = _scenario_for_exit(_integer(run["exit_code"]) or 0)
    if args.scenario != expected_scenario:
        raise ValueError(f"--scenario must be '{expected_scenario}' for receipt {run_id}")
    if any(event["kind"] == args.record and event["run_id"] == run_id for event in manual_events):
        raise ValueError(f"a {args.record} record already exists for receipt {run_id}")
    if args.record == "paired" and args.rtk_exit != _integer(run["exit_code"]):
        raise ValueError("--rtk-exit must match the recorded RTK receipt exit code")

    event: dict[str, Any] = {
        "schema": EVENT_SCHEMA,
        "at_utc": _iso(recorded_at),
        "run_id": run_id,
        "kind": args.record,
        "scenario": args.scenario,
        "note": args.note.strip(),
    }
    if args.record == "paired":
        event.update(
            raw_exit_code=args.raw_exit,
            rtk_exit_code=args.rtk_exit,
            raw_elapsed_ms=args.raw_ms,
            rtk_elapsed_ms=args.rtk_ms,
        )

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")
    return event


def _print_text(report: dict[str, Any]) -> None:
    observed = report["observed"]
    print(f"RTK pytest pilot: {report['status'].upper()}")
    print(
        "runs={runs} active_days={active_days} selectors={distinct_selectors} "
        "input_est={input_est_tokens:,} savings={weighted_savings_pct:.1f}% "
        "p10={p10}".format(
            **observed,
            p10="n/a" if observed["p10_savings_pct"] is None else f"{observed['p10_savings_pct']:.1f}%",
        )
    )
    print(
        "reviews={sufficient_reviews} failure_reviews={failure_reviews} paired={paired_checks} "
        "fallbacks={fallbacks} ({fallback_rate_pct:.1f}%) incidents={semantic_incidents} "
        "exit_mismatches={exit_code_mismatches}".format(**observed)
    )
    for reason in report["blocking_reasons"] + report["awaiting_evidence"]:
        print(f"  - {reason}")
    for receipt in report["recent_receipts"][:3]:
        reviews = ",".join(receipt["reviews"]) or "unreviewed"
        print(
            f"  receipt={receipt['run_id']} scenario={receipt['scenario']} "
            f"exit={receipt['exit_code']} reviews={reviews}"
        )
    print(f"next: {report['next_action']}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Measure the RTK pytest pilot and emit an advisory adoption trigger.")
    parser.add_argument("--record", choices=RECORD_KINDS, help="append a human review record instead of reporting")
    parser.add_argument("--scenario", choices=SCENARIOS, help="case reviewed: pass, failure, or no-tests")
    parser.add_argument("--run-id", help="wrapper receipt being reviewed; find it with `adoption --json`")
    parser.add_argument(
        "--note", default="", help="what was compared or why raw output was needed; never include secrets"
    )
    parser.add_argument("--raw-exit", type=int, help="raw pytest exit code (paired record only)")
    parser.add_argument("--rtk-exit", type=int, help="RTK pytest exit code (paired record only)")
    parser.add_argument("--raw-ms", type=int, help="raw pytest elapsed milliseconds (paired record only)")
    parser.add_argument("--rtk-ms", type=int, help="RTK pytest elapsed milliseconds (paired record only)")
    parser.add_argument("--json", action="store_true", help="print a machine-readable result")
    parser.add_argument(
        "--require-eligible",
        action="store_true",
        help="exit 0 only when status is eligible_for_review; does not perform adoption",
    )
    args = parser.parse_args()

    try:
        if args.record:
            event = _record_event(args)
            report = build_report()
            write_status(report)
            if args.json:
                print(json.dumps({"recorded": event, "report": report}, ensure_ascii=False, indent=2))
            else:
                print(f"recorded {event['kind']} review for {event['scenario']} on receipt {event['run_id']}")
                _print_text(report)
        else:
            report = build_report()
            write_status(report)
    except (OSError, ValueError) as exc:
        print(f"rtk pilot gate: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        _print_text(report)
    return 0 if not args.require_eligible or report["status"] == "eligible_for_review" else 1


if __name__ == "__main__":
    sys.exit(main())
