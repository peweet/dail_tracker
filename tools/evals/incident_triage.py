"""Offline, append-only incident review capture for the Linnaeus pilot.

This module records bounded structured snapshots only.  It does not verify human
identity, evidence, privacy, or model readiness; ``human`` is an attested label
used for aggregate pilot accounting.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

MAX_JSON_BYTES = 16 * 1024
MAX_DIAGNOSIS_SECONDS = 365 * 24 * 60 * 60
DEFAULT_DATABASE = Path(__file__).resolve().parents[2] / "logs" / "linnaeus-pilot" / "reviews.sqlite3"
TABLE = "incident_revisions"

FIELDS = {
    "schema_version",
    "incident_id",
    "incident_at",
    "workflow",
    "operation_state",
    "record_kind",
    "label_guide_version",
    "family",
    "cause_group_id",
    "review_state",
    "reviewer_kind",
    "reviewer_ref",
    "reviewed_at",
    "evidence_ref",
    "resolution_ref",
    "evidence_sufficient",
    "privacy_reviewed",
    "diagnosis_seconds",
    "effort_method",
}
WORKFLOWS = ("extraction", "ci", "support")
OPERATION_STATES = ("failed", "blocked", "unknown")
RECORD_KINDS = ("operational", "synthetic")
FAMILIES = ("source_access", "environment", "parsing_schema", "application", "resource_admission", "unknown")
REVIEW_STATES = ("unreviewed", "confirmed", "uncertain")
REVIEWER_KINDS = ("unreviewed", "human", "agent")
EFFORT_METHODS = ("unmeasured", "timed", "self_reported")
TOKEN = re.compile(r"[a-z0-9][a-z0-9_.-]{0,79}\Z")


class IncidentError(ValueError):
    """A safe, user-facing rejection without record contents."""


def _reject_constant(value: str) -> Any:
    raise IncidentError("nonfinite number")


def _pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IncidentError("duplicate field")
        result[key] = value
    return result


def _load_json(path: Path) -> dict[str, Any]:
    try:
        with path.open("rb") as handle:
            raw = handle.read(MAX_JSON_BYTES + 1)
    except OSError as exc:
        raise IncidentError("input unavailable") from exc
    if len(raw) > MAX_JSON_BYTES:
        raise IncidentError("input too large")
    try:
        value = json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_reject_constant)
    except IncidentError:
        raise
    except (UnicodeDecodeError, ValueError, RecursionError) as exc:
        raise IncidentError("invalid JSON") from exc
    if not isinstance(value, dict):
        raise IncidentError("record must be an object")
    return value


def _token(value: Any, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or TOKEN.fullmatch(value) is None:
        raise IncidentError("invalid token")
    return value


def _enum(value: Any, values: tuple[str, ...]) -> str:
    if not isinstance(value, str) or value not in values:
        raise IncidentError("invalid enum")
    return value


def _aware_time(value: Any, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or len(value) > 64:
        raise IncidentError("invalid time")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise IncidentError("invalid time") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IncidentError("time must include timezone")
    return value


def _bool_or_null(value: Any) -> bool | None:
    if value is not None and type(value) is not bool:
        raise IncidentError("invalid boolean")
    return value


def _number_or_null(value: Any) -> float | int | None:
    if value is None:
        return None
    if type(value) not in (int, float) or value < 0:
        raise IncidentError("invalid diagnosis seconds")
    try:
        finite = math.isfinite(value)
    except OverflowError as exc:
        raise IncidentError("invalid diagnosis seconds") from exc
    if not finite or value > MAX_DIAGNOSIS_SECONDS:
        raise IncidentError("invalid diagnosis seconds")
    return value


def validate_record(record: dict[str, Any], *, require_incident_at: bool = True) -> dict[str, Any]:
    if set(record) != FIELDS:
        raise IncidentError("unsupported or missing field")
    if type(record["schema_version"]) is not int or record["schema_version"] != 1:
        raise IncidentError("unsupported schema")
    _token(record["incident_id"])
    incident_at = _aware_time(record["incident_at"], nullable=True)
    if require_incident_at and incident_at is None:
        raise IncidentError("incident time required")
    _enum(record["workflow"], WORKFLOWS)
    _enum(record["operation_state"], OPERATION_STATES)
    _enum(record["record_kind"], RECORD_KINDS)
    if record["label_guide_version"] != "v1":
        raise IncidentError("unsupported label guide")
    _enum(record["family"], FAMILIES)
    _token(record["cause_group_id"], nullable=True)
    review_state = _enum(record["review_state"], REVIEW_STATES)
    reviewer_kind = _enum(record["reviewer_kind"], REVIEWER_KINDS)
    reviewer_ref = _token(record["reviewer_ref"], nullable=True)
    reviewed_at = _aware_time(record["reviewed_at"], nullable=True)
    evidence_ref = _token(record["evidence_ref"], nullable=True)
    resolution_ref = _token(record["resolution_ref"], nullable=True)
    evidence_sufficient = _bool_or_null(record["evidence_sufficient"])
    privacy_reviewed = _bool_or_null(record["privacy_reviewed"])
    diagnosis_seconds = _number_or_null(record["diagnosis_seconds"])
    effort_method = _enum(record["effort_method"], EFFORT_METHODS)

    if effort_method == "unmeasured" and diagnosis_seconds is not None:
        raise IncidentError("unmeasured effort must be null")
    if effort_method != "unmeasured" and diagnosis_seconds is None:
        raise IncidentError("measured effort requires seconds")
    if review_state == "unreviewed":
        if reviewer_kind != "unreviewed":
            raise IncidentError("unreviewed record has reviewer")
    elif reviewer_kind == "unreviewed":
        raise IncidentError("reviewed record lacks reviewer")
    if review_state == "confirmed" and (
        record["family"] == "unknown"
        or record["cause_group_id"] is None
        or reviewer_ref is None
        or reviewed_at is None
        or evidence_ref is None
        or resolution_ref is None
        or evidence_sufficient is not True
        or privacy_reviewed is not True
    ):
        raise IncidentError("confirmed record lacks supporting provenance")
    return record


def _template() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "incident_id": "replace-me",
        "incident_at": None,
        "workflow": "extraction",
        "operation_state": "unknown",
        "record_kind": "synthetic",
        "label_guide_version": "v1",
        "family": "unknown",
        "cause_group_id": None,
        "review_state": "unreviewed",
        "reviewer_kind": "unreviewed",
        "reviewer_ref": None,
        "reviewed_at": None,
        "evidence_ref": None,
        "resolution_ref": None,
        "evidence_sufficient": None,
        "privacy_reviewed": None,
        "diagnosis_seconds": None,
        "effort_method": "unmeasured",
    }


def _connect(path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    try:
        if readonly:
            return sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True, timeout=5)
        path.parent.mkdir(parents=True, exist_ok=True)
        return sqlite3.connect(path, timeout=5)
    except (OSError, sqlite3.Error) as exc:
        raise IncidentError("database unavailable") from exc


def _check_schema(connection: sqlite3.Connection, *, create: bool) -> None:
    try:
        tables = {
            row[0]
            for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            if not row[0].startswith("sqlite_")
        }
        if TABLE not in tables:
            if tables or not create:
                raise IncidentError("unsupported database schema")
            connection.execute(
                f"CREATE TABLE {TABLE} (incident_id TEXT NOT NULL, revision INTEGER NOT NULL, "
                "recorded_at TEXT NOT NULL, payload TEXT NOT NULL, "
                "PRIMARY KEY (incident_id, revision))"
            )
            return
        if tables != {TABLE}:
            raise IncidentError("unsupported database schema")
        columns = connection.execute(f"PRAGMA table_info({TABLE})").fetchall()
        expected = (
            ("incident_id", "TEXT", 1, 1),
            ("revision", "INTEGER", 1, 2),
            ("recorded_at", "TEXT", 1, 0),
            ("payload", "TEXT", 1, 0),
        )
        if len(columns) != len(expected) or any(
            (row[1], str(row[2]).upper(), row[3], row[5]) != wanted
            for row, wanted in zip(columns, expected, strict=True)
        ):
            raise IncidentError("unsupported database schema")
    except sqlite3.Error as exc:
        raise IncidentError("corrupt database") from exc


def record_incident(path: Path, record: dict[str, Any], expected_revision: int) -> int:
    if type(expected_revision) is not int or expected_revision < 0:
        raise IncidentError("invalid revision")
    validate_record(record)
    connection = _connect(path)
    try:
        with connection:
            connection.execute("BEGIN IMMEDIATE")
            _check_schema(connection, create=True)
            rows = connection.execute(
                f"SELECT incident_id,revision,recorded_at,payload FROM {TABLE} ORDER BY incident_id,revision"
            ).fetchall()
            _, histories, _ = _validated_rows(rows)
            current = histories.get(record["incident_id"], [0])[-1]
            if current != expected_revision:
                raise IncidentError("stale revision")
            next_revision = current + 1
            recorded_at = datetime.now().astimezone().isoformat()
            connection.execute(
                f"INSERT INTO {TABLE} (incident_id,revision,recorded_at,payload) VALUES (?,?,?,?)",
                (record["incident_id"], next_revision, recorded_at, json.dumps(record, separators=(",", ":"))),
            )
            return next_revision
    except IncidentError:
        raise
    except sqlite3.Error as exc:
        raise IncidentError("database write rejected") from exc
    finally:
        connection.close()


def _validated_rows(
    rows: list[tuple[Any, ...]],
) -> tuple[dict[str, dict[str, Any]], dict[str, list[int]], int]:
    histories: dict[str, list[int]] = {}
    latest: dict[str, dict[str, Any]] = {}
    for incident_id, revision, recorded_at, payload in rows:
        if not isinstance(incident_id, str) or TOKEN.fullmatch(incident_id) is None:
            raise IncidentError("corrupt database")
        if type(revision) is not int or revision < 1:
            raise IncidentError("corrupt database")
        _aware_time(recorded_at)
        if not isinstance(payload, str) or len(payload.encode("utf-8")) > MAX_JSON_BYTES:
            raise IncidentError("corrupt database")
        try:
            record = json.loads(payload, object_pairs_hook=_pairs, parse_constant=_reject_constant)
        except IncidentError as exc:
            raise IncidentError("corrupt database") from exc
        except (json.JSONDecodeError, TypeError, ValueError, RecursionError) as exc:
            raise IncidentError("corrupt database") from exc
        if not isinstance(record, dict) or record.get("incident_id") != incident_id:
            raise IncidentError("corrupt database")
        validate_record(record)
        histories.setdefault(incident_id, []).append(revision)
        latest[incident_id] = record
    for revisions in histories.values():
        if revisions != list(range(1, len(revisions) + 1)):
            raise IncidentError("corrupt database")
    return latest, histories, len(rows)


def _read_snapshots(path: Path) -> tuple[list[dict[str, Any]], int]:
    if not path.exists():
        return [], 0
    connection = _connect(path, readonly=True)
    try:
        _check_schema(connection, create=False)
        rows = connection.execute(
            f"SELECT incident_id,revision,recorded_at,payload FROM {TABLE} ORDER BY incident_id,revision"
        ).fetchall()
        latest, _, count = _validated_rows(rows)
        return list(latest.values()), count
    except sqlite3.Error as exc:
        raise IncidentError("corrupt database") from exc
    finally:
        connection.close()


def _eligible(record: dict[str, Any]) -> bool:
    return (
        record["record_kind"] == "operational"
        and record["operation_state"] in {"failed", "blocked"}
        and record["review_state"] == "confirmed"
        and record["reviewer_kind"] == "human"
        and record["family"] != "unknown"
        and record["cause_group_id"] is not None
        and record["evidence_ref"] is not None
        and record["resolution_ref"] is not None
        and record["evidence_sufficient"] is True
        and record["privacy_reviewed"] is True
    )


def summarize(path: Path) -> dict[str, Any]:
    latest, history_revisions = _read_snapshots(path)
    by_family = {value: 0 for value in FAMILIES}
    by_workflow = {value: 0 for value in WORKFLOWS}
    by_operation_state = {value: 0 for value in OPERATION_STATES}
    eligible = [record for record in latest if _eligible(record)]
    for record in latest:
        by_family[record["family"]] += 1
        by_workflow[record["workflow"]] += 1
        by_operation_state[record["operation_state"]] += 1
    effort = {"timed": 0, "self_reported": 0, "unknown": 0, "timed_seconds": 0, "self_reported_seconds": 0}
    cause_groups: set[str] = set()
    for record in eligible:
        method = record["effort_method"]
        effort["unknown" if method == "unmeasured" else method] += 1
        if method in {"timed", "self_reported"}:
            seconds_key = f"{method}_seconds"
            total = effort[seconds_key] + record["diagnosis_seconds"]
            if not math.isfinite(total) or total > MAX_DIAGNOSIS_SECONDS * max(1, len(eligible)):
                raise IncidentError("effort total out of range")
            effort[seconds_key] = total
        cause_groups.add(record["cause_group_id"])
    return {
        "schema_version": 1,
        "latest_incidents": len(latest),
        "history_revisions": history_revisions,
        "counts_by_family": by_family,
        "counts_by_workflow": by_workflow,
        "counts_by_operation_state": by_operation_state,
        "label_eligible_incidents": len(eligible),
        "distinct_eligible_cause_groups": len(cause_groups),
        "eligible_effort": effort,
        "excluded_or_unreviewed": len(latest) - len(eligible),
    }


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise IncidentError("invalid arguments")


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(prog="incident_triage")
    commands = parser.add_subparsers(dest="command", required=True, parser_class=_ArgumentParser)
    template = commands.add_parser("template")
    template.add_argument("--output", required=True, type=Path)
    record = commands.add_parser("record")
    record.add_argument("--input", required=True, type=Path)
    record.add_argument("--expected-revision", required=True, type=int)
    record.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    summary = commands.add_parser("summary")
    summary.add_argument("--database", type=Path, default=DEFAULT_DATABASE)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        if args.command == "template":
            payload = json.dumps(_template(), indent=2) + "\n"
            with args.output.open("x", encoding="utf-8", newline="\n") as handle:
                handle.write(payload)
            print(json.dumps({"status": "created"}))
            return 0
        if args.command == "record":
            revision = record_incident(args.database, _load_json(args.input), args.expected_revision)
            print(json.dumps({"status": "recorded", "revision": revision}))
            return 0
        print(json.dumps(summarize(args.database), separators=(",", ":")))
        return 0
    except SystemExit as exc:
        if exc.code == 0:
            return 0
        print("error: request rejected", file=sys.stderr)
        return 2
    except (IncidentError, OSError, argparse.ArgumentError):
        print("error: request rejected", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
