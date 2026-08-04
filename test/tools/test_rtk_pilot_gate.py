"""Contracts for the advisory RTK pytest-pilot adoption trigger."""

from __future__ import annotations

import importlib.util
import json
import sqlite3
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

REPO = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location("rtk_pilot_gate", REPO / "tools" / "rtk_pilot_gate.py")
    mod = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def _create_db(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.execute(
        """
        CREATE TABLE commands (
            id INTEGER PRIMARY KEY,
            timestamp TEXT NOT NULL,
            original_cmd TEXT NOT NULL,
            rtk_cmd TEXT NOT NULL,
            input_tokens INTEGER NOT NULL,
            output_tokens INTEGER NOT NULL,
            saved_tokens INTEGER NOT NULL,
            savings_pct REAL NOT NULL,
            exec_time_ms INTEGER DEFAULT 0,
            project_path TEXT DEFAULT ''
        )
        """
    )
    return connection


def _append(path: Path, row: dict) -> None:
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row) + "\n")


def _eligible_evidence(tmp_path: Path):
    gate = _load()
    now = datetime(2026, 8, 4, 12, tzinfo=UTC)
    repo = tmp_path / "repo"
    repo.mkdir()
    db = tmp_path / "history.db"
    events = tmp_path / "pilot-events.jsonl"
    connection = _create_db(db)

    for index in range(30):
        at = now - timedelta(days=index % 5, minutes=index)
        command = f"pytest test/test_{index % 10}.py -q"
        connection.execute(
            """
            INSERT INTO commands (
                timestamp, original_cmd, rtk_cmd, input_tokens, output_tokens,
                saved_tokens, savings_pct, exec_time_ms, project_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                gate._iso(at),
                command,
                f"rtk {command}",
                400,
                200,
                200,
                50.0,
                2_000,
                str(repo),
            ),
        )
        _append(
            events,
            {
                "schema": gate.EVENT_SCHEMA,
                "at_utc": gate._iso(at),
                "started_at_utc": gate._iso(at - timedelta(seconds=1)),
                "run_id": f"run-{index}",
                "kind": "rtk_run",
                "command": "pytest",
                "exit_code": 1 if index < 3 else (5 if index == 3 else 0),
                "elapsed_ms": 2_000,
                "rtk_version": gate.RTK_VERSION,
            },
        )
    connection.commit()
    connection.close()

    reviews = (
        ("pass", "run-4"),
        ("failure", "run-0"),
        ("no-tests", "run-3"),
        ("failure", "run-1"),
        ("failure", "run-2"),
    )
    for scenario, run_id in reviews:
        _append(
            events,
            {
                "schema": gate.EVENT_SCHEMA,
                "at_utc": gate._iso(now),
                "run_id": run_id,
                "kind": "sufficient",
                "scenario": scenario,
                "note": "Reviewed compact output against raw pytest and found the needed facts intact.",
            },
        )
    for scenario, run_id in reviews:
        _append(
            events,
            {
                "schema": gate.EVENT_SCHEMA,
                "at_utc": gate._iso(now),
                "run_id": run_id,
                "kind": "paired",
                "scenario": scenario,
                "note": "Paired raw and RTK runs selected the same test and preserved the exit code.",
                "raw_exit_code": 1 if scenario == "failure" else (5 if scenario == "no-tests" else 0),
                "rtk_exit_code": 1 if scenario == "failure" else (5 if scenario == "no-tests" else 0),
                "raw_elapsed_ms": 2_000,
                "rtk_elapsed_ms": 2_050,
            },
        )
    return gate, now, repo, db, events


def test_empty_history_collects_evidence_without_claiming_adoption(tmp_path):
    gate = _load()
    report = gate.build_report(
        db_path=tmp_path / "missing.db",
        events_path=tmp_path / "missing.jsonl",
        repo=tmp_path,
        now=datetime(2026, 8, 4, tzinfo=UTC),
    )

    assert report["status"] == "collecting"
    assert report["automatic_action"] == "none"
    assert report["observed"]["runs"] == 0


def test_full_boundary_evidence_is_eligible_for_human_review(tmp_path):
    gate, now, repo, db, events = _eligible_evidence(tmp_path)

    report = gate.build_report(db_path=db, events_path=events, repo=repo, now=now)

    assert report["status"] == "eligible_for_review"
    assert report["observed"]["runs"] == 30
    assert report["observed"]["weighted_savings_pct"] == 50.0
    assert report["observed"]["p10_savings_pct"] == 50.0
    assert report["blocking_reasons"] == []


def test_failure_reviews_must_link_to_distinct_real_failure_receipts(tmp_path):
    gate, now, repo, db, events = _eligible_evidence(tmp_path)
    rows = [json.loads(line) for line in events.read_text(encoding="utf-8").splitlines()]
    for row in rows:
        if row.get("kind") == "rtk_run" and row.get("run_id") in {"run-0", "run-1", "run-2"}:
            row["exit_code"] = 0
    events.write_text("\n".join(json.dumps(row) for row in rows) + "\n", encoding="utf-8")

    report = gate.build_report(db_path=db, events_path=events, repo=repo, now=now)

    assert report["status"] == "blocked"
    assert report["observed"]["failure_reviews"] == 0
    assert report["observed"]["malformed_event_lines"] >= 3


def test_future_dated_history_or_receipts_are_rejected(tmp_path):
    gate, now, repo, db, events = _eligible_evidence(tmp_path)
    connection = sqlite3.connect(db)
    connection.execute(
        """
        INSERT INTO commands (
            timestamp, original_cmd, rtk_cmd, input_tokens, output_tokens,
            saved_tokens, savings_pct, exec_time_ms, project_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            gate._iso(now + timedelta(minutes=1)),
            "pytest test/future.py -q",
            "rtk pytest test/future.py -q",
            999_999,
            1,
            999_998,
            100.0,
            1,
            str(repo),
        ),
    )
    connection.commit()
    connection.close()
    _append(
        events,
        {
            "schema": gate.EVENT_SCHEMA,
            "at_utc": gate._iso(now + timedelta(minutes=1)),
            "started_at_utc": gate._iso(now + timedelta(minutes=1)),
            "run_id": "future-run",
            "kind": "rtk_run",
            "command": "pytest",
            "exit_code": 0,
            "elapsed_ms": 1,
            "rtk_version": gate.RTK_VERSION,
        },
    )

    report = gate.build_report(db_path=db, events_path=events, repo=repo, now=now)

    assert report["status"] == "blocked"
    assert report["observed"]["runs"] == 30
    assert report["observed"]["invalid_history_rows"] == 1
    assert report["observed"]["malformed_event_lines"] == 1


def test_semantic_incident_blocks_even_when_the_numeric_thresholds_pass(tmp_path):
    gate, now, repo, db, events = _eligible_evidence(tmp_path)
    _append(
        events,
        {
            "schema": gate.EVENT_SCHEMA,
            "at_utc": gate._iso(now),
            "run_id": "run-0",
            "kind": "semantic-incident",
            "scenario": "failure",
            "note": "Compact result omitted a decision-relevant assertion from the failure output.",
        },
    )

    report = gate.build_report(db_path=db, events_path=events, repo=repo, now=now)

    assert report["status"] == "blocked"
    assert report["observed"]["semantic_incidents"] == 1
    assert any("semantic incident" in reason for reason in report["blocking_reasons"])


def test_unmatched_wrapper_and_database_runs_block_the_trigger(tmp_path):
    gate, now, repo, db, events = _eligible_evidence(tmp_path)
    rows = events.read_text(encoding="utf-8").splitlines()
    for index, row in enumerate(rows):
        if json.loads(row).get("kind") == "rtk_run":
            rows.pop(index)
            break
    events.write_text("\n".join(rows) + "\n", encoding="utf-8")

    report = gate.build_report(db_path=db, events_path=events, repo=repo, now=now)

    assert report["status"] == "blocked"
    assert any("wrapper receipts" in reason for reason in report["blocking_reasons"])


def test_raw_rerun_rate_above_limit_blocks_the_trigger(tmp_path):
    gate, now, repo, db, events = _eligible_evidence(tmp_path)
    for index in range(2):
        _append(
            events,
            {
                "schema": gate.EVENT_SCHEMA,
                "at_utc": gate._iso(now - timedelta(seconds=index)),
                "run_id": f"run-{index}",
                "kind": "raw-rerun",
                "scenario": "failure",
                "note": "Needed raw pytest output because the compact result lacked enough failure context.",
            },
        )

    report = gate.build_report(db_path=db, events_path=events, repo=repo, now=now)

    assert report["status"] == "blocked"
    assert report["observed"]["fallback_rate_pct"] == pytest.approx(6.7)


def test_paired_record_requires_complete_timing_and_exit_data(tmp_path):
    gate = _load()
    args = SimpleNamespace(
        record="paired",
        scenario="pass",
        note="Compared real raw and RTK pytest executions with the same selector.",
        raw_exit=0,
        rtk_exit=0,
        raw_ms=100,
        rtk_ms=None,
    )

    with pytest.raises(ValueError, match="paired records require"):
        gate._record_event(args, path=tmp_path / "events.jsonl")


def test_manual_records_must_link_to_a_wrapper_receipt(tmp_path):
    gate = _load()
    args = SimpleNamespace(
        record="sufficient",
        scenario="pass",
        run_id="missing-receipt",
        note="Compared the compact output to its saved full output and found it complete.",
    )

    with pytest.raises(ValueError, match="must reference an in-window pytest receipt"):
        gate._record_event(args, path=tmp_path / "events.jsonl")


def test_record_command_refreshes_the_status_snapshot(monkeypatch, capsys):
    gate = _load()
    event = {"kind": "sufficient", "scenario": "pass", "run_id": "run-1"}
    report = {"status": "collecting"}
    written: list[dict] = []
    monkeypatch.setattr(gate, "_record_event", lambda args: event)
    monkeypatch.setattr(gate, "build_report", lambda: report)
    monkeypatch.setattr(gate, "write_status", lambda value: written.append(value))
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "rtk_pilot_gate.py",
            "--record",
            "sufficient",
            "--scenario",
            "pass",
            "--run-id",
            "run-1",
            "--note",
            "Compared compact output against raw pytest output for the same selected test.",
            "--json",
            "--require-eligible",
        ],
    )

    assert gate.main() == 1
    assert written == [report]
    assert '"status": "collecting"' in capsys.readouterr().out
