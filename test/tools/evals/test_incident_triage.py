from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest


def _record(**changes: object) -> dict[str, object]:
    record: dict[str, object] = {
        "schema_version": 1,
        "incident_id": "incident-1",
        "incident_at": "2026-11-11T10:00:00+00:00",
        "workflow": "extraction",
        "operation_state": "failed",
        "record_kind": "operational",
        "label_guide_version": "v1",
        "family": "parsing_schema",
        "cause_group_id": "cause-a",
        "review_state": "confirmed",
        "reviewer_kind": "human",
        "reviewer_ref": "reviewer-a",
        "reviewed_at": "2026-11-11T11:00:00+00:00",
        "evidence_ref": "evidence-a",
        "resolution_ref": "resolution-a",
        "evidence_sufficient": True,
        "privacy_reviewed": True,
        "diagnosis_seconds": 12.5,
        "effort_method": "timed",
    }
    record.update(changes)
    return record


def _write(path: Path, record: dict[str, object]) -> None:
    path.write_text(json.dumps(record), encoding="utf-8")


def test_template_writes_safe_schema_with_exclusive_create(tmp_path: Path) -> None:
    from tools.evals import incident_triage

    output = tmp_path / "template.json"
    assert incident_triage.main(["template", "--output", str(output)]) == 0
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["schema_version"] == 1
    assert payload["incident_at"] is None
    assert payload["operation_state"] == "unknown"
    assert payload["family"] == "unknown"
    assert payload["review_state"] == "unreviewed"
    assert payload["reviewer_kind"] == "unreviewed"
    assert payload["diagnosis_seconds"] is None
    assert payload["effort_method"] == "unmeasured"
    assert incident_triage.main(["template", "--output", str(output)]) == 2


def test_record_rejects_invalid_effort_duplicate_fields_and_raw_fields(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from tools.evals import incident_triage

    input_path = tmp_path / "record.json"
    for value in ("NaN", True, -1):
        record = _record(diagnosis_seconds=value)
        _write(input_path, record)
        assert (
            incident_triage.main(
                ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(tmp_path / "db")]
            )
            == 2
        )
    input_path.write_text('{"schema_version":1,"schema_version":1}', encoding="utf-8")
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(tmp_path / "db")]
        )
        == 2
    )
    raw = _record(raw="free text")
    _write(input_path, raw)
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(tmp_path / "db")]
        )
        == 2
    )
    assert "incident-1" not in capsys.readouterr().err


def test_load_json_reads_only_bounded_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.evals import incident_triage

    reads: list[int] = []

    class BoundedFile:
        def __enter__(self) -> BoundedFile:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def read(self, size: int = -1) -> bytes:
            reads.append(size)
            return b"{}"

    class BoundedPath:
        def open(self, mode: str) -> BoundedFile:
            assert mode == "rb"
            return BoundedFile()

    assert incident_triage._load_json(BoundedPath()) == {}
    assert reads == [incident_triage.MAX_JSON_BYTES + 1]


def test_load_json_rejects_parser_value_errors_without_traceback(monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.evals import incident_triage

    def raise_value_error(*args: object, **kwargs: object) -> object:
        raise ValueError("parser limit")

    monkeypatch.setattr(incident_triage.json, "loads", raise_value_error)
    with pytest.raises(incident_triage.IncidentError):
        incident_triage._load_json(Path(__file__))


def test_help_is_success_and_invalid_arguments_are_safe(capsys: pytest.CaptureFixture[str]) -> None:
    from tools.evals import incident_triage

    assert incident_triage.main(["--help"]) == 0
    help_output = capsys.readouterr()
    assert "usage:" in help_output.out
    assert help_output.err == ""
    assert incident_triage.main(["record", "--input", "secret-input", "--expected-revision", "bad"]) == 2
    error_output = capsys.readouterr()
    assert "secret-input" not in error_output.out + error_output.err
    assert "bad" not in error_output.out + error_output.err


def test_record_requires_confirmed_supporting_provenance(tmp_path: Path) -> None:
    from tools.evals import incident_triage

    input_path = tmp_path / "record.json"
    _write(input_path, _record(evidence_ref=None))
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(tmp_path / "db")]
        )
        == 2
    )


def test_correction_is_append_only_and_stale_writer_is_rejected(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    from tools.evals import incident_triage

    database = tmp_path / "reviews.sqlite3"
    input_path = tmp_path / "record.json"
    _write(input_path, _record())
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 0
    )
    _write(input_path, _record(review_state="uncertain", reviewer_kind="human"))
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "1", "--database", str(database)]
        )
        == 0
    )
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "1", "--database", str(database)]
        )
        == 2
    )
    assert "incident-1" not in capsys.readouterr().err
    with sqlite3.connect(database) as connection:
        rows = connection.execute("SELECT revision,payload FROM incident_revisions ORDER BY revision").fetchall()
        assert len(rows) == 2
        first, second = (json.loads(row[1]) for row in rows)
        assert [first["review_state"], second["review_state"]] == ["confirmed", "uncertain"]
        assert first["evidence_ref"] == "evidence-a"
        assert first["resolution_ref"] == "resolution-a"


def test_summary_uses_latest_only_and_distinct_cause_groups(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from tools.evals import incident_triage

    database = tmp_path / "reviews.sqlite3"
    input_path = tmp_path / "record.json"
    _write(
        input_path,
        _record(
            incident_id="incident-secret-1",
            cause_group_id="cause-secret-a",
            reviewer_ref="reviewer-secret",
            evidence_ref="evidence-secret",
            resolution_ref="resolution-secret",
            diagnosis_seconds=None,
            effort_method="unmeasured",
        ),
    )
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 0
    )
    _write(
        input_path,
        _record(
            incident_id="incident-secret-2", cause_group_id="cause-secret-a", diagnosis_seconds=0, effort_method="timed"
        ),
    )
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 0
    )
    _write(input_path, _record(incident_id="incident-secret-3", cause_group_id="cause-secret-b", reviewer_kind="agent"))
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 0
    )
    _write(input_path, _record(incident_id="incident-secret-4", record_kind="synthetic"))
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 0
    )
    assert incident_triage.main(["summary", "--database", str(database)]) == 0
    before_capture = capsys.readouterr()
    before_correction = json.loads(before_capture.out.splitlines()[-1])
    assert before_correction["label_eligible_incidents"] == 2
    assert before_correction["eligible_effort"] == {
        "timed": 1,
        "self_reported": 0,
        "unknown": 1,
        "timed_seconds": 0,
        "self_reported_seconds": 0,
    }
    _write(
        input_path,
        _record(
            incident_id="incident-secret-2",
            cause_group_id="cause-secret-a",
            review_state="uncertain",
            reviewer_kind="human",
        ),
    )
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "1", "--database", str(database)]
        )
        == 0
    )
    assert incident_triage.main(["summary", "--database", str(database)]) == 0
    captured = capsys.readouterr()
    summary = json.loads(captured.out.splitlines()[-1])
    assert summary["latest_incidents"] == 4
    assert summary["history_revisions"] == 5
    assert summary["label_eligible_incidents"] == 1
    assert summary["distinct_eligible_cause_groups"] == 1
    assert summary["eligible_effort"] == {
        "timed": 0,
        "self_reported": 0,
        "unknown": 1,
        "timed_seconds": 0,
        "self_reported_seconds": 0,
    }
    assert summary["excluded_or_unreviewed"] == 3
    for secret in ("incident-secret", "cause-secret", "reviewer-secret", "evidence-secret", "resolution-secret"):
        assert secret not in before_capture.out + before_capture.err + captured.out + captured.err


def test_unknown_operation_is_excluded_from_eligibility(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from tools.evals import incident_triage

    database = tmp_path / "reviews.sqlite3"
    input_path = tmp_path / "record.json"
    _write(input_path, _record(incident_id="unknown-operation", operation_state="unknown"))
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 0
    )
    assert incident_triage.main(["summary", "--database", str(database)]) == 0
    summary = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert summary["label_eligible_incidents"] == 0
    assert summary["eligible_effort"]["timed"] == 0


def test_empty_summary_does_not_create_database(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    from tools.evals import incident_triage

    database = tmp_path / "missing" / "reviews.sqlite3"
    assert incident_triage.main(["summary", "--database", str(database)]) == 0
    assert not database.exists()
    summary = json.loads(capsys.readouterr().out)
    assert summary["latest_incidents"] == 0


@pytest.mark.parametrize("literal", ["NaN", "Infinity", "-Infinity"])
def test_record_rejects_actual_nonfinite_json_numbers(tmp_path: Path, literal: str) -> None:
    from tools.evals import incident_triage

    input_path = tmp_path / "record.json"
    content = json.dumps(_record()).replace("12.5", literal)
    input_path.write_text(content, encoding="utf-8")
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(tmp_path / "db")]
        )
        == 2
    )


def test_schema_version_refusal_and_corrupt_database_are_safe(tmp_path: Path) -> None:
    from tools.evals import incident_triage

    input_path = tmp_path / "record.json"
    _write(input_path, _record(schema_version=2))
    database = tmp_path / "db"
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 2
    )
    valid_input = tmp_path / "valid-record.json"
    _write(valid_input, _record())
    corrupt = tmp_path / "corrupt.sqlite3"
    corrupt.write_bytes(b"not sqlite")
    assert incident_triage.main(["summary", "--database", str(corrupt)]) == 2
    assert (
        incident_triage.main(
            ["record", "--input", str(valid_input), "--expected-revision", "0", "--database", str(corrupt)]
        )
        == 2
    )
    wrong = tmp_path / "wrong.sqlite3"
    with sqlite3.connect(wrong) as connection:
        connection.execute("CREATE TABLE wrong (value TEXT)")
    assert incident_triage.main(["summary", "--database", str(wrong)]) == 2
    assert (
        incident_triage.main(
            ["record", "--input", str(valid_input), "--expected-revision", "0", "--database", str(wrong)]
        )
        == 2
    )


@pytest.mark.parametrize(
    "ddl",
    [
        "CREATE TABLE incident_revisions (incident_id INTEGER NOT NULL, revision TEXT NOT NULL, recorded_at BLOB NOT NULL, payload BLOB NOT NULL, PRIMARY KEY (incident_id, revision))",
        "CREATE TABLE incident_revisions (incident_id TEXT NOT NULL, revision INTEGER NOT NULL, recorded_at TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (revision, incident_id))",
    ],
)
def test_wrong_sqlite_types_or_primary_key_are_rejected_without_insert(tmp_path: Path, ddl: str) -> None:
    from tools.evals import incident_triage

    database = tmp_path / "wrong.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(ddl)
    input_path = tmp_path / "record.json"
    _write(input_path, _record())
    assert incident_triage.main(["summary", "--database", str(database)]) == 2
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 2
    )
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM incident_revisions").fetchone() == (0,)


def test_malformed_current_revision_and_corrupt_history_are_rejected_without_insert(tmp_path: Path) -> None:
    from tools.evals import incident_triage

    database = tmp_path / "corrupt.sqlite3"
    with sqlite3.connect(database) as connection:
        connection.execute(
            "CREATE TABLE incident_revisions (incident_id TEXT NOT NULL, revision INTEGER NOT NULL, recorded_at TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (incident_id, revision))"
        )
        payload = json.dumps(_record())
        connection.execute(
            "INSERT INTO incident_revisions VALUES (?,?,?,?)",
            ("incident-1", 1.5, "2026-11-11T12:00:00+00:00", payload),
        )
    input_path = tmp_path / "record.json"
    _write(input_path, _record())
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "1", "--database", str(database)]
        )
        == 2
    )
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM incident_revisions").fetchone() == (1,)

    with sqlite3.connect(database) as connection:
        connection.execute("DELETE FROM incident_revisions")
        connection.execute(
            "INSERT INTO incident_revisions VALUES (?,?,?,?)",
            ("incident-1", 1, "2026-11-11T12:00:00+00:00", "{"),
        )
    assert incident_triage.main(["summary", "--database", str(database)]) == 2
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(database)]
        )
        == 2
    )


def test_nested_snapshot_and_hash_path_fail_safely(tmp_path: Path) -> None:
    from tools.evals import incident_triage

    nested = tmp_path / "nested.sqlite3"
    with sqlite3.connect(nested) as connection:
        connection.execute(
            "CREATE TABLE incident_revisions (incident_id TEXT NOT NULL, revision INTEGER NOT NULL, recorded_at TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (incident_id, revision))"
        )
        connection.execute(
            "INSERT INTO incident_revisions VALUES (?,?,?,?)",
            ("nested", 1, "2026-11-11T12:00:00+00:00", "[" * 2000 + "]" * 2000),
        )
    assert incident_triage.main(["summary", "--database", str(nested)]) == 2
    with sqlite3.connect(nested) as connection:
        assert connection.execute("SELECT COUNT(*) FROM incident_revisions").fetchone() == (1,)

    hashed = tmp_path / "reviews#fragment.sqlite3"
    input_path = tmp_path / "record.json"
    _write(input_path, _record(incident_id="hash-path"))
    assert (
        incident_triage.main(
            ["record", "--input", str(input_path), "--expected-revision", "0", "--database", str(hashed)]
        )
        == 0
    )
    assert incident_triage.main(["summary", "--database", str(hashed)]) == 0


def test_concurrent_expected_zero_allows_one_revision(tmp_path: Path) -> None:
    from tools.evals import incident_triage

    database = tmp_path / "reviews.sqlite3"
    record = _record(incident_id="concurrent-incident")

    def write_once() -> int | str:
        try:
            return incident_triage.record_incident(database, record, 0)
        except incident_triage.IncidentError:
            return "rejected"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(lambda _: write_once(), range(2)))
    assert sorted(results, key=str) == [1, "rejected"]
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM incident_revisions").fetchone() == (1,)
