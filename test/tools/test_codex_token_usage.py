from __future__ import annotations

import json
from datetime import UTC, datetime

from tools import codex_token_usage as usage

NOW = datetime(2026, 10, 4, 14, tzinfo=UTC)


def _meta(session, cwd, source="vscode"):
    return {
        "type": "session_meta",
        "payload": {
            "session_id": session,
            "cwd": str(cwd),
            "source": source,
            "model_provider": "openai",
        },
    }


def _record(session, response="r1", cached=80, timestamp="2026-10-04T13:00:00Z"):
    return {
        "type": "token_usage_record",
        "timestamp": timestamp,
        "ordinal": 1,
        "payload": {
            "session_id": session,
            "thread_id": session,
            "turn_id": "t1",
            "response_id": response,
            "usage": {
                "input_tokens": 100,
                "cached_input_tokens": cached,
                "cache_write_input_tokens": 5,
                "output_tokens": 10,
                "reasoning_output_tokens": 2,
                "total_tokens": 110,
            },
        },
    }


def _write(path, events):
    path.write_text("\n".join(json.dumps(e) for e in events) + "\n", encoding="utf-8")


def _report(tmp_path, **kwargs):
    return usage.build_report(tmp_path, cwd=tmp_path, now=NOW, **kwargs)


def test_report_counts_per_response_once_and_separates_subagents(tmp_path):
    event = _record("root")
    _write(
        tmp_path / "root.jsonl",
        [
            _meta("root", tmp_path),
            {"type": "turn_context", "payload": {"turn_id": "t1", "model": "test-model"}},
            event,
            event,
            {
                "type": "event_msg",
                "payload": {"type": "token_count", "info": {"total_token_usage": {"input_tokens": 99999}}},
            },
            {"type": "response_item", "payload": {"content": "SECRET_PROMPT"}},
        ],
    )
    _write(
        tmp_path / "sub.jsonl",
        [
            _meta("child", tmp_path, {"subagent": {"thread_spawn": {"parent_thread_id": "root"}}}),
            _record("child", cached=50),
        ],
    )
    report = _report(tmp_path)
    root = next(g for g in report["groups"] if g["scope"] == "root")
    assert root["records"] == root["sessions"] == 1
    assert root["input_tokens"] == 100
    assert root["cached_input_tokens"] == 80
    assert root["uncached_input_tokens"] == 20
    assert root["cache_write_input_tokens"] == 5
    assert root["total_tokens"] == 110
    assert root["model"] == "test-model"
    assert report["totals"]["records"] == 2
    assert report["diagnostics"]["duplicates"] == 1
    assert report["diagnostics"]["legacy_snapshots_ignored"] == 1
    assert "SECRET_PROMPT" not in json.dumps(report)


def test_report_filters_event_dates_workspace_and_scope(tmp_path):
    _write(
        tmp_path / "root.jsonl",
        [
            _meta("root", tmp_path),
            _record("root"),
            _record("root", "old", timestamp="2026-09-01T13:00:00Z"),
            _record("root", "future", timestamp="2026-10-05T13:00:00Z"),
        ],
    )
    _write(tmp_path / "other.jsonl", [_meta("other", tmp_path / "other"), _record("other")])
    _write(tmp_path / "sub.jsonl", [_meta("child", tmp_path, {"subagent": {}}), _record("child")])
    report = _report(tmp_path, days=1, scope="root")
    assert report["totals"]["records"] == 1


def test_conflicting_duplicate_is_excluded_and_invalid_input_is_reported(tmp_path):
    _write(
        tmp_path / "one.jsonl",
        [
            _meta("root", tmp_path),
            _record("root"),
            _record("root", cached=70),
            _record("root", "invalid", cached=101),
            _record("root", "good"),
        ],
    )
    with (tmp_path / "one.jsonl").open("a", encoding="utf-8") as stream:
        stream.write('{"unfinished":\n')
    report = _report(tmp_path)
    assert report["totals"]["records"] == 1
    assert report["diagnostics"]["conflicts"] == 1
    assert report["diagnostics"]["invalid_usage_records"] == 1
    assert report["diagnostics"]["malformed_lines"] == 1


def test_duplicate_across_transcript_copies_and_missing_response_id(tmp_path):
    record = _record("root")
    del record["payload"]["response_id"]
    for name in ["one.jsonl", "copy.jsonl"]:
        _write(tmp_path / name, [_meta("root", tmp_path), record])
    report = _report(tmp_path)
    assert report["totals"]["records"] == 1
    assert report["diagnostics"]["duplicates"] == 1


def test_scan_budget_marks_partial_report(tmp_path):
    _write(tmp_path / "one.jsonl", [_meta("root", tmp_path), _record("root")])
    report = _report(tmp_path, max_bytes=10)
    assert report["partial"] is True
    assert report["totals"]["records"] == 0


def test_malformed_id_and_grouping_fields_are_skipped_with_diagnostics(tmp_path):
    malformed = _record("root", response=["unhashable"])
    malformed["payload"]["session_id"] = {"unhashable": True}
    malformed["payload"]["thread_id"] = {"unhashable": True}
    malformed["payload"]["turn_id"] = ["unhashable"]
    valid = _record("root", response="valid")
    _write(
        tmp_path / "one.jsonl",
        [
            _meta("root", tmp_path),
            {"type": "turn_context", "payload": {"turn_id": ["bad"], "model": {"bad": True}}},
            malformed,
            valid,
        ],
    )
    metadata = json.loads((tmp_path / "one.jsonl").read_text(encoding="utf-8").splitlines()[0])
    metadata["payload"]["model_provider"] = ["bad"]
    lines = (tmp_path / "one.jsonl").read_text(encoding="utf-8").splitlines()
    lines[0] = json.dumps(metadata)
    (tmp_path / "one.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")

    report = _report(tmp_path)

    assert report["totals"]["records"] == 0
    assert report["diagnostics"]["invalid_usage_records"] == 2
    assert report["diagnostics"]["invalid_context_records"] == 1


def test_candidate_stat_failure_marks_report_partial(tmp_path, monkeypatch):
    path = tmp_path / "one.jsonl"
    _write(path, [_meta("root", tmp_path), _record("root")])
    original_stat = type(path).stat

    def fail_candidate_stat(candidate, *args, **kwargs):
        if candidate == path:
            raise OSError("candidate vanished")
        return original_stat(candidate, *args, **kwargs)

    monkeypatch.setattr(type(path), "stat", fail_candidate_stat)
    report = _report(tmp_path)

    assert report["partial"] is True
    assert report["diagnostics"]["unreadable_files"] == 1


def test_cli_is_read_only_and_returns_json(tmp_path, capsys):
    path = tmp_path / "one.jsonl"
    _write(path, [_meta("root", tmp_path), _record("root")])
    before = path.read_bytes()
    assert (
        usage.main(["--sessions-dir", str(tmp_path), "--cwd", str(tmp_path), "--days", "36500", "--format", "json"])
        == 0
    )
    report = json.loads(capsys.readouterr().out)
    assert "groups" in report
    assert list(tmp_path.iterdir()) == [path]
    assert path.read_bytes() == before
