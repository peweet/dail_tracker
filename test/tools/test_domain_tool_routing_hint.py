"""Tests for tools/hooks/domain_tool_routing_hint.py (PostToolUse, matcher Bash|PowerShell).

Sibling to test_grep_routing_hint.py: pins classification (both a data-query
signal AND a topic keyword required, neither alone) and the once-per-session
nudge + trial-log contract, so a regression in either goes noticed instead of
silently making the domain-tool half of the adoption gap unmeasurable again.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import uuid
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _load():
    spec = importlib.util.spec_from_file_location(
        "domain_tool_routing_hint", REPO / "tools" / "hooks" / "domain_tool_routing_hint.py"
    )
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _run(mod, payload: dict, monkeypatch) -> int:
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    return mod.main()


def _bash(command: str, sid: str, tool: str = "Bash") -> dict:
    return {"tool_name": tool, "session_id": sid, "tool_input": {"command": command}}


# ── classification ───────────────────────────────────────────────────────────


def test_classify_duckdb_plus_topic_is_procurement():
    mod = _load()
    cmd = "duckdb -c \"select * from read_parquet('data/gold/parquet/ted_ie_awards.parquet') limit 5\""
    assert mod._classify(cmd) == "procurement"


def test_classify_topic_word_alone_is_silent():
    mod = _load()
    # no duckdb/polars/parquet signal at all — e.g. a commit or doc edit
    assert mod._classify("git commit -m 'fix procurement award parsing'") is None


def test_classify_data_query_alone_without_topic_is_silent():
    mod = _load()
    assert mod._classify("duckdb -c \"select * from read_parquet('data/gold/parquet/members.parquet')\"") is None


def test_classify_votes_topic_with_polars():
    mod = _load()
    cmd = "python -c \"import polars as pl; pl.read_parquet('data/gold/parquet/divisions.parquet')\""
    assert mod._classify(cmd) == "votes"


def test_classify_empty_command_is_silent():
    mod = _load()
    assert mod._classify("") is None


# ── nudge + trial log ────────────────────────────────────────────────────────


def test_nudge_fires_once_and_both_calls_are_logged(monkeypatch, capsys, tmp_path):
    mod = _load()
    log = tmp_path / "trial.jsonl"
    monkeypatch.setattr(mod, "_TRIAL_LOG", str(log))
    sid = uuid.uuid4().hex
    cmd = "duckdb -c \"select * from read_parquet('data/gold/parquet/payments.parquet') limit 5\""

    assert _run(mod, _bash(cmd, sid), monkeypatch) == 0
    out = json.loads(capsys.readouterr().out)
    assert "payments_by_year" in out["hookSpecificOutput"]["additionalContext"]

    # same session + category again -> silent, but still logged
    assert _run(mod, _bash(cmd, sid), monkeypatch) == 0
    assert capsys.readouterr().out == ""

    rows = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
    assert [r["nudged"] for r in rows] == [True, False]
    assert all(r["category"] == "payments" and r["session"] == sid[:12] for r in rows)


def test_powershell_tool_name_also_matches(monkeypatch, capsys, tmp_path):
    mod = _load()
    monkeypatch.setattr(mod, "_TRIAL_LOG", str(tmp_path / "trial.jsonl"))
    cmd = "duckdb -c \"select * from read_parquet('data/gold/parquet/lobbying_orgs.parquet')\""
    assert _run(mod, _bash(cmd, uuid.uuid4().hex, tool="PowerShell"), monkeypatch) == 0
    out = json.loads(capsys.readouterr().out)
    assert "lobbying_organisations" in out["hookSpecificOutput"]["additionalContext"]


def test_non_matching_tool_is_ignored(monkeypatch, capsys, tmp_path):
    mod = _load()
    log = tmp_path / "trial.jsonl"
    monkeypatch.setattr(mod, "_TRIAL_LOG", str(log))
    payload = {
        "tool_name": "Read",
        "session_id": uuid.uuid4().hex,
        "tool_input": {"file_path": "data/gold/parquet/procurement.parquet"},
    }
    assert _run(mod, payload, monkeypatch) == 0
    assert capsys.readouterr().out == ""
    assert not log.exists()


def test_fails_open_on_garbage_payload(monkeypatch, capsys):
    mod = _load()
    monkeypatch.setattr(sys, "stdin", io.StringIO("not json{{"))
    assert mod.main() == 0
    assert capsys.readouterr().out == ""


def test_unwritable_log_never_breaks_the_nudge(monkeypatch, capsys):
    mod = _load()
    # a directory path can't be opened for append -- the log write must swallow it
    monkeypatch.setattr(mod, "_TRIAL_LOG", str(REPO / "tools"))
    sid = uuid.uuid4().hex
    cmd = "duckdb -c \"select * from read_parquet('data/gold/parquet/votes.parquet')\""
    assert _run(mod, _bash(cmd, sid), monkeypatch) == 0
    out = json.loads(capsys.readouterr().out)
    assert "list_recent_votes" in out["hookSpecificOutput"]["additionalContext"]
