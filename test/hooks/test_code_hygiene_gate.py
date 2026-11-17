"""Deterministic coverage for tools/hooks/code_hygiene_gate.py.

State dirs (this hook's and verifier_nudge's) point at tmp_path, and the edited files live in a
throwaway git repo so the hygiene gate never looks at this checkout.
"""

from __future__ import annotations

import hashlib
import importlib.util
import io
import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import tools.check_code_hygiene as hygiene

HOOK = Path(__file__).resolve().parents[2] / "tools" / "hooks" / "code_hygiene_gate.py"
SESSION = "sess-hygiene-1"


def complex_source(branches: int) -> str:
    lines = ["def g(a):", "    x = 0"]
    for i in range(branches):
        lines += [f"    if a == {i}:", f"        x += {i + 1}"]
    lines.append("    return x")
    return "\n".join(lines) + "\n"


def git(repo: Path, *args: str) -> None:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    cmd = ["git", "-C", str(repo), "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args]
    subprocess.run(cmd, check=True, capture_output=True, env=env, stdin=subprocess.DEVNULL)


@pytest.fixture
def hook(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location("code_hygiene_gate_under_test", HOOK)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_STATE_DIR", tmp_path / "hook_state")
    monkeypatch.setattr(module.verifier_nudge, "_STATE_DIR", tmp_path / "nudge_state")
    monkeypatch.setattr(hygiene, "CACHE_DIR", tmp_path / "cache")
    (tmp_path / "hook_state").mkdir()
    (tmp_path / "nudge_state").mkdir()
    monkeypatch.setenv("CLAUDE_PROJECT_DIR", str(tmp_path / "project"))
    monkeypatch.delenv("DAIL_SKIP_HYGIENE_GATE", raising=False)
    monkeypatch.delenv("DAIL_HYGIENE_VERIFIER", raising=False)
    return module


@pytest.fixture
def repo(tmp_path):
    root = (tmp_path / "repo").resolve()
    root.mkdir()
    git(root, "init", "-q")
    (root / "pkg").mkdir()
    (root / "pkg" / "mod.py").write_text(complex_source(11), encoding="utf-8", newline="\n")
    git(root, "add", "-A")
    git(root, "commit", "-q", "-m", "base")
    return root


def record_edits(hook, files: list[Path]) -> None:
    payload = [str(f).replace("\\", "/") for f in files]
    hook.verifier_nudge._state_path(SESSION).write_text(json.dumps({"files": payload, "fired": True}), encoding="utf-8")


def fire(hook, monkeypatch, **payload) -> int:
    payload.setdefault("session_id", SESSION)
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    return hook.main()


def stop(hook, monkeypatch) -> int:
    return fire(hook, monkeypatch, hook_event_name="Stop")


def plain_files(repo: Path, count: int) -> list[Path]:
    files = []
    for i in range(count):
        path = repo / "pkg" / f"plain{i}.py"
        path.write_text("X = 1\n", encoding="utf-8")
        files.append(path)
    return files


def test_reviewer_event_does_not_create_snapshot(hook, monkeypatch, repo, capsys):
    files = plain_files(repo, 2)
    record_edits(hook, files)
    code = fire(
        hook,
        monkeypatch,
        hook_event_name="PostToolUse",
        tool_name="Agent",
        tool_input={"subagent_type": "verifier"},
    )
    assert code == 0 and capsys.readouterr().out == ""
    assert "verified_files" not in hook._load_state(SESSION)


def test_verifier_event_never_credits_review(hook, monkeypatch, repo):
    files = plain_files(repo, 3)
    record_edits(hook, files)
    assert (
        fire(
            hook,
            monkeypatch,
            hook_event_name="SubagentStop",
            agent_type="verifier",
        )
        == 0
    )
    assert stop(hook, monkeypatch) == 2


def test_successful_receipt_credits_only_matching_reviewed_paths(hook, monkeypatch, repo):
    files = plain_files(repo, 3)
    record_edits(hook, files)
    receipt = hook._receipt_path(SESSION)
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(
        json.dumps(
            {
                "schema_version": 1,
                "session_id": SESSION,
                "checkout": str(repo),
                "reviewer_role": "reviewer",
                "acceptance": "MATCHES BRIEF",
                "result": "PASS",
                "reviewed_paths": {str(files[0]).replace("\\", "/"): hook._sha256(files[0])},
            }
        ),
        encoding="utf-8",
    )
    assert stop(hook, monkeypatch) == 2
    files[1].write_text("X = 2\n", encoding="utf-8")
    assert stop(hook, monkeypatch) == 2


@settings(suppress_health_check=[HealthCheck.function_scoped_fixture], max_examples=20, deadline=None)
@given(st.permutations(["subset", "edit", "fail", "pass", "restart"]))
def test_receipt_lifecycle_only_credits_current_content(hook, monkeypatch, repo, operations):
    session = "hypothesis-receipt-" + hashlib.sha1("/".join(operations).encode()).hexdigest()[:8]
    monkeypatch.setattr(hook, "_STATE_DIR", repo / ".hook-state")
    files = plain_files(repo, 3)
    expected: set[str] = set()
    receipt = hook._receipt_path(session)

    def write_receipt(result: str, covered: list[Path]) -> None:
        receipt.parent.mkdir(parents=True, exist_ok=True)
        receipt.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "session_id": session,
                    "checkout": str(repo),
                    "reviewer_role": "reviewer",
                    "acceptance": "MATCHES BRIEF",
                    "result": result,
                    "reviewed_paths": {str(path).replace("\\", "/"): hook._sha256(path) for path in covered},
                }
            ),
            encoding="utf-8",
        )

    for operation in operations:
        if operation == "subset":
            write_receipt("PASS", files[:2])
            expected = {str(path) for path in files[:2]}
        elif operation == "edit":
            files[1].write_text("X = 2\n", encoding="utf-8")
            expected.discard(str(files[1]))
        elif operation == "fail":
            write_receipt("FAIL", files)
            expected = set()
        elif operation == "pass":
            write_receipt("PASS", files)
            expected = {str(path) for path in files}
        else:
            # A fresh read after a restart must produce the same content-bound result.
            pass
        assert hook._reviewed_files(session, [str(path) for path in files]) == expected


def test_incomplete_duplicate_check_blocks_and_records_debt(hook, monkeypatch, repo, capsys):
    record_edits(hook, plain_files(repo, 1))
    result = {"regressions": [], "improved": [], "checked_files": [], "notes": ["DUP not checked (budget)"]}
    monkeypatch.setattr(hook, "_hygiene_result", lambda _: result)
    assert stop(hook, monkeypatch) == 2
    assert "DUP not checked" in capsys.readouterr().err


def test_verifier_stop_event_does_not_create_snapshot(hook, monkeypatch, repo):
    record_edits(hook, plain_files(repo, 1))
    assert fire(hook, monkeypatch, hook_event_name="SubagentStop", agent_type="verifier") == 0
    assert "verified_files" not in hook._load_state(SESSION)


def test_other_agents_do_not_record_a_snapshot(hook, monkeypatch, repo):
    record_edits(hook, plain_files(repo, 1))
    fire(hook, monkeypatch, hook_event_name="SubagentStop", agent_type="explorer")
    fire(hook, monkeypatch, hook_event_name="PostToolUse", tool_name="Agent", tool_input={"subagent_type": "x"})
    assert "verified_files" not in hook._load_state(SESSION)


def test_stop_stays_blocked_after_verifier_event_without_receipt(hook, monkeypatch, repo, capsys):
    files = plain_files(repo, 3)
    record_edits(hook, files)
    assert stop(hook, monkeypatch) == 2
    err = capsys.readouterr().err
    assert "reviewer" in err and "plain0.py" in err
    fire(hook, monkeypatch, hook_event_name="SubagentStop", agent_type="verifier")
    assert stop(hook, monkeypatch) == 2
    assert "reviewer receipt" in capsys.readouterr().err


def test_files_outside_any_repo_never_count_as_unverified(hook, monkeypatch, repo, tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    scripts = [outside / "a.py", outside / "b.py"]
    for script in scripts:
        script.write_text("X = 1\n", encoding="utf-8")
    record_edits(hook, [*scripts, *plain_files(repo, 1)])
    assert stop(hook, monkeypatch) == 0
    fire(hook, monkeypatch, hook_event_name="SubagentStop", agent_type="verifier")
    assert "verified_files" not in hook._load_state(SESSION)


def test_missing_files_never_count_as_unverified(hook, monkeypatch, repo):
    files = plain_files(repo, 3)
    files[0].unlink()
    files[1].unlink()
    record_edits(hook, files)
    assert stop(hook, monkeypatch) == 0


def test_verifier_switch_off_passes_three_unverified_files(hook, monkeypatch, repo):
    record_edits(hook, plain_files(repo, 3))
    monkeypatch.setenv("DAIL_HYGIENE_VERIFIER", "0")
    assert stop(hook, monkeypatch) == 0


def test_verifier_switch_off_still_blocks_a_hygiene_regression(hook, monkeypatch, repo, capsys):
    target = repo / "pkg" / "mod.py"
    target.write_text(complex_source(14), encoding="utf-8", newline="\n")
    record_edits(hook, [target, *plain_files(repo, 2)])
    monkeypatch.setenv("DAIL_HYGIENE_VERIFIER", "0")
    assert stop(hook, monkeypatch) == 2
    err = capsys.readouterr().err
    assert "C901" in err and "no verifier has seen" not in err


def test_failed_import_fails_open(hook, monkeypatch, repo):
    record_edits(hook, plain_files(repo, 3))
    monkeypatch.setattr(hook, "hygiene", None)
    assert stop(hook, monkeypatch) == 0


def test_stop_allows_two_unverified_files(hook, monkeypatch, repo):
    record_edits(hook, plain_files(repo, 2))
    assert stop(hook, monkeypatch) == 0


def test_hygiene_regression_blocks_twice_then_logs_debt_once(hook, monkeypatch, repo, tmp_path, capsys):
    target = repo / "pkg" / "mod.py"
    target.write_text(complex_source(14), encoding="utf-8", newline="\n")
    record_edits(hook, [target])
    assert stop(hook, monkeypatch) == 2
    err = capsys.readouterr().err
    assert "C901 complexity weight 2 -> 5 (+3)" in err
    assert "only the owner adds a waiver" in err
    assert stop(hook, monkeypatch) == 2
    capsys.readouterr()
    debt = tmp_path / "project" / "logs" / "code_hygiene_debt.jsonl"
    assert not debt.exists()
    assert stop(hook, monkeypatch) == 0
    assert stop(hook, monkeypatch) == 0
    rows = [json.loads(line) for line in debt.read_text(encoding="utf-8").splitlines()]
    assert len(rows) == 1
    assert rows[0]["session"] == SESSION
    assert any(line.startswith("C901") for line in rows[0]["regressions"])
    assert rows[0]["unverified"] == []


def test_clean_edit_passes_silently(hook, monkeypatch, repo, capsys):
    target = repo / "pkg" / "mod.py"
    target.write_text(complex_source(11) + "Y = 2\n", encoding="utf-8", newline="\n")
    record_edits(hook, [target])
    assert stop(hook, monkeypatch) == 0
    captured = capsys.readouterr()
    assert captured.out == "" and captured.err == ""


def test_stop_ignores_stop_hook_active(hook, monkeypatch, repo):
    target = repo / "pkg" / "mod.py"
    target.write_text(complex_source(14), encoding="utf-8", newline="\n")
    record_edits(hook, [target])
    assert fire(hook, monkeypatch, hook_event_name="Stop", stop_hook_active=True) == 2


def test_kill_switch(hook, monkeypatch, repo):
    target = repo / "pkg" / "mod.py"
    target.write_text(complex_source(14), encoding="utf-8", newline="\n")
    record_edits(hook, [target])
    monkeypatch.setenv("DAIL_SKIP_HYGIENE_GATE", "1")
    assert stop(hook, monkeypatch) == 0


@pytest.mark.parametrize("raw", ["not json", "[1, 2]", "", '{"hook_event_name": "Stop"}'])
def test_malformed_or_empty_stdin_exits_zero(hook, monkeypatch, raw):
    monkeypatch.setattr(sys, "stdin", io.StringIO(raw))
    assert hook.main() == 0


def test_internal_error_fails_open(hook, monkeypatch, repo):
    record_edits(hook, plain_files(repo, 3))

    def boom(*_args, **_kwargs):
        raise RuntimeError("boom")

    monkeypatch.setattr(hook, "handle_stop", boom)
    assert stop(hook, monkeypatch) == 0
