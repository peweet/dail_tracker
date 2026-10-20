"""Public persistence and hook seams; no provider calls or model-quality claims."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tools import task_contracts
from tools.hooks import task_context
from tools.task_contracts import ContractError, TaskStore, check_action, session_from_environment

ROOT = Path(__file__).resolve().parents[2]


def start(store, session="session-a"):
    return store.start(
        session_id=session,
        owner="captain",
        objective="Improve the coding agent harness architecture",
        scope="Task persistence and objective drift evaluation",
        mode="implementation",
        allowed_paths=["tools/task_contracts.py", "test/tools", "doc/AGENT_HARNESS.md"],
        acceptance=["Resume restores the original objective", "Scope amendments are explicit"],
        unresolved=["Live client hook execution remains unverified"],
    )


def test_resume_restores_contract_after_process_restart_and_short_approval(tmp_path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    database = checkout / "logs" / "agent_task_contracts.sqlite3"
    original = start(TaskStore(database, checkout))
    payload = {"session_id": "session-a", "cwd": str(checkout), "hook_event_name": "SessionStart", "source": "compact"}
    env = dict(os.environ, DAIL_TASK_CONTRACT_DB=str(database), PYTHONUTF8="1")
    for event in (payload, {**payload, "hook_event_name": "UserPromptSubmit", "prompt": "ok approved and proceed"}):
        result = subprocess.run(
            [sys.executable, str(ROOT / "tools/hooks/task_context.py")],
            input=json.dumps(event),
            capture_output=True,
            text=True,
            encoding="utf-8",
            env=env,
            timeout=10,
        )
        assert result.returncode == 0, result.stderr
        output = json.loads(result.stdout)["hookSpecificOutput"]
        assert output["hookEventName"] == event["hook_event_name"]
        assert "Improve the coding agent harness architecture" in output["additionalContext"]
        assert "Scope amendments are explicit" in output["additionalContext"]
        assert len(output["additionalContext"]) <= 6000
    assert TaskStore(database, checkout).load("session-a") == original


def test_progress_cannot_change_scope_and_amendments_need_reference(tmp_path):
    store = TaskStore(tmp_path / "state.sqlite3", tmp_path)
    original = start(store)
    with pytest.raises(ContractError, match="checkpoint"):
        store.checkpoint("session-a", "captain", 1, {"scope": "private product pilot"})
    with pytest.raises(ContractError, match="authorization"):
        store.amend("session-a", "captain", 1, {"mode": "research"}, authorization_reference="")
    assert store.load("session-a") == original
    amended = store.amend(
        "session-a",
        "captain",
        1,
        {"mode": "research"},
        authorization_reference="user: explicit research-only correction",
    )
    assert amended["revision"] == 2
    assert amended["original_objective"] == original["original_objective"]
    assert amended["authorization_reference"].startswith("user:")
    assert not check_action(amended, "edit", ["tools/task_contracts.py"])["allowed"]


def test_stale_writer_owner_and_other_session_cannot_overwrite(tmp_path):
    store = TaskStore(tmp_path / "state.sqlite3", tmp_path)
    start(store)
    updated = store.checkpoint("session-a", "captain", 1, {"progress": "Focused checks passed"})
    for owner, revision in (("sidecar", 2), ("captain", 1)):
        with pytest.raises(ContractError):
            store.checkpoint("session-a", owner, revision, {"progress": "overwrite"})
    assert store.load("session-a") == updated
    assert store.load("other-session") is None
    assert TaskStore(tmp_path / "state.sqlite3", tmp_path / "other-checkout").load("session-a") is None


def test_concurrent_writers_cannot_commit_the_same_revision(tmp_path):
    store = TaskStore(tmp_path / "state.sqlite3", tmp_path)
    start(store)

    def update(note):
        try:
            store.checkpoint("session-a", "captain", 1, {"progress": note})
            return "committed"
        except ContractError:
            return "conflict"

    with ThreadPoolExecutor(max_workers=2) as executor:
        results = list(executor.map(update, ["First observation", "Second observation"]))
    assert sorted(results) == ["committed", "conflict"]
    assert [row["revision"] for row in store.history("session-a")] == [1, 2]


@pytest.mark.parametrize("path", ["../escape", "/absolute", "C:/outside", "tools/../outside", "tools\\..\\outside"])
def test_unsafe_scope_paths_are_rejected(tmp_path, path):
    store = TaskStore(tmp_path / "state.sqlite3", tmp_path)
    with pytest.raises(ContractError, match="path"):
        store.start(
            session_id="a",
            owner="captain",
            objective="Harness",
            scope="Harness",
            mode="implementation",
            allowed_paths=[path],
            acceptance=["Scope holds"],
            unresolved=[],
        )


def test_action_scope_checks_boundaries_and_resolved_symlinks(tmp_path):
    store = TaskStore(tmp_path / "state.sqlite3", tmp_path)
    contract = start(store)
    assert check_action(contract, "edit", ["test/tools/test_example.py"])["allowed"]
    assert not check_action(contract, "edit", ["test/tools_extra/file.py"])["allowed"]
    assert not check_action(contract, "edit", ["planning/product/engine.py"])["allowed"]
    assert not check_action(contract, "edit", [])["allowed"]
    assert not check_action(contract, "shell", ["test/tools"])["allowed"]


def test_completed_task_is_not_reinjected_and_old_revisions_are_retained(tmp_path):
    store = TaskStore(tmp_path / "state.sqlite3", tmp_path)
    original = start(store)
    store.complete("session-a", "captain", 1, evidence="Focused mechanical checks passed; live eval NOT RUN")
    assert store.load("session-a")["status"] == "completed"
    assert store.history("session-a")[0] == original
    assert len(store.history("session-a")) == 2


def test_corrupt_database_is_reported_without_creating_a_replacement(tmp_path):
    path = tmp_path / "state.sqlite3"
    path.write_text("not a SQLite database", encoding="utf-8")
    with pytest.raises(ContractError, match="unavailable"):
        TaskStore(path, tmp_path).load("session-a")
    assert path.read_text(encoding="utf-8") == "not a SQLite database"


def test_hooks_are_registered_once_for_both_continuation_events():
    config = tomllib.loads((ROOT / ".codex/config.toml").read_text(encoding="utf-8"))
    for event in ("SessionStart", "UserPromptSubmit"):
        hooks = [hook for group in config["hooks"][event] for hook in group["hooks"]]
        task_hooks = [hook for hook in hooks if "task_context.py" in hook["command"]]
        assert len(task_hooks) == 1
        assert "task_context.py" in task_hooks[0]["command_windows"]
        assert task_hooks[0]["additionalContextLimit"] == 2000
    # hooks.json is a deliberately ignored, workstation-local adjunct.  Check it
    # when present so a developer cannot accidentally duplicate the injection,
    # but keep this public-contract test valid in a clean checkout.
    adjacent_path = ROOT / ".codex/hooks.json"
    if adjacent_path.is_file():
        adjacent = json.loads(adjacent_path.read_text(encoding="utf-8"))
        assert "task_context.py" not in json.dumps(adjacent)


def test_session_id_follows_the_host_and_refuses_to_pick_between_two(monkeypatch):
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    assert session_from_environment() is None
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "claude-1")
    assert session_from_environment() == "claude-1"
    monkeypatch.setenv("CODEX_THREAD_ID", "codex-1")
    assert session_from_environment() is None  # a shell inheriting both clients' ids
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID")
    assert session_from_environment() == "codex-1"


def test_hook_restores_from_the_claude_session_variable_when_the_payload_has_no_id(tmp_path):
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    database = checkout / "logs" / "agent_task_contracts.sqlite3"
    start(TaskStore(database, checkout), session="claude-session")
    env = {key: value for key, value in os.environ.items() if key != "CODEX_THREAD_ID"}
    env.update(DAIL_TASK_CONTRACT_DB=str(database), CLAUDE_CODE_SESSION_ID="claude-session", PYTHONUTF8="1")
    result = subprocess.run(
        [sys.executable, str(ROOT / "tools/hooks/task_context.py")],
        input=json.dumps({"cwd": str(checkout), "hook_event_name": "SessionStart", "source": "resume"}),
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=10,
    )
    assert result.returncode == 0, result.stderr
    context = json.loads(result.stdout)["hookSpecificOutput"]["additionalContext"]
    assert "Improve the coding agent harness architecture" in context


TASK_FILE = {
    "objective": "Extend the harness to Claude Code",
    "scope": "Hook registration and session id plumbing",
    "mode": "implementation",
    "allowed_paths": ["tools"],
    "acceptance": ["Resume restores the objective"],
    "unresolved": [],
}


def _cli(monkeypatch, capsys, *argv):
    monkeypatch.setattr(sys, "argv", ["task_contracts.py", *argv])
    code = task_contracts.main()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def test_cli_uses_the_claude_session_variable_when_no_flag_is_given(tmp_path, monkeypatch, capsys):
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "claude-session")
    task = tmp_path / "task.json"
    task.write_text(json.dumps(TASK_FILE), encoding="utf-8")
    base = ("--checkout", str(tmp_path), "--database", str(tmp_path / "state.sqlite3"))

    code, out, _ = _cli(monkeypatch, capsys, "start", *base, "--owner", "captain", "--input", str(task))
    assert code == 0 and json.loads(out)["session_id"] == "claude-session"
    code, out, _ = _cli(monkeypatch, capsys, "show", *base)
    assert code == 0 and json.loads(out)["objective"] == TASK_FILE["objective"]

    # With no id from either client the CLI reports it instead of picking a record.
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID")
    code, out, err = _cli(monkeypatch, capsys, "show", *base)
    assert (code, out) == (2, "") and "session_id" in err


def _hook(monkeypatch, capsys, payload):
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    monkeypatch.setattr(task_context.sys, "stdin", io.StringIO(raw))
    code = task_context.main()
    return code, capsys.readouterr().out


@pytest.fixture
def task_db(tmp_path, monkeypatch):
    monkeypatch.delenv("CODEX_THREAD_ID", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_SESSION_ID", raising=False)
    database = tmp_path / "state.sqlite3"
    monkeypatch.setenv("DAIL_TASK_CONTRACT_DB", str(database))
    return database


def _start_in(database, checkout, session):
    return TaskStore(database, checkout).start(owner="captain", session_id=session, **TASK_FILE)


def test_hook_restores_an_active_task_for_either_event_and_a_subdirectory_cwd(task_db, monkeypatch, capsys):
    _start_in(task_db, ROOT, "claude-session")  # keyed by the hook's own checkout; the database is a temp file
    for event in ("SessionStart", "UserPromptSubmit"):
        payload = {"session_id": "claude-session", "cwd": str(ROOT / "tools"), "hook_event_name": event}
        code, out = _hook(monkeypatch, capsys, payload)
        output = json.loads(out)["hookSpecificOutput"]
        assert code == 0 and output["hookEventName"] == event
        assert TASK_FILE["objective"] in output["additionalContext"]


def test_hook_keys_a_session_in_another_worktree_by_that_worktree(task_db, tmp_path, monkeypatch, capsys):
    worktree = tmp_path / "worktree"
    worktree.mkdir()
    _start_in(task_db, worktree, "claude-session")
    payload = {"session_id": "claude-session", "cwd": str(worktree), "hook_event_name": "SessionStart"}
    _, out = _hook(monkeypatch, capsys, payload)
    assert TASK_FILE["objective"] in json.loads(out)["hookSpecificOutput"]["additionalContext"]
    # The same session id recorded under the hook's own checkout is a different task.
    assert _hook(monkeypatch, capsys, {**payload, "cwd": str(ROOT)}) == (0, "")


def test_hook_takes_the_session_from_the_claude_variable_when_the_payload_has_none(task_db, monkeypatch, capsys):
    _start_in(task_db, ROOT, "claude-session")
    monkeypatch.setenv("CLAUDE_CODE_SESSION_ID", "claude-session")
    _, out = _hook(monkeypatch, capsys, {"hook_event_name": "SessionStart", "cwd": str(ROOT)})
    assert TASK_FILE["objective"] in json.loads(out)["hookSpecificOutput"]["additionalContext"]


@pytest.mark.parametrize("payload", [{"hook_event_name": "SessionStart", "cwd": str(ROOT)}, {"session_id": "other"}])
def test_hook_is_silent_without_a_matching_session(task_db, monkeypatch, capsys, payload):
    _start_in(task_db, ROOT, "claude-session")
    assert _hook(monkeypatch, capsys, payload) == (0, "")


def test_hook_is_silent_for_other_events_and_completed_tasks(task_db, monkeypatch, capsys):
    contract = _start_in(task_db, ROOT, "claude-session")
    stop = {"session_id": "claude-session", "hook_event_name": "Stop", "cwd": str(ROOT)}
    assert _hook(monkeypatch, capsys, stop) == (0, "")
    TaskStore(task_db, ROOT).complete("claude-session", "captain", contract["revision"], evidence="done")
    start = {**stop, "hook_event_name": "SessionStart"}
    assert _hook(monkeypatch, capsys, start) == (0, "")


@pytest.mark.parametrize(
    ("raw", "event"),
    [
        ("not json", "SessionStart"),
        ("[]", "SessionStart"),
        ({"session_id": "claude-session", "cwd": str(ROOT), "hook_event_name": "UserPromptSubmit"}, "UserPromptSubmit"),
    ],
)
def test_hook_reports_unreadable_state_instead_of_staying_silent(task_db, monkeypatch, capsys, raw, event):
    task_db.write_text("not a SQLite database", encoding="utf-8")
    code, out = _hook(monkeypatch, capsys, raw)
    output = json.loads(out)["hookSpecificOutput"]
    assert code == 0 and output["hookEventName"] == event
    assert "Active task record unavailable" in output["additionalContext"]


def test_claude_registers_the_task_hook_on_session_start_only():
    path = ROOT / ".claude/settings.json"
    if not path.is_file():
        pytest.skip(".claude/ is gitignored, so a clean checkout has no Claude settings to check")
    settings = json.loads(path.read_text(encoding="utf-8"))

    def commands(event):
        return [hook["command"] for group in settings["hooks"].get(event, []) for hook in group["hooks"]]

    assert sum("task_context.py" in command for command in commands("SessionStart")) == 1
    # Claude saves UserPromptSubmit context in the transcript and replays it, and SessionStart
    # fires again on resume and compact, so a per-prompt copy only re-adds the same record.
    assert not any("task_context.py" in command for command in commands("UserPromptSubmit"))


def test_actual_symlink_escape_is_denied(tmp_path):
    (tmp_path / "test").mkdir()
    outside = tmp_path.parent / (tmp_path.name + "-outside")
    outside.mkdir()
    try:
        (tmp_path / "test/tools").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("host does not allow directory symlinks")
    contract = start(TaskStore(tmp_path / "state.sqlite3", tmp_path))
    assert not check_action(contract, "edit", ["test/tools/escape.py"])["allowed"]
