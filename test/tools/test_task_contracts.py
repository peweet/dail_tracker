"""Public persistence and hook seams; no provider calls or model-quality claims."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tomllib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from tools.task_contracts import ContractError, TaskStore, check_action

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
    adjacent = json.loads((ROOT / ".codex/hooks.json").read_text(encoding="utf-8"))
    assert "task_context.py" not in json.dumps(adjacent)  # both sources can load; avoid duplicate injection


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
