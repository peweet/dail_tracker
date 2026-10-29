"""Deterministic coverage for tools/hooks/dated_doc_hint.py.

The hook is advisory context injected before a tool call touches a DATED doc under
planning/product/doc. Its first version matched the Read tool only, so a session that read
documents through Bash (`sed`, `cat`, `grep`) never saw it and quoted June and August
performance figures as current (2026-10-22). These tests pin the read paths it must cover
and the front-matter wording, hermetically: fixture docs live under tmp_path in a
``planning/product/doc`` layout, and the git-backed commit count is monkeypatched.
"""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import uuid
from pathlib import Path

import pytest

HOOK = Path(__file__).resolve().parents[1] / "tools" / "hooks" / "dated_doc_hint.py"


@pytest.fixture(scope="module")
def hook():
    spec = importlib.util.spec_from_file_location("dated_doc_hint", HOOK)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def docs(tmp_path: Path) -> dict[str, Path]:
    base = tmp_path / "planning" / "product" / "doc"
    (base / "plans").mkdir(parents=True)
    live = base / "PLAN_ENGINE_PARALLELISM_2026_08_24.md"
    live.write_text(
        "---\nstatus: LIVE\nupdated: 2026-08-24\n---\n\n# Plan\n\n## 1. Refuted\n\n## 2. Measured\n",
        encoding="utf-8",
    )
    historical = base / "plans" / "PERFORMANCE_AUDIT_2026_08_30.md"
    historical.write_text(
        "---\nstatus: HISTORICAL\nupdated: 2026-08-30\n---\n\n# Audit\n\n"
        "**Status, 22 October 2026 — historical record.**\n\n## Seal\n",
        encoding="utf-8",
    )
    undated = base / "PERFORMANCE_EXECUTION_LEDGER.md"
    undated.write_text("# Ledger\n\n## Current\n", encoding="utf-8")
    return {"live": live, "historical": historical, "undated": undated}


def _run(hook, monkeypatch, capsys, tool: str, tool_input: dict, session: str | None = None) -> str:
    payload = {"tool_name": tool, "tool_input": tool_input, "session_id": session or f"t-{uuid.uuid4().hex[:8]}"}
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(payload)))
    monkeypatch.setattr(hook, "_commits_since", lambda _date, _paths: 42)
    assert hook.main() == 0
    out = capsys.readouterr().out.strip()
    if not out:
        return ""
    return json.loads(out)["hookSpecificOutput"]["additionalContext"]


def test_read_fires_and_reads_live_front_matter(hook, docs, monkeypatch, capsys):
    message = _run(hook, monkeypatch, capsys, "Read", {"file_path": str(docs["live"])})
    assert "PLAN_ENGINE_PARALLELISM_2026_08_24.md" in message
    assert "status: LIVE" in message and "not currency" in message
    assert "42 engine/report-path commit(s)" in message
    assert "PERFORMANCE_EXECUTION_LEDGER.md" in message, "performance docs must name the ledger"


def test_bash_command_with_forward_slashes_fires(hook, docs, monkeypatch, capsys):
    cmd = f"sed -n '1,30p' {docs['live'].as_posix()}"
    assert "PLAN_ENGINE_PARALLELISM_2026_08_24.md" in _run(hook, monkeypatch, capsys, "Bash", {"command": cmd})


def test_powershell_command_with_backslashes_fires(hook, docs, monkeypatch, capsys):
    cmd = f"Get-Content {str(docs['historical'])} -TotalCount 20"
    message = _run(hook, monkeypatch, capsys, "PowerShell", {"command": cmd})
    assert "PERFORMANCE_AUDIT_2026_08_30.md" in message
    assert "status: HISTORICAL" in message and "historical record" in message
    assert "Status** block at the top supersedes" in message


def test_grep_path_fires(hook, docs, monkeypatch, capsys):
    message = _run(hook, monkeypatch, capsys, "Grep", {"pattern": "seal", "path": str(docs["historical"])})
    assert "PERFORMANCE_AUDIT_2026_08_30.md" in message


def test_undated_doc_and_plain_commands_stay_silent(hook, docs, monkeypatch, capsys):
    assert _run(hook, monkeypatch, capsys, "Bash", {"command": f"cat {docs['undated'].as_posix()}"}) == ""
    assert _run(hook, monkeypatch, capsys, "Bash", {"command": "git status --short"}) == ""
    assert _run(hook, monkeypatch, capsys, "Edit", {"file_path": str(docs["live"])}) == ""


def test_same_file_is_hinted_once_per_session(hook, docs, monkeypatch, capsys):
    session = f"once-{uuid.uuid4().hex[:8]}"
    first = _run(hook, monkeypatch, capsys, "Read", {"file_path": str(docs["live"])}, session)
    second = _run(hook, monkeypatch, capsys, "Bash", {"command": f"cat {docs['live'].as_posix()}"}, session)
    assert first and second == ""


def test_several_docs_in_one_command_are_bounded(hook, docs, monkeypatch, capsys):
    cmd = f"cat {docs['live'].as_posix()} {docs['historical'].as_posix()} {docs['live'].as_posix()}"
    message = _run(hook, monkeypatch, capsys, "Bash", {"command": cmd})
    assert message.count("[dated-doc]") == 2, "duplicates collapse; each file is reported once"


def test_resolve_rejects_paths_outside_the_doc_tree(hook, tmp_path):
    stray = tmp_path / "elsewhere" / "NOTES_2026_08_24.md"
    stray.parent.mkdir()
    stray.write_text("# x\n", encoding="utf-8")
    assert hook._resolve(str(stray)) is None
