"""Claude `Agent` spawn hint: Claude-shaped payloads, advisory by default, no provider calls."""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from tools.hooks import spawn_packet_hint as hint

ROOT = Path(__file__).resolve().parents[2]
PACKET = "Objective: map the path\nScope: shared/example.py\nInvariants: read-only\nAcceptance: cite it\nResult contract: findings\n"


def _run(monkeypatch, capsys, payload) -> tuple[int, str, str]:
    raw = payload if isinstance(payload, str) else json.dumps(payload)
    monkeypatch.setattr(hint.sys, "stdin", io.StringIO(raw))
    code = hint.main()
    captured = capsys.readouterr()
    return code, captured.out, captured.err


def _agent(prompt, subagent_type="builder") -> dict:
    return {"tool_name": "Agent", "tool_input": {"prompt": prompt, "description": "d", "subagent_type": subagent_type}}


@pytest.fixture(autouse=True)
def _advisory_mode(monkeypatch):
    monkeypatch.delenv("DAIL_SPAWN_PACKET_MODE", raising=False)


def test_complete_packet_is_silent(monkeypatch, capsys):
    assert _run(monkeypatch, capsys, _agent(PACKET)) == (0, "", "")


def test_markdown_headings_bold_labels_and_list_markers_count(monkeypatch, capsys):
    prompt = (
        "**Objective:** map the path\n## Scope\nshared/example.py\n1. **Invariants**: read-only\n"
        "- Acceptance - cite it\n> Result contract: findings\n"
    )
    assert _run(monkeypatch, capsys, _agent(prompt)) == (0, "", "")


def test_missing_sections_are_named_and_the_call_is_not_decided(monkeypatch, capsys):
    code, out, err = _run(monkeypatch, capsys, _agent("Objective: x\nScope: y\n"))
    assert (code, err) == (0, "")
    output = json.loads(out)["hookSpecificOutput"]
    assert output["hookEventName"] == "PreToolUse"
    assert "permissionDecision" not in output  # advisory: must not change the permission flow
    assert "no line for: Invariants, Acceptance, Result contract" in output["additionalContext"]
    assert "'builder'" in output["additionalContext"]


def test_a_spawn_without_a_subagent_type_is_named_general_purpose(monkeypatch, capsys):
    payload = {"tool_name": "Agent", "tool_input": {"prompt": "Objective: x"}}
    _, out, _ = _run(monkeypatch, capsys, payload)
    assert "'general-purpose'" in json.loads(out)["hookSpecificOutput"]["additionalContext"]


def test_prose_that_merely_starts_with_a_section_word_does_not_count(monkeypatch, capsys):
    _, out, _ = _run(
        monkeypatch, capsys, _agent("Scoped changes only. Objective is clear. Acceptance criteria follow.")
    )
    context = json.loads(out)["hookSpecificOutput"]["additionalContext"]
    assert "no line for: Objective, Scope, Invariants, Acceptance, Result contract" in context


def test_block_mode_denies_with_the_codex_exit_code(monkeypatch, capsys):
    monkeypatch.setenv("DAIL_SPAWN_PACKET_MODE", "block")
    code, out, err = _run(monkeypatch, capsys, _agent("Objective: x"))
    assert (code, out) == (2, "")
    assert "missing task-packet sections: Scope, Invariants, Acceptance, Result contract" in err


@pytest.mark.parametrize(
    "payload",
    [
        {"tool_name": "Bash", "tool_input": {"command": "git status"}},
        {"tool_name": "Agent", "tool_input": {"prompt": 7}},
        {"tool_name": "Agent", "tool_input": "not an object"},
        "not json",
        "[]",
    ],
)
def test_other_tools_and_malformed_input_never_block(monkeypatch, capsys, payload):
    assert _run(monkeypatch, capsys, payload) == (0, "", "")


def test_claude_registers_the_hint_and_not_the_codex_only_guard():
    path = ROOT / ".claude/settings.json"
    if not path.is_file():
        pytest.skip(".claude/ is gitignored, so a clean checkout has no Claude settings to check")
    settings = json.loads(path.read_text(encoding="utf-8"))
    groups = settings["hooks"]["PreToolUse"]
    agent_commands = [hook["command"] for group in groups if group.get("matcher") == "Agent" for hook in group["hooks"]]
    assert len(agent_commands) == 1 and "spawn_packet_hint.py" in agent_commands[0]
    # guard_subagent_spawn.py requires Codex's `fork_turns` and role names, so on Claude's
    # prompt/subagent_type input it rejects every spawn, complete packet included.
    assert "guard_subagent_spawn.py" not in json.dumps(settings)
