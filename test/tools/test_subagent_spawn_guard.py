from __future__ import annotations

import io
import json

from tools.hooks import guard_subagent_spawn as guard


def _packet() -> str:
    return """Objective: map the active path
Scope: shared/example.py only
Invariants: read-only and evidence-first
Acceptance: cite the owning symbol
Result contract: findings, checks not run, and residual risk
"""


def _run(monkeypatch, capsys, payload: dict) -> tuple[int, str]:
    monkeypatch.setattr(guard.sys, "stdin", io.StringIO(json.dumps(payload)))
    result = guard.main()
    return result, capsys.readouterr().err


def test_allows_a_conforming_project_role(monkeypatch, capsys):
    payload = {
        "tool_name": "spawn_agent",
        "tool_input": {
            "agent_type": "scout",
            "fork_turns": "none",
            "message": _packet(),
        },
    }
    assert _run(monkeypatch, capsys, payload) == (0, "")


def test_rejects_unconfigured_role_and_inherited_context(monkeypatch, capsys):
    payload = {
        "tool_name": "spawn_agent",
        "tool_input": {
            "agent_type": "default",
            "fork_turns": "all",
            "message": _packet(),
        },
    }
    result, error = _run(monkeypatch, capsys, payload)
    assert result == 2
    assert "agent_type must be one of" in error
    assert 'fork_turns must be explicitly set to "none"' in error


def test_rejects_model_override_and_incomplete_packet(monkeypatch, capsys):
    payload = {
        "tool_name": "Agent",
        "tool_input": {
            "agent_type": "reviewer",
            "fork_turns": "none",
            "model": "gpt-5.6-sol",
            "reasoning_effort": "low",
            "message": "Objective: review the patch",
        },
    }
    result, error = _run(monkeypatch, capsys, payload)
    assert result == 2
    assert "reviewer model override must be gpt-5.6-luna" in error
    assert "reviewer reasoning effort override must be xhigh" in error
    assert "Scope, Invariants, Acceptance, Result contract" in error


def test_fails_closed_when_project_role_configuration_is_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(guard, "ROLE_CONFIG_DIR", tmp_path)

    errors = guard.validation_errors(
        {"tool_name": "spawn_agent", "tool_input": {"agent_type": "scout", "fork_turns": "none"}}
    )

    assert len(errors) == 1
    assert errors[0].startswith("project role configuration is unavailable:")


def test_ignores_non_spawn_tools():
    payload = {"tool_name": "Bash", "tool_input": {"command": "git status --short"}}
    assert guard.validation_errors(payload) == []
