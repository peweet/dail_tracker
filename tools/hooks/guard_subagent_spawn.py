#!/usr/bin/env python
"""PreToolUse hook that enforces the repository's subagent spawn contract."""

from __future__ import annotations

import json
import re
import sys
import tomllib
from collections.abc import Mapping
from pathlib import Path

ROLE_CONFIG_DIR = Path(__file__).resolve().parents[2] / ".codex" / "agents"
ALLOWED_ROLE_NAMES = frozenset({"scout", "reviewer", "worker"})
REQUIRED_SECTIONS = ("Objective", "Scope", "Invariants", "Acceptance", "Result contract")
SPAWN_TOOL_NAMES = {"agent", "spawn_agent"}


def _role_contracts() -> dict[str, tuple[str, str]]:
    contracts: dict[str, tuple[str, str]] = {}
    for role_name in ALLOWED_ROLE_NAMES:
        role = tomllib.loads((ROLE_CONFIG_DIR / f"{role_name}.toml").read_text(encoding="utf-8"))
        contracts[role_name] = (str(role.get("model", "")), str(role.get("model_reasoning_effort", "")))
    return contracts


def _first_text(values: Mapping[str, object], *keys: str) -> str:
    for key in keys:
        value = values.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _tool_input(payload: Mapping[str, object]) -> Mapping[str, object]:
    value = payload.get("tool_input") or payload.get("toolInput") or payload.get("input")
    return value if isinstance(value, Mapping) else {}


def _is_spawn(payload: Mapping[str, object], tool_input: Mapping[str, object]) -> bool:
    tool_name = _first_text(payload, "tool_name", "toolName").lower()
    if tool_name:
        return tool_name in SPAWN_TOOL_NAMES
    return any(key in tool_input for key in ("agent_type", "agentType", "subagent_type"))


def validation_errors(payload: Mapping[str, object]) -> list[str]:
    tool_input = _tool_input(payload)
    if not _is_spawn(payload, tool_input):
        return []

    errors: list[str] = []
    try:
        role_contracts = _role_contracts()
    except (OSError, tomllib.TOMLDecodeError) as exc:
        return [f"project role configuration is unavailable: {exc}"]
    role = _first_text(tool_input, "agent_type", "agentType", "subagent_type", "role")
    if role not in role_contracts:
        allowed = ", ".join(sorted(role_contracts))
        errors.append(f"agent_type must be one of: {allowed}")
    else:
        expected_model, expected_effort = role_contracts[role]
        model = _first_text(tool_input, "model")
        effort = _first_text(tool_input, "reasoning_effort", "reasoningEffort")
        if model and model != expected_model:
            errors.append(f"{role} model override must be {expected_model}, not {model}")
        if effort and effort != expected_effort:
            errors.append(f"{role} reasoning effort override must be {expected_effort}, not {effort}")

    fork_turns = tool_input.get("fork_turns", tool_input.get("forkTurns"))
    if fork_turns != "none":
        errors.append('fork_turns must be explicitly set to "none"')

    message = _first_text(tool_input, "message", "prompt")
    if not message:
        errors.append("message must contain a self-contained task packet")
    else:
        missing = [
            section
            for section in REQUIRED_SECTIONS
            if not re.search(rf"(?im)^\s*(?:#+\s*)?{re.escape(section)}\s*:?(?:\s|$)", message)
        ]
        if missing:
            errors.append(f"message is missing task-packet sections: {', '.join(missing)}")
    return errors


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except (json.JSONDecodeError, TypeError):
        sys.stderr.write("Blocked subagent spawn: hook input was not valid JSON.\n")
        return 2
    if not isinstance(payload, Mapping):
        sys.stderr.write("Blocked subagent spawn: hook input must be a JSON object.\n")
        return 2

    errors = validation_errors(payload)
    if not errors:
        return 0
    sys.stderr.write("Blocked subagent spawn:\n- " + "\n- ".join(errors) + "\n")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
