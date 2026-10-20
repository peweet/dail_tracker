#!/usr/bin/env python
"""Restore explicit task state without interpreting prompts or editing the store."""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.task_contracts import (  # noqa: E402
    ContractError,
    TaskStore,
    default_database,
    render_context,
    session_from_environment,
)


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        if not isinstance(payload, dict):
            raise ContractError("invalid hook input")
        event = payload.get("hook_event_name", "SessionStart")
        if event not in {"SessionStart", "UserPromptSubmit"}:
            return 0
        session = payload.get("session_id") or session_from_environment()
        if not session:
            return 0  # never guess from another session's latest record
        checkout = Path(payload.get("cwd") or Path(__file__).resolve().parents[2]).resolve()
        # Session cwd may be a subdirectory; resolve against this hook's checkout.
        hook_checkout = Path(__file__).resolve().parents[2]
        if checkout.is_relative_to(hook_checkout):
            checkout = hook_checkout
        contract = TaskStore(default_database(checkout), checkout).load(session)
        if not contract or contract["status"] != "active":
            return 0
        context = render_context(contract)
    except (ContractError, OSError, TypeError, ValueError):
        event = (
            "UserPromptSubmit"
            if isinstance(locals().get("payload"), dict) and payload.get("hook_event_name") == "UserPromptSubmit"
            else "SessionStart"
        )
        context = "Active task record unavailable. Recover the objective and scope from the user before dependent work."
    print(json.dumps({"hookSpecificOutput": {"hookEventName": event, "additionalContext": context}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
