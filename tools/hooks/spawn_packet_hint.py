#!/usr/bin/env python
"""PreToolUse hook: report which task-packet sections a Claude `Agent` prompt lacks.

Claude's Agent input is prompt/description/subagent_type/model. The Codex guard
(guard_subagent_spawn.py) validates role names and `fork_turns`, so it rejects every
Claude spawn, packet or not. Only the five-section packet carries over, and this hook
checks just that. Default mode adds context and never blocks; set
DAIL_SPAWN_PACKET_MODE=block (for example in .claude/settings.json "env") to deny with
the same exit code the Codex guard uses.
"""

from __future__ import annotations

import json
import os
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.hooks.guard_subagent_spawn import REQUIRED_SECTIONS  # noqa: E402

# A section counts when a line opens with its name (after list, heading or emphasis marks)
# and the name is followed by a colon, a dash, or the end of the line.
_LEAD = r"^[\s>#*_\-\d.)]*"
_TAIL = r"[*_\s]*(?::|[-–—]|$)"


def missing_sections(prompt: str) -> list[str]:
    return [
        section for section in REQUIRED_SECTIONS if not re.search(f"(?im){_LEAD}{re.escape(section)}{_TAIL}", prompt)
    ]


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except (json.JSONDecodeError, TypeError):
        return 0
    if not isinstance(payload, dict) or str(payload.get("tool_name", "")).lower() != "agent":
        return 0
    tool_input = payload.get("tool_input")
    prompt = tool_input.get("prompt") if isinstance(tool_input, dict) else None
    if not isinstance(prompt, str):
        return 0
    missing = missing_sections(prompt)
    if not missing:
        return 0
    detail = f"message is missing task-packet sections: {', '.join(missing)}"
    if os.environ.get("DAIL_SPAWN_PACKET_MODE", "").strip().lower() == "block":
        sys.stderr.write(f"Blocked subagent spawn:\n- {detail}\n")
        return 2
    role = tool_input.get("subagent_type") or "general-purpose"
    note = (
        f"The Agent prompt for subagent_type '{role}' has no line for: {', '.join(missing)}. "
        "This repository's task packet (doc/AGENT_HARNESS.md, 'Reusable task packet') is "
        f"{', '.join(REQUIRED_SECTIONS)}, each opening a line and ending in a colon."
    )
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": note}}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
