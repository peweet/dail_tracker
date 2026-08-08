#!/usr/bin/env python
"""PreToolUse hook — fail fast on an MCP tool call when free RAM is already gone.

Written 2026-08-08 after three `dail-tracker` MCP calls (code_outline x2, py_deps)
each hung to the full 360 s client timeout in one session, with 11 claude.exe
processes open and ~770 MB of physical RAM free. `session_context._mcp_connect_probe`
already proves the server binary starts fine in isolation (~5.2 s cold) and
`guard_memory.py` already blocks known-heavy Bash/PowerShell commands below 1.5 GB
free — but that matcher is scoped to `Bash|PowerShell` and never sees MCP tool
calls, so a starved host silently turned every MCP tool into a 6-minute hang
instead of a diagnosable failure. This hook closes that gap; it does not add
memory the host doesn't have. See MEMORY.md project_mcp_resource_policy_2026_07_27
for why the server's own DuckDB/idle caps cannot fix a starved host, and
project_claude_session_accumulation_2026_07_31 for the >=4-session tripwire this
situation blew past (11 open).

Guardrail tier (see MEMORY.md feedback_guardrail_determinism_tiers): hard check —
the consequence is a silent multi-minute hang with no diagnosis, not a style wobble.

Escape hatch: set DAIL_SKIP_MCP_MEM_GUARD=1 when a call is worth the risk anyway.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from guard_memory import FLOOR_MB, record, sample_memory  # noqa: E402


def _tool_name(payload: dict) -> str:
    for key in ("tool_name", "toolName", "tool"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    return ""


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0  # never break the agent on a parse hiccup

    tool = _tool_name(payload)
    if not tool.startswith("mcp__"):
        return 0

    import os

    mem = sample_memory()
    free = mem.get("free_mb")
    blocked = free is not None and free < FLOOR_MB and os.environ.get("DAIL_SKIP_MCP_MEM_GUARD") != "1"
    record(
        {
            "kind": f"mcp tool call ({tool})",
            "blocked": blocked,
            **mem,
            "ts": __import__("datetime").datetime.now(__import__("datetime").UTC).isoformat(timespec="seconds"),
        }
    )
    if not blocked:
        return 0

    sys.stderr.write(
        f"Blocked: {free} MB of physical RAM free (floor {FLOOR_MB} MB) before calling "
        f"{tool}. Below this floor the MCP server subprocess can take minutes to schedule "
        "instead of its normal ~5s cold start, and the call hangs to the 360s client "
        "timeout with no diagnostic. Close surplus Claude/VS Code session tabs first "
        "(each holds ~1.2 GB: client + its own MCP server) or fall back to Read/Grep for "
        "this lookup. Override with DAIL_SKIP_MCP_MEM_GUARD=1 if you mean it.\n"
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
