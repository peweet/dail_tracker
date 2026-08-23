#!/usr/bin/env python
"""PreToolUse hook — stop a NEW dail-tracker MCP tool from inheriting blanket trust.

.claude/settings.json grants `mcp__dail-tracker__*` as a single wildcard, so every tool
that server exposes is pre-approved and never prompts. That is correct for the surface as
reviewed -- the server's own MCP instructions declare it read-only, and its 73 tools are
query endpoints. It is NOT correct for a tool the server gains later: a future
write-capable endpoint would be auto-approved on its first call, with no prompt and no
diff to notice, because the wildcard was written before it existed.

This hook pins the reviewed surface in mcp_reviewed_tools.json and asks once when a name
appears that is not on it. Adapted from lesson s14 of shareAI-lab/learn-claude-code, whose
MCP host keeps a per-(server, tool) policy table and defaults UNCONFIGURED tools to
"confirm" rather than inheriting the server's trust. The default-deny-on-unknown is the
part worth having; the per-tool allow/confirm table is not, because every reviewed tool
here has the same answer.

Scope is deliberately ONE server. Other MCP servers (siting-private, next-devtools, the
claude.ai connectors) carry no wildcard in settings.json, so they already prompt per call
and need nothing from this hook. Guarding them too would add friction where the permission
system is already doing the job.

Guardrail tier (see MEMORY.md feedback_guardrail_determinism_tiers): hard check -- the
consequence is an unreviewed tool mutating data under a grant written before it existed,
and "is this exact string in this list" is not a judgement call. Note it deliberately does
NOT guess from the tool's NAME whether it mutates: per
feedback_read_the_code_dont_infer_from_names, a name is not evidence, so the answer is
"a human looks once", not "the hook decides".

Fires never on today's surface: all 73 reviewed tools are listed, so the steady-state cost
is one file read per MCP call. Escape hatch: DAIL_SKIP_MCP_CATALOGUE=1.

Exit contract: 0 = allow, 2 = block with the reason on stderr. Fails OPEN on every error
path, including an unreadable or malformed catalogue -- a broken guard must not sever
access to the data layer.
"""

from __future__ import annotations

import json
import os
import sys

GUARDED_PREFIX = "mcp__dail-tracker__"
CATALOGUE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "mcp_reviewed_tools.json")


def _tool_name(payload: dict) -> str:
    for key in ("tool_name", "toolName", "tool"):
        value = payload.get(key)
        if isinstance(value, str):
            return value
    return ""


def reviewed_tools() -> set[str]:
    """Return the reviewed tool names, or an EMPTY set when the catalogue is unusable.

    An empty set is the fail-open signal: main() treats it as "cannot judge" and allows.
    Returning a partial set on a malformed file would silently block real tools, which is
    the failure mode feedback_empty_string_collapses_identity_guard warns about.
    """
    try:
        with open(CATALOGUE, encoding="utf-8") as fh:
            data = json.load(fh)
        tools = data.get("tools")
        if not isinstance(tools, list) or not tools:
            return set()
        names = {t for t in tools if isinstance(t, str) and t}
        return names if len(names) == len(tools) else set()
    except Exception:
        return set()


def main() -> int:
    if os.environ.get("DAIL_SKIP_MCP_CATALOGUE") == "1":
        return 0
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    if not isinstance(payload, dict):
        return 0

    try:
        tool = _tool_name(payload)
        if not tool.startswith(GUARDED_PREFIX):
            return 0  # other servers already prompt per call; nothing to add

        bare = tool[len(GUARDED_PREFIX) :]
        known = reviewed_tools()
        if not known or bare in known:
            return 0

        sys.stderr.write(
            f"Unreviewed MCP tool: `{tool}` is not in the reviewed dail-tracker surface.\n\n"
            ".claude/settings.json allows `mcp__dail-tracker__*` as one wildcard, so this tool "
            "was auto-approved without ever being looked at. That grant was written for a "
            "read-only query surface.\n\n"
            "Check what it actually does -- read its handler, do not infer from the name -- then:\n"
            f'  * read-only  -> add "{bare}" to tools/hooks/mcp_reviewed_tools.json and re-run.\n'
            "  * writes/mutates -> keep it out of the wildcard and grant it explicitly per call.\n"
            "One-off override: DAIL_SKIP_MCP_CATALOGUE=1."
        )
        return 2
    except Exception:
        return 0


if __name__ == "__main__":
    sys.exit(main())
