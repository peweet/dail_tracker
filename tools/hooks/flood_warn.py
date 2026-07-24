#!/usr/bin/env python
"""PostToolUse hook — one-line nudge when a tool result actually flooded context.

Output-aware, not pre-emptive: fires only AFTER a Bash/PowerShell/Grep result
exceeded FLOOD_CHARS, so quiet calls never see a nag. This follows the ACON
finding (arXiv 2510.00615: compressing observations cut peak tokens 26-54%
while IMPROVING task success) and the transcript audit of 2026-07-23: Bash
results alone carried ~1.35M tokens across 120 sessions, mostly unaggregated
stdout. The nudge names the cheaper pattern for the tool that flooded.

Deliberately soft (advisory additionalContext, never a block): per the
guardrail-determinism tiers, blocking on style teaches evasion; the hard blocks
stay in guard_data_reads.py where the consequence is a genuine context bomb.

Rate-limited to at most one nudge per session per tool via an mtime-bounded
marker in the system temp dir — repeat warnings add the exact noise this hook
exists to remove.

Cross-tool notes (same as guard_data_reads.py): self-filters on tool_name from
the payload, tries snake_case and camelCase keys, fails open everywhere.
"""
from __future__ import annotations

import json
import os
import sys
import tempfile

FLOOD_CHARS = 8_000  # ~2k tokens: p95-ish for Bash results (audit avg was 1.1k chars)

NUDGE = {
    "Bash": (
        "That command printed ~{tok:,} tokens of stdout. Cheaper: have scripts print "
        "aggregates/counts only, pipe through tail/head, or write bulk output to a "
        "scratchpad file and Read the slice you need."
    ),
    "PowerShell": (
        "That command printed ~{tok:,} tokens of output. Cheaper: Select-Object -First N, "
        "Measure-Object for counts, or write to a file and Read a slice."
    ),
    "Grep": (
        "That search returned ~{tok:,} tokens. Cheaper: output_mode=files_with_matches "
        "first, add glob:/type: scoping and head_limit, then Read only the hit."
    ),
}


def _payload_str(v) -> int:
    """Total chars of a tool_response in any of its harness shapes."""
    if isinstance(v, str):
        return len(v)
    if isinstance(v, list):
        return sum(_payload_str(x) for x in v)
    if isinstance(v, dict):
        # common shapes: {"text": ...} blocks, {"stdout": ..., "stderr": ...}
        return sum(_payload_str(v.get(k)) for k in ("text", "stdout", "stderr", "content", "output") if v.get(k))
    return 0


def _already_nudged(session: str, tool: str) -> bool:
    """One nudge per session per tool. Marker file in temp dir; never in the repo."""
    try:
        marker = os.path.join(tempfile.gettempdir(), f"dail_flood_warn_{session[:12]}_{tool}")
        if os.path.exists(marker):
            return True
        with open(marker, "w") as fh:
            fh.write("1")
        return False
    except Exception:
        return True  # can't track -> stay silent rather than risk nagging every call


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    tool = payload.get("tool_name") or payload.get("toolName") or ""
    if tool not in NUDGE:
        return 0
    resp = payload.get("tool_response") or payload.get("toolResponse") or {}
    n = _payload_str(resp)
    if n < FLOOD_CHARS:
        return 0
    session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")
    if _already_nudged(session, tool):
        return 0
    msg = NUDGE[tool].format(tok=n // 4)
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "PostToolUse",
            "additionalContext": f"[context-efficiency] {msg}",
        }
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
