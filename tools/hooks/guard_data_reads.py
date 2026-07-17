#!/usr/bin/env python
"""PreToolUse hook — block reads of large tracked data files.

Turns the CLAUDE.md "never Read data files" rule (and the deny-Read entries in
.claude/settings.json) into deterministic enforcement for EVERY agent and
subagent, in both Claude Code and VS Code Copilot. Reading a 35 MB parquet floods
the context window; describe_dataset / list_datasets return the same shape cheaply.

Cross-tool notes (see doc/AGENT_CUSTOMIZATION_PLAN.md):
  * VS Code ignores Claude-Code matcher syntax and fires every hook under an
    event — so this script self-filters on the file path, not on a matcher.
  * Tool-input keys differ across tools (snake_case vs camelCase) — we try all.
  * A deny is signalled by writing the reason to stderr and exiting 2, which
    blocks in both tools. Exit 0 = allow.

Input: the tool-call JSON on stdin. We only care about the target file path.
"""
from __future__ import annotations

import json
import sys

# Path fragments that identify a heavy/committed data file. Kept in sync with the
# deny-Read block in .claude/settings.json. Matched against a forward-slashed path.
BLOCKED_SUBSTR = (
    "data/bronze/",
    "data/silver/",
    "data/gold/",
    "doc/source_pdfs/",
)
BLOCKED_SUFFIX = (".parquet", ".duckdb", ".duckdb.wal")
# raw archives under otherwise-browsable trees
BLOCKED_GLOBISH = ("planning_rules/", "/raw/")  # both must appear (raw archive)
SANDBOX_HEAVY = ("pipeline_sandbox/",)  # with /samples/ or /corpus/


def _extract_path(payload: dict) -> str:
    ti = payload.get("tool_input") or payload.get("toolInput") or payload.get("input") or {}
    if not isinstance(ti, dict):
        return ""
    for k in ("file_path", "filePath", "path", "file", "notebook_path", "notebookPath"):
        v = ti.get(k)
        if isinstance(v, str) and v:
            return v
    return ""


def _is_blocked(path: str) -> bool:
    p = path.replace("\\", "/").lower()
    if any(p.endswith(sfx) for sfx in BLOCKED_SUFFIX):
        return True
    if any(sub in p for sub in BLOCKED_SUBSTR):
        return True
    if all(g in p for g in BLOCKED_GLOBISH):  # planning_rules + raw archive
        return True
    if any(s in p for s in SANDBOX_HEAVY) and ("/samples/" in p or "/corpus/" in p):
        return True
    return False


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0  # never break the agent on a parse hiccup
    path = _extract_path(payload)
    if not path or not _is_blocked(path):
        return 0
    sys.stderr.write(
        "Blocked: don't read data files directly -- a large parquet/CSV floods the "
        f"context window ({path}). Use the dail-tracker MCP instead: describe_dataset("
        "'<stem>') for columns/rows/grain, list_datasets(domain=...) to discover facts, or "
        "search_project('<topic>') to find the right dataset/view/doc. If you truly need a "
        "few rows, query with polars/duckdb and a LIMIT."
    )
    return 2  # exit 2 blocks the tool call in both Claude Code and VS Code


if __name__ == "__main__":
    sys.exit(main())
