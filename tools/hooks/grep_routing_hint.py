#!/usr/bin/env python
"""PostToolUse hook — nudge toward a dail-tracker index tool when a Grep call's

shape matches one of CLAUDE.md's "First move — route the question" rows almost
exactly (symbol call-sites, SQL view/column lineage, Python import blast radius).
Grounded in a 2026-07-31 transcript audit: these tools were invoked only 51 times
across 21 of 142 sessions against 8,452 Grep/Read/Glob/etc. calls in the same
window (see memory/feedback_grep_default_over_index_tools.md) — the routing table
existed but nothing surfaced it at the moment a Grep call was already being made.

Deliberately soft (advisory additionalContext, never a block, never re-routes the
call already in flight): per the guardrail-determinism tiers, "does this query need
the index tool" is a semantic judgment, and each of these tools has real blind spots
(py_refs misses dynamic dispatch, e.g.) where a plain Grep is still correct. Same
one-nudge-per-session-per-category rate limit as flood_warn.py/discovery_hint.py —
repeat nudges are the exact noise this hook exists to avoid.

Confidence-gated per category (see _classify): each rule requires the SAME combination
of signals the CLAUDE.md row itself names (repo-wide scope + symbol-shaped pattern,
or a path inside sql_views/, etc.), not a single weak keyword — the discovery_hint.py
postmortem (2/2 irrelevant hints on one weak signal) is why.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from datetime import UTC, datetime

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_TRIAL_LOG = os.path.join(_REPO, "logs", "grep_vs_index_trial.jsonl")

_BARE_IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SYMBOL_SHAPED = re.compile(r"_|[a-z][A-Z]")  # snake_case or camelCase/PascalCase interior
_IMPORT_LIKE = re.compile(r"^\s*(from\s+[\w.]+\s+import\b|import\s+[\w.]+)")
_REGEX_META = re.compile(r"[\\()\[\]|^$*+?{}]")
_FILE_SCOPED = re.compile(r"\.(py|md|json|ya?ml|toml|sql|txt|jsonl)$", re.I)
# planning/product and apps/public-signal are nested private repos with their own
# MCP index; the public py_refs/search_project never see them.
_PRIVATE_SCOPE = re.compile(r"planning/product|apps/public-signal")
_PRIVATE_TOOL = {
    "py_refs": "siting_py_refs(symbol)",
    "py_deps": "siting_py_deps(path)",
    "search_project": "search_siting_project(query)",
}

NUDGE = {
    "py_refs": (
        "That Grep pattern is a single Python symbol swept across a directory. "
        "py_refs(path, name) answers 'who calls this' at the line, alias- and "
        "re-export-aware — a plain Grep can't tell a def from a call site."
    ),
    "sql_lineage": (
        "That Grep targeted sql_views/. view_deps(view) / column_deps(view, col) "
        "trace the real DuckDB-parsed dependency graph — a text match can miss "
        "aliased columns and SELECT * fan-out that break silently."
    ),
    "py_deps": (
        "That Grep pattern looks like an import statement. py_deps(path) returns "
        "every repo module that imports this one (the blast radius) in one call, "
        "instead of grepping import lines by hand."
    ),
    "search_project": (
        "That Grep looks like a 'where does this topic live' search. "
        "search_project(query) ranks matches across datasets/docs/views/code in "
        "one call, returning line spans instead of a text dump."
    ),
}


def _tool_input(payload: dict) -> dict:
    ti = payload.get("tool_input") or payload.get("toolInput") or payload.get("input") or {}
    return ti if isinstance(ti, dict) else {}


def _classify(ti: dict) -> str | None:
    pattern = str(ti.get("pattern") or "")
    path = str(ti.get("path") or "").replace("\\", "/")
    glob = str(ti.get("glob") or "")
    file_type = str(ti.get("type") or "")
    output_mode = str(ti.get("output_mode") or "")
    if not pattern:
        return None

    if "sql_views" in path.lower() or "sql_views" in glob.lower():
        return "sql_lineage"

    py_scoped = file_type in ("", "py") and (not glob or ".py" in glob)
    # A grep at one named file is Grep's job; a directory or repo-wide sweep is the
    # index's. Requiring repo-wide missed 867 of 874 real greps on 2026-08-21.
    dir_scoped = not _FILE_SCOPED.search(path)

    if _IMPORT_LIKE.search(pattern) and py_scoped:
        return "py_deps"

    if (
        dir_scoped
        and py_scoped
        and _BARE_IDENT.fullmatch(pattern)
        and len(pattern) >= 4
        and _SYMBOL_SHAPED.search(pattern)
        and output_mode in ("", "content", "files_with_matches")
    ):
        return "py_refs"

    if (
        dir_scoped
        and not glob
        and not file_type
        and output_mode in ("", "content", "files_with_matches")
        and " " in pattern.strip()
        and not _REGEX_META.search(pattern)
    ):
        return "search_project"

    return None


def _log_trial(session: str, category: str, nudged: bool, pattern: str) -> None:
    """Auto-log every routing-table-shaped Grep call — the manual self-logging the
    memory asked for (memory/feedback_grep_default_over_index_tools.md) had zero
    entries after being requested, so record the occurrence here instead of relying
    on the assistant to remember. Best-effort: never let logging break the hook.
    """
    try:
        row = {
            "ts": datetime.now(UTC).isoformat(),
            "session": session[:12],
            "category": category,
            "nudged": nudged,
            "pattern": pattern[:80],
        }
        os.makedirs(os.path.dirname(_TRIAL_LOG), exist_ok=True)
        with open(_TRIAL_LOG, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(row) + "\n")
    except Exception:
        pass


def _already_nudged(session: str, category: str) -> bool:
    try:
        marker = os.path.join(tempfile.gettempdir(), f"dail_grep_route_{session[:12]}_{category}")
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
    if tool != "Grep":
        return 0
    try:
        category = _classify(_tool_input(payload))
    except Exception:
        return 0
    if not category:
        return 0
    session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")
    already = _already_nudged(session, category)
    _log_trial(session, category, nudged=not already, pattern=str(_tool_input(payload).get("pattern") or ""))
    if already:
        return 0
    message = NUDGE[category]
    scope = str(_tool_input(payload).get("path") or "").replace("\\", "/")
    if _PRIVATE_SCOPE.search(scope) and category in _PRIVATE_TOOL:
        message = f"{message} That path is a nested private repo — use {_PRIVATE_TOOL[category]}."
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "PostToolUse",
                    "additionalContext": f"[tool-routing] {message}",
                }
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
