#!/usr/bin/env python
"""PostToolUse hook — once a session has edited three code files, ask for fresh eyes.

The tip "always spawn a review subagent" was rejected on cost (doc/AGENT_HARNESS.md,
"Deliberately not implemented"; feedback_subagent_fanout_economics). The failure it was
aimed at is real, though: a wide change claimed done with no fresh-context check. This hook
counts the distinct non-test code files this session has edited and, the first time the count
reaches the threshold, injects one reminder to run the `reviewer` agent with the brief and the
acceptance check (.claude/rules/work-queue.md rule 4). It fires once per session.

State is a small per-session JSON file in the temp dir. Advisory only; fails open.
"""

from __future__ import annotations

import contextlib
import json
import re
import sys
import tempfile
from pathlib import Path

THRESHOLD = 3
_CODE_EXT = {".py", ".ts", ".tsx", ".js", ".jsx", ".sql", ".rs"}
_TEST_NAME = re.compile(r"(^test_.*\.py$|_test\.py$|^conftest\.py$|\.test\.tsx?$|\.spec\.tsx?$)", re.I)
# "scratchpad": the session's throwaway scripts are not code under review.
_SKIP_PARTS = {"test", "tests", ".claude", "doc", "node_modules", "scratchpad"}
_STATE_DIR = Path(tempfile.gettempdir())


def is_code(path: Path) -> bool:
    if path.suffix.lower() not in _CODE_EXT:
        return False
    if _TEST_NAME.search(path.name):
        return False
    return not any(p.lower() in _SKIP_PARTS for p in path.parts)


def _state_path(session: str) -> Path:
    return _STATE_DIR / f"dail_verifier_nudge_{session[:12]}.json"


def record(session: str, path: Path) -> tuple[list[str], bool]:
    """Add `path`; return (all code files so far, fired-before flag)."""
    sp = _state_path(session)
    state = {"files": [], "fired": False}
    try:
        if sp.exists():
            loaded = json.loads(sp.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                state.update(loaded)
    except Exception:
        pass
    key = str(path).replace("\\", "/")
    if key not in state["files"]:
        state["files"].append(key)
    return state["files"], bool(state["fired"])


def _save(session: str, files: list[str], fired: bool) -> None:
    with contextlib.suppress(Exception):
        _state_path(session).write_text(json.dumps({"files": files, "fired": fired}), encoding="utf-8")


def message(files: list[str]) -> str:
    shown = ", ".join(files[-THRESHOLD:])
    return (
        f"[reviewer] This session has now edited {len(files)} non-test code files ({shown}). Per "
        ".claude/rules/work-queue.md rule 4, before claiming done run the fresh-context `reviewer` agent with "
        "the brief, the changed-file list and the acceptance check; it reports MATCHES BRIEF / GAPS FOUND / "
        "CHECK FAILED and never edits; issue a content-bound receipt after PASS. One well-briefed spawn, not several."
    )


def _tool_succeeded(payload: dict) -> bool:
    for key in ("is_error", "tool_error", "error"):
        if payload.get(key):
            return False
    for key in ("tool_response", "tool_result"):
        result = payload.get(key)
        if isinstance(result, dict) and (result.get("is_error") or result.get("error")):
            return False
    return True


def _apply_patch_paths(command: str, cwd: Path) -> list[Path]:
    paths: list[Path] = []
    for line in command.splitlines():
        match = re.match(r"^\*\*\*\s+(?:(?:Update|Add)\s+File:|Move to:)\s*(.+?)\s*$", line)
        if match:
            path = Path(match.group(1).strip())
            paths.append(path if path.is_absolute() else cwd / path)
    return paths


def _edited_paths(payload: dict) -> list[Path]:
    if not _tool_succeeded(payload):
        return []
    tool_name = str(payload.get("tool_name") or "")
    tool_input = payload.get("tool_input") or {}
    if not isinstance(tool_input, dict):
        return []
    if tool_name == "apply_patch":
        command = tool_input.get("command")
        return _apply_patch_paths(str(command), Path(str(payload.get("cwd") or Path.cwd()))) if command else []
    if tool_name in {"Edit", "MultiEdit", "Write", "NotebookEdit"} and tool_input.get("file_path"):
        return [Path(str(tool_input["file_path"]))]
    return []


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        paths = [path for path in _edited_paths(payload) if is_code(path)]
        if not paths:
            return 0
        session = str(payload.get("session_id") or "nosession")
        files, fired = record(session, paths[0])
        for path in paths[1:]:
            key = str(path).replace("\\", "/")
            if key not in files:
                files.append(key)
        if fired or len(files) < THRESHOLD:
            _save(session, files, fired)
            return 0
        _save(session, files, True)
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": message(files)}}))
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
