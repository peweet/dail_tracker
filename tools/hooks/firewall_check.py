#!/usr/bin/env python
"""PostToolUse hook — warn (don't block) on a logic-firewall violation.

After an Edit/Write to a Streamlit page (utility/pages_code/), run the existing
firewall checker on just that file. Pages must contain NO business logic — queries
and transforms belong in utility/data_access/. A violation is surfaced as a
non-blocking warning (systemMessage): the same checker also runs in CI/review, so
this is an early nudge, not a gate.

Cross-tool: self-filters on the edited path (VS Code ignores matchers); reads the
path under several key spellings; always exits 0.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
CHECKER = REPO / "tools" / "check_streamlit_logic_firewall.py"
GUARDED = "utility/pages_code/"


def _extract_path(payload: dict) -> str:
    ti = payload.get("tool_input") or payload.get("toolInput") or payload.get("input") or {}
    if not isinstance(ti, dict):
        return ""
    for k in ("file_path", "filePath", "path", "file"):
        v = ti.get(k)
        if isinstance(v, str) and v:
            return v
    return ""


def _tool_name(payload: dict) -> str:
    return str(payload.get("tool_name") or payload.get("toolName") or "")


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    name = _tool_name(payload).lower()
    if name and name not in ("edit", "write", "multiedit", "notebookedit", "applypatch", "createfile"):
        # Unknown edit-like tools still pass the path check below, so only skip
        # clearly non-edit tools when a name is present.
        pass
    path = _extract_path(payload)
    if not path:
        return 0
    norm = path.replace("\\", "/")
    if GUARDED not in norm or not norm.endswith(".py"):
        return 0
    if not CHECKER.exists():
        return 0
    try:
        res = subprocess.run(
            [sys.executable, str(CHECKER), path],
            cwd=str(REPO), capture_output=True, text=True, timeout=25,
        )
    except Exception:
        return 0
    if res.returncode == 0:
        return 0
    detail = (res.stdout or res.stderr or "").strip()
    msg = (
        f"Logic-firewall violation in {norm}: pages must hold no business logic — move "
        f"queries/transforms into utility/data_access/. Checker output:\n{detail[:1200]}"
    )
    out = {
        "systemMessage": msg,
        "hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": msg},
    }
    sys.stdout.write(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
