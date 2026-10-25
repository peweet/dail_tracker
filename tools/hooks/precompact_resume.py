#!/usr/bin/env python
"""PreCompact hook — write a resume file so the post-compaction session can carry on.

CLAUDE.md asks that a compaction summary preserve, verbatim, the modified-file list, the
commands still to run and the user's requirements. That is a request to a summariser; it
fails silently when the summary drops a path. This hook records the same facts
deterministically, from the transcript and git, into logs/resume/<session>.md, and
resume_context.py injects that file on the SessionStart that follows a compaction.

It is the alternative to "kill any session that compacts" (AgentWorkflow tips, 2026-10-25):
the user's 2026-08-21 steer is that long sessions on one thread are valuable, so the fix is
to make compaction survivable, not to end the session.

Bounded: reads the transcript's last few MB only, caps every list, times out git. Never
blocks compaction — exit 0 on every path, silent on failure.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from datetime import datetime
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_RESUME_DIR = _REPO / "logs" / "resume"
_TAIL_BYTES = 6 * 1024 * 1024
_MAX_FILES = 60
_MAX_CMDS = 6
_EDIT_TOOLS = {"Edit", "Write", "MultiEdit", "NotebookEdit"}
_CHECK_CMD = re.compile(
    r"pytest|dev\.py (?:check|verify|preflight|test|conventions|lint)|check_\w+\.py|tools/wiki\.py", re.I
)


def _tail_lines(path: Path) -> list[str]:
    size = path.stat().st_size
    with path.open("rb") as fh:
        if size > _TAIL_BYTES:
            fh.seek(size - _TAIL_BYTES)
            fh.readline()  # drop the partial first line
        data = fh.read()
    return data.decode("utf-8", errors="replace").splitlines()


def _scan_transcript(path: Path) -> tuple[list[str], list[str], str]:
    """Return (edited files, check commands, last user request) from the transcript tail."""
    edited: dict[str, None] = {}
    cmds: list[str] = []
    last_user = ""
    for line in _tail_lines(path):
        try:
            o = json.loads(line)
        except Exception:
            continue
        if not isinstance(o, dict):
            continue
        msg = o.get("message") or {}
        content = msg.get("content")
        typ = o.get("type")
        if typ == "user" and not o.get("isMeta"):
            text = ""
            if isinstance(content, str):
                text = content
            elif isinstance(content, list):
                text = " ".join(b.get("text", "") for b in content if isinstance(b, dict) and b.get("type") == "text")
            text = text.strip()
            if text and not text.startswith("<"):
                last_user = text[:600]
        if typ != "assistant" or not isinstance(content, list):
            continue
        for b in content:
            if not isinstance(b, dict) or b.get("type") != "tool_use":
                continue
            name = b.get("name")
            inp = b.get("input") or {}
            if name in _EDIT_TOOLS:
                fp = inp.get("file_path") or inp.get("notebook_path")
                if fp:
                    edited.pop(fp, None)
                    edited[fp] = None
            elif name in {"Bash", "PowerShell"}:
                cmd = str(inp.get("command") or "")
                if _CHECK_CMD.search(cmd):
                    cmds.append(cmd.strip().replace("\n", " ")[:300])
    return list(edited)[-_MAX_FILES:], cmds[-_MAX_CMDS:], last_user


def _git_status(root: Path, limit: int) -> list[str]:
    try:
        out = subprocess.run(
            ["git", "-C", str(root), "status", "--short"], capture_output=True, text=True, timeout=8, check=False
        )
        lines = [ln for ln in out.stdout.splitlines() if ln.strip()]
        if len(lines) > limit:
            lines = lines[:limit] + [f"... and {len(lines) - limit} more"]
        return lines
    except Exception:
        return []


def _active_task(session: str, cwd: Path) -> str:
    try:
        sys.path.insert(0, str(_REPO))
        from tools.task_contracts import TaskStore, default_database, render_context  # noqa: PLC0415

        contract = TaskStore(default_database(_REPO), _REPO).load(session)
        if contract and contract.get("status") == "active":
            return render_context(contract)[:2000]
    except Exception:
        pass
    return ""


def build_resume(payload: dict) -> str:
    session = str(payload.get("session_id") or "unknown")
    cwd = Path(payload.get("cwd") or _REPO)
    transcript = Path(str(payload.get("transcript_path") or ""))
    edited, cmds, last_user = ([], [], "")
    if transcript.is_file():
        edited, cmds, last_user = _scan_transcript(transcript)
    edited_lines = [f"- {p}" for p in edited] or ["(none recorded)"]
    cmd_lines = [f"- `{c}`" for c in cmds] or ["(none recorded)"]
    status_lines = _git_status(_REPO, 80) or ["(clean or unavailable)"]
    parts = [
        f"# Resume state for session {session[:12]}",
        f"Captured {datetime.now().isoformat(timespec='seconds')} before a `{payload.get('trigger', '?')}` compaction. "
        "Git status is a snapshot: other sessions share this tree, re-run it before relying on it.",
        "",
        "## Last user request (verbatim, truncated)",
        last_user or "(not found in the transcript tail)",
        "",
        "## Files this session edited (oldest first)",
        *edited_lines,
        "",
        "## Last check commands run",
        *cmd_lines,
        "",
        "## git status --short (repo root)",
        *status_lines,
    ]
    product = _REPO / "planning" / "product"
    if product.is_dir():
        parts += [
            "",
            "## git status --short (planning/product)",
            *(_git_status(product, 40) or ["(clean or unavailable)"]),
        ]
    task = _active_task(session, cwd)
    if task:
        parts += ["", "## Active task contract", task]
    return "\n".join(parts) + "\n"


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        if not isinstance(payload, dict):
            return 0
        session = str(payload.get("session_id") or "")
        if not session:
            return 0
        _RESUME_DIR.mkdir(parents=True, exist_ok=True)
        target = _RESUME_DIR / f"{session[:12]}.md"
        target.write_text(build_resume(payload), encoding="utf-8", newline="\n")
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
