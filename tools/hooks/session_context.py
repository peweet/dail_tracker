#!/usr/bin/env python
"""SessionStart hook — inject a tiny, fresh project status line.

Gives every new session a few load-bearing facts without the agent spending tool
calls to derive them: current git branch, whether doc/INDEX.md is stale, and the
most recent data-refresh heartbeat. Kept to a couple of lines of context on purpose.

Emits Claude-Code's structured additionalContext (nested + flat for VS Code
compatibility). Always exits 0 and never raises — a status line must not be able
to break a session.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def _git_branch() -> str:
    try:
        out = subprocess.run(
            ["git", "rev-parse", "--abbrev-ref", "HEAD"],
            cwd=str(REPO), capture_output=True, text=True, timeout=5,
        )
        return out.stdout.strip() or "?"
    except Exception:
        return "?"


def _doc_index_note() -> str:
    """Cheap staleness proxy: is any doc/*.md newer than the generated INDEX.md?"""
    try:
        docdir = REPO / "doc"
        index = docdir / "INDEX.md"
        if not index.exists():
            return "doc/INDEX.md missing (run tools/build_doc_index.py)"
        idx_m = index.stat().st_mtime
        newer = [p.name for p in docdir.glob("*.md") if p.name != "INDEX.md" and p.stat().st_mtime > idx_m]
        if newer:
            return f"doc/INDEX.md STALE ({len(newer)} doc(s) changed since; run tools/build_doc_index.py)"
        return "doc/INDEX.md fresh"
    except Exception:
        return ""


def _heartbeat_note() -> str:
    try:
        hb = REPO / "data" / "_meta" / "heartbeats"
        if not hb.is_dir():
            return ""
        files = sorted(hb.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not files:
            return ""
        latest = files[0]
        stamp = ""
        with contextlib_suppress():
            data = json.loads(latest.read_text(encoding="utf-8"))
            stamp = str(data.get("last_success") or data.get("ended_utc") or data.get("timestamp") or "")
        return f"{len(files)} refresh heartbeat(s); latest: {latest.stem}{(' @ ' + stamp) if stamp else ''}"
    except Exception:
        return ""


class contextlib_suppress:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return True  # swallow everything


def main() -> int:
    parts = [f"branch: {_git_branch()}"]
    for note in (_doc_index_note(), _heartbeat_note()):
        if note:
            parts.append(note)
    ctx = "Project status — " + " · ".join(parts) + (
        ". Data lives behind the dail-tracker MCP (describe_dataset / list_datasets / "
        "search_project) — don't scan parquet."
    )
    out = {
        "additionalContext": ctx,
        "hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": ctx},
    }
    sys.stdout.write(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
