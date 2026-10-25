#!/usr/bin/env python
"""SessionStart hook — after a compaction, inject the resume file precompact_resume.py wrote.

Fires only when the SessionStart `source` is `compact` (or `resume`, a session picked up after
an interruption). Reads logs/resume/<session>.md, caps it, and returns it as additionalContext
with one instruction: re-read the listed files before continuing and re-run git status.

Fails open to silence: a missing or stale resume file means nothing was captured, and the
existing CLAUDE.md compaction rule still applies.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

_REPO = Path(__file__).resolve().parents[2]
_RESUME_DIR = _REPO / "logs" / "resume"
_MAX_CHARS = 4000
_MAX_AGE_S = 7 * 86400
_SOURCES = {"compact", "resume"}


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        if not isinstance(payload, dict) or payload.get("source") not in _SOURCES:
            return 0
        session = str(payload.get("session_id") or "")
        if not session:
            return 0
        path = _RESUME_DIR / f"{session[:12]}.md"
        if not path.is_file() or time.time() - path.stat().st_mtime > _MAX_AGE_S:
            return 0
        body = path.read_text(encoding="utf-8", errors="replace")
        if len(body) > _MAX_CHARS:
            body = body[:_MAX_CHARS] + "\n... (truncated; full file at logs/resume/)"
        context = (
            "[resume] State captured before this session compacted. Re-read the edited files "
            "before continuing, treat the git status as a snapshot, and re-run the last check "
            "command before claiming anything passed.\n\n" + body
        )
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "SessionStart", "additionalContext": context}}))
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
