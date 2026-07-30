#!/usr/bin/env python
"""PostToolUse hook — mark harness-customization edits UNVALIDATED until re-evaled.

The eval stack (tools/evals/promptfooconfig.yaml + the SDK probes) exists but ran
only when someone remembered — an edited skill/hook/rule could steer sessions for
weeks without its regression gate ever re-running. This hook closes that gap the
cheap way: whenever Edit/Write touches a reusable customization (CLAUDE.md, the
.claude/ tree, tools/hooks/, tools/evals/), the file is recorded UNVALIDATED in
tools/evals/validation_ledger.json. Nothing runs here — evals spend real API money
and the probes are blocked when launched from inside an agent session (see
promptfooconfig.yaml header), so validation happens later from a plain terminal:

    python tools/evals/validate_customizations.py            # list the backlog
    python tools/evals/validate_customizations.py --run      # run the gate ($)
    python tools/evals/validate_customizations.py --mark-validated <path>

The backlog is surfaced once per session by session_context.py. Design rules,
shared with the sibling hooks: side effect only, no stdout, fails open on every
path, always exits 0.
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LEDGER = REPO / "tools" / "evals" / "validation_ledger.json"

WATCHED_PREFIXES = (".claude/", "tools/hooks/", "tools/evals/")
WATCHED_EXACT = ("CLAUDE.md",)
IGNORED = ("tools/evals/validation_ledger.json",)


def _watched(path_str: str) -> str | None:
    """Return the repo-relative path if it is a tracked customization, else None."""
    try:
        p = Path(path_str)
        rel = (p if p.is_absolute() else REPO / p).resolve().relative_to(REPO)
    except Exception:
        return None
    rel_posix = rel.as_posix()
    if rel_posix in IGNORED:
        return None
    if rel_posix in WATCHED_EXACT or rel_posix.startswith(WATCHED_PREFIXES):
        return rel_posix
    return None


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        tin = payload.get("tool_input") or payload.get("toolInput") or {}
        rel = _watched(str(tin.get("file_path") or tin.get("filePath") or ""))
        if not rel:
            return 0
        ledger = {}
        if LEDGER.exists():
            try:
                ledger = json.loads(LEDGER.read_text(encoding="utf-8"))
            except Exception:
                ledger = {}
        entry = ledger.get(rel) or {}
        ledger[rel] = {
            "status": "UNVALIDATED",
            "edited": datetime.now().isoformat(timespec="seconds"),
            "edits": int(entry.get("edits", 0)) + 1,
        }
        LEDGER.write_text(json.dumps(ledger, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    except Exception:
        return 0  # marking must never break a session
    return 0


if __name__ == "__main__":
    sys.exit(main())
