#!/usr/bin/env python
"""PreToolUse hook — when a Read targets a DATED doc under planning/product/doc, say
where its state of record is and how far the engine has moved since.

Grounded in 2026-10-21: the first 60 lines of OUTCOME_PANEL_2026_09_28.md (the pre-work
"refinements, ranked" plan) were quoted as the current state, while the 600 lines below
recorded the fixes, an out-of-sample test and a 4,848-appeal wide scan that superseded
every figure — and 67 engine/catalogue commits had landed after the doc's last section.
The habit fix is "read the headings to the END first"; this hook does that mechanically
at the one moment it matters, before the Read lands.

Advisory only (additionalContext, never a block), once per session per file. Cost: one
small file read plus one bounded `git log`; fails open to silence on any error.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import sys
import tempfile

_REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
_PRODUCT = os.path.join(_REPO, "planning", "product")
_DATED_DOC = re.compile(r"planning/product/doc/[^/]*?(20\d\d)[-_](\d\d)[-_](\d\d)[^/]*\.md$", re.I)
_HEADING = re.compile(r"^(#{1,3})\s+(.*)$")
_DATE_IN_TEXT = re.compile(
    r"(20\d\d-\d\d-\d\d|\b\d{1,2} (?:January|February|March|April|May|June|July|August|September|October|November|December) 20\d\d)"
)
_STATUS_BLOCK = re.compile(r"^\*\*Status, .*?20\d\d", re.M)
_ENGINE_PATHS = ("core/evaluation", "rules/issue_catalogue.yaml")
_MAX_BYTES = 2 * 1024 * 1024


def _tool_input(payload: dict) -> dict:
    ti = payload.get("tool_input") or payload.get("toolInput") or payload.get("input") or {}
    return ti if isinstance(ti, dict) else {}


def _already(session: str, key: str) -> bool:
    try:
        marker = os.path.join(tempfile.gettempdir(), f"dail_dated_doc_{session[:12]}_{key}")
        if os.path.exists(marker):
            return True
        with open(marker, "w") as fh:
            fh.write("1")
        return False
    except Exception:
        return True


def _engine_commits_since(iso_date: str) -> int | None:
    try:
        out = subprocess.run(
            ["git", "-C", _PRODUCT, "log", "--format=%h", f"--since={iso_date}", "--", *_ENGINE_PATHS],
            capture_output=True,
            text=True,
            timeout=8,
            check=False,
        )
        if out.returncode != 0:
            return None
        return sum(1 for line in out.stdout.splitlines() if line.strip())
    except Exception:
        return None


def _summarise(path: str, doc_date: str) -> str | None:
    if os.path.getsize(path) > _MAX_BYTES:
        return None
    with open(path, encoding="utf-8", errors="replace") as fh:
        text = fh.read()
    headings = [
        (len(m.group(1)), m.group(2).strip()) for m in (_HEADING.match(line) for line in text.splitlines()) if m
    ]
    h2 = [title for level, title in headings if level == 2]
    dated = [title for title in h2 if _DATE_IN_TEXT.search(title)]
    has_status = bool(_STATUS_BLOCK.search(text[:4000]))
    commits = _engine_commits_since(doc_date)
    bits = [f"[dated-doc] {os.path.basename(path)}: {len(h2)} H2 sections"]
    if has_status:
        bits.append("a **Status** block at the top supersedes everything above the section it names — read it first")
    if dated:
        bits.append(f'last dated section: "{dated[-1][:70]}"')
    if h2:
        tail = " | ".join(t[:40] for t in h2[-3:])
        bits.append(f"final sections: {tail}")
    if commits is not None:
        bits.append(
            f"{commits} engine/catalogue commit(s) since {doc_date} — figures here are "
            + ("current only if a later measurement says so" if commits else "measured on the current engine")
        )
    bits.append("read the headings to the END before quoting a figure from the top")
    return "; ".join(bits)


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    tool = payload.get("tool_name") or payload.get("toolName") or ""
    if tool != "Read":
        return 0
    raw = str(_tool_input(payload).get("file_path") or "").replace("\\", "/")
    m = _DATED_DOC.search(raw)
    if not m or not os.path.isfile(raw):
        return 0
    session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")
    key = re.sub(r"[^A-Za-z0-9]", "_", os.path.basename(raw))[:40]
    if _already(session, key):
        return 0
    try:
        message = _summarise(raw, f"{m.group(1)}-{m.group(2)}-{m.group(3)}")
    except Exception:
        return 0
    if not message:
        return 0
    print(json.dumps({"hookSpecificOutput": {"hookEventName": "PreToolUse", "additionalContext": message}}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
