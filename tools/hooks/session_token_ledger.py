#!/usr/bin/env python
"""SessionEnd hook — append this one session's token/turn totals to a ledger.

Closes the automation gap in tools/token_ledger.py: that tool measures per-session
spend but scans EVERY transcript, so it is far too heavy to run in a hook (the same
reason session_context.py refuses to call it). This hook is the cheap counterpart --
it reads only the single transcript that just ended, named in the SessionEnd payload,
and appends one compact JSON line to logs/session_token_ledger.jsonl. Field names and
the assistant-usage parsing match token_ledger.scan() so the two stay comparable.

The ledger is append-only telemetry, one row per session: output tokens (generation
cost), fresh input, cache-read input, assistant turns, exploration-tool calls, memory
writes, plus session id and end reason. Query it later with jq/polars -- it never
floods a context window because nothing reads it back into a session automatically.

Design rules, shared with the other hooks in this dir:
  * Reads only the one transcript in the payload -- never globs the project dir.
  * Fails open on every path: a telemetry line must never break session teardown.
  * Emits nothing to stdout/stderr (SessionEnd output is not shown); side effect only.
  * Always exits 0.

Input: the SessionEnd JSON on stdin (session_id, transcript_path, reason).
"""

from __future__ import annotations

import json
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LEDGER = REPO / "logs" / "session_token_ledger.jsonl"

# Kept in sync with tools/token_ledger.py so a hook-written row and a full-scan row
# count the same things.
EXPLORE_TOOLS = {"Read", "Grep", "Glob", "Bash", "PowerShell", "Explore", "Agent", "Task"}


def _text_of(block) -> str:
    return block.get("text", "") or "" if isinstance(block, dict) else ""


def _scan_one(path: Path) -> dict:
    """Aggregate a single transcript. Mirrors token_ledger.scan()'s per-file logic."""
    out = fresh_in = cache_read = turns = explore = mem_writes = mcp = 0
    first_prompt = ""
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            try:
                o = json.loads(line)
            except Exception:
                continue
            typ = o.get("type")
            msg = o.get("message") or {}
            if typ == "user" and not first_prompt:
                c = msg.get("content")
                if isinstance(c, str):
                    first_prompt = c[:140]
                elif isinstance(c, list):
                    for b in c:
                        t = _text_of(b)
                        if t and "<system-reminder>" not in t:
                            first_prompt = t[:140]
                            break
            elif typ == "assistant":
                turns += 1
                u = msg.get("usage") or {}
                out += u.get("output_tokens", 0) or 0
                fresh_in += (u.get("input_tokens", 0) or 0) + (u.get("cache_creation_input_tokens", 0) or 0)
                cache_read += u.get("cache_read_input_tokens", 0) or 0
                content = msg.get("content")
                if isinstance(content, list):
                    for b in content:
                        if isinstance(b, dict) and b.get("type") == "tool_use":
                            name = b.get("name", "")
                            if name in EXPLORE_TOOLS:
                                explore += 1
                            if name.startswith("mcp__dail-tracker__"):
                                mcp += 1
                            if name in ("Write", "Edit"):
                                p = (b.get("input") or {}).get("file_path", "")
                                if "/memory/" in p.replace("\\", "/") or "MEMORY.md" in p:
                                    mem_writes += 1
    return {
        "out": out,
        "fresh_in": fresh_in,
        "cache_read": cache_read,
        "turns": turns,
        "explore": explore,
        "mcp": mcp,
        "mem_writes": mem_writes,
        "prompt": first_prompt,
    }


def _transcript_path(payload: dict) -> str:
    for k in ("transcript_path", "transcriptPath", "transcript"):
        v = payload.get(k)
        if isinstance(v, str) and v:
            return v
    return ""


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    try:
        tp = _transcript_path(payload)
        if not tp:
            return 0
        path = Path(tp)
        if not path.is_file():
            return 0
        row = _scan_one(path)
        # Skip empty/trivial sessions -- no assistant turn means nothing to record.
        if row["turns"] == 0:
            return 0
        row["session"] = str(payload.get("session_id") or payload.get("sessionId") or path.stem)[:12]
        row["reason"] = str(payload.get("reason") or "")
        row["ts"] = datetime.now().isoformat(timespec="seconds")
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        with LEDGER.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    except Exception:
        return 0  # telemetry must never break teardown
    return 0


if __name__ == "__main__":
    sys.exit(main())
