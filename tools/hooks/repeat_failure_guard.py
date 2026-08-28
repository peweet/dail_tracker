#!/usr/bin/env python
"""PostToolUse hook — speak up when the SAME failure repeats inside one session.

Closes the largest measured waste bucket in the 2026-08-28 transcript audit
(21 seeded-random sessions, 523 refutation events, 118 verified recurrence
patterns / 538 occurrences -- memory/feedback_recurrence_gaps_are_attention_not_retrieval.md):
66.5% of recurring failures had the answer ALREADY IN THE CONTEXT WINDOW, and the
single biggest slice -- 112 occurrences, 20.8% -- was the error text naming its own
cause while the next action varied only the surface. Measured instances: five
EUR-Lex fetches failing identically (URL path, then tool, then User-Agent varied;
only a HOST change worked), three consecutive 404s on one directory in ~10 seconds,
and `cd` into the nested private repo wedging every PreToolUse hook three separate
times after being diagnosed in full the first time.

No index, memory card or doc fixes those -- the information was on screen. What
they need is a nudge at the moment of the repeat, which is what this provides.

Deliberately quiet, by design:
  * fires on the SECOND identical failure, never the first -- one error is normal
    work, a same-signature retry is the defect. This is what keeps it off the
    "cries wolf" path recorded in feedback_new_checker_first_run_measures_the_checker.
  * advisory additionalContext, NEVER a block: per the guardrail-determinism tiers
    (feedback_guardrail_determinism_tiers), "is this retry informed?" is a semantic
    judgment, so it stays soft. The hard blocks live in guard_data_reads.py where
    the consequence is deterministic.
  * at most MAX_NUDGES_PER_SESSION nudges, and at most one per signature.
  * quotes the error's own first line back, because that line is the finding.

Design rules shared with the other hooks here: reads only the payload on stdin,
self-filters on tool_name, tries snake_case and camelCase keys, fails open on
every path, always exits 0.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile

WATCHED = {"Bash", "PowerShell", "Read", "Grep", "Glob", "Edit", "Write", "WebFetch"}
MAX_NUDGES_PER_SESSION = 4  # past this it is noise, not a reminder
SIG_CHARS = 240  # enough to separate distinct failures, short enough to survive rewording

# Anchored at a line start so a grep result that merely CONTAINS the word "error"
# does not register as a failure. Ordered roughly by how often the audit saw them.
_ERROR_MARKERS = (
    r"^\s*error\b",
    r"^\s*Error:",
    r"^\s*Traceback ",
    r"^\s*BLOCKED\b",
    r"^\s*Blocked:",
    r"^\s*fatal:",
    r"^\s*usage:",
    r"No such file or directory",
    r"command not found",
    r"is not recognized as",
    r"Permission to use \w+ .* has been denied",
    r"hook error",
    r"No files found",
    r"No matches found",
    r"HTTP (?:4\d\d|5\d\d)\b",
    r"^\s*ERROR:",
    r"Exit code [1-9]",
    r"^\s*[A-Za-z_]*Error: ",  # NameError:, TypeError:, AttributeError: ...
)
_ERROR_RE = re.compile("|".join(_ERROR_MARKERS), re.MULTILINE)


def _text_of(v) -> str:
    """Flatten a tool_response in any of its harness shapes (mirrors flood_warn.py)."""
    if isinstance(v, str):
        return v
    if isinstance(v, list):
        return "\n".join(_text_of(x) for x in v)
    if isinstance(v, dict):
        parts = [_text_of(v.get(k)) for k in ("text", "stdout", "stderr", "content", "output", "error") if v.get(k)]
        return "\n".join(p for p in parts if p)
    return ""


def _is_error(resp, text: str) -> bool:
    if isinstance(resp, dict):
        for k in ("is_error", "isError", "error"):
            if resp.get(k):
                return True
    return bool(_ERROR_RE.search(text[:4000]))


def _signature(tool: str, text: str) -> str:
    """Normalise away the surface so a re-worded retry hashes to the same failure.

    Digits, hex blobs and paths are exactly what a surface-varied retry changes --
    stripping them is what makes 'same failure, new costume' detectable.
    """
    s = text[:4000].lower()
    s = re.sub(r"[a-z]:[\\/][^\s'\"]+", "<path>", s)  # windows paths
    s = re.sub(r"(?<![\w])/[\w./-]{4,}", "<path>", s)  # posix paths
    s = re.sub(r"\b[0-9a-f]{7,}\b", "<hex>", s)
    s = re.sub(r"\d+", "#", s)
    s = re.sub(r"\s+", " ", s).strip()
    return hashlib.sha1(f"{tool}|{s[:SIG_CHARS]}".encode()).hexdigest()[:16]


def _first_error_line(text: str) -> str:
    for line in text.splitlines():
        line = line.strip()
        if line and _ERROR_RE.search(line):
            return line[:200]
    for line in text.splitlines():
        if line.strip():
            return line.strip()[:200]
    return ""


def _state_path(session: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "", session or "nosession")[:64]
    return os.path.join(tempfile.gettempdir(), f"dail_repeat_failure_{safe}.json")


def _load(session: str) -> dict:
    try:
        with open(_state_path(session), encoding="utf-8") as fh:
            d = json.load(fh)
            return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save(session: str, state: dict) -> None:
    try:
        with open(_state_path(session), "w", encoding="utf-8") as fh:
            json.dump(state, fh)
    except Exception:
        pass  # can't persist -> at worst we miss a nudge, never a crash


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    try:
        tool = payload.get("tool_name") or payload.get("toolName") or ""
        if tool not in WATCHED:
            return 0
        resp = payload.get("tool_response") or payload.get("toolResponse") or {}
        text = _text_of(resp)
        if not text or not _is_error(resp, text):
            return 0

        session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")
        state = _load(session)
        counts = state.get("counts") or {}
        nudged = set(state.get("nudged") or [])

        sig = _signature(tool, text)
        n = int(counts.get(sig, 0)) + 1
        counts[sig] = n
        state["counts"] = counts

        # First failure of this signature: record, stay silent. Normal work.
        if n < 2 or sig in nudged or len(nudged) >= MAX_NUDGES_PER_SESSION:
            state["nudged"] = sorted(nudged)
            _save(session, state)
            return 0

        nudged.add(sig)
        state["nudged"] = sorted(nudged)
        _save(session, state)

        line = _first_error_line(text)
        msg = (
            f"[repeat-failure] This is failure #{n} with the same signature from {tool}. "
            f'Its own text says: "{line}" — read that line before the next attempt and name the '
            "cause out loud. A retry that varies only the surface (path, flag, tool, wording) is the "
            "single largest measured waste pattern in this repo's sessions "
            "(112 of 538 recurrence occurrences, 2026-08-28 audit). If the cause is genuinely "
            "unknown, say so and change the CLASS of approach, not the surface."
        )
        print(json.dumps({"hookSpecificOutput": {"hookEventName": "PostToolUse", "additionalContext": msg}}))
    except Exception:
        return 0  # advisory telemetry must never break a tool call
    return 0


if __name__ == "__main__":
    sys.exit(main())
