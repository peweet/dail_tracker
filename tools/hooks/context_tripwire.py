#!/usr/bin/env python
"""UserPromptSubmit hook — nudge toward /clear when this session's context balloons.

THE MEASURED CASE (2026-07-31 week review, memory
project_token_spend_week_review_2026_07_31): 78% of the week's cache-read tokens were
billed on turns whose context had already passed 200k (36% past 400k); peak contexts ran
500-830k. Cache-read was 62% of price-weighted spend, so long-context turns ARE the
bill. CLAUDE.md's "/clear between unrelated tasks" rule demonstrably doesn't fire on its
own — this hook raises it in-context at the moment it's actionable (a new user prompt =
a task boundary).

Context size is read from the transcript itself: the last assistant turn's
usage.cache_read_input_tokens is the exact prompt-cache size, no estimation. Only the
tail of the file is read (TAIL_BYTES seek), never the whole transcript — same
lightness rule as session_context.py.

Deliberately soft (advisory additionalContext, never a block): per the
guardrail-determinism tiers, "is now a good moment to /clear" is a judgment call, and
/clear itself is the USER'S action — the nudge tells the agent to raise it, not to stop
working. One nudge per threshold per session via temp-dir markers (flood_warn pattern);
after the 400k nudge the hook stays silent for the session.

Fails open on every path; always exits 0.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

TAIL_BYTES = 262_144  # plenty to find the last assistant turn; never the whole file
WARN_1 = 200_000
WARN_2 = 400_000

NUDGE_1 = (
    "[context-tripwire] This session's context has passed 200k tokens — every further "
    "turn re-bills all of it as cache-read. Finish the task in hand, then suggest the "
    "user /clear (or open a fresh session) before starting anything unrelated."
)
NUDGE_2 = (
    "[context-tripwire] Context has passed 400k tokens — each turn now bills roughly 4x "
    "a fresh session's. Offer the user a handoff at the next natural break: write a "
    "short closeout note (task state, open items, key file:line pointers — "
    "tools/session_closeout.py is the existing surface), then they /clear and the next "
    "session resumes from the note for a few k tokens instead of this context."
)


def _last_context_tokens(path: str) -> int:
    """cache_read + cache_creation + input of the LAST assistant turn in the tail."""
    try:
        size = os.path.getsize(path)
        with open(path, "rb") as fh:
            if size > TAIL_BYTES:
                fh.seek(size - TAIL_BYTES)
            tail = fh.read().decode("utf-8", errors="replace")
    except Exception:
        return 0
    for line in reversed(tail.splitlines()):
        try:
            o = json.loads(line)
        except Exception:
            continue  # first line of a seeked tail is usually a fragment
        if o.get("type") != "assistant":
            continue
        u = (o.get("message") or {}).get("usage") or {}
        return (
            (u.get("cache_read_input_tokens", 0) or 0)
            + (u.get("cache_creation_input_tokens", 0) or 0)
            + (u.get("input_tokens", 0) or 0)
        )
    return 0


def _already_nudged(session: str, level: int) -> bool:
    """One nudge per threshold per session. Marker in temp dir; never in the repo."""
    try:
        marker = os.path.join(tempfile.gettempdir(), f"dail_ctx_tripwire_{session[:12]}_{level}")
        if os.path.exists(marker):
            return True
        with open(marker, "w") as fh:
            fh.write("1")
        return False
    except Exception:
        return True  # can't track -> stay silent rather than nag every prompt


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    tp = ""
    for k in ("transcript_path", "transcriptPath", "transcript"):
        v = payload.get(k)
        if isinstance(v, str) and v:
            tp = v
            break
    if not tp or not os.path.isfile(tp):
        return 0
    ctx = _last_context_tokens(tp)
    if ctx < WARN_1:
        return 0
    session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")
    if ctx >= WARN_2:
        level, msg = 2, NUDGE_2
    else:
        level, msg = 1, NUDGE_1
    if _already_nudged(session, level):
        return 0
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": msg,
                }
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
