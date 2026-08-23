#!/usr/bin/env python
"""UserPromptSubmit hook — price a ballooning context, without ending useful work.

THE MEASURED CASE (2026-07-31 week review, memory
project_token_spend_week_review_2026_07_31): 78% of the week's cache-read tokens were
billed on turns whose context had already passed 200k (36% past 400k); peak contexts ran
500-830k. Cache-read was 62% of price-weighted spend, so long-context turns ARE the
bill. CLAUDE.md's "/clear between unrelated tasks" rule demonstrably doesn't fire on its
own — this hook raises it in-context at the moment it's actionable (a new user prompt =
a task boundary).

RE-SCOPED 2026-08-21 on user feedback ("sometimes the long sessions are very
valuable"). The original wording told the agent to finish up and suggest /clear at 200k
regardless of what the session was doing, which is wrong in the case that matters most:
a long session sustained on ONE thread is frequently the cheap option, because the
alternative is paying to re-derive everything it already holds. Size alone is not the
signal — size PLUS a topic change is. So the nudge now states the cost, tells the agent
to keep going when the prompt continues the work, offers /compact as the middle option
that preserves the session, and reserves /clear for an unrelated task. The cost lever
that survives in every case is not ending the session but keeping its context from
growing: spans over dumps, subagents for sweeps, bounded Reads.

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
    "[context-tripwire] Context is ~{ctx:,} tokens, so each further turn re-bills all of "
    "it as cache-read. If this prompt CONTINUES the current work, keep going — a long "
    "session on one thread is often the cheaper option, because ending it means "
    "re-deriving what it already holds. Just stop the context growing: prefer index "
    "spans over file dumps, send bulky sweeps to a subagent, and Read with offset/limit. "
    "Only raise /clear if this prompt starts something UNRELATED."
)
NUDGE_2 = (
    "[context-tripwire] Context is ~{ctx:,} tokens — each turn bills roughly 4x a fresh "
    "session's, and most of that prefix is probably spent work rather than live state. "
    "If the thread is still valuable, offer /compact first: it keeps the session and the "
    "task, and drops the dead weight. Reserve the full handoff (a closeout note via "
    "tools/session_closeout.py, then /clear) for a genuine topic change. Never end a "
    "session that is mid-task purely because it is large — say the cost and let the user "
    "choose."
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
    msg = msg.format(ctx=ctx)
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
