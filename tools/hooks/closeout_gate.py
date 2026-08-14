#!/usr/bin/env python
"""Stop hook — force a session_closeout.py record before a long session keeps going unreviewed.

tools/session_closeout.py already computes "pending" deterministically from the token
ledger, and session_context.py surfaces the pending count every SessionStart, and
context_tripwire.py nudges toward writing a closeout note once context passes 400k.
All three are advisory. Measured effect (2026-07-31 audit): 37 sessions pending, 1 ever
recorded, since the tool shipped 2026-07-30 — a ~3% compliance rate on a nudge that has
been surfaced from three different angles. A nudge nobody acts on is not a working
control; this hook turns "did a closeout record get written for THIS session" into a
mechanically-checkable fact and blocks once when it hasn't, the same escalation
style_lint.py already uses for its evidence-grain check.

This does NOT judge the closeout's content — promoted vs already-captured vs
no-durable-delta stays a judgment call made by whoever runs --record (a
no-durable-delta record is a legitimate, complete closeout). It only forces the ACT of
recording one of the three outcomes, WITH a note, to exist — session_closeout.py itself
requires --note (20+ chars) on every outcome as of 2026-07-31, closing the gap where a
bare no-durable-delta cost nothing to type and proved nothing was assessed. Per
feedback_guardrail_determinism_tiers: "does this block a mechanically-unambiguous
action" — yes, presence-of-a-record-with-a-note is not a judgment call, unlike whether
the note is actually insightful, which stays Tier 3 and unenforced on purpose.

Turn counting: the ledger row for THIS session doesn't exist until SessionEnd, so
pending() can't see the current session while it's still running. This hook keeps its
own live counter in a per-session temp marker, bumped once per Stop call — cheaper than
re-scanning the transcript every turn (session_token_ledger.py's own reason for only
running at SessionEnd).

Fires at most once per session (TURNS_MIN, matching session_closeout.py so the trigger
lines up with how "pending" is later defined). Ignoring the block doesn't re-trigger it
on the immediate forced continuation (stop_hook_active guards that, same as
style_lint.py) or on any later turn this session — a repeating block would be exactly
the over-nagging the jargon warnings were demoted for in 2026-07-25. If it's ignored,
the SessionStart nudge for the NEXT session still lists this one as pending.

Exit contract: 0 = allow, 2 = block with the reason on stderr. Fails open on every
error path, like the other hooks here.
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile

TURNS_MIN = 500  # kept in sync with tools/session_closeout.py TURNS_MIN (raised from 20, 2026-08-14)

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REVIEWS = os.path.join(REPO, "logs", "closeout_reviews.jsonl")


def _safe_id(session_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "", session_id or "nosession")[:64]


def _counter_path(session_id: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"dail_closeout_turns_{_safe_id(session_id)}.count")


def _gated_path(session_id: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"dail_closeout_gated_{_safe_id(session_id)}.flag")


def _bump_turns(session_id: str) -> int:
    path = _counter_path(session_id)
    n = 0
    try:
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                n = int((fh.read() or "0").strip() or "0")
    except Exception:
        n = 0
    n += 1
    try:
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(str(n))
    except Exception:
        pass
    return n


def _already_gated(session_id: str) -> bool:
    return os.path.exists(_gated_path(session_id))


def _set_gated(session_id: str) -> None:
    try:
        with open(_gated_path(session_id), "w", encoding="utf-8") as fh:
            fh.write("1")
    except Exception:
        pass


def _already_recorded(session_id_12: str) -> bool:
    if not os.path.exists(REVIEWS):
        return False
    try:
        with open(REVIEWS, encoding="utf-8") as fh:
            for line in fh:
                try:
                    row = json.loads(line)
                except Exception:
                    continue
                if row.get("session") == session_id_12:
                    return True
    except Exception:
        return False
    return False


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    if not isinstance(payload, dict):
        return 0
    if payload.get("stop_hook_active") or payload.get("stopHookActive"):
        return 0

    session_id = str(payload.get("session_id") or payload.get("sessionId") or "")
    if not session_id:
        return 0

    try:
        if _already_gated(session_id):
            _bump_turns(session_id)
            return 0

        turns = _bump_turns(session_id)
        if turns < TURNS_MIN:
            return 0

        if _already_recorded(session_id[:12]):
            _set_gated(session_id)
            return 0

        _set_gated(session_id)
        sys.stderr.write(
            f"Session closeout gate: {turns} assistant turns this session, no closeout record "
            "yet. tools/session_closeout.py has been advisory-only since 2026-07-30 with a "
            "1/38 compliance rate — before continuing:\n"
            "  1. Assess this session for ONE durable learning or repeat process — something "
            "that, captured now, saves tokens next time by preventing a re-ask or a re-derive "
            "(a config quirk, a recurring correction, a gap between what a tool reports and "
            "what actually happened). If it's worth memory, write it.\n"
            f"  2. Then run: python tools/session_closeout.py --record {session_id[:12]} "
            '<promoted|already-captured|no-durable-delta> --note "..." (20+ chars, required for '
            "every outcome — name what was checked, even for no-durable-delta, not just the "
            "verdict).\n"
            "This does not require the session's work to have been notable — a genuine "
            "no-durable-delta is a complete closeout. It requires the assessment to have "
            "actually happened. Fires once per session; continuing without complying will not "
            "block again."
        )
        return 2
    except Exception:
        return 0


if __name__ == "__main__":
    sys.exit(main())
