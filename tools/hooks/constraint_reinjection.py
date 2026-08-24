#!/usr/bin/env python
"""UserPromptSubmit hook — periodically re-state the rules nothing else re-checks.

Grounds: arXiv:2604.20911 ("Omission Constraints Decay While Commission Constraints
Persist in Long-Context LLM Agents") measures prohibition-type rules ("never do X")
decaying as a session runs long and tool-schema volume grows, while requirement-type
rules stay compliant -- the compliant ones get reinforced because the model's own prior
outputs become implicit few-shot examples; a prohibition leaves no such trace. The
paper's own fix is not a stronger check, it's periodic re-injection: re-stating the rule
resets its distance from the model's effective attention range. That is a different move
from a hard gate -- it doesn't turn judgment into a pattern-matcher, it just keeps the
judgment-triggering text from receding out of range.

This targets specifically the rules in this repo that get NO other mechanical treatment:
Rule 2 (evidence grain) is Stop-hook blocked every turn (style_lint.py); Rules 1/3/4 and,
since 2026-08-24, Rule 5 are silently logged every turn and surfaced in a weekly digest
(session_context.py::_style_digest_note). Rules 6/7 and the Tier-3 semantic rules --
never-sum the 3 money grains, provenance-is-the-user's-domain -- get nothing at all
(feedback_guardrail_determinism_tiers in memory): no block, no log, no digest. Those are
exactly the highest-consequence rules this repo has, and the paper's mechanism doesn't
care about consequence, only about position in context.

Deliberately NOT a gate and NOT a per-reply nudge tied to content (unlike style_lint.py,
this fires on context size alone, blind to what was just said) -- per
feedback_guardrail_determinism_tiers, hardening a Tier-3 judgment rule into a checker
risks exactly the "crushes the cross-referencing value" failure the user flagged
2026-07-18. Re-statement preserves judgment; it only fights attentional distance.

Cadence is a first guess, not measured: fires once context crosses FIRST_THRESHOLD tokens,
then again every REPEAT_EVERY tokens after that (band-keyed markers, unlike
context_tripwire.py's one-shot-per-level pattern -- recurrence is the point here).
Thresholds are far below context_tripwire.py's WARN_1=200k because that hook prices COST;
this one fights a decay the paper shows starting within the first ~15-25k tokens of
schema-heavy tool use, an order of magnitude earlier.

Context size is read the same way context_tripwire.py reads it: the last assistant turn's
usage tokens from the tail of the transcript, no estimation.

Fails open on every path; always exits 0.
"""

from __future__ import annotations

import json
import os
import sys
import tempfile

TAIL_BYTES = 262_144
FIRST_THRESHOLD = 40_000
REPEAT_EVERY = 80_000

MESSAGE = (
    "[constraint-refresh] Long thread — the rules nothing else re-checks are the ones "
    "most likely to have faded by now (arXiv:2604.20911): Rule 5 (prose by default, no "
    "gratuitous bullets), Rule 6 (name what gets harder), Rule 7 (delete-test each "
    "sentence), never-sum the 3 money grains, provenance is the user's domain. Not a "
    "gate — just a reminder they exist."
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
            continue
        if o.get("type") != "assistant":
            continue
        u = (o.get("message") or {}).get("usage") or {}
        return (
            (u.get("cache_read_input_tokens", 0) or 0)
            + (u.get("cache_creation_input_tokens", 0) or 0)
            + (u.get("input_tokens", 0) or 0)
        )
    return 0


def _band(ctx: int) -> int:
    """Which repeat-band this context size falls in; 0 below FIRST_THRESHOLD."""
    if ctx < FIRST_THRESHOLD:
        return 0
    return 1 + (ctx - FIRST_THRESHOLD) // REPEAT_EVERY


def _already_fired(session: str, band: int) -> bool:
    """One firing per (session, band). Marker in temp dir; never in the repo."""
    try:
        marker = os.path.join(tempfile.gettempdir(), f"dail_constraint_reinject_{session[:12]}_{band}")
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
    band = _band(ctx)
    if band < 1:
        return 0
    session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")
    if _already_fired(session, band):
        return 0
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": MESSAGE,
                }
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
