"""Session closeout — deterministic review gate over the session token ledger.

Retrospectives here happened when someone remembered; learnings from a heavy
session evaporated if the next session started on a different topic. This tool
makes the selection deterministic without any new teardown code: the SessionEnd
hook (tools/hooks/session_token_ledger.py) already records one row per session,
so "awaiting closeout" is computed, not stored — substantive ledger rows
(turns >= TURNS_MIN, the same threshold session_context.py's adoption tripwire
uses for "substantive") minus the sessions already recorded in
logs/closeout_reviews.jsonl.

    python tools/session_closeout.py                        # list pending
    python tools/session_closeout.py --record <session> <outcome> [--note "..."]

Outcomes (all three are legitimate closeouts — a null result is recorded, never
suppressed for looking bad):
    promoted          — something durable was written to memory/docs (say what in --note)
    already-captured  — the session's learnings were in memory before it ended
    no-durable-delta  — nothing worth keeping; recording that IS the record

--note is REQUIRED for all three outcomes (20+ chars, 2026-07-31) — the gap this closes:
a bare `--record <s> no-durable-delta` cost nothing to type and proved nothing was
actually assessed. The note forces the assessment to have happened: name the ONE durable
learning or repeat-question/token-waste pattern considered, even when the answer is "none
found" — say what was checked, not just the verdict.

The pending count is surfaced once per session by session_context.py; tools/hooks/
closeout_gate.py blocks once at TURNS_MIN with no record for the session at all.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
LEDGER = REPO / "logs" / "session_token_ledger.jsonl"
REVIEWS = REPO / "logs" / "closeout_reviews.jsonl"

# "substantive", kept in sync with tools/hooks/closeout_gate.py.
# Raised 20 -> 500 on 2026-08-14: at 20 the bar caught 253 of 281 sessions since SINCE,
# against a median session length of 161 turns — so "substantive" selected essentially
# every session and the backlog was unreviewable by construction (29 ever recorded, ~10%
# compliance). 500 is ~3x the median; it selects 16 of the same 281. No history is
# discarded — sub-500 ledger rows simply stop counting as debt.
TURNS_MIN = 500
OUTCOMES = ("promoted", "already-captured", "no-durable-delta")
NOTE_MIN_CHARS = 20  # forces a real one-line assessment, not a bare dismissal
# Sessions that ended before the gate shipped predate the practice — don't
# retro-burden the backlog with history nobody committed to reviewing.
SINCE = "2026-07-30"


def _rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            out.append(json.loads(line))
        except Exception:
            continue
    return out


def pending() -> list[dict]:
    reviewed = {r.get("session") for r in _rows(REVIEWS)}
    return [
        r
        for r in _rows(LEDGER)
        if r.get("turns", 0) >= TURNS_MIN and str(r.get("ts", "")) >= SINCE and r.get("session") not in reviewed
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="List or record session closeouts.")
    ap.add_argument("--record", nargs=2, metavar=("SESSION", "OUTCOME"), help=f"outcome: {'|'.join(OUTCOMES)}")
    ap.add_argument("--note", default="", help="what was promoted / why no delta (free text)")
    args = ap.parse_args()

    if args.record:
        session, outcome = args.record
        if outcome not in OUTCOMES:
            print(f"outcome must be one of {OUTCOMES}, got {outcome!r}")
            return 1
        if len(args.note.strip()) < NOTE_MIN_CHARS:
            print(
                f"--note required for every outcome ({NOTE_MIN_CHARS}+ chars) — what was assessed, "
                "not just which bucket it landed in. 'promoted' names WHAT was promoted (the citation "
                "is the value); 'already-captured' names where; 'no-durable-delta' names what pattern "
                "or repeat-question risk was actually checked for and ruled out."
            )
            return 1
        REVIEWS.parent.mkdir(parents=True, exist_ok=True)
        row = {
            "session": session,
            "outcome": outcome,
            "note": args.note,
            "ts": datetime.now().isoformat(timespec="seconds"),
        }
        with REVIEWS.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"recorded: {session} → {outcome}")
        return 0

    rows = pending()
    if not rows:
        print("no sessions awaiting closeout.")
        return 0
    print(f"{len(rows)} substantive session(s) (turns >= {TURNS_MIN}) awaiting closeout:")
    for r in rows:
        prompt = (r.get("prompt") or "").replace("\n", " ")[:90]
        print(
            f"  {r.get('session', '?'):>12}  {r.get('ts', '?')}  turns={r.get('turns', 0):<4} "
            f"mem_writes={r.get('mem_writes', 0):<3} {prompt}"
        )
    print('record: python tools/session_closeout.py --record <session> <outcome> [--note "..."]')
    return 0


if __name__ == "__main__":
    sys.exit(main())
