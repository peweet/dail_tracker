"""Record evidenced session reviews and list pending Claude ledger sessions.

Pending selection deduplicates the Claude SessionEnd ledger, selecting sessions
with at least 500 assistant messages since SINCE and no valid review. This is not
a Codex coverage report. Both clients can record a milestone review by full ID;
the live Stop hook separately counts Stop invocations, not transcript messages.

    python tools/session_closeout.py                        # list pending
    python tools/session_closeout.py --record <session> <outcome> [--note "..."]

Outcomes (all three are legitimate closeouts — a null result is recorded, never
suppressed for looking bad):
    promoted          — something durable was written to memory/docs (say what in --note)
    already-captured  — the session's learnings were in memory before it ended
    no-durable-delta  — nothing worth keeping; recording that IS the record

Notes need at least 20 characters naming what was assessed or where the lesson
was saved. This validates a record's structure, not the quality of its judgment.
The tool does not write lessons or call a model. Repeating an identical review is
idempotent; a later, different lesson can be recorded in the same session.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterable
from datetime import date, datetime
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
NOTE_MIN_CHARS = 20
# Sessions that ended before the gate shipped predate the practice — don't
# retro-burden the backlog with history nobody committed to reviewing.
SINCE = "2026-07-30"


def canonical_session_id(value: object) -> str:
    """Return a usable session ID, preserving full IDs and rejecting other types."""
    return value.strip() if isinstance(value, str) else ""


def normalize_note(value: object) -> str:
    """Normalize whitespace so repeated --record calls can be idempotent."""
    return " ".join(value.split()) if isinstance(value, str) else ""


def valid_review(row: object) -> bool:
    """Recognize only complete closeout records; malformed rows are ignored."""
    if not isinstance(row, dict):
        return False
    return (
        bool(canonical_session_id(row.get("session")))
        and row.get("outcome") in OUTCOMES
        and len(normalize_note(row.get("note"))) >= NOTE_MIN_CHARS
    )


def session_identity_key(session_id: object, known_ids: Iterable[str] = ()) -> str:
    """Canonicalize a legacy 12-character ID only when its full ID is unambiguous."""
    value = canonical_session_id(session_id)
    if len(value) != 12:
        return value
    full_ids = {candidate for candidate in known_ids if len(candidate) > 12 and candidate.startswith(value)}
    return next(iter(full_ids)) if len(full_ids) == 1 else value


def session_ids_match(stored: object, requested: object, known_ids: Iterable[str] = ()) -> bool:
    """Match full IDs exactly, with an ambiguity-safe historical 12-character fallback."""
    stored_id = canonical_session_id(stored)
    requested_id = canonical_session_id(requested)
    if not stored_id or not requested_id:
        return False
    if stored_id == requested_id:
        return True
    if len(stored_id) == 12 and len(requested_id) > 12 and requested_id.startswith(stored_id):
        full_ids = {candidate for candidate in known_ids if len(candidate) > 12 and candidate.startswith(stored_id)}
        return not full_ids or full_ids == {requested_id}
    if len(requested_id) == 12 and len(stored_id) > 12 and stored_id.startswith(requested_id):
        return session_ids_match(requested_id, stored_id, known_ids)
    return False


def _valid_ledger_timestamp(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return False
    return parsed.date() >= date.fromisoformat(SINCE)


def _rows(path: Path) -> list[dict]:
    if not path.exists():
        return []
    out = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line)
        except Exception:
            continue
        if isinstance(row, dict):
            out.append(row)
    return out


def pending() -> list[dict]:
    ledger_rows = _rows(LEDGER)
    known_ids = {canonical_session_id(row.get("session")) for row in ledger_rows}
    reviewed = [row for row in _rows(REVIEWS) if valid_review(row)]
    known_ids.update(canonical_session_id(row.get("session")) for row in reviewed)
    selected: dict[str, tuple[tuple[int, str, int], dict]] = {}
    for index, row in enumerate(ledger_rows):
        session = canonical_session_id(row.get("session"))
        ts = row.get("ts")
        turns = row.get("turns")
        if not session or not _valid_ledger_timestamp(ts) or not isinstance(turns, int) or isinstance(turns, bool):
            continue
        turns_value = turns
        if turns_value < TURNS_MIN:
            continue
        key = session_identity_key(session, known_ids)
        rank = (turns_value, ts, index)
        if key not in selected or rank > selected[key][0]:
            selected[key] = (rank, row)

    return [
        row
        for key, (_, row) in selected.items()
        if not any(session_ids_match(review.get("session"), key, known_ids) for review in reviewed)
    ]


def main() -> int:
    ap = argparse.ArgumentParser(description="List or record session closeouts.")
    ap.add_argument("--record", nargs=2, metavar=("SESSION", "OUTCOME"), help=f"outcome: {'|'.join(OUTCOMES)}")
    ap.add_argument("--note", default="", help="what was promoted / why no delta (free text)")
    args = ap.parse_args()

    if args.record:
        session, outcome = args.record
        session = canonical_session_id(session)
        note = normalize_note(args.note)
        if not session:
            print("session must be a nonblank ID")
            return 1
        if outcome not in OUTCOMES:
            print(f"outcome must be one of {OUTCOMES}, got {outcome!r}")
            return 1
        if len(note) < NOTE_MIN_CHARS:
            print(
                f"--note required for every outcome ({NOTE_MIN_CHARS}+ chars) — what was assessed, "
                "not just which bucket it landed in. 'promoted' names WHAT was promoted (the citation "
                "is the value); 'already-captured' names where; 'no-durable-delta' names what pattern "
                "or repeat-question risk was actually checked for and ruled out."
            )
            return 1
        REVIEWS.parent.mkdir(parents=True, exist_ok=True)
        existing = _rows(REVIEWS)
        known_ids = {canonical_session_id(row.get("session")) for row in existing}
        known_ids.update(canonical_session_id(row.get("session")) for row in _rows(LEDGER))
        known_ids.add(session)
        if any(
            valid_review(row)
            and session_ids_match(row.get("session"), session, known_ids)
            and row.get("outcome") == outcome
            and normalize_note(row.get("note")) == note
            for row in existing
        ):
            print(f"already recorded: {session} -> {outcome}")
            return 0
        row = {
            "session": session,
            "outcome": outcome,
            "note": note,
            "ts": datetime.now().isoformat(timespec="seconds"),
        }
        with REVIEWS.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        print(f"recorded: {session} → {outcome}")
        return 0

    rows = pending()
    if not rows:
        print("no Claude ledger sessions awaiting closeout; Codex ledger coverage is not available.")
        return 0
    print(f"{len(rows)} Claude ledger session(s) (assistant messages >= {TURNS_MIN}) awaiting closeout:")
    for r in rows:
        prompt_value = r.get("prompt")
        prompt = (prompt_value if isinstance(prompt_value, str) else "").replace("\n", " ")[:90]
        try:
            turns = int(r.get("turns", 0))
        except (TypeError, ValueError):
            turns = 0
        try:
            mem_writes = int(r.get("mem_writes", 0))
        except (TypeError, ValueError):
            mem_writes = 0
        print(
            f"  {r.get('session', '?'):>12}  {r.get('ts', '?')}  turns={turns:<4} mem_writes={mem_writes:<3} {prompt}"
        )
    print('record: python tools/session_closeout.py --record <session> <outcome> [--note "..."]')
    return 0


if __name__ == "__main__":
    sys.exit(main())
