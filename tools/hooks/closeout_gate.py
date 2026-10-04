#!/usr/bin/env python
"""One-time Stop hook for recording a long-session closeout.

The counter measures Stop hook invocations, not assistant turns in the SessionEnd
ledger. It never reads transcripts or asks an LLM to judge the record, and fails open
on malformed input or filesystem errors.
"""

from __future__ import annotations

import hashlib
import json
import os
import sys
import tempfile
from pathlib import Path

TURNS_MIN = 500
REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
REVIEWS = os.path.join(REPO, "logs", "closeout_reviews.jsonl")
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from tools.session_closeout import (  # noqa: E402 - standalone hook path bootstrap
    LEDGER,
    _rows,
    canonical_session_id,
    session_ids_match,
    valid_review,
)


def _marker_key(session_id: str) -> str:
    return hashlib.sha256(f"{REPO}\0{session_id}".encode()).hexdigest()


def _counter_path(session_id: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"dail_closeout_turns_{_marker_key(session_id)}.count")


def _gated_path(session_id: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"dail_closeout_gated_{_marker_key(session_id)}.flag")


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


def _already_recorded(session_id: str) -> bool:
    if not os.path.exists(REVIEWS):
        return False
    try:
        rows = _rows(Path(REVIEWS))
        known_ids = {canonical_session_id(row.get("session")) for row in rows}
        known_ids.update(canonical_session_id(row.get("session")) for row in _rows(LEDGER))
        known_ids.add(session_id)
        return any(valid_review(row) and session_ids_match(row.get("session"), session_id, known_ids) for row in rows)
    except Exception:
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

    session_id = canonical_session_id(payload.get("session_id") or payload.get("sessionId"))
    if not session_id:
        return 0

    try:
        if _already_gated(session_id):
            return 0

        turns = _bump_turns(session_id)
        if turns < TURNS_MIN:
            return 0

        if _already_recorded(session_id):
            _set_gated(session_id)
            return 0

        _set_gated(session_id)
        sys.stderr.write(
            f"Session closeout gate: {turns} Stop invocations; no valid closeout record.\n"
            "Before continuing, assess one durable learning or repeat process and record it:\n"
            f"  python tools/session_closeout.py --record {session_id} "
            '<promoted|already-captured|no-durable-delta> --note "..."\n'
            "The note must be at least 20 nonblank characters. This reminder fires once."
        )
        return 2
    except Exception:
        return 0


if __name__ == "__main__":
    sys.exit(main())
