#!/usr/bin/env python
"""UserPromptSubmit hook — inject the cached discovery one-liner(s) matching this prompt.

Closes the adoption gap measured in project_token_optimization_measurement_2026_07_18:
the discovery index only pays off if a session consults it at the right moment, and
that reflex never formed (navigation-tool calls ~0/100 turns across 122 sessions).
Instead of hoping the agent runs `python tools/discoveries.py <topic>`, this pushes
the matching row into context at the exact moment the topic comes up — the same
"feedback at the moment it can change behaviour" principle as flood_warn.py.

Deliberately soft and quiet (per the guardrail-determinism tiers: keyword matching
is a semantic judgment, so never a block):
- at most MAX_ROWS rows per prompt, each one line;
- a row is injected at most once per session (marker file in temp, like flood_warn);
- fires only on a confident match: >=2 distinct trigger hits, or one multi-word
  trigger (multi-word phrases are specific enough on their own — "audited financial
  statements" can't fire by accident the way "gross" could);
- stays silent on slash commands, short prompts, and any error (fails open).

Reads ONLY tools/discoveries.jsonl (~13 KB) — never token_ledger.py or transcripts,
same heaviness rule as session_context.py's _discoveries_note.
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA = os.path.join(REPO, "tools", "discoveries.jsonl")

MAX_ROWS = 2          # per the design note: 1-2 rows max, or it becomes prompt noise
MIN_PROMPT_CHARS = 20  # below this it's "yes"/"ok"/a path — nothing to match on
MAX_LINE_CHARS = 320   # cap a runaway one-liner rather than flood the prompt
_BAND_RANK = {"high": 0, "med": 1, "low": 2}


def _load_rows() -> list[dict]:
    rows = []
    with open(DATA, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                o = json.loads(line)
            except Exception:
                continue
            if str(o.get("id", "")).startswith("_"):
                continue
            if o.get("discovery") and o.get("trigger"):
                rows.append(o)
    return rows


def _match(prompt_lc: str, row: dict) -> int:
    """Score = distinct triggers present in the prompt (word-boundary match).

    Returns 0 unless the match is confident: >=2 triggers, or 1 multi-word trigger.
    """
    hits = []
    for t in row.get("trigger", []):
        t = str(t).strip().lower()
        if not t:
            continue
        if re.search(r"\b" + re.escape(t) + r"\b", prompt_lc):
            hits.append(t)
    if len(hits) >= 2:
        return len(hits)
    if len(hits) == 1 and " " in hits[0]:
        return 1
    return 0


def _seen_path(session: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"dail_discovery_hint_{session[:12]}.json")


def _load_seen(session: str) -> set[str]:
    try:
        with open(_seen_path(session), encoding="utf-8") as fh:
            return set(json.load(fh))
    except Exception:
        return set()


def _save_seen(session: str, seen: set[str]) -> None:
    try:
        with open(_seen_path(session), "w", encoding="utf-8") as fh:
            json.dump(sorted(seen), fh)
    except Exception:
        pass  # can't persist -> worst case is a repeat hint next prompt, not a crash


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    prompt = str(payload.get("prompt") or "")
    if len(prompt) < MIN_PROMPT_CHARS or prompt.lstrip().startswith("/"):
        return 0
    session = str(payload.get("session_id") or payload.get("sessionId") or "nosession")

    try:
        rows = _load_rows()
    except Exception:
        return 0
    prompt_lc = prompt.lower()
    scored = [(r, _match(prompt_lc, r)) for r in rows]
    hits = [(r, s) for r, s in scored if s > 0]
    if not hits:
        return 0
    hits.sort(key=lambda rs: (-rs[1], _BAND_RANK.get(rs[0].get("cost_band"), 3)))

    seen = _load_seen(session)
    picked = [r for r, _ in hits if r["id"] not in seen][:MAX_ROWS]
    if not picked:
        return 0
    _save_seen(session, seen | {r["id"] for r in picked})

    lines = []
    for r in picked:
        one = str(r["discovery"])[:MAX_LINE_CHARS]
        mem = r.get("memory", "")
        lines.append(f"- {r['id']}: {one}" + (f" (detail: memory/{mem}.md)" if mem else ""))
    ctx = (
        "[discovery-index] Cached finding(s) matching this prompt — read before re-deriving:\n"
        + "\n".join(lines)
    )
    print(json.dumps({
        "hookSpecificOutput": {
            "hookEventName": "UserPromptSubmit",
            "additionalContext": ctx,
        }
    }))
    return 0


if __name__ == "__main__":
    sys.exit(main())
