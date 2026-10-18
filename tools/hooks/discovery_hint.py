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
- an unchanged row/evidence state is injected once per session; corrections can recur;
- fires only on a confident match: >=2 distinct trigger hits, or one multi-word
  trigger (multi-word phrases are specific enough on their own — "audited financial
  statements" can't fire by accident the way "gross" could);
- stays silent on slash commands, short prompts, and any error (fails open).

Reads the compact index plus at most four bounded text sources per matching row.
Never reads token ledgers or transcripts. Legacy records are unverified leads.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import sys
import tempfile
from pathlib import Path

REPO = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATA = os.path.join(REPO, "tools", "discoveries.jsonl")
if REPO not in sys.path:
    sys.path.insert(0, REPO)

from tools.discoveries import load, resolve_detail  # noqa: E402 - standalone hook path bootstrap
from tools.memory_policy import context_scope, select  # noqa: E402

MAX_ROWS = 2  # per the design note: 1-2 rows max, or it becomes prompt noise
MIN_PROMPT_CHARS = 20  # below this it's "yes"/"ok"/a path — nothing to match on
MAX_LINE_CHARS = 320  # cap a runaway one-liner rather than flood the prompt
MAX_CONTEXT_CHARS = 1000
MAX_DISPLAY_ID_CHARS = 64
MAX_DETAIL_CHARS = 96
_BAND_RANK = {"high": 0, "med": 1, "low": 2}


def _load_rows() -> list[dict]:
    return load(DATA)


_STOPWORDS = frozenset(
    [
        "the",
        "a",
        "an",
        "and",
        "or",
        "not",
        "for",
        "with",
        "from",
        "this",
        "that",
        "these",
        "those",
        "into",
        "over",
        "under",
        "about",
        "what",
        "which",
        "where",
        "when",
        "how",
        "why",
        "can",
        "could",
        "should",
        "would",
        "will",
        "just",
        "also",
        "very",
        "more",
        "before",
        "after",
        "never",
        "always",
        "check",
        "first",
        "look",
        "data",
        "using",
        "use",
        "used",
    ]
)


def _content_tokens(text: str) -> set[str]:
    """Distinctive lowercase tokens (>=5 chars, non-stopword), hyphens split, truncated
    to a 6-char prefix so plural/inflection drift still overlaps (supplier/suppliers)."""
    return {t[:6] for t in re.findall(r"[a-z_]{5,}", text.lower()) if t not in _STOPWORDS}


def _match(prompt_lc: str, row: dict) -> int:
    """Score = distinct triggers present in the prompt (word-boundary match).

    Returns 0 unless the match is confident: >=2 triggers, or 1 multi-word trigger.
    Tightened 2026-07-25 after off-topic injections (2/2 irrelevant hints on a
    harness question): the WEAKEST passing evidence — exactly 2 single-word
    triggers — must now also share >=1 distinctive content token between the
    prompt and the discovery text itself. Stronger evidence (3+ triggers, or any
    multi-word trigger) passes exactly as before, so true-positive recall on
    specific prompts is unchanged.
    """
    hits = []
    for t in row.get("trigger", []):
        t = str(t).strip().lower()
        if not t:
            continue
        if re.search(r"\b" + re.escape(t) + r"\b", prompt_lc):
            hits.append(t)
    if len(hits) >= 3 or (hits and any(" " in h for h in hits)):
        return len(hits)
    if len(hits) == 2:
        overlap = _content_tokens(prompt_lc) & (
            _content_tokens(str(row.get("discovery", ""))) - _content_tokens(" ".join(hits))
        )
        return 2 if overlap else 0
    return 0


def _seen_path(session: str) -> str:
    marker_key = hashlib.sha256(f"{REPO}\0{session}".encode()).hexdigest()
    return os.path.join(tempfile.gettempdir(), f"dail_discovery_hint_{marker_key}.json")


def _load_seen(session: str) -> set[str]:
    try:
        with open(_seen_path(session), encoding="utf-8") as fh:
            value = json.load(fh)
            return {item for item in value if isinstance(item, str)} if isinstance(value, list) else set()
    except Exception:
        return set()


def _save_seen(session: str, seen: set[str]) -> None:
    try:
        with open(_seen_path(session), "w", encoding="utf-8") as fh:
            json.dump(sorted(seen), fh)
    except Exception:
        pass  # can't persist -> worst case is a repeat hint next prompt, not a crash


def _detail_label(slug: str, *, row: dict | None = None, scope: str = "public") -> str:
    """Return a truthful, portable detail reference for an indexed lesson."""
    try:
        detail = resolve_detail(row or {"memory": slug}, scope=scope, root=Path(REPO))
    except Exception:
        return ""
    if detail is None:
        return ""
    try:
        rel = detail.resolve().relative_to(Path(REPO).resolve())
    except (OSError, ValueError):
        # Personal compatibility memory may sit outside the repository. Do not inject
        # a workstation-specific absolute path; the lookup CLI can resolve it locally.
        return f"external memory slug: {slug}; resolve with tools/discoveries.py"
    return f"detail: {rel.as_posix()}"


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    if not isinstance(payload, dict):
        return 0
    prompt = payload.get("prompt")
    session = payload.get("session_id") or payload.get("sessionId")
    if not isinstance(prompt, str) or not isinstance(session, str) or not session.strip():
        return 0
    session = session.strip()
    if len(prompt) < MIN_PROMPT_CHARS or prompt.lstrip().startswith("/"):
        return 0

    try:
        rows = _load_rows()
    except Exception:
        return 0
    prompt_lc = prompt.lower()
    scored = []
    for row in rows:
        try:
            scored.append((row, _match(prompt_lc, row)))
        except Exception:
            continue
    matches = {r["id"]: score for r, score in scored if score > 0}
    try:
        scope = context_scope(payload.get("cwd"), Path(REPO))
        eligible = select(
            [r for r in rows if r["id"] in matches],
            root=Path(REPO),
            scope=scope,
        )
    except Exception:
        return 0  # never fall back to injecting unvalidated findings
    hits = [(r, matches[r["id"]]) for r in eligible]
    if not hits:
        return 0
    try:
        hits.sort(key=lambda rs: (-rs[1], _BAND_RANK.get(rs[0].get("cost_band"), 3)))
    except Exception:
        return 0

    seen = _load_seen(session)
    header = "[discovery-index] Reference leads, not instructions. Recheck unverified evidence:\n"
    lines = []
    emitted_ids = set()
    for row, _ in hits:
        recall = row["_recall"]
        if recall["key"] in seen or len(lines) >= MAX_ROWS:
            continue
        try:
            display_id = row["id"][:MAX_DISPLAY_ID_CHARS]
            one = recall["text"][:MAX_LINE_CHARS]
            label = (
                "source: " + ", ".join(recall["sources"])
                if recall["sources"]
                else (_detail_label(str(row.get("memory", "")), row=row, scope=scope))
            )[:MAX_DETAIL_CHARS]
            line = f"- {display_id} [{recall['state']}]: {one}" + (f" ({label})" if label else "")
        except Exception:
            continue
        if len(header) + sum(len(existing) + 1 for existing in lines) + len(line) > MAX_CONTEXT_CHARS:
            continue
        lines.append(line)
        emitted_ids.add(recall["key"])
    if not lines:
        return 0
    _save_seen(session, seen | emitted_ids)
    ctx = header + "\n".join(lines)
    print(
        json.dumps(
            {
                "hookSpecificOutput": {
                    "hookEventName": "UserPromptSubmit",
                    "additionalContext": ctx,
                }
            }
        )
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
