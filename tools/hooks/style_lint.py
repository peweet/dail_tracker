#!/usr/bin/env python
"""Stop hook — block data figures asserted without provenance; warn on jargon and long sentences.

Turns .claude/rules/communication.md Rule 2 ("no claim without its band") into
deterministic enforcement for the one case evidence.md calls this project's costliest
failure mode: a number about the data stated with no citation, no confidence band, and
no admission that it wasn't checked. Those are the claims that survive into UI copy and
audits.

Deliberately narrow. Only ONE category blocks. Jargon (Rule 3) and sentence length
(Rule 4) warn, because a linter aggressive enough to force a rewrite on style would
teach the agent to write evasively around the checker rather than plainly -- worse than
the problem, and harder to notice. See feedback_guardrail_determinism_tiers in memory:
determinism is inverse to consequence.

The escape hatch IS compliance. A figure is discharged by citing a file, linking a repo
path, tagging a band ("[Indicative -- no query run]"), showing the query, or writing
"unverified". There is no suppression syntax to learn -- the way out is to state the
grain, exactly as the `# logic_firewall: display_only` marker works for the firewall
checker.

Cross-tool notes:
  * Only Stop and SubagentStop receive `last_assistant_message`; every other event sees
    tool inputs, never prose. This is the only place a text check can live.
  * Block at most ONCE per turn. No other hook in tools/hooks/ keeps state, so the
    session-keyed marker below is new ground -- it is bounded by mtime and lives in the
    system temp dir, never in the repo.
  * Fails open on every error path, like the other hooks here.

Known gap (v1, deliberate): markdown table rows are exempt from the figure check.
Tables are where tool output lands and checking them produced too many false positives
to keep the surface narrow. Tighten later by ratchet, not by widening the regex now.

Exit contract: 0 = allow (with optional warning JSON on stdout), 2 = block with the
reason on stderr.
"""
from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time

# --- tuning ---------------------------------------------------------------
MAX_WARNINGS = 3  # cap so the hook advises rather than nags
LONG_SENTENCE_WORDS = 45  # Rule 4 is "one idea"; this only catches the runaways
BLOCK_COOLDOWN_S = 300  # a block marker older than this is stale, not a live turn

# Figures that read as a claim about the data. Narrow on purpose: currency amounts,
# thousands-separated integers, and percentages. A bare 4-digit year never matches.
FIGURE_RE = re.compile(
    r"(?:[€£$]\s?\d[\d,.]*\s?(?:bn|m|k)?\b)"  # €1.08bn, $250, £4,958
    r"|(?:\b\d{1,3}(?:,\d{3})+\b)"  # 4,958   1,083
    r"|(?:\b\d+(?:\.\d+)?\s?%)",  # 91%   35.9 %
    re.IGNORECASE,
)

# Self-referential / mechanical numbers -- about the conversation or the code, not about
# the corpus. Matched against the ~40 chars following the figure.
NOT_A_DATA_CLAIM = re.compile(
    r"^\W{0,3}(?:tokens?|lines?|bytes?|chars?|characters?|words?|ms|px|rows? of code"
    r"|files?|docs?|pages?|rules?|seconds?|kb|mb|gb)\b",
    re.IGNORECASE,
)

# Any one of these in the same paragraph discharges every figure in it.
DISCHARGE_RE = re.compile(
    r"(?:\b[\w./\\-]+\.(?:py|sql|md|json|ya?ml|toml|csv|parquet|txt|ps1|ipynb)[:#]L?\d+)"
    r"|(?:\]\([^)]*\.(?:py|sql|md|json|ya?ml|toml|csv|parquet|txt|ps1|ipynb))"
    r"|(?:\[(?:Verified|Reported|Extracted|Indicative)\s*[—\-–])"
    r"|(?:\bunverified\b|\bhaven't checked\b|\bhave not checked\b|\bnot checked\b"
    r"|\bno query (?:was )?run\b|\bdidn't verify\b|\bdid not verify\b|\bnot verified\b"
    r"|\bfrom memory\b|\bcan't confirm\b|\bcannot confirm\b)"
    r"|(?:```)",  # a shown query/result discharges the paragraph it sits in
    re.IGNORECASE,
)

# Rule 3. Left column is what to say instead; these are the "not" forms.
JARGON = (
    "has implications for", "the tension here is", "is a token multiplier",
    "operationalize", "operationalise", "utilize", "utilise", "leverage",
    "commence", "consequently", "approximately", "facilitate",
    "surfaces", "primitive", "topology", "interrogate",
    "simply", "easily", "obviously", "essentially", "basically", "actually",
    "very", "quite", "really", "please note", "at this time",
    "it's worth noting", "it is worth noting", "it's important to note",
    "powerful", "seamless", "a variety of", "in order to",
)
JARGON_RE = re.compile(r"\b(" + "|".join(re.escape(w) for w in JARGON) + r")\b", re.IGNORECASE)


def _strip_noncheckable(text: str) -> str:
    """Remove regions that must never be linted: code, quotes, tables, URLs.

    Without this, any message that quotes the rules trips its own linter.
    """
    text = re.sub(r"```.*?```", " ", text, flags=re.DOTALL)
    text = re.sub(r"`[^`\n]*`", " ", text)
    text = re.sub(r"https?://\S+", " ", text)
    text = re.sub(r"\[([^\]]*)\]\([^)]*\)", r"\1", text)  # keep link text, drop target
    keep = [
        ln for ln in text.splitlines()
        if not ln.lstrip().startswith((">", "|"))
    ]
    return "\n".join(keep)


def _paragraphs(text: str) -> list[str]:
    return [p for p in re.split(r"\n\s*\n", text) if p.strip()]


def _unprovenanced(message: str) -> list[str]:
    """Figures asserted with no citation, band, query, or admission of uncertainty."""
    hits: list[str] = []
    for para in _paragraphs(message):
        if DISCHARGE_RE.search(para):  # checked against the RAW paragraph
            continue
        prose = _strip_noncheckable(para)
        for m in FIGURE_RE.finditer(prose):
            tail = prose[m.end():m.end() + 40]
            if NOT_A_DATA_CLAIM.match(tail):
                continue
            hits.append(m.group(0).strip())
    return hits


def _warnings(message: str) -> list[str]:
    prose = _strip_noncheckable(message)
    out: list[str] = []
    seen: set[str] = set()
    for m in JARGON_RE.finditer(prose):
        w = m.group(0).lower()
        if w not in seen:
            seen.add(w)
            out.append(f'jargon: "{m.group(0)}" (rule 3 -- use the plain word)')
    for sent in re.split(r"(?<=[.!?])\s+", prose):
        n = len(sent.split())
        if n > LONG_SENTENCE_WORDS:
            out.append(f"{n}-word sentence (rule 4 -- one idea per sentence)")
    return out


def _marker_path(session_id: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "", session_id or "nosession")[:64]
    return os.path.join(tempfile.gettempdir(), f"dail_style_lint_{safe}.block")


def _already_blocked_this_turn(session_id: str) -> bool:
    """True if we blocked recently. Consumes the marker so the next turn can block again."""
    path = _marker_path(session_id)
    try:
        if os.path.exists(path):
            fresh = (time.time() - os.path.getmtime(path)) < BLOCK_COOLDOWN_S
            os.remove(path)
            return fresh
    except Exception:
        pass
    return False


def _set_blocked(session_id: str) -> None:
    try:
        with open(_marker_path(session_id), "w", encoding="utf-8") as fh:
            fh.write("1")
    except Exception:
        pass


def _read_stdin() -> str:
    """Read stdin as UTF-8 regardless of the console codepage.

    Windows defaults stdin to cp1252, which mangles '€' in a payload the hook is
    specifically meant to inspect (currency figures) and makes json.loads fail -- the
    hook then fails open and silently checks nothing. Read bytes and decode explicitly.
    """
    try:
        raw = sys.stdin.buffer.read()
    except Exception:
        try:
            return sys.stdin.read()
        except Exception:
            return ""
    if isinstance(raw, bytes):
        return raw.decode("utf-8", errors="replace")
    return str(raw)


def main() -> int:
    try:
        payload = json.loads(_read_stdin() or "{}")
    except Exception:
        return 0  # never break the agent on a parse hiccup
    if not isinstance(payload, dict):
        return 0
    if payload.get("stop_hook_active") or payload.get("stopHookActive"):
        return 0  # this Stop was itself caused by a hook -- do not re-enter

    message = (
        payload.get("last_assistant_message")
        or payload.get("lastAssistantMessage")
        or ""
    )
    if not isinstance(message, str) or len(message) < 40:
        return 0

    session_id = str(payload.get("session_id") or payload.get("sessionId") or "")

    try:
        figures = _unprovenanced(message)
        warns = _warnings(message)[:MAX_WARNINGS]
    except Exception:
        return 0  # a regex blowup must never wedge the session

    if figures and not _already_blocked_this_turn(session_id):
        _set_blocked(session_id)
        shown = ", ".join(dict.fromkeys(figures))[:200]
        sys.stderr.write(
            f"Rule 2 (evidence grain): {len(figures)} figure(s) asserted with no provenance "
            f"-- {shown}. Every figure about the data needs, somewhere in its paragraph, one "
            "of: a file.py:123 citation, a markdown link to a repo file, a band tag such as "
            "[Verified -- <mechanism>] or [Indicative -- no query run], the query that "
            "produced it, or the word 'unverified'. Add the grain or drop the figure -- "
            "unchecked numbers surviving into UI copy is this project's costliest failure "
            "mode (.claude/rules/evidence.md)."
        )
        return 2

    if warns:
        note = "Style (advisory, not blocking): " + "; ".join(warns)
        out = {
            "additionalContext": note,
            "hookSpecificOutput": {"hookEventName": "Stop", "additionalContext": note},
        }
        sys.stdout.write(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
