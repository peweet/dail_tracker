#!/usr/bin/env python
"""Stop hook — block unprovenanced claims; warn on jargon, long sentences and long replies.

Turns .claude/rules/communication.md Rule 2 ("no claim without its band") into
deterministic enforcement for the one case evidence.md calls this project's costliest
failure mode: a claim stated with no citation, no confidence band, and no admission that
it wasn't checked. Those are the claims that survive into UI copy and audits.

TWO categories block, split by what can discharge them (widened 2026-08-08 after an audit
found the original check blind to the claim shape a whole week of work was using):

  * about OUR data — money, percentages, comma-grouped integers AND bare counts of
    corpus things ("67 nodes"). Discharged by a repo citation, a band, a shown query,
    or the word 'unverified'.
  * about the WORLD — what a statute does, what an external body requires. A repo file
    or memory card does NOT discharge one of these: it is [Reported] at best. Only a web
    source or an explicit admission does. Check the internet, cite the URL.

Jargon (Rule 3), sentence length (Rule 4) and reply length (Rule 1) are LOGGED SILENTLY
to logs/style_lint_log.jsonl (demoted from per-reply advisory 2026-07-25) and surfaced as
a weekly digest by session_context.py — a linter aggressive enough to force a rewrite on
style teaches the agent to write evasively around the checker rather than plainly. See
feedback_guardrail_determinism_tiers in memory: determinism is inverse to consequence.

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
# p90 of 574 real replies measured from the session transcripts 2026-08-08 (p50=82,
# p75=238, p90=406, p95=504). Logged, never blocking: a deliverable is legitimately long
# and the register rules exempt it, so a hard cap here would fire on the wrong thing.
LONG_REPLY_WORDS = 400
BLOCK_COOLDOWN_S = 300  # a block marker older than this is stale, not a live turn

# Figures that read as a claim about the data. Narrow on purpose: currency amounts,
# thousands-separated integers, and percentages. A bare 4-digit year never matches.
FIGURE_RE = re.compile(
    r"(?:[€£$]\s?\d[\d,.]*\s?(?:bn|m|k)?\b)"  # €1.08bn, $250, £4,958
    r"|(?:\b\d{1,3}(?:,\d{3})+\b)"  # 4,958   1,083
    r"|(?:\b\d+(?:\.\d+)?\s?%)",  # 91%   35.9 %
    re.IGNORECASE,
)

# --- count claims (added 2026-08-08) -------------------------------------
# FIGURE_RE only ever matched money, comma-grouped integers and percentages, so a bare
# integer under 1,000 was invisible to it -- and that is the shape of EVERY figure in an
# engine/corpus audit ("67 nodes", "116 layers", "27 councils"). A whole week of
# capability claims passed a live checker that had nothing to match. Measured over 574
# real replies from the session transcripts: this fires on 7.0% of them, and the sampled
# hits were all genuine corpus claims.
#
# The noun list is the precision mechanism: it names things the CORPUS contains, so
# "3 hooks" or "2 files" (about the code or the conversation) never matches. The
# lookbehind keeps date fragments out -- '2026-08-02 cases' is not a count claim.
DATA_NOUNS = (
    r"nodes?|layers?|cases?|councils?|datasets?|views?|columns?|rows?|records?|tables?"
    r"|members?|TDs?|questions?|speeches?|votes?|divisions?|suppliers?|notices?|tenders?"
    r"|awards?|payments?|applications?|decisions?|precedents?|casebooks?|entries|entities"
    r"|constituencies|parties|committees?|bills?|amendments?|authorities|bodies|schemes?"
)
#   (?<![-/\d.])        date fragments out: '2026-08-02 cases' is not a count claim
#   (?!(?:19|20)\d{2}\b) a year qualifying a noun ('2026 planning applications') is not one either
#   (?:\s+[a-z][\w-]*){0,2}  adjectives between number and noun -- '67 assessment nodes',
#                            '116 registered layers'. Requiring adjacency missed both.
COUNT_RE = re.compile(
    rf"(?<![-/\d.])\b(?!(?:19|20)\d{{2}}\b)\d{{1,4}}(?:\s+[a-z][\w-]*){{0,2}}\s+(?:{DATA_NOUNS})\b",
    re.IGNORECASE,
)

# --- real-world assertions (added 2026-08-08) ----------------------------
# A claim about the world outside this repo -- what a statute does, what an external body
# requires -- cannot be discharged by a repo citation. A doc/*.md or a memory card saying
# "s.247(3) bars evaluative output" is [Reported] at best; the source of law is the source
# of law. These need a web source or an explicit admission that they are unchecked.
#
# Fires only where a legal REFERENCE and an EFFECT claim share a paragraph: a bare mention
# ("the s.247 card") is meta-talk, not an assertion. Measured over the same 574 replies:
# 8 paragraphs qualify, 5 already carry a web source or an admission, 3 would block
# (0.52% of replies) and all 3 are genuine unsourced legal claims.
LEGAL_REF_RE = re.compile(
    r"\b(?:Act|Regulations?|Directive|Statutory Instrument)\s+\d{4}\b"
    r"|\bS\.?I\.?\s*(?:No\.?\s*)?\d+\s+of\s+\d{4}\b"
    r"|\bs\.\s?\d+(?:\(\d+\))?\b",
    re.IGNORECASE,
)
EFFECT_RE = re.compile(
    # legal OPERATION -- the verbs these claims actually use, taken from the measured corpus
    r"\b(?:bars?|barred|prohibits?|forbids?|requires?|mandates?|obliges?|entitles?"
    r"|permits?|allows?|exempts?|applies to|covers?|governs?|imposes?)\b"
    # lifecycle
    r"|\b(?:repealed|commenced|uncommenced|enacted|amended|revoked|superseded|substituted"
    r"|came into (?:force|operation)|in force|deprecated|discontinued|withdrawn)\b",
    re.IGNORECASE,
)
# Only these discharge a real-world claim. Bare domains count -- "galway.preplanning.ie/en/terms"
# is a source even without a scheme.
WEB_SOURCE_RE = re.compile(
    r"https?://"
    r"|\b[\w-]+(?:\.[\w-]+)*\.(?:ie|com|org|gov|eu|uk|net|int)\b(?:/\S*)?"
    r"|\bweb search\b|\bvia search\b|\bsearched the web\b",
    re.IGNORECASE,
)
ADMISSION_RE = re.compile(
    r"\bunverified\b|\bhaven't (?:checked|verified|confirmed)\b|\bnot (?:verified|confirmed)\b"
    r"|\bbest reasoned guess\b|\[(?:Indicative|Reported|Verified|Extracted)\s*[—\-–]"
    r"|\bneeds? (?:to be )?check|\bneed to check\b|\blet me check\b|\bworth checking\b"
    r"|\b(?:lawyer|solicitor)\b|\bdon't have a confirmed\b|\bno confirmed answer\b"
    r"|\bI don't know\b|\bbefore I use it\b",
    re.IGNORECASE,
)

# Self-referential / mechanical numbers -- about the conversation or the code, not about
# the corpus. Matched against the ~40 chars following the figure.
NOT_A_DATA_CLAIM = re.compile(
    r"^\W{0,3}(?:tokens?|lines?|bytes?|chars?|characters?|words?|ms|px|rows? of code"
    r"|files?|docs?|pages?|rules?|seconds?|kb|mb|gb"
    r"|figures?)\b",  # 'the 78% figure' -- meta-talk about a number, not the number
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
    "has implications for",
    "the tension here is",
    "is a token multiplier",
    "operationalize",
    "operationalise",
    "utilize",
    "utilise",
    "leverage",
    "commence",
    "consequently",
    "approximately",
    "facilitate",
    "surfaces",
    "primitive",
    "topology",
    "interrogate",
    "simply",
    "easily",
    "obviously",
    "essentially",
    "basically",
    "actually",
    "very",
    "quite",
    "really",
    "please note",
    "at this time",
    "it's worth noting",
    "it is worth noting",
    "it's important to note",
    "powerful",
    "seamless",
    "a variety of",
    "in order to",
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
    keep = [ln for ln in text.splitlines() if not ln.lstrip().startswith((">", "|"))]
    return "\n".join(keep)


def _paragraphs(text: str) -> list[str]:
    return [p for p in re.split(r"\n\s*\n", text) if p.strip()]


def _unprovenanced(message: str) -> list[str]:
    """Figures and counts asserted with no citation, band, query, or admission."""
    hits: list[str] = []
    for para in _paragraphs(message):
        if DISCHARGE_RE.search(para):  # checked against the RAW paragraph
            continue
        prose = _strip_noncheckable(para)
        for rx in (FIGURE_RE, COUNT_RE):
            for m in rx.finditer(prose):
                tail = prose[m.end() : m.end() + 40]
                if NOT_A_DATA_CLAIM.match(tail):
                    continue
                hits.append(m.group(0).strip())
    return hits


def _unsourced_world_claims(message: str) -> list[str]:
    """Claims about the world outside the repo, with no web source and no admission.

    Deliberately NOT discharged by DISCHARGE_RE: a repo file or memory card is not the
    source of law, and citing one for a statutory claim is the failure this catches.
    """
    hits: list[str] = []
    for para in _paragraphs(message):
        if not (LEGAL_REF_RE.search(para) and EFFECT_RE.search(para)):
            continue
        if WEB_SOURCE_RE.search(para) or ADMISSION_RE.search(para):
            continue
        hits.extend(m.group(0).strip() for m in LEGAL_REF_RE.finditer(_strip_noncheckable(para)))
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
    total = len(prose.split())
    if total > LONG_REPLY_WORDS:
        out.append(f"{total}-word reply (rule 1 -- answer at the size of the question)")
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

    message = payload.get("last_assistant_message") or payload.get("lastAssistantMessage") or ""
    if not isinstance(message, str) or len(message) < 40:
        return 0

    session_id = str(payload.get("session_id") or payload.get("sessionId") or "")

    try:
        figures = _unprovenanced(message)
        world = _unsourced_world_claims(message)
        warns = _warnings(message)[:MAX_WARNINGS]
    except Exception:
        return 0  # a regex blowup must never wedge the session

    if (figures or world) and not _already_blocked_this_turn(session_id):
        _set_blocked(session_id)
        parts = []
        if figures:
            shown = ", ".join(dict.fromkeys(figures))[:200]
            parts.append(
                f"Rule 2 (evidence grain): {len(figures)} figure(s) asserted with no provenance "
                f"-- {shown}. Every figure about the data needs, somewhere in its paragraph, one "
                "of: a file.py:123 citation, a markdown link to a repo file, a band tag such as "
                "[Verified -- <mechanism>] or [Indicative -- no query run], the query that "
                "produced it, or the word 'unverified'. Add the grain or drop the figure -- "
                "unchecked numbers surviving into UI copy is this project's costliest failure "
                "mode (.claude/rules/evidence.md)."
            )
        if world:
            shown = ", ".join(dict.fromkeys(world))[:120]
            parts.append(
                f"Rule 2 (real-world assertion): {len(world)} claim(s) about external law or "
                f"published fact with no external source -- {shown}. A repo file or memory card "
                "does NOT discharge these; it is [Reported] at best, and the source of law is "
                "the source of law. Check the internet and cite the URL, or write 'unverified'."
            )
        sys.stderr.write(" ".join(parts))
        return 2

    if warns:
        # DEMOTED 2026-07-25 from per-reply advisory to a silent weekly log.
        # Grounds: Guardrails-Beat-Guidance (arXiv:2604.11088) — prescriptive style
        # directives are the rule class where measured harm concentrates, and the
        # per-reply nudge forced rewrite turns (5 in one session on 07-25) that cost
        # more than the prose they fixed. The provenance BLOCK above is untouched —
        # that one prevents this project's costliest failure mode. The log is
        # surfaced as a one-line digest at SessionStart at most once per week
        # (session_context._style_digest_note).
        try:
            import datetime

            log = os.path.join(
                os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
                "logs",
                "style_lint_log.jsonl",
            )
            os.makedirs(os.path.dirname(log), exist_ok=True)
            with open(log, "a", encoding="utf-8") as fh:
                fh.write(
                    json.dumps(
                        {
                            "ts": datetime.datetime.now().isoformat(timespec="seconds"),
                            "session": session_id[:12],
                            "warns": warns,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        except Exception:
            pass  # telemetry only — never surface, never break the turn
    return 0


if __name__ == "__main__":
    sys.exit(main())
