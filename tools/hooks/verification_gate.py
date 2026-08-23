#!/usr/bin/env python
"""Stop hook — block a claim that a test/check run PASSED when the transcript disagrees.

closeout_gate.py forces the ACT of recording a closeout; style_lint.py forces a claim to
carry SOME citation or band. Neither ever asks the one question that matters for a
"tests pass" claim: did a run actually happen, did it pass, and was it still current when
the claim was made. This hook closes that gap.

Adapted from lesson s17 of shareAI-lab/learn-claude-code, whose goal loop calls a separate
tool-less model to judge "is the goal met?" against the transcript, explicitly instructed
not to assume an unreported command succeeded, and on failure appends a synthetic message
telling the agent to surface the missing evidence. The IDEA is kept; the model call is not.
A Stop hook fires every turn, so a per-turn API call would tax every stop and make a
blocking control non-deterministic -- and per feedback_guardrail_determinism_tiers,
determinism is inverse to consequence: a hook that interrupts the agent must be decidable
from the transcript alone. The three states below are that judgement, made mechanically.

WHAT BLOCKS -- a success claim about a RUN, in one of three evidence states. Each rule was
narrowed by replaying it over 509 real transcripts (see "Measured" below); the loose form
of each is recorded at its definition so the next reader knows what was already tried.
  * NO_RUN  -- no test/lint/typecheck command anywhere in this session's transcript
  * FAILED  -- EVERY run in the session reported failures. Not "the last run failed": a
               reply summarising several suites can be telling the truth about a green one
               while a later, unrelated suite is red.
  * STALE   -- a source file was edited after the last run AND nothing has been re-run
               since, so the green demonstrably predates the current code.

Measured over 509 transcripts / 19,535 assistant messages: 491 replies (2.51%) make a run
claim at all, and 70 (0.36%) would block -- NO_RUN 22, FAILED 17, STALE 31. That sits just
under style_lint.py's own measured 0.52% block rate, which is the precedent for how loud a
Stop-hook block is allowed to be here.

WHAT DOES NOT BLOCK (measured, not guessed -- see the numbers below):
  * The bare word "verified"/"confirmed"/"fixed". Measured over 19,520 real assistant
    messages in 509 transcripts: "verified" appears in 19.95% of replies but 14.81% are
    this project's own `[Verified -- file.py:12]` evidence-band syntax, and "confirmed"
    (7.28%) / "fixed" (5.80%) are overwhelmingly about facts and code, not run outcomes.
    Matching any of them would fire on roughly one reply in five, almost all wrongly.
    Only claims about a RUN are in scope: tests_pass 2.05%, lint_clean 0.76%,
    ran_and_passed 0.49%, suite_green 0.39%.
  * A claim already honestly down-banded as `[Reported`/`[Extracted`/`[Indicative`. Those
    say out loud that the run was not reproduced here, which is the admission this gate
    exists to force -- blocking them would punish the honest phrasing.
  * A hypothetical or forward-looking mention ("let me run the suite", "once CI is green",
    "they passed before the fix too"). MODAL_RE below carries that exclusion.

Deliberately NOT a discharge: a `[Verified` tag. style_lint.py accepts any citation as
compliance because its question is "is this claim sourced at all". This hook's question is
narrower and evidence.md defines the answer: `Verified` means "reproduced THIS SESSION
against real code/data", so `[Verified -- tests pass]` with no run in the transcript is
precisely the false claim being caught. Citing does not discharge it; running does.

Escape hatch: DAIL_SKIP_VERIFICATION_GATE=1 when a claim is legitimately about a run made
outside this transcript (another session, a subagent, CI) and down-banding it is wrong.

Exit contract: 0 = allow, 2 = block with the reason on stderr. Fails open on every error
path, like the other hooks here. Blocks at most once per turn (marker file, mtime-bounded)
and never re-fires on the forced continuation (stop_hook_active).
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
import time

# --- tuning ---------------------------------------------------------------
# Largest transcript on this box is 21.5 MB (509 files, p90 3.19 MB, median 0.86 MB),
# so a full streaming pass fits the hook timeout. Past the budget we fail open rather
# than risk a slow Stop: a missed block costs less than a hung turn.
MAX_TRANSCRIPT_BYTES = 48 * 1024 * 1024
BLOCK_COOLDOWN_S = 300  # a marker older than this is a stale turn, not a live one

# Source edits that invalidate a green run. Prose and notes deliberately excluded --
# editing a .md after the suite passed does not make the suite stale. Config suffixes
# (.toml/.yaml/.cfg) were in this list in v1 and measured as a false-positive source:
# a pyproject.toml touch blocked a claim about an unrelated, genuinely-green suite.
SOURCE_SUFFIXES = (".py", ".ts", ".tsx", ".js", ".jsx", ".sql")

# Commands whose output is a verification result. Widened after the v1 replay showed
# `npm run typecheck` / `vitest` / `eslint` runs going unseen, which mislabelled real
# green runs as NO_RUN -- the worst failure mode this hook can have.
VERIFY_CMD_RE = re.compile(
    r"\b(?:pytest|ruff\s+check|ruff\s+format|mypy|pyright|tsc\b|vitest|jest|eslint|"
    r"dev\.py\s+(?:check|verify|test)|tox|unittest|cargo\s+test|go\s+test|"
    r"(?:npm|pnpm|yarn)\s+(?:test|run\s+(?:test|lint|check|typecheck|tsc))"
    r"|nox|behave|playwright\s+test)\b",
    re.IGNORECASE,
)

# Claims that a RUN came back clean. Narrow by measurement (see docstring).
CLAIM_RE = re.compile(
    r"\b(?:all\s+)?(?:the\s+)?tests?\s+(?:now\s+)?(?:pass|passed|passing)\b"
    r"|\btests?\s+are\s+green\b"
    r"|\b(?:test\s+suite|suite|checks?|build|CI|gates?)\s+(?:is|are)\s+(?:now\s+)?(?:green|passing|clean)\b"
    r"|\b(?:ruff|lint|mypy|typecheck|type\s*check)\w*\s+(?:is\s+)?(?:clean|passes|passed|green)\b",
    re.IGNORECASE,
)

# Forward-looking / hypothetical / historical-contrast context. Measured against the real
# samples: "let me run the final full test pass", "once the running suite is green",
# "those tests passed before the fix too" are all discussion, not a success claim.
# `let'?s` and the `to <verb>` purpose clauses were added after the v1 replay blocked
# "Now let's run the full GIS test suite again to confirm the new tests pass".
MODAL_RE = re.compile(
    r"\b(?:let\s+me|let'?s|let\s+us|I'?ll|I\s+will|we'?ll|going\s+to|about\s+to|next\s+step|"
    r"should|would|could|might|if|once|when|until|after|waiting|expect|hope|"
    r"to\s+(?:confirm|check|verify|prove|see)|"
    r"before\s+the\s+fix|used\s+to|previously|nothing\s+ran|no\s+longer)\b",
    re.IGNORECASE,
)

# Confessional / mixed-outcome context. The v1 replay's worst false positives were the
# agent CRITICISING its own green run -- "tests passed because I exempted the node",
# "my stash test passed by luck", "57/57 then pytest crashed". That is precisely the
# honest self-report this project wants; blocking it would train the opposite behaviour.
SELF_CRITICAL_RE = re.compile(
    r"\b(?:because|by\s+luck|by\s+accident|only\s+because|even\s+though|despite|"
    r"misleading|falsely|exempt(?:ed|ing)?|masked|hid|crashed|but\s+then|then\s+\w+\s+crashed|"
    r"vacuous(?:ly)?|trivially|no-?op|did\s+not\s+actually|never\s+actually)\b",
    re.IGNORECASE,
)

# Causative / infinitive constructions where "tests pass" is a generic object, not a
# report: "a fake store can MAKE a test pass", "enough TO GET the suite green".
PRE_GUARD_RE = re.compile(
    r"\b(?:make|makes|made|making|let|lets|letting|get|gets|got|getting|keep|keeps|"
    r"want|wants|need|needs|help|helps|ensure|ensures|watch|see|whether|so\s+that)\s+"
    r"(?:\w+\s+){0,3}$",
    re.IGNORECASE,
)

# An honest lower band already admits the run was not reproduced here.
DOWNBAND_RE = re.compile(r"\[\s*(?:Reported|Extracted|Indicative)\b", re.IGNORECASE)

# Failure markers win over pass markers: "2 failed / 2586 passed" is a FAILED run.
#
# The zero-guards are load-bearing. v1 used a bare `\d+\s+error(?:s)?`, which matched the
# SUCCESS output of the two tools most often run here -- mypy's "Found 0 errors" and any
# "0 failed" summary -- and silently reclassified clean runs as failures. That single
# regex produced most of v1's 65 FAILED hits. Counts must be non-zero to mean anything.
FAIL_COUNT_RE = re.compile(
    r"\b(?!0\b)\d+\s+(?:failed|errors?)\b|\bFound\s+(?!0\b)\d+\s+error",
    re.IGNORECASE,
)
# Case-SENSITIVE on purpose: pytest prints "FAILED test_x.py::test_y" and a "== ERRORS ==="
# banner in caps. Lower-case "error" appears constantly in ordinary prose and log noise.
FAIL_MARK_RE = re.compile(r"\bFAILED\b|={2,}\s*ERRORS\s*={2,}|\bTraceback\b|\bAssertionError\b|\bexit\s+code\s+[1-9]\b")
PASS_RE = re.compile(
    r"\b\d+\s+passed\b|\ball\s+checks\s+passed\b|\bno\s+issues\s+found\b"
    r"|\bSuccess:\s+no\s+issues\b|\b0\s+failed\b|\bFound\s+0\s+errors?\b",
    re.IGNORECASE,
)


def run_failed(text: str) -> bool:
    return bool(FAIL_COUNT_RE.search(text) or FAIL_MARK_RE.search(text))


def new_state() -> dict:
    """Evidence state. Passing and failing runs are tracked SEPARATELY on purpose.

    v2 kept only "the last run and its outcome", which conflated suites: a session that
    ran suite A green, then ran suite B red, was judged to have "failed" even when the
    reply's claim was about A. Measured, that shape was the bulk of the FAILED bucket
    ("Gates at close: 80 tests pass, full private suite 1,199 passed"). Holding both
    indices lets the decision ask the sharper question below.
    """
    return {
        "verify_seen": False,
        "last_pass_idx": -1,
        "last_fail_idx": -1,
        "last_verify_idx": -1,
        "last_verify_cmd": "",
        "last_fail_cmd": "",
        "last_source_edit_idx": -1,
        "last_source_edit": "",
        "truncated": False,
    }


def apply_block(state: dict, b: dict, idx: int, pending: dict) -> None:
    """Advance evidence state for one content block. Shared by the hook and its replay."""
    btype = b.get("type")
    if btype == "tool_use":
        name = b.get("name") or ""
        inp = b.get("input") or {}
        if name in ("Bash", "PowerShell"):
            cmd = str(inp.get("command", ""))
            if VERIFY_CMD_RE.search(cmd):
                state["verify_seen"] = True
                state["last_verify_idx"] = idx
                state["last_verify_cmd"] = cmd.strip()[:160]
                tid = b.get("id")
                if isinstance(tid, str):
                    pending[tid] = (idx, cmd.strip()[:160])
        elif name in ("Edit", "Write", "MultiEdit", "NotebookEdit"):
            fp = str(inp.get("file_path", "")).replace("\\", "/")
            if fp.lower().endswith(SOURCE_SUFFIXES):
                state["last_source_edit_idx"] = idx
                state["last_source_edit"] = fp.rsplit("/", 1)[-1]
    elif btype == "tool_result":
        tid = b.get("tool_use_id")
        if isinstance(tid, str) and tid in pending:
            run_idx, cmd = pending.pop(tid)
            text = _result_text(b)
            if b.get("is_error") or run_failed(text):
                if run_idx > state["last_fail_idx"]:
                    state["last_fail_idx"] = run_idx
                    state["last_fail_cmd"] = cmd
            elif PASS_RE.search(text):
                state["last_pass_idx"] = max(state["last_pass_idx"], run_idx)


def _safe_id(session_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_-]", "", session_id or "nosession")[:64]


def _marker_path(session_id: str) -> str:
    return os.path.join(tempfile.gettempdir(), f"dail_verifgate_{_safe_id(session_id)}.flag")


def _blocked_recently(session_id: str) -> bool:
    path = _marker_path(session_id)
    try:
        if not os.path.exists(path):
            return False
        return (time.time() - os.path.getmtime(path)) < BLOCK_COOLDOWN_S
    except Exception:
        return False


def _mark_blocked(session_id: str) -> None:
    try:
        with open(_marker_path(session_id), "w", encoding="utf-8") as fh:
            fh.write(str(time.time()))
    except Exception:
        pass


def _paragraphs(text: str):
    for para in re.split(r"\n\s*\n", text or ""):
        if para.strip():
            yield para


def find_claim(message: str) -> str:
    """Return the offending claim sentence, or "" when the message makes no run-claim.

    Scoped to the paragraph so an honest band elsewhere in a long reply cannot launder an
    unbanded claim, mirroring style_lint.py's per-paragraph grain.
    """
    for para in _paragraphs(message):
        m = CLAIM_RE.search(para)
        if not m:
            continue
        if DOWNBAND_RE.search(para):
            continue
        # Sentence-level checks: keep the claim only if it is asserted flatly, is not
        # the agent criticising its own run, and is not a causative construction.
        start = para.rfind(".", 0, m.start()) + 1
        end = para.find(".", m.end())
        sentence = para[start : end if end != -1 else len(para)]
        if MODAL_RE.search(sentence) or SELF_CRITICAL_RE.search(sentence):
            continue
        if PRE_GUARD_RE.search(para[max(0, m.start() - 60) : m.start()]):
            continue
        return sentence.strip()
    return ""


def _blocks(msg: dict):
    content = msg.get("content")
    if isinstance(content, list):
        for b in content:
            if isinstance(b, dict):
                yield b


def _result_text(block: dict) -> str:
    content = block.get("content")
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return " ".join((c.get("text") or "") for c in content if isinstance(c, dict) and c.get("type") == "text")
    return ""


def scan_transcript(path: str) -> dict:
    """Single streaming pass. Returns last verify run, its outcome, and last source edit."""
    state = new_state()
    pending: dict[str, int] = {}  # tool_use_id -> line index, for verify runs awaiting a result
    try:
        if os.path.getsize(path) > MAX_TRANSCRIPT_BYTES:
            state["truncated"] = True
            return state
    except Exception:
        state["truncated"] = True
        return state

    idx = 0
    with open(path, encoding="utf-8", errors="replace") as fh:
        for line in fh:
            idx += 1
            try:
                o = json.loads(line)
            except Exception:
                continue
            msg = o.get("message") or {}
            for b in _blocks(msg):
                apply_block(state, b, idx, pending)
    return state


def decide(claim: str, state: dict) -> tuple[str, str]:
    """Return (verdict, detail). verdict is "" when the claim is supported or undecidable.

    Order matters: the cheapest and least ambiguous evidence state is checked first, and
    anything undecidable falls through to "allow". This hook only ever blocks on a fact
    it can point at in the transcript.
    """
    if state.get("truncated"):
        return "", ""
    if not state["verify_seen"]:
        return "NO_RUN", "no test/lint/typecheck run appears anywhere in this session's transcript"

    last_pass = state["last_pass_idx"]
    last_fail = state["last_fail_idx"]

    # FAILED fires only when the session has NO passing run to point at. If any run went
    # green, a reply summarising several suites ("80 use-class tests pass, full suite
    # 1,199 passed") may be telling the truth about the green one, and a later red run on
    # a different suite is not evidence against it. Measured: requiring zero passing runs
    # is what separates the genuine "claimed green, everything was red" case from a
    # multi-suite summary.
    if last_fail > last_pass and last_pass == -1:
        return "FAILED", f"every run in this session reported failures -- `{state['last_fail_cmd']}`"

    if last_pass == -1:
        return "", ""  # nothing conclusive either way: fail open rather than guess

    # STALE compares the edit against the last run OF ANY OUTCOME, not the last PASSING
    # run. If anything was re-run after the edit, this stays quiet even when that run's
    # output was unparseable -- Claude Code truncates long tool results and can cut
    # pytest's summary line, and an unread summary must never be scored as "not green".
    # Measured: comparing against last_pass_idx instead inflated STALE from 22 to 56,
    # and the extra hits were re-run sessions whose output simply did not parse.
    if state["last_source_edit_idx"] > state["last_verify_idx"]:
        return (
            "STALE",
            f"`{state['last_source_edit']}` was edited AFTER the last run "
            f"(`{state['last_verify_cmd']}`), and nothing has been re-run since, so that "
            "green predates the current code",
        )
    return "", ""


def main() -> int:
    if os.environ.get("DAIL_SKIP_VERIFICATION_GATE") == "1":
        return 0
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0
    if not isinstance(payload, dict):
        return 0
    if payload.get("stop_hook_active") or payload.get("stopHookActive"):
        return 0

    try:
        message = payload.get("last_assistant_message") or payload.get("lastAssistantMessage") or ""
        if not isinstance(message, str) or not message.strip():
            return 0
        claim = find_claim(message)
        if not claim:
            return 0

        session_id = str(payload.get("session_id") or payload.get("sessionId") or "")
        if _blocked_recently(session_id):
            return 0

        path = ""
        for k in ("transcript_path", "transcriptPath", "transcript"):
            v = payload.get(k)
            if isinstance(v, str) and v:
                path = v
                break
        if not path or not os.path.exists(path):
            return 0

        verdict, detail = decide(claim, scan_transcript(path))
        if not verdict:
            return 0

        _mark_blocked(session_id)
        sys.stderr.write(
            f"Verification gate ({verdict}): the reply claims a run came back clean, but "
            f"{detail}.\n\n"
            f'  claim: "{claim[:220]}"\n\n'
            "Per .claude/rules/evidence.md, `Verified` means reproduced THIS SESSION against "
            "real code or data -- citing the claim does not discharge it, running it does. "
            "Do ONE of:\n"
            "  1. Run the check now and report its real output.\n"
            "  2. Down-band the claim to what actually happened -- `[Reported -- <who ran it>]` "
            "or `[Indicative -- not run this session]` -- which this gate accepts.\n"
            "  3. If the run genuinely happened outside this transcript (another session, a "
            "subagent, CI), say so explicitly in the claim.\n"
            "Fires once per turn; continuing without complying will not block again."
        )
        return 2
    except Exception:
        return 0


if __name__ == "__main__":
    sys.exit(main())
