#!/usr/bin/env python
"""Stop hook: block a finish that leaves code debt or skips fresh eyes.

One script, three events:

    PostToolUse / SubagentStop
        are advisory events only; they never credit a review.
    Stop
        (a) hygiene: run the code-hygiene gate on the edited .py files; a regression blocks.
        (b) fresh eyes: edited code files not covered by a successful reviewer receipt, once
            they reach verifier_nudge.THRESHOLD, block until a matching review exists.

A block is exit 2 with the reason on stderr. The loop guard is a per-signature counter: after
MAX_BLOCKS blocks of the same signature the debt is written once to logs/code_hygiene_debt.jsonl
and the stop is allowed. stop_hook_active is deliberately ignored, so another Stop hook's block
cannot bypass this one.

Only files that still exist and sit inside a git repository count toward (b); a throwaway script
outside any repo is never "unreviewed".

Switches: DAIL_SKIP_HYGIENE_GATE=1 disables the whole hook. DAIL_HYGIENE_VERIFIER=0 disables
only (b), the reviewer requirement, while (a) still runs and still blocks (mandatory review was
rejected on cost, doc/AGENT_HARNESS.md). Any error, including a failed import, fails open.
"""

from __future__ import annotations

import contextlib
import glob
import hashlib
import json
import os
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import tools.check_code_hygiene as hygiene  # noqa: E402

    # One loader for the sibling hook: the gate already loads verifier_nudge by file path, and a
    # bare `import verifier_nudge` reads as an undeclared package to the dependency check.
    verifier_nudge = hygiene._nudge()
except Exception:  # fail open: main() exits 0 when the gate or the sibling hook cannot load
    verifier_nudge = None
    hygiene = None

MAX_BLOCKS = 2
DUP_BUDGET_SECONDS = 10
_STATE_DIR = Path(tempfile.gettempdir())
_WAIVER_ADVICE = (
    "Simplify the code you changed until the numbers are back at or under the base, or tell the owner "
    "why the increase is warranted (only the owner adds a waiver)."
)


def _state_file(session: str) -> Path:
    return _STATE_DIR / f"dail_code_hygiene_{session[:12]}.json"


def _load_state(session: str) -> dict:
    try:
        data = json.loads(_state_file(session).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_state(session: str, state: dict) -> None:
    path = _state_file(session)
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    with contextlib.suppress(OSError):
        tmp.write_text(json.dumps(state), encoding="utf-8")
        os.replace(tmp, path)


def _receipt_path(session: str) -> Path:
    """The configured receipt, or the session's single receipt name used before 2026-11-19."""
    configured = os.environ.get("DAIL_REVIEW_RECEIPT", "").strip()
    return Path(configured) if configured else _STATE_DIR / f"dail_review_receipt_{session[:12]}.json"


def _receipt_paths(session: str) -> list[Path]:
    """Every receipt for this session: one per checkout (tools/review_receipt.py), or only the configured one.

    A session that edits two repositories needs a receipt for each; one shared name let the second overwrite the
    first, so the stop could never be covered (2026-11-19).
    """
    if os.environ.get("DAIL_REVIEW_RECEIPT", "").strip():
        return [_receipt_path(session)]
    return sorted(_STATE_DIR.glob(f"dail_review_receipt_{glob.escape(session[:12])}*.json"))


def _latest_receipts(session: str) -> list[dict]:
    """The newest receipt for each checkout, so a later FAIL for a checkout withdraws its earlier PASS."""
    latest: dict[str, dict] = {}
    for path in _receipt_paths(session):
        try:
            receipt = json.loads(path.read_text(encoding="utf-8"))
            key = os.path.normcase(str(Path(receipt["checkout"]).resolve(strict=False)))
        except (OSError, KeyError, TypeError, ValueError):
            continue
        if receipt.get("session_id") != session:
            continue
        if key not in latest or str(receipt.get("issued_utc", "")) >= str(latest[key].get("issued_utc", "")):
            latest[key] = receipt
    return list(latest.values())


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _receipt_metadata_matches(receipt: dict, session: str) -> bool:
    return (
        receipt.get("schema_version") == 1
        and receipt.get("session_id") == session
        and receipt.get("reviewer_role") == "reviewer"
        and receipt.get("result") == "PASS"
        and isinstance(receipt.get("acceptance"), str)
        and bool(receipt["acceptance"].strip())
    )


def _reviewed_path_matches(name: str, checkout: Path, reviewed: dict) -> bool:
    path = Path(name).resolve(strict=False)
    try:
        path.relative_to(checkout)
        expected = reviewed.get(str(path).replace("\\", "/"))
        return (
            hygiene.git_root(path.parent) == checkout
            and isinstance(expected, str)
            and path.is_file()
            and _sha256(path) == expected
        )
    except (OSError, ValueError):
        return False


def _reviewed_files(session: str, edited: list[str]) -> set[str]:
    """Return edited paths covered by the current, successful reviewer receipt for their checkout."""
    covered: set[str] = set()
    for receipt in _latest_receipts(session):
        reviewed = receipt.get("reviewed_paths")
        if not _receipt_metadata_matches(receipt, session) or not isinstance(reviewed, dict):
            continue
        checkout = Path(receipt["checkout"]).resolve(strict=False)
        covered |= {name for name in edited if _reviewed_path_matches(name, checkout, reviewed)}
    return covered


def edited_files(session: str) -> list[str]:
    """Edited non-test code files recorded by verifier_nudge for this session."""
    try:
        data = json.loads(verifier_nudge._state_path(session).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return []
    files = data.get("files", []) if isinstance(data, dict) else []
    return [str(f) for f in files]


def live_files(files: list[str]) -> list[str]:
    """Keep files that still exist and sit inside a git repository (root cached per directory)."""
    roots: dict = {}
    out = []
    for name in files:
        path = Path(name)
        if not path.is_file():
            continue
        if path.parent not in roots:
            roots[path.parent] = hygiene.git_root(path.parent)
        if roots[path.parent] is not None:
            out.append(name)
    return out


def is_reviewer_event(payload: dict) -> bool:
    event = payload.get("hook_event_name")
    if event == "SubagentStop":
        return payload.get("agent_type") == "reviewer"
    if event == "PostToolUse" and payload.get("tool_name") in ("Agent", "Task"):
        tool_input = payload.get("tool_input")
        return isinstance(tool_input, dict) and tool_input.get("subagent_type") == "reviewer"
    return False


def _hygiene_result(edited: list[str]) -> dict:
    candidates = [f for f in edited if f.lower().endswith(".py") and Path(f).is_file()]
    if not candidates:
        return {"regressions": [], "improved": [], "checked_files": [], "notes": []}
    return hygiene.gate(candidates, dup_budget_seconds=DUP_BUDGET_SECONDS)


def _signature(headers: list[str], unverified: list[str]) -> str:
    blob = json.dumps([sorted(headers), sorted(unverified)])
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def _reason(detail: list[str], unverified: list[str]) -> str:
    parts = []
    if detail:
        parts.append("Code hygiene regressed against the base:\n" + "\n".join(detail))
        parts.append(_WAIVER_ADVICE)
    if unverified:
        parts.append(
            f"This session edited {len(unverified)} code files no reviewer receipt covers. Run the `reviewer` "
            "agent, then issue a successful receipt for each checkout with `python -m tools.review_receipt --help`.\n"
            + "\n".join(f"  {f}" for f in unverified)
        )
    return "\n".join(parts)


def _log_debt(session: str, headers: list[str], unverified: list[str]) -> None:
    base = Path(os.environ.get("CLAUDE_PROJECT_DIR") or Path.cwd())
    row = {"ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "session": session}
    row.update({"regressions": headers, "unverified": unverified})
    with contextlib.suppress(OSError):
        (base / "logs").mkdir(parents=True, exist_ok=True)
        with (base / "logs" / "code_hygiene_debt.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row) + "\n")


def _decide(session: str, state: dict, headers: list[str], unverified: list[str], detail: list[str]) -> int:
    sig = _signature(headers, unverified)
    blocks = state.setdefault("blocks", {})
    if blocks.get(sig, 0) < MAX_BLOCKS:
        blocks[sig] = blocks.get(sig, 0) + 1
        _save_state(session, state)
        print(_reason(detail, unverified), file=sys.stderr)
        return 2
    logged = state.setdefault("debt_logged", [])
    if sig not in logged:
        _log_debt(session, headers, unverified)
        logged.append(sig)
        _save_state(session, state)
    return 0


def handle_stop(session: str) -> int:
    edited = live_files(edited_files(session))
    if not edited:
        return 0
    state = _load_state(session)
    verified = _reviewed_files(session, edited)
    unverified = sorted(f for f in edited if f not in verified)
    if len(edited) < verifier_nudge.THRESHOLD or os.environ.get("DAIL_HYGIENE_VERIFIER") == "0":
        unverified = []
    result = _hygiene_result(edited)
    incomplete = [note for note in result.get("notes", []) if "not checked" in note]
    headers = hygiene.regression_lines(result) + incomplete
    if not headers and not unverified:
        return 0
    return _decide(session, state, headers, unverified, hygiene.format_result(result) if headers else [])


def dispatch(payload: dict) -> int:
    session = str(payload.get("session_id") or "nosession")
    if is_reviewer_event(payload):
        # Agent/SubagentStop is not evidence of a review. Only a receipt that binds
        # the current content and acceptance result can credit paths at Stop.
        return 0
    if payload.get("hook_event_name") == "Stop":
        return handle_stop(session)
    return 0


def main() -> int:
    if os.environ.get("DAIL_SKIP_HYGIENE_GATE") == "1" or hygiene is None or verifier_nudge is None:
        return 0
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        return dispatch(payload) if isinstance(payload, dict) else 0
    except Exception:
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
