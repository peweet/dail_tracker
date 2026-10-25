#!/usr/bin/env python
"""PostToolUse hook — warn when an edit removes a test assertion or adds a skip marker.

.claude/rules/tests.md says assertions and baselines are off limits: fix the code, or stop and
ask. Nothing enforced that mechanically. The gold-quality guards protect data baselines and the
expected-failures ledger needs a reason, but a deleted `assert` or a new `pytest.mark.skip`
in a test file reached CI unremarked. This hook compares the edited file with its HEAD version
and says what moved, once per distinct state per session.

It counts whole-file totals, not diff hunks: a line diff pairs moved lines arbitrarily (an
unchanged `assert` reads as removed-and-added when a decorator lands above it), while totals
cannot be fooled by movement. A reworded assertion that keeps the count is NOT detected; that
judgement stays with the reviewer.

Advisory (additionalContext), never a block: a legitimate test refactor also drops assert
lines, and whether this one is legitimate is for the model and the user (guardrail
determinism tiers). Fails open to silence.
"""

from __future__ import annotations

import hashlib
import json
import re
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

_TEST_NAME = re.compile(r"(^test_.*\.py$|_test\.py$|^conftest\.py$)", re.I)
_BASELINE_NAME = re.compile(r"quality|baseline|regression|expected_failures", re.I)
_ASSERT = re.compile(r"^\s*(assert\b|self\.assert\w*\(|with pytest\.raises\(|pytest\.fail\()")
_SKIP = re.compile(r"pytest\.mark\.(skip|xfail)|pytest\.skip\(|pytest\.importorskip\(|unittest\.skip")
_BROAD_EXCEPT = re.compile(r"^\s*except\s*(Exception|BaseException)?\s*:\s*$")


def _classify(path: Path) -> str:
    parts = {p.lower() for p in path.parts}
    if _TEST_NAME.search(path.name) or "test" in parts or "tests" in parts:
        return "test"
    if "_meta" in parts and _BASELINE_NAME.search(path.name):
        return "baseline"
    if path.name == "expected_failures.yaml":
        return "baseline"
    return ""


def _head_version(path: Path) -> str | None:
    """The committed (HEAD) content of `path`, or None when untracked or git is unavailable."""
    try:
        root = subprocess.run(
            ["git", "-C", str(path.parent), "rev-parse", "--show-toplevel"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
            check=False,
        )
        if root.returncode != 0:
            return None
        top = Path(root.stdout.strip()).resolve()
        rel = path.resolve().relative_to(top).as_posix()
        out = subprocess.run(
            ["git", "-C", str(top), "show", f"HEAD:{rel}"],
            capture_output=True,
            encoding="utf-8",
            errors="replace",
            timeout=8,
            check=False,
        )
        return out.stdout if out.returncode == 0 else None
    except Exception:
        return None


def _counts(text: str) -> tuple[Counter[str], int, int]:
    asserts: Counter[str] = Counter()
    skips = broad = 0
    for line in text.splitlines():
        if _ASSERT.match(line):
            asserts[line.strip()] += 1
        if _SKIP.search(line):
            skips += 1
        if _BROAD_EXCEPT.match(line):
            broad += 1
    return asserts, skips, broad


def analyse(head_text: str, new_text: str) -> dict:
    """Net change in assertions, skip markers and broad excepts from HEAD to the edited text."""
    h_asserts, h_skips, h_broad = _counts(head_text)
    n_asserts, n_skips, n_broad = _counts(new_text)
    return {
        "removed": list((h_asserts - n_asserts).elements()),
        "removed_count": max(0, sum(h_asserts.values()) - sum(n_asserts.values())),
        "skips": max(0, n_skips - h_skips),
        "broad": max(0, n_broad - h_broad),
    }


def _once(session: str, key: str) -> bool:
    try:
        marker = Path(tempfile.gettempdir()) / f"dail_assert_guard_{session[:12]}_{key}"
        if marker.exists():
            return False
        marker.write_text("1")
        return True
    except Exception:
        return False


def message(path: Path, kind: str, found: dict) -> str:
    rel = str(path).replace("\\", "/")
    if kind == "baseline":
        return (
            f"[assertion-guard] {rel} is a recorded baseline. Per .claude/rules/tests.md a baseline moves only "
            "with data-anchored evidence and the user's agreement, in its own commit that says what moved and why."
        )
    bits = []
    if found["removed_count"]:
        shown = "; ".join(found["removed"][:2])[:160]
        bits.append(f"{found['removed_count']} assertion line(s) removed vs HEAD ({shown})")
    if found["skips"]:
        bits.append(f"{found['skips']} skip/xfail marker(s) added")
    if found["broad"]:
        bits.append(f"{found['broad']} bare/broad except added")
    return (
        f"[assertion-guard] {rel}: " + "; ".join(bits) + ". Assertions and baselines are off limits "
        "(.claude/rules/tests.md): fix the code under test, or stop and ask. An expected failure goes through "
        "test/expected_failures.yaml with a reason. If this is a deliberate refactor that keeps the seam asserted, "
        "say so in the report."
    )


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
        ti = payload.get("tool_input") or {}
        fp = ti.get("file_path") if isinstance(ti, dict) else None
        if not fp:
            return 0
        path = Path(str(fp))
        kind = _classify(path)
        if not kind or not path.is_file():
            return 0
        head = _head_version(path)
        if head is None:
            return 0
        new = path.read_text(encoding="utf-8", errors="replace")
        if kind == "baseline":
            if head == new:
                return 0
            found = {"removed": [], "removed_count": 0, "skips": 0, "broad": 0}
        else:
            found = analyse(head, new)
            if not (found["removed_count"] or found["skips"] or found["broad"]):
                return 0
        session = str(payload.get("session_id") or "nosession")
        sig = hashlib.sha1(
            f"{path}|{found['removed_count']}|{found['skips']}|{found['broad']}|{kind}".encode()
        ).hexdigest()[:10]
        if not _once(session, sig):
            return 0
        print(
            json.dumps(
                {
                    "hookSpecificOutput": {
                        "hookEventName": "PostToolUse",
                        "additionalContext": message(path, kind, found),
                    }
                }
            )
        )
    except Exception:
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
