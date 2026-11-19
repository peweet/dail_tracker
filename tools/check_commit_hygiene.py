"""Commit hygiene guards for a working tree shared by concurrent agent sessions.

Called by .githooks/pre-commit and .githooks/commit-msg. Two failure modes, both
observed live on 2026-08-14 (three incidents in one session):

1. SWEEP COMMITS. Several Claude sessions share this one working tree and index, so
   ``git add -A`` / ``commit -a`` stages another session's in-flight work. A wide
   staged set while more than one agent session is running is presumptively a sweep:
   the incident commit bundled 8 files across 6 top-level directories belonging to a
   different session under an unrelated message.

2. LABEL-FREE MESSAGES. "changes", "planning changes", "Checkpoint: ..." make
   bisecting and unbundling impossible exactly when a sweep needs undoing.

Thresholds are tuned against the recorded incidents and this repo's legitimate
commits (largest legitimate same-day commit: 6 files / 2 top dirs; the sweep:
8 files / 6 top dirs). Overrides, one commit each:

    DAIL_ALLOW_SWEEP=1      git commit ...   # deliberate wide commit
    DAIL_ALLOW_LOOSE_MSG=1  git commit ...   # deliberate short message

The guard FAILS OPEN on internal errors: a broken guard must not lock commits, and
the pre-push gate plus CI still stand behind it. What failing open authorises is a
sweep slipping through while the guard itself is broken -- accepted, because the
alternative (failing closed) blocks every commit including the fix to the guard.
"""

from __future__ import annotations

import argparse
import os
import re
import subprocess
import sys
from collections import Counter

SWEEP_FILE_FLOOR = 15  # staged files at/above this = presumptive sweep
SWEEP_DIR_FLOOR = 5  # distinct top-level dirs at/above this = presumptive sweep
MIN_MESSAGE_LEN = 8

# Messages that carry no information. Two-word forms cover the recorded
# "planning changes" / "engine changes" incidents.
_GENERIC_MSG = re.compile(
    r"^(changes?|wip|checkpoint\b.*|fix(es)?|updates?|misc|stuff|tmp|temp|cleanup|commit|asdf+|x+|\.+)$"
    r"|^\S+ (changes?|fixes?|updates?)$",
    re.IGNORECASE,
)
_EXEMPT_MSG = re.compile(r"^(Merge |Revert |fixup!|squash!)")

_AGENT_IMAGES = ("claude.exe", "codex.exe")


def _run(args: list[str]) -> str:
    return subprocess.run(args, capture_output=True, text=True, check=False).stdout


def agent_session_count() -> int:
    """Running agent processes; 0 off Windows (the gate is then inert)."""
    if sys.platform != "win32":
        return 0
    total = 0
    for image in _AGENT_IMAGES:
        out = _run(["tasklist", "/FI", f"IMAGENAME eq {image}", "/FO", "CSV", "/NH"])
        total += sum(1 for line in out.splitlines() if image in line.lower())
    return total


def is_generic_message(first_line: str) -> bool:
    line = first_line.strip()
    if _EXEMPT_MSG.match(line):
        return False
    return bool(_GENERIC_MSG.match(line)) or len(line) < MIN_MESSAGE_LEN


def sweep_verdict(staged: list[str], sessions: int) -> str | None:
    """A reason string when the staged set looks like a parallel-session sweep."""
    if sessions < 2:
        return None
    top_dirs = {p.split("/", 1)[0] for p in staged}
    if len(staged) >= SWEEP_FILE_FLOOR:
        return f"{len(staged)} files staged (floor {SWEEP_FILE_FLOOR})"
    if len(top_dirs) >= SWEEP_DIR_FLOOR:
        return f"{len(top_dirs)} top-level directories staged (floor {SWEEP_DIR_FLOOR})"
    return None


def _in_replay_state() -> bool:
    """Merge/cherry-pick replays legitimately stage wide sets -- exempt them."""
    for ref in ("MERGE_HEAD", "CHERRY_PICK_HEAD", "REVERT_HEAD"):
        if (
            subprocess.run(
                ["git", "rev-parse", "-q", "--verify", ref],
                capture_output=True,
                check=False,
            ).returncode
            == 0
        ):
            return True
    return False


def check_pre_commit() -> int:
    if os.environ.get("DAIL_ALLOW_SWEEP") == "1" or _in_replay_state():
        return 0
    staged = [p for p in _run(["git", "diff", "--cached", "--name-only"]).splitlines() if p]
    reason = sweep_verdict(staged, agent_session_count())
    if reason is None:
        return 0
    print(
        f"pre-commit: BLOCKED as a presumptive parallel-session sweep: {reason}, "
        "with 2+ agent sessions sharing this working tree.\n"
        "Another session's in-flight work may be in your staged set. Staged:",
        file=sys.stderr,
    )
    for p in staged[:15]:
        print(f"  {p}", file=sys.stderr)
    if len(staged) > 15:
        print(f"  ... and {len(staged) - 15} more", file=sys.stderr)
    print(
        "Stage explicit paths you actually edited, or if this wide commit is "
        "deliberate: DAIL_ALLOW_SWEEP=1 git commit ...",
        file=sys.stderr,
    )
    return 1


# Assertion changes (added 2026-11-19). A staged diff that removes or rewrites an `assert` line in an existing test
# is how a change gets made to pass: on 2026-11-17 an existing assertion was inverted and passed two reviews. Such a
# commit must name each test in an `Assertion-Change: <test>: <why>` line. Only the first line of an assert is seen;
# a line moved within one file, or within a renamed file, nets out.
ASSERTION_TRAILER = "Assertion-Change:"
_ASSERT_LINE = re.compile(r"\s*(assert\b|self\.assert\w*\()")  # pytest asserts and unittest assert methods
_TEST_PATHSPECS = (":(glob)**/test_*.py", ":(glob)test/**/*.py")


def removed_assertions(diff: str) -> list[str]:
    """`path: assert ...` for each assert line a unified diff removes from an existing file, net of re-added lines."""
    removed: dict[str, Counter] = {}
    added: dict[str, Counter] = {}
    path, in_hunk = "", False
    for line in diff.splitlines():
        if line.startswith("diff --git "):
            path, in_hunk = "", False
        elif not in_hunk and line.startswith("--- "):
            path = line[6:].rstrip("\t") if line.startswith("--- a/") else ""
        elif line.startswith("@@"):
            in_hunk = True
        elif in_hunk and path and line[:1] in ("-", "+") and _ASSERT_LINE.match(line[1:]):
            side = removed if line[0] == "-" else added
            side.setdefault(path, Counter())[line[1:].strip()] += 1
    return [
        f"{name}: {text}"
        for name, lines in sorted(removed.items())
        for text in sorted((lines - added.get(name, Counter())).elements())
    ]


def check_assertion_changes(message_lines: list[str]) -> int:
    if _in_replay_state() or any(ln.startswith(ASSERTION_TRAILER) for ln in message_lines):
        return 0
    diff = subprocess.run(
        # core.quotepath=false: git would otherwise quote a non-ASCII path and the header would not parse.
        [
            "git",
            "-c",
            "core.quotepath=false",
            "diff",
            "--cached",
            "-U0",
            "--no-color",
            "--no-ext-diff",
            "--diff-filter=MDR",
            "--",
            *_TEST_PATHSPECS,
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    ).stdout
    dropped = removed_assertions(diff)
    if not dropped:
        return 0
    print(
        "commit-msg: BLOCKED -- this commit removes or rewrites existing test assertions:\n  "
        + "\n  ".join(dropped[:10])
        + ("\n  ..." if len(dropped) > 10 else "")
        + "\nAn assertion is never weakened to make a change pass. If the change is deliberate, add one line per "
        f"test to the message: {ASSERTION_TRAILER} <test>: <why the old assertion was wrong>",
        file=sys.stderr,
    )
    return 1


def check_commit_msg(msg_file: str) -> int:
    with open(msg_file, encoding="utf-8", errors="replace") as fh:
        lines = [ln for ln in fh.read().splitlines() if not ln.startswith("#")]
    first = next((ln for ln in lines if ln.strip()), "")
    if os.environ.get("DAIL_ALLOW_LOOSE_MSG") != "1" and is_generic_message(first):
        print(
            f"commit-msg: BLOCKED -- {first.strip()!r} says nothing about the change. "
            "Mislabeled commits are what make parallel-session sweeps unrecoverable "
            "(three live incidents 2026-08-14). Say what the commit does, or for a "
            "deliberate exception: DAIL_ALLOW_LOOSE_MSG=1 git commit ...",
            file=sys.stderr,
        )
        return 1
    return check_assertion_changes(lines)


def _assertion_guard_selftest() -> list[str]:
    inverted = (
        "diff --git a/test/t.py b/test/t.py\n--- a/test/t.py\n+++ b/test/t.py\n@@ -3 +3 @@\n"
        "-    assert verdict() == 1\n+    assert verdict() == 0\n"
    )
    failures = []
    if removed_assertions(inverted) != ["test/t.py: assert verdict() == 1"]:
        failures.append("should flag an inverted assertion")
    if removed_assertions(inverted.replace("== 0", "== 1")):
        failures.append("should allow an assertion moved within its file")
    return failures


def selftest() -> int:
    bad_msgs = ["changes", "planning changes", "engine changes", "wip", "Checkpoint: local work", "x", "fix"]
    good_msgs = [
        "Redact raw DuckDB errors from caller-visible query failures",
        "Merge branch 'feature'",
        'Revert "engine changes RFSA"',
        "Fix typo in siting docstring",
    ]
    failures = []
    for m in bad_msgs:
        if not is_generic_message(m):
            failures.append(f"should block message: {m!r}")
    for m in good_msgs:
        if is_generic_message(m):
            failures.append(f"should allow message: {m!r}")
    wide = [f"dir{i}/f.py" for i in range(6)]
    many = [f"pkg/f{i}.py" for i in range(15)]
    small = ["a/one.py", "b/two.py", "a/three.py"]
    if sweep_verdict(wide, sessions=2) is None:
        failures.append("should block 6 top dirs at 2 sessions")
    if sweep_verdict(many, sessions=2) is None:
        failures.append("should block 15 files at 2 sessions")
    if sweep_verdict(small, sessions=2) is not None:
        failures.append("should allow 3 files / 2 dirs")
    if sweep_verdict(wide, sessions=1) is not None:
        failures.append("should allow anything at 1 session")
    failures += _assertion_guard_selftest()
    for f in failures:
        print(f"SELFTEST FAIL: {f}", file=sys.stderr)
    print(
        f"selftest: {'FAIL' if failures else 'PASS'} ({len(bad_msgs) + len(good_msgs) + 6 - len(failures)} assertions)"
    )
    return 1 if failures else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--pre-commit", action="store_true")
    mode.add_argument("--commit-msg", metavar="MSG_FILE")
    mode.add_argument("--selftest", action="store_true")
    args = parser.parse_args()
    if args.selftest:
        return selftest()
    try:
        if args.pre_commit:
            return check_pre_commit()
        return check_commit_msg(args.commit_msg)
    except Exception as exc:  # noqa: BLE001 -- fail open, never lock commits (see module docstring)
        print(f"commit-hygiene guard errored ({exc!r}); allowing the commit.", file=sys.stderr)
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
