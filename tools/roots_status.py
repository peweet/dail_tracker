#!/usr/bin/env python
"""Report working-tree/push status across the repo's three independent git roots.

WHY THIS EXISTS
    dail_tracker (this checkout) is deliberately multi-root: the commercial siting
    engine (planning/product/) and the PublicSignal product (apps/public-signal/)
    are excluded from the public repo via .gitignore and live as their own nested
    repos with their own private remotes. Root `git status` is blind to both —
    that is by design, not a bug — but it means work can pile up uncommitted or
    unpushed in a nested repo with nothing surfacing it. This script is the single
    place that checks all three at once instead of three separate `cd && git
    status` habits that are easy to forget under one of them.

    See CLAUDE.md "Multi-root git layout" for the roots this checks and why each
    one's remote is public or private.

WORKTREES ARE CHECKOUTS TOO (added 2026-08-14, after this script reported a false
all-clear twice in one session)
    A `git worktree` is a second working tree on the same object store, and every
    command here used to run against the PRIMARY checkout only. Two real cases hid
    behind that on 2026-08-14: three commits shipping the council-resolution spine
    sat in a worktree at C:/tmp/mainwt (a production 500 fix, invisible for days),
    and an abandoned benchmark worktree held a 2026-07-30 line 339 commits divergent
    from main. Neither appeared in this report. Every checkout is now walked.

WHY "ON NO REMOTE" AND NOT "AHEAD OF UPSTREAM"
    The question worth answering is "does this work exist only on this machine?",
    which is exactly `HEAD --not --remotes`. The old ahead-of-@{u} count answered a
    different question and was wrong in both directions: it reported "21 commit(s)
    not pushed" for a branch whose every commit was already on origin/main (pushed
    via `push HEAD:main`, so its own tracking ref lagged), while a detached-HEAD
    worktree has no upstream at all and silently counted zero. A commit that is
    reachable from any remote-tracking ref is safe; one that is not is the only
    thing that can actually be lost.

    Caveat: this reads remote-tracking refs as they are on disk and does not fetch.
    Stale refs make it OVER-report, never under-report — the safe direction for a
    guard whose job is to stop work being lost.

Usage:
    python tools/roots_status.py            # human-readable report
    python tools/roots_status.py --quiet    # only print problems

Exit code 0 when every checkout is clean and published, 1 otherwise.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ROOTS: tuple[tuple[str, Path], ...] = (
    ("dail_tracker (public)", ROOT),
    ("planning/product (private)", ROOT / "planning" / "product"),
    ("apps/public-signal (private)", ROOT / "apps" / "public-signal"),
)


@dataclass(frozen=True)
class CheckoutStatus:
    """One working tree — the primary checkout or any of its `git worktree` siblings."""

    path: Path
    label: str  # branch name, or "detached at <sha>"
    is_primary: bool
    exists: bool
    dirty_count: int
    unpublished: int  # commits reachable from HEAD but from no remote-tracking ref
    behind: int


@dataclass(frozen=True)
class RootStatus:
    label: str
    path: Path
    exists: bool
    remote: str
    checkouts: tuple[CheckoutStatus, ...] = field(default_factory=tuple)


def _run(path: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(path), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def _int(value: str) -> int:
    return int(value) if value.isdigit() else 0


def _worktree_paths(path: Path) -> list[Path]:
    """Every checkout on this root's object store, primary first.

    `--porcelain` is the stable machine format; the human format's columns shift with
    branch-name width and would break on a path containing spaces.
    """
    paths: list[Path] = []
    for line in _run(path, "worktree", "list", "--porcelain").splitlines():
        if line.startswith("worktree "):
            paths.append(Path(line[len("worktree ") :].strip()))
    return paths or [path]


def _checkout_label(path: Path) -> str:
    branch = _run(path, "branch", "--show-current")
    if branch:
        return branch
    return f"detached at {_run(path, 'rev-parse', '--short', 'HEAD') or '?'}"


def _unpublished(path: Path) -> int:
    """Commits reachable from HEAD but from no remote-tracking ref.

    With no remote-tracking refs at all every commit would count as unpublished, which
    is technically true but useless noise for a local-only repo — fall back to the
    upstream comparison there instead.
    """
    if not _run(path, "for-each-ref", "--count=1", "--format=%(refname)", "refs/remotes"):
        counts = _run(path, "rev-list", "--left-right", "--count", "@{u}...HEAD").split()
        return _int(counts[1]) if len(counts) == 2 else 0
    return _int(_run(path, "rev-list", "--count", "HEAD", "--not", "--remotes"))


def check_checkout(path: Path, *, is_primary: bool) -> CheckoutStatus:
    # A worktree whose directory was deleted without `git worktree prune` still lists
    # here; every git command below would fail against it, so say so and move on.
    if not path.exists():
        return CheckoutStatus(path, "(missing)", is_primary, False, 0, 0, 0)

    dirty_count = len([line for line in _run(path, "status", "--short").splitlines() if line.strip()])
    behind_counts = _run(path, "rev-list", "--left-right", "--count", "@{u}...HEAD").split()
    behind = _int(behind_counts[0]) if len(behind_counts) == 2 else 0

    return CheckoutStatus(
        path=path,
        label=_checkout_label(path),
        is_primary=is_primary,
        exists=True,
        dirty_count=dirty_count,
        unpublished=_unpublished(path),
        behind=behind,
    )


def check_root(label: str, path: Path) -> RootStatus:
    if not (path / ".git").exists():
        return RootStatus(label, path, exists=False, remote="")

    remote = _run(path, "remote", "get-url", "origin") or "(no remote)"
    primary, *extra = _worktree_paths(path)
    checkouts = [check_checkout(primary, is_primary=True)]
    checkouts += [check_checkout(p, is_primary=False) for p in extra]
    return RootStatus(label, path, exists=True, remote=remote, checkouts=tuple(checkouts))


def _problems(checkout: CheckoutStatus) -> int:
    if not checkout.exists:
        return 1
    return bool(checkout.dirty_count) + bool(checkout.unpublished)


def _report_checkout(checkout: CheckoutStatus) -> None:
    # The primary checkout's lines stay unprefixed so the common single-checkout report
    # reads exactly as it did before worktrees were walked.
    where = "" if checkout.is_primary else f"worktree {checkout.path} [{checkout.label}]: "
    if not checkout.exists:
        print(f"      {where}directory is gone — run `git worktree prune`")
        return
    if checkout.dirty_count:
        print(f"      {where}{checkout.dirty_count} uncommitted change(s)")
    if checkout.unpublished:
        print(f"      {where}{checkout.unpublished} commit(s) on no remote")
    if checkout.behind:
        print(f"      {where}{checkout.behind} commit(s) behind remote")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true", help="only print roots with a problem")
    args = parser.parse_args()

    problems = 0
    for label, path in ROOTS:
        status = check_root(label, path)
        if not status.exists:
            print(f"[MISSING] {label}: no .git at {path}")
            problems += 1
            continue

        root_problems = sum(_problems(c) for c in status.checkouts)
        problems += root_problems
        if root_problems == 0 and args.quiet:
            continue

        print(f"[{'OK' if root_problems == 0 else '!!'}] {label}  ({status.remote})")
        for checkout in status.checkouts:
            _report_checkout(checkout)

    if problems == 0 and not args.quiet:
        print("All roots clean and pushed.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
