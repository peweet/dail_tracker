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

ACTING, NOT JUST REPORTING (added 2026-08-28, narrowed after worktree audit)
    For a long time this script could only tell you all three roots were dirty; it
    could not do anything about it. `--commit` and `--push` put the multi-root layout
    in the tool instead of in your head, but an action must name one repo and one
    checkout. A commit must also name its intended paths. This is essential in a
    Codex worktree: Git lists the primary checkout first, so an implicit "primary"
    action can otherwise mutate a different task's files. `git add -A` is never used.

    `--push` is deliberately NOT implied by `--commit`. Publishing is a separate,
    explicit act, and the two are kept apart so reviewing what was committed can
    happen in between.

    Neither flag bypasses a root's own hooks. A root whose pre-commit or commit-msg
    guard rejects the commit is reported as blocked and the walk continues to the
    next root — the guards, not this script, decide what is safe to commit.

Usage:
    python tools/roots_status.py
    python tools/roots_status.py --repo siting
    python tools/roots_status.py --repo public --checkout . --commit \
        --path tools/example.py --path test/tools/test_example.py -m "Fix example"
    python tools/roots_status.py --repo siting --checkout planning/product --push
    python tools/roots_status.py --repo public --checkout . --commit \
        --path tools/example.py -m "Fix example" --dry-run

Exit code 0 when every checkout is clean and published (or every requested action
succeeded), 1 otherwise.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath, PureWindowsPath

ROOT = Path(__file__).resolve().parents[1]

ROOTS: tuple[tuple[str, str, Path], ...] = (
    ("public", "dail_tracker (public)", ROOT),
    ("siting", "planning/product (private)", ROOT / "planning" / "product"),
    ("public-signal", "apps/public-signal (private)", ROOT / "apps" / "public-signal"),
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


def _run_checked(path: Path, *args: str) -> tuple[int, str]:
    """Run git and return (exit code, combined output).

    The action paths need the exit code and stderr that `_run` discards — a hook rejecting a
    commit writes its reason to stderr and is the single most important thing to surface here.
    """
    result = subprocess.run(
        ["git", "-C", str(path), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.returncode, (result.stdout + result.stderr).strip()


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


def _normalise_action_paths(paths: list[str]) -> tuple[str, ...]:
    normalised: list[str] = []
    for raw in paths:
        value = raw.replace("\\", "/").strip().rstrip("/")
        windows = PureWindowsPath(value)
        posix = PurePosixPath(value)
        if (
            not value
            or value in {".", "*", "**"}
            or windows.is_absolute()
            or windows.drive
            or posix.is_absolute()
            or ".." in posix.parts
            or any(char in value for char in "*?[")
        ):
            raise ValueError(f"--path must be one bounded relative file or directory, got {raw!r}")
        normalised.append(posix.as_posix())
    return tuple(dict.fromkeys(normalised))


def _selected(path: str, selections: tuple[str, ...]) -> bool:
    normalised = path.replace("\\", "/").strip("/")
    return any(normalised == item or normalised.startswith(item + "/") for item in selections)


def _staged_paths(checkout: Path) -> tuple[str, ...]:
    return tuple(line for line in _run(checkout, "diff", "--cached", "--name-only").splitlines() if line.strip())


def commit_checkout(checkout: CheckoutStatus, message: str, paths: list[str], *, dry_run: bool) -> int:
    """Stage only declared paths in one exact checkout and commit its index."""
    selections = _normalise_action_paths(paths)
    staged_elsewhere = [path for path in _staged_paths(checkout.path) if not _selected(path, selections)]
    if staged_elsewhere:
        print("      REFUSED: the checkout already has staged paths outside this task:")
        for path in staged_elsewhere[:20]:
            print(f"        {path}")
        return 1

    scoped = _run(checkout.path, "status", "--short", "--", *selections).splitlines()
    if not any(line.strip() for line in scoped):
        print("      REFUSED: none of the declared paths has a change to commit")
        return 1

    if dry_run:
        print(f"      would stage and commit only {len(selections)} declared path(s) in {checkout.path}")
        for line in scoped[:20]:
            print(f"        {line}")
        return 0

    code, output = _run_checked(checkout.path, "add", "--", *selections)
    if code != 0:
        print(f"      STAGE FAILED: {output.splitlines()[0] if output else 'git add failed'}")
        return 1

    staged = _staged_paths(checkout.path)
    staged_elsewhere = [path for path in staged if not _selected(path, selections)]
    if staged_elsewhere:
        print("      REFUSED AFTER STAGE: Git index contains paths outside this task")
        return 1
    if not staged:
        print("      REFUSED: declared paths produced no staged change")
        return 1

    code, output = _run_checked(checkout.path, "commit", "-m", message)
    if code != 0:
        # The overwhelmingly likely cause is this root's own pre-commit/commit-msg guard. Print
        # its reason verbatim — a summarised hook message is useless for acting on.
        print("      COMMIT REJECTED (this root's hooks, or nothing to commit):")
        for line in output.splitlines():
            print(f"        {line}")
        print("      Intended paths remain staged for inspection; unrelated paths were not touched.")
        return 1

    print(f"      committed {len(staged)} file(s): {_run(checkout.path, 'rev-parse', '--short', 'HEAD')}")
    return 0


def push_checkout(checkout: CheckoutStatus, *, dry_run: bool) -> int:
    """Push one explicitly selected checkout when it holds unpublished commits."""
    if not checkout.unpublished:
        return 0
    if checkout.label.startswith("detached at"):
        print("      SKIPPED: detached HEAD — push it by hand, deliberately")
        return 1

    if dry_run:
        print(f"      would push {checkout.unpublished} commit(s) from {checkout.path} to origin {checkout.label}")
        return 0

    code, output = _run_checked(checkout.path, "push", "origin", f"HEAD:{checkout.label}")
    if code != 0:
        print("      PUSH REJECTED (this root's pre-push guard, or the remote):")
        for line in output.splitlines():
            print(f"        {line}")
        return 1
    print(f"      pushed {checkout.unpublished} commit(s) to origin/{checkout.label}")
    return 0


def _action_checkout(status: RootStatus, requested: Path) -> CheckoutStatus | None:
    target = requested.resolve()
    return next((checkout for checkout in status.checkouts if checkout.path.resolve() == target), None)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--quiet", action="store_true", help="only print roots with a problem")
    parser.add_argument("--repo", choices=tuple(root[0] for root in ROOTS), help="limit status or action to one repo")
    parser.add_argument(
        "--checkout",
        type=Path,
        help="exact checkout to act on; defaults to the selected repo path in this checkout",
    )
    parser.add_argument(
        "--commit",
        action="store_true",
        help="stage declared --path values and commit one selected repo/checkout (requires -m)",
    )
    parser.add_argument(
        "--path", action="append", default=[], help="intended relative file/directory; repeat as needed"
    )
    parser.add_argument("-m", "--message", help="commit message used for every root committed")
    parser.add_argument(
        "--push",
        action="store_true",
        help="push the explicitly selected repo/checkout; never implied by --commit",
    )
    parser.add_argument("--dry-run", action="store_true", help="print what --commit/--push would do")
    args = parser.parse_args()

    if args.commit and not args.message:
        parser.error("--commit requires -m/--message")
    if args.commit and not args.path:
        parser.error("--commit requires at least one explicit --path")
    if args.message and not args.commit:
        parser.error("-m/--message only applies with --commit")
    if args.path and not args.commit:
        parser.error("--path only applies with --commit")

    acting = args.commit or args.push
    if acting and not args.repo:
        parser.error("--commit/--push require exactly one --repo")
    problems = 0
    roots = [root for root in ROOTS if args.repo is None or root[0] == args.repo]
    for alias, label, path in roots:
        status = check_root(label, path)
        if not status.exists:
            print(f"[MISSING] {label}: no .git at {path}")
            problems += 1
            continue

        root_problems = sum(_problems(c) for c in status.checkouts)
        if root_problems == 0 and args.quiet and not acting:
            problems += root_problems
            continue

        print(f"[{'OK' if root_problems == 0 else '!!'}] {label}  ({status.remote})")
        for checkout in status.checkouts:
            _report_checkout(checkout)

        if acting:
            requested = args.checkout or path
            checkout = _action_checkout(status, requested)
            if checkout is None:
                print(f"      REFUSED: {requested.resolve()} is not a checkout of {alias}")
                problems += 1
                continue
            failures = 0
            if args.commit:
                failures += commit_checkout(checkout, args.message, args.path, dry_run=args.dry_run)
            if args.push:
                refreshed_status = check_root(label, path)
                refreshed_checkout = _action_checkout(refreshed_status, requested)
                if refreshed_checkout is None:
                    failures += 1
                else:
                    failures += push_checkout(refreshed_checkout, dry_run=args.dry_run)
            # Other worktrees stay visible above, but their unrelated state does not make
            # an explicitly scoped action fail. The action result belongs to this checkout.
            root_problems = failures
        problems += root_problems

    if problems == 0 and not args.quiet:
        if acting:
            qualifier = "dry-run validated" if args.dry_run else "completed"
            print(f"Selected checkout action {qualifier}; unrelated reported state was not changed.")
        else:
            print("All roots clean and pushed.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
