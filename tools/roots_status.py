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

Usage:
    python tools/roots_status.py            # human-readable report
    python tools/roots_status.py --quiet    # only print problems

Exit code 0 when every root is clean and pushed, 1 otherwise.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]

ROOTS: tuple[tuple[str, Path], ...] = (
    ("dail_tracker (public)", ROOT),
    ("planning/product (private)", ROOT / "planning" / "product"),
    ("apps/public-signal (private)", ROOT / "apps" / "public-signal"),
)


@dataclass(frozen=True)
class RootStatus:
    label: str
    path: Path
    exists: bool
    dirty_count: int
    ahead: int
    behind: int
    remote: str
    error: str = ""


def _run(path: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(path), *args],
        capture_output=True,
        text=True,
        check=False,
    )
    return result.stdout.strip()


def check_root(label: str, path: Path) -> RootStatus:
    if not (path / ".git").exists():
        return RootStatus(label, path, exists=False, dirty_count=0, ahead=0, behind=0, remote="")

    dirty_count = len([line for line in _run(path, "status", "--short").splitlines() if line.strip()])
    remote = _run(path, "remote", "get-url", "origin") or "(no remote)"

    ahead, behind = 0, 0
    counts = _run(path, "rev-list", "--left-right", "--count", "@{u}...HEAD")
    if counts:
        parts = counts.split()
        if len(parts) == 2 and all(p.isdigit() for p in parts):
            behind, ahead = int(parts[0]), int(parts[1])

    return RootStatus(label, path, exists=True, dirty_count=dirty_count, ahead=ahead, behind=behind, remote=remote)


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

        clean = status.dirty_count == 0 and status.ahead == 0
        if clean and args.quiet:
            continue

        marker = "OK" if clean else "!!"
        print(f"[{marker}] {label}  ({status.remote})")
        if status.dirty_count:
            print(f"      {status.dirty_count} uncommitted change(s)")
            problems += 1
        if status.ahead:
            print(f"      {status.ahead} commit(s) not pushed")
            problems += 1
        if status.behind:
            print(f"      {status.behind} commit(s) behind remote")

    if problems == 0 and not args.quiet:
        print("All roots clean and pushed.")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
