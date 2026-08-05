"""Classify a Git push as an allow-listed data-refresh-only change.

This is deliberately conservative. A classification failure, an empty diff, or
any path outside ``tools.publish_data.PUBLISH_PATHS`` returns ``false`` so the
full code CI remains the default.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from collections.abc import Iterable
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.publish_data import PUBLISH_PATHS  # noqa: E402


def is_publish_path(path: str) -> bool:
    """Whether one repo-relative path is inside the unattended publish scope."""
    normalized = path.replace("\\", "/").lstrip("./")
    return any(normalized == root or normalized.startswith(f"{root.rstrip('/')}/") for root in PUBLISH_PATHS)


def is_data_refresh_only(paths: Iterable[str]) -> bool:
    """True only for a non-empty diff made entirely of publishable data files."""
    changed = [path for path in paths if path]
    return bool(changed) and all(is_publish_path(path) for path in changed)


def paths_between(base: str, head: str) -> list[str] | None:
    """Return changed paths, failing closed for renames or an unknown Git status."""
    result = subprocess.run(
        ["git", "diff", "--name-status", "-z", "-M", base, head],
        capture_output=True,
        check=False,
        text=True,
    )
    if result.returncode:
        return None

    tokens = iter(token for token in result.stdout.split("\0") if token)
    paths: list[str] = []
    for status in tokens:
        if status.startswith(("R", "C")):
            return None
        if status not in {"A", "D", "M", "T"}:
            return None
        try:
            paths.append(next(tokens))
        except StopIteration:
            return None
    return paths


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", required=True, help="Git revision before the push")
    parser.add_argument("--head", required=True, help="Git revision after the push")
    args = parser.parse_args(argv)

    changed = paths_between(args.base, args.head)
    print("true" if changed is not None and is_data_refresh_only(changed) else "false")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
