"""Local guard — the private siting PyO3 kernel is present in this venv.

WHY THIS EXISTS
    `siting-native` is the bounded PyO3 junction kernel built from
    planning/product/native/siting_native, which is gitignored (the private
    dail-siting-private repo). It used to be declared in the public pyproject.toml
    as a `[tool.uv.sources]` path dependency. That broke every `uv` invocation on a
    clean public checkout — uv resolves sources eagerly, even for extras nobody
    selected — and took all of public CI red from 2026-08-16 to 2026-08-28:

        error: Failed to generate package metadata for `siting-native==0.1.0`
          Caused by: Distribution not found at: .../planning/product/native/siting_native

    The declaration was removed 2026-08-28. The kernel is now installed manually into
    the shared venv, which means `uv sync` / `uv run --locked` PRUNES it exactly the way
    it prunes the paddle OCR stack. That pruning is silent, and the siting engine only
    notices at import time, mid-run. This check makes it loud and cheap to spot.

WHAT IT DOES
    - private tree ABSENT (public-only clone, or CI): nothing to check, exit 0.
    - private tree PRESENT and the kernel imports: exit 0.
    - private tree PRESENT and the kernel is gone: exit 1 with the reinstall command.

Run:  python tools/dev.py siting-native

NAMING: this file is deliberately NOT called check_siting_native.py. `tools/check_no_private_ip.py`
blocks any tracked PATH containing "siting" (DENY_SUBSTRINGS), and its own comment records the
invariant "NO civic-lane path contains 'siting', so this cannot false-positive". Allowlisting a
convenience tool would have been the first hole in that; renaming costs nothing. The dev.py TASK
is still `siting-native` — task names are not paths, so they do not trip the guard.
"""

from __future__ import annotations

import importlib.util
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
NATIVE_DIR = REPO_ROOT / "planning" / "product" / "native" / "siting_native"
MODULE = "siting_native"
REINSTALL = "uv pip install --no-deps planning/product/native/siting_native"


def main() -> int:
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print("siting-native: skipped on CI — the private tree is never checked out there.")
        return 0

    if not NATIVE_DIR.is_dir():
        print(
            f"siting-native: skipped — no private tree at {NATIVE_DIR.relative_to(REPO_ROOT)} "
            "(public-only clone). Nothing to check."
        )
        return 0

    spec = importlib.util.find_spec(MODULE)
    if spec is not None:
        print(f"OK — {MODULE} is importable ({spec.origin}).")
        return 0

    print(
        f"FAIL — the private tree is present at {NATIVE_DIR.relative_to(REPO_ROOT)} but "
        f"`{MODULE}` is NOT importable in this environment.\n"
        "  A `uv sync` / `uv run --locked` has pruned it: it is deliberately undeclared in the\n"
        "  public pyproject.toml, because declaring it there breaks every clean checkout.\n"
        f"  Reinstall with:\n\n      {REINSTALL}\n\n"
        "  Do NOT re-add it to an extra or to [tool.uv.sources] — that is the 12-day CI outage.",
        file=sys.stderr,
    )
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
