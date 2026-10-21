"""Preflight guard — the engine-validation records agree with the artefacts they summarise.

WHY THIS EXISTS
    On 2026-10-21 `planning/product/doc/validation_claims.json` (the ledger CLAUDE.md says to
    quote) was found still carrying the 2026-08-21 numbers (94.9% / 91.6%), a month after
    the regression baselines had been re-saved (91.2% / 82.9%). Nothing ran the ledger's own
    `check`, so the stale figures were quoted as current. In the same month 67 commits touched
    the engine and the catalogue with no outcome panel re-run, and the harness changed the
    evening after the baselines were saved, which makes the regression gate INCOMPARABLE
    without anyone noticing.

WHAT IT DOES
    - private tree ABSENT (public-only clone, or CI): nothing to check, exit 0.
    - runs `planning/product/tools/sentinels/validation_claims.py check` — exit 1 on drift
      (the fix is its `save`, seconds, after reading the diff it prints);
    - runs `planning/product/tools/sentinels/measurement_debt.py` — a WARNING line, exit 0,
      unless `--strict`, which fails above MEASUREMENT_DEBT_FAIL_ABOVE engine commits.
    - `--note`: one line for the SessionStart status (never fails).

Run:  python tools/dev.py validation-records
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SENTINELS = REPO_ROOT / "planning" / "product" / "tools" / "sentinels"
LEDGER = SENTINELS / "validation_claims.py"
DEBT = SENTINELS / "measurement_debt.py"
MEASUREMENT_DEBT_FAIL_ABOVE = 25


def _run(script: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(script), *args], capture_output=True, text=True, timeout=120, check=False, cwd=REPO_ROOT
    )


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    note_only = "--note" in argv
    strict = "--strict" in argv

    if os.environ.get("GITHUB_ACTIONS") == "true":
        if not note_only:
            print("validation-records: skipped on CI — the private tree is never checked out there.")
        return 0
    if not SENTINELS.is_dir():
        if not note_only:
            print(
                f"validation-records: skipped — no private tree at {SENTINELS.relative_to(REPO_ROOT)} "
                "(public-only clone). Nothing to check."
            )
        return 0

    if note_only:
        ledger = _run(LEDGER, "check")
        debt = _run(DEBT, "--note")
        parts = []
        if ledger.returncode != 0:
            parts.append(
                "validation ledger DRIFTED from the saved baselines — `python tools/dev.py validation-records`"
            )
        line = debt.stdout.strip().splitlines()[-1] if debt.stdout.strip() else ""
        if line:
            parts.append(line)
        print(" · ".join(parts))
        return 0

    rc = 0
    ledger = _run(LEDGER, "check")
    sys.stdout.write(ledger.stdout)
    sys.stderr.write(ledger.stderr)
    if ledger.returncode != 0:
        print(
            "validation-records: the ledger disagrees with the saved baselines; read the diff above, then "
            "`python planning/product/tools/sentinels/validation_claims.py save`."
        )
        rc = 1

    debt_args = ["--fail-above", str(MEASUREMENT_DEBT_FAIL_ABOVE)] if strict else []
    debt = _run(DEBT, *debt_args)
    sys.stdout.write(debt.stdout)
    sys.stderr.write(debt.stderr)
    if debt.returncode != 0:
        rc = 1
    return rc


if __name__ == "__main__":
    sys.exit(main())
