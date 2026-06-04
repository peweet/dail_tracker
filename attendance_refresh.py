"""attendance_refresh.py — plenary attendance extraction.

Single-step chain today; lives as a chain for consistency and because adding
member-enrichment (the same pattern as payments_refresh) is a one-line change.

    1. attendance     parses bronze Record-of-Attendance PDFs
                      → silver/gold attendance tables

Bronze PDFs are picked up by bootstrap_refresh.step_poll_oireachtas. Run
bootstrap first if there have been new publications.

CLI:
    python attendance_refresh.py
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time

from paths import PROJECT_ROOT as _ROOT

_log = logging.getLogger("attendance_refresh")


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def _subprocess(script: str) -> bool:
    t = time.monotonic()
    r = subprocess.run([sys.executable, script], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_extract() -> bool:
    _hr("[1/1] attendance — plenary attendance PDF parser")
    return _subprocess("attendance.py")


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("attendance_refresh")
    started = time.monotonic()
    failures: list[str] = []
    if not step_extract():
        failures.append("extract")
    _hr(f"[done] attendance_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
