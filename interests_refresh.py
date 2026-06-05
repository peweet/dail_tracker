"""interests_refresh.py — Register of Members' Interests extraction.

Single-step chain today; lives as a chain for consistency and so a future
member-enrichment step can be added the same way it was for payments.

    1. member_interests   parses bronze Register-of-Interests PDFs
                          → silver/gold member_interests tables

Bronze PDFs are picked up by bootstrap_refresh.step_poll_oireachtas. Run
bootstrap first if there have been new publications.

CLI:
    python interests_refresh.py
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time

from paths import PROJECT_ROOT as _ROOT

_log = logging.getLogger("interests_refresh")


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def _subprocess(script: str) -> bool:
    t = time.monotonic()
    r = subprocess.run([sys.executable, script], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def _module(mod: str) -> bool:
    """Run a packaged step via ``python -m <mod>`` (cwd=root → ``import config``
    resolves). Used for steps that live in a package dir, not at repo root."""
    t = time.monotonic()
    r = subprocess.run([sys.executable, "-m", mod], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_extract() -> bool:
    _hr("[1/1] member_interests — Register of Members' Interests PDF parser")
    return _module("members.member_interests")


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("interests_refresh")
    started = time.monotonic()
    failures: list[str] = []
    if not step_extract():
        failures.append("extract")
    _hr(f"[done] interests_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
