"""payments_refresh.py — Parliamentary Standard Allowance refresh.

    1. payments_full_psa_etl              parses TAA + PRA PDFs
                                          → data/gold/parquet/payments_full_psa.parquet
    2. payments_member_enrichment         adds unique_member_code / party_name /
                                          constituency to the gold parquet
                                          → fixes "Not on file" hero on Member Overview

Bronze PDFs are picked up by bootstrap_refresh.step_poll_oireachtas. Run
bootstrap first if there have been new publications.

CLI:
    python payments_refresh.py
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_log = logging.getLogger("payments_refresh")


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def _subprocess(script: str) -> bool:
    t = time.monotonic()
    r = subprocess.run([sys.executable, script], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_extract() -> bool:
    _hr("[1/2] payments_full_psa_etl — TAA + PRA payments parser")
    return _subprocess("payments_full_psa_etl.py")


def step_member_enrichment() -> bool:
    _hr("[2/2] payments_member_enrichment — add unique_member_code / party / constituency")
    return _subprocess("payments_member_enrichment.py")


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    started = time.monotonic()
    failures: list[str] = []
    for name, fn in [
        ("extract", step_extract),
        ("member_enrichment", step_member_enrichment),
    ]:
        if not fn():
            failures.append(name)
    _hr(f"[done] payments_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
