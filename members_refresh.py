"""members_refresh.py — member-profile enrichment chain.

Wikidata pulls + long-format committee unpivot. All three steps read silver
flattened-members and produce derived per-member artefacts. Each step is
isolated so a transient Wikidata outage can't poison the others.

    1. wikidata_socials_etl       Twitter/X + Wikipedia links per member
                                  → consumed by Member Overview hero chips
    2. ministerial_tenure_build   minister-of-the-day table
                                  → consumed by iris_refresh.step_si_gold
    3. committees_long_format_etl unpivots wide committee_N_*/office_N_*
                                  columns into long-format parquets
                                  → consumed by the Committees page

Run AFTER bootstrap (all steps need flattened_members.parquet) and BEFORE iris
(iris consumes ministerial_tenure for SI signatory attribution).

CLI:
    python members_refresh.py
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_log = logging.getLogger("members_refresh")


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def _subprocess(script: str) -> bool:
    t = time.monotonic()
    r = subprocess.run([sys.executable, script], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_wikidata_socials() -> bool:
    _hr("[1/3] wikidata_socials_etl — member external links")
    return _subprocess("wikidata_socials_etl.py")


def step_ministerial_tenure() -> bool:
    _hr("[2/3] ministerial_tenure_build — Wikidata minister-of-the-day table")
    return _subprocess("ministerial_tenure_build.py")


def step_committees_long_format() -> bool:
    _hr("[3/3] committees_long_format_etl — committee_N_* / office_N_* unpivot")
    return _subprocess("committees_long_format_etl.py")


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    started = time.monotonic()
    failures: list[str] = []
    for name, fn in [
        ("wikidata_socials", step_wikidata_socials),
        ("ministerial_tenure", step_ministerial_tenure),
        ("committees_long_format", step_committees_long_format),
    ]:
        if not fn():
            failures.append(name)
    _hr(f"[done] members_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
