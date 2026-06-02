"""legislation_refresh.py — bills / questions / amendments / votes / enrich chain.

Everything in this chain reads JSON written by bootstrap_refresh.step_members_api,
so it MUST run after bootstrap. transform_votes must precede enrich (enrich.py
reads silver/pretty_votes.csv).

    1. legislation              bills + sponsors + stages + related docs
    2. questions                paginated parliamentary questions → silver
    3. bill_amendments_flatten  amendment text + sponsors
    4. transform_votes          divisions + per-TD vote patterns → silver
    5. enrich                   cross-dataset enrichment over silver outputs

CLI:
    python legislation_refresh.py
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time

from paths import PROJECT_ROOT as _ROOT

_log = logging.getLogger("legislation_refresh")


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def _subprocess(script: str) -> bool:
    t = time.monotonic()
    r = subprocess.run([sys.executable, script], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_legislation() -> bool:
    _hr("[1/5] legislation — bills + sponsors + stages")
    return _subprocess("legislation.py")


def step_questions() -> bool:
    _hr("[2/5] questions — parliamentary questions → silver")
    return _subprocess("questions.py")


def step_bill_amendments() -> bool:
    _hr("[3/5] bill_amendments_flatten — amendment text + sponsors")
    return _subprocess("bill_amendments_flatten.py")


def step_transform_votes() -> bool:
    _hr("[4/5] transform_votes — divisions + per-TD vote patterns")
    return _subprocess("transform_votes.py")


def step_enrich() -> bool:
    _hr("[5/5] enrich — cross-dataset enrichment over silver outputs")
    return _subprocess("enrich.py")


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    started = time.monotonic()
    failures: list[str] = []
    for name, fn in [
        ("legislation", step_legislation),
        ("questions", step_questions),
        ("bill_amendments", step_bill_amendments),
        ("transform_votes", step_transform_votes),
        ("enrich", step_enrich),
    ]:
        if not fn():
            failures.append(name)
    _hr(f"[done] legislation_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
