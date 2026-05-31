"""lobbying_refresh.py — lobbying.ie + CRO + charities register chain.

CRO and charities live in this chain because the lobbying org profile is their
primary UI consumer (CRO/Charity enrichment is what populates the funding
profile, trustee count, dominant income source etc. on each org card).

    1. lobbying_poller          fetch YTD lobbying CSV (~80s; hash-skip on no-change)
    2. lobby_processing         flatten CSV to silver
    3. lobbying_pdf_extract     extract embedded PDF URLs from return free-text
    4. cro_normalise            CRO bulk export → silver
    5. charity_normalise        Charities Regulator XLSX → silver
    6. charity_resolved         CRO ⨝ charity Tier-A join
    7. charity_enriched         gold charity table: NACE sector + compliance flags

Steps 4–7 are independent of steps 1–3 but only useful as a unit.

CLI:
    python lobbying_refresh.py
"""
from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parent
_log = logging.getLogger("lobbying_refresh")


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def _subprocess(script: str) -> bool:
    t = time.monotonic()
    r = subprocess.run([sys.executable, script], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_poll() -> bool:
    _hr("[1/7] lobbying_poller — YTD lobbying.ie CSV fetch")
    return _subprocess("lobbying_poller.py")


def step_process() -> bool:
    _hr("[2/7] lobby_processing — CSV → silver")
    return _subprocess("lobby_processing.py")


def step_pdf_extract() -> bool:
    _hr("[3/7] lobbying_pdf_extract — embedded PDF URLs from return free-text")
    return _subprocess("lobbying_pdf_extract.py")


def step_cro_normalise() -> bool:
    _hr("[4/7] cro_normalise — CRO bulk export → silver")
    return _subprocess("cro_normalise.py")


def step_charity_normalise() -> bool:
    _hr("[5/7] charity_normalise — Charities Regulator XLSX → silver")
    return _subprocess("charity_normalise.py")


def step_charity_resolved() -> bool:
    _hr("[6/7] charity_resolved — CRO ⨝ charity Tier-A join")
    return _subprocess("charity_resolved.py")


def step_charity_enriched() -> bool:
    _hr("[7/7] charity_enriched — gold charity table with NACE + compliance flags")
    return _subprocess("charity_enriched.py")


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    started = time.monotonic()
    failures: list[str] = []
    for name, fn in [
        ("poll", step_poll),
        ("process", step_process),
        ("pdf_extract", step_pdf_extract),
        ("cro_normalise", step_cro_normalise),
        ("charity_normalise", step_charity_normalise),
        ("charity_resolved", step_charity_resolved),
        ("charity_enriched", step_charity_enriched),
    ]:
        if not fn():
            failures.append(name)
    _hr(f"[done] lobbying_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
