"""ministerial_diaries_refresh.py — diary sandbox → gold TRANSFORM chain.

Promotes the deterministic tail of the ministerial-diary pipeline out of the
manual run-book and into pipeline.py. It runs the pure transforms only:

    1. diary_entry_classify   assign entry_class (govt/oireachtas/media/…)
    2. diary_org_match        org gazetteer lookup -> diary_org_mentions sandbox
    3. diary_lobbying_overlap  distil to defensible signal + lobbying corroboration
    4. diary_promote_gold     sandbox -> gold (minister resolution, state-body split)
    5. diary_company_influence cross-ref gold overlap × procurement awards/payments

WHAT THIS CHAIN DELIBERATELY DOES NOT DO — the EXTRACT and OCR steps upstream
(ministerial_diaries_extract.py + diary_ocr.py) stay manual: gov.ie fronts a WAF
that trips on bot fingerprints (minute-scale cooldown windows the nightly run
can't absorb) and the DPER/Taoiseach scans need off-box GPU OCR. So the diary
working table — ``data/sandbox/enrichment/ministerial_diary_entries.parquet`` —
is produced by hand and is GITIGNORED (not in a clean checkout).

This chain therefore GUARDS on that input: when the sandbox entries table is
absent (every cloud / fresh-checkout run) it logs and exits 0 — a clean no-op,
never a failed chain that would block the gated publish. When the table IS
present (a local box where the extract has been run) it re-derives the gold the
"Who Ministers Meet" page reads. Steps 1–4 are network-free; step 5 reads the
committed procurement gold, so this chain is ordered AFTER procurement_consolidate
in pipeline.py.

CLI:
    python ministerial_diaries_refresh.py
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time

from paths import PROJECT_ROOT as _ROOT

_log = logging.getLogger("ministerial_diaries_refresh")

# The hand-produced, gitignored working table every transform step below reads.
_SANDBOX_ENTRIES = _ROOT / "data" / "sandbox" / "enrichment" / "ministerial_diary_entries.parquet"


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def _subprocess(script: str) -> bool:
    t = time.monotonic()
    r = subprocess.run([sys.executable, script], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def main() -> int:
    argparse.ArgumentParser(description=__doc__.splitlines()[0]).parse_args()
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("ministerial_diaries_refresh")

    if not _SANDBOX_ENTRIES.exists():
        _hr("[skip] ministerial_diaries_refresh — diary sandbox absent")
        print(
            "  data/sandbox/enrichment/ministerial_diary_entries.parquet not found.\n"
            "  The diary EXTRACT + OCR are manual (gov.ie WAF + off-box GPU OCR) and the\n"
            "  working table is gitignored, so this transform chain is a no-op here.\n"
            "  Run extractors/ministerial_diaries_extract.py on a box with a warm WAF\n"
            "  window (and the off-box OCR merge) to populate it, then re-run this chain."
        )
        return 0

    started = time.monotonic()
    failures: list[str] = []
    # Ordered: classify -> match -> overlap -> promote (steps 1-4 network-free),
    # then company_influence (reads committed procurement gold). A step's failure
    # is recorded but the chain presses on so one bad step doesn't mask the rest.
    for label, script in [
        ("[1/5] diary_entry_classify", "extractors/diary_entry_classify.py"),
        ("[2/5] diary_org_match", "extractors/diary_org_match.py"),
        ("[3/5] diary_lobbying_overlap", "extractors/diary_lobbying_overlap.py"),
        ("[4/5] diary_promote_gold", "extractors/diary_promote_gold.py"),
        ("[5/5] diary_company_influence", "extractors/diary_company_influence.py"),
    ]:
        _hr(f"{label} — {script}")
        if not _subprocess(script):
            failures.append(label.split("] ", 1)[1])

    _hr(f"[done] ministerial_diaries_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
