"""iris_refresh.py — one-shot Iris refresh: poll + silver delta + derived gold.

Chains the steps that have to happen together to keep the Iris-derived gold
parquets (and the Streamlit pages reading them) in sync:

    1. iris_oifigiuil_poller        fetch any new Tue/Fri PDFs into bronze
    2. iris_silver_rebuild          delta-rebuild the silver notice CSVs
    3. si_entity_enrichment         -> data/gold/parquet/statutory_instruments.parquet
    4. iris_si_bill_enrichment      -> data/gold/parquet/bill_statutory_instruments.parquet
    5. public_appointments enrichment -> data/gold/parquet/public_appointments.parquet
    6. corporate_notices_enrichment -> data/gold/parquet/corporate_notices.parquet
    7. si_legislation_directory_extract -> data/gold/parquet/si_current_state.parquet
                                        (eISB legal-state: revoked/amended per SI)
    8. si_lrc (classlist extract + build) -> data/gold/parquet/si_lrc_enrichment_summary.parquet
                                        (LRC subject classification; discovery only)

Steps 3-5 are the ones that quietly went stale after the silver rebuild on
2026-05-31 (the live regression that prompted this script). Each step is
independent: any can be skipped, any failure logs and continues so the next
step still gets a chance. Step 7 crawls the eISB Legislation Directory and is
freshness-gated (only re-crawls years whose "Updated to" date moved), so a
steady-state run adds only ~11 cheap index requests.

CLI:
    python iris_refresh.py                 # all seven steps
    python iris_refresh.py --skip-poll     # only refresh from existing bronze
    python iris_refresh.py --skip-silver   # only refresh derived gold from current silver
    python iris_refresh.py --skip-derived  # poll + silver only
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time

from paths import PROJECT_ROOT as _ROOT

_log = logging.getLogger("iris_refresh")


def _hr(label: str) -> None:
    print(f"\n{'─' * 74}\n{label}\n{'─' * 74}")


def step_poll() -> bool:
    _hr("[1/7] iris_oifigiuil_poller — fetch new PDFs into bronze")
    t = time.monotonic()
    r = subprocess.run([sys.executable, "iris_oifigiuil_poller.py"], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_silver() -> bool:
    _hr("[2/7] iris_silver_rebuild — delta from bronze to silver")
    t = time.monotonic()
    try:
        from iris_silver_rebuild import rebuild_silver_from_bronze

        rebuild_silver_from_bronze()
    except Exception as exc:
        _log.exception("silver rebuild failed: %s", exc)
        return False
    print(f"  done in {time.monotonic() - t:.1f}s")
    return True


def step_si_gold() -> bool:
    _hr("[3/7] si_entity_enrichment — statutory_instruments.parquet")
    t = time.monotonic()
    try:
        # import-and-call avoids subprocess overhead; si_entity_enrichment.run()
        # is the same entry point its __main__ uses.
        import si_entity_enrichment

        si_entity_enrichment.run()
    except Exception as exc:
        _log.exception("si_entity_enrichment failed: %s", exc)
        return False
    print(f"  done in {time.monotonic() - t:.1f}s")
    return True


def step_bill_si_gold() -> bool:
    _hr("[4/7] iris_si_bill_enrichment — bill_statutory_instruments.parquet")
    t = time.monotonic()
    try:
        import iris_si_bill_enrichment

        iris_si_bill_enrichment.run()
    except Exception as exc:
        _log.exception("iris_si_bill_enrichment failed: %s", exc)
        return False
    print(f"  done in {time.monotonic() - t:.1f}s")
    return True


def step_appointments_gold() -> bool:
    _hr("[5/7] public_appointments_enrichment — public_appointments.parquet")
    t = time.monotonic()
    # Subprocess (own __main__/argparse, not set up to import as a library).
    # Pass --write so it persists the parquet.
    script = _ROOT / "public_appointments_enrichment.py"
    r = subprocess.run([sys.executable, str(script), "--write"], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_corporate_gold() -> bool:
    _hr("[6/7] corporate_notices_enrichment — corporate_notices.parquet")
    t = time.monotonic()
    script = _ROOT / "corporate_notices_enrichment.py"
    r = subprocess.run([sys.executable, str(script), "--write"], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_si_legal_state() -> bool:
    _hr("[7/8] si_legislation_directory_extract — si_current_state.parquet (eISB legal-state)")
    t = time.monotonic()
    # Subprocess: the crawler has its own __main__/argparse and lives in
    # extractors/. Freshness-gated by default — it re-checks each year
    # index and only re-crawls range pages whose "Updated to" date moved, so a
    # steady-state run is ~11 cheap requests rather than a full ~150-page crawl.
    script = _ROOT / "extractors" / "si_legislation_directory_extract.py"
    r = subprocess.run([sys.executable, str(script)], cwd=_ROOT)
    print(f"  done in {time.monotonic() - t:.1f}s (exit {r.returncode})")
    return r.returncode == 0


def step_si_lrc_enrichment() -> bool:
    _hr("[8/8] si_lrc — si_lrc_enrichment_summary.parquet (LRC subject classification)")
    t = time.monotonic()
    # Two pipeline-invoked extractors: the classlist fetch+parse
    # (36 polite requests, bronze-cached) then the deterministic gold build over
    # current SI gold. DISCOVERY/CLASSIFICATION only — never asserts legal status.
    extract = _ROOT / "extractors" / "si_lrc_classlist_extract.py"
    build = _ROOT / "extractors" / "si_lrc_enrichment_build.py"
    rc = 0
    for script in (extract, build):
        r = subprocess.run([sys.executable, str(script)], cwd=_ROOT)
        rc = rc or r.returncode
    print(f"  done in {time.monotonic() - t:.1f}s (exit {rc})")
    return rc == 0


def main() -> int:
    from services.logging_setup import setup_standalone_logging

    setup_standalone_logging("iris_refresh")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument(
        "--skip-poll", action="store_true", help="skip the iris_oifigiuil_poller step (use existing bronze)"
    )
    ap.add_argument(
        "--skip-silver",
        action="store_true",
        help="skip the silver delta-rebuild (refresh derived gold from current silver)",
    )
    ap.add_argument(
        "--skip-derived", action="store_true", help="skip the derived gold enrichments (poll + silver only)"
    )
    args = ap.parse_args()

    started = time.monotonic()
    failures: list[str] = []

    if not args.skip_poll and not step_poll():
        failures.append("poll")
    if not args.skip_silver and not step_silver():
        failures.append("silver")
    if not args.skip_derived:
        if not step_si_gold():
            failures.append("si_gold")
        if not step_bill_si_gold():
            failures.append("bill_si_gold")
        if not step_appointments_gold():
            failures.append("appointments_gold")
        if not step_corporate_gold():
            failures.append("corporate_gold")
        if not step_si_legal_state():
            failures.append("si_legal_state")
        if not step_si_lrc_enrichment():
            failures.append("si_lrc_enrichment")

    _hr(f"[done] iris_refresh complete in {time.monotonic() - started:.1f}s")
    if failures:
        print(f"  FAILED steps: {', '.join(failures)}")
        return 1
    print("  all steps succeeded.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
