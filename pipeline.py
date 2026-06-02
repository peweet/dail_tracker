"""Dáil Tracker data pipeline orchestrator.

Runs domain refresh chains in the default order below. Each chain is a
self-contained ``<domain>_refresh.py`` script that orchestrates its own
step sequence (poll → extract → enrich) and prints progress to stdout.

This file is a thin dispatcher around the chains. Per-chain logs land at
``logs/runs/<run_id>/steps/NN_<slug>.log``, the manifest records each chain,
and chain-level try/except keeps one flaky source from poisoning the rest.

Default order:

    bootstrap → members → payments → attendance → seanad → interests
                                          → lobbying → iris → legislation

Cross-chain dependencies (run upstream first if you `--select` standalone):

    * every chain assumes bootstrap has refreshed flattened_members.parquet
    * iris.step_si_gold assumes members.ministerial_tenure has run
    * legislation assumes bootstrap.members_api has fetched questions/votes JSON

CLI:

    python pipeline.py                            # full run
    python pipeline.py --list                     # show chains and exit
    python pipeline.py --select iris              # only iris
    python pipeline.py --select members,iris      # subset, comma-separated
    python pipeline.py --exclude lobbying         # everything except lobbying
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
from pathlib import Path

from manifest import (
    create_run_manifest,
    record_step_finished,
    record_step_started,
    run_finished_at,
)
from services.logging_setup import setup_logging
from services.run_paths import ENV_RUN_ID, make_run_id, run_dir, step_log_path

# Domain refresh chains in the default execution order. Each tuple is
# (chain_name, script_path). Chain name is used by --select/--exclude.
CHAINS: list[tuple[str, str]] = [
    ("bootstrap", "bootstrap_refresh.py"),
    ("members", "members_refresh.py"),
    ("payments", "payments_refresh.py"),
    ("attendance", "attendance_refresh.py"),
    ("seanad", "seanad_refresh.py"),
    ("interests", "interests_refresh.py"),
    ("lobbying", "lobbying_refresh.py"),
    ("iris", "iris_refresh.py"),
    ("legislation", "legislation_refresh.py"),
    # cbi runs last: its corporate-notices xref joins gold corporate_notices
    # (produced by iris) against the CBI register extract. Skips re-download
    # when the source PDFs are cached, so routine runs are extract+xref only.
    ("cbi", "pipeline_sandbox/cbi_registers_extract.py"),
    # freshness runs last: it reads the silver + gold the chains above produced
    # and writes data/_meta/freshness.json (the data-age signal the Streamlit
    # badge + scheduled report read). Pure read — never mutates pipeline data.
    ("freshness", "tools/check_freshness.py"),
]

_CHAIN_BLURBS: dict[str, str] = {
    "bootstrap": "shared inputs: poll PDFs + Members API + flatten members & debates",
    "members": "Wikidata socials + ministerial tenure + committees long-format",
    "payments": "Parliamentary Standard Allowance: PSA ETL + member enrichment",
    "attendance": "plenary attendance PDF extraction",
    "seanad": "Seanad parity: votes + payments + attendance + gold (reuses Dáil parsers)",
    "interests": "Register of Members' Interests PDF extraction",
    "lobbying": "lobbying.ie YTD + CRO + charities Tier-A + gold enrichment",
    "iris": "Iris Oifigiúil: poller + silver + SI/appointments/notices gold",
    "legislation": "bills + questions + amendments + votes + cross-dataset enrich",
    "cbi": "CBI register extract + corporate-notices xref (gold)",
    "freshness": "data-age signal per domain -> data/_meta/freshness.json",
}

_SUMMARY_SKIP_PREFIXES = ("warning:", "warn:", "[warn", "deprecation")


def _summarise_log(lines: list[str]) -> str | None:
    """Pick a useful summary line for the manifest.

    Walk from the end, skip blanks and obvious noise (warnings, deprecations),
    return the first remaining line.
    """
    for line in reversed(lines):
        candidate = line.strip()
        if not candidate:
            continue
        if candidate.lower().startswith(_SUMMARY_SKIP_PREFIXES):
            continue
        return candidate[:500]
    return None


def _run_subprocess(run_id: str, name: str, script: str, log_path: Path) -> tuple[int, str | None]:
    """Run a chain script and tee its combined stdout/stderr to ``log_path``.

    Forces UTF-8 on the child's stdio so non-ASCII chars (→, é, etc.) survive
    on Windows where the console codepage is usually cp1252.
    """
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUNBUFFERED"] = "1"
    env[ENV_RUN_ID] = run_id

    tail: list[str] = []
    with open(log_path, "w", encoding="utf-8", newline="") as logf:
        logf.write(f"# === {name} ({script}) ===\n")
        logf.flush()
        proc = subprocess.Popen(
            [sys.executable, script],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            bufsize=1,
            text=True,
            encoding="utf-8",
            errors="replace",
            env=env,
        )
        assert proc.stdout is not None
        for line in proc.stdout:
            sys.stdout.write(line)
            logf.write(line)
            logf.flush()
            tail.append(line)
            if len(tail) > 100:
                tail = tail[-100:]
        exit_code = proc.wait()
    return exit_code, _summarise_log(tail)


def _run_chain(
    run_id: str, ordinal: int, total: int, name: str, script: str
) -> tuple[str, int | None, str | None, str | None]:
    """Returns (status, exit_code, summary, error)."""
    print(f"\n=== [{ordinal:02d}/{total}] {name} ===")
    logging.info("Pipeline chain started: %s", name)

    log_path = step_log_path(run_id, ordinal, name)
    record_step_started(run_id, ordinal, name, script, log_path)

    try:
        exit_code, summary = _run_subprocess(run_id, name, script, log_path)
        if exit_code != 0:
            err = f"exit code {exit_code}"
            logging.error("Pipeline chain %s failed: %s", name, err)
            return "failed", exit_code, summary, err
        logging.info("Pipeline chain finished: %s", name)
        return "ok", exit_code, summary, None
    except Exception as e:  # noqa: BLE001 — orchestrator must isolate every failure mode
        logging.error("Pipeline chain %s failed: %s", name, e)
        return "failed", None, None, str(e)


def _parse_csv_list(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]


def _filter_chains(selected: list[str], excluded: list[str]) -> list[tuple[str, str]]:
    known = {name for name, _ in CHAINS}
    for name in selected + excluded:
        if name not in known:
            raise SystemExit(f"unknown chain: {name!r} (known: {', '.join(sorted(known))})")
    if selected:
        wanted = set(selected)
        chains = [(n, s) for n, s in CHAINS if n in wanted]
    else:
        chains = list(CHAINS)
    if excluded:
        skip = set(excluded)
        chains = [(n, s) for n, s in chains if n not in skip]
    return chains


def _print_chain_list() -> None:
    print("Available chains (default run order):\n")
    width = max(len(n) for n, _ in CHAINS)
    for name, script in CHAINS:
        blurb = _CHAIN_BLURBS.get(name, "")
        print(f"  {name:<{width}}  {script:<26}  {blurb}")
    print()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  python pipeline.py                       # full run, all chains\n"
            "  python pipeline.py --list                # show chains and exit\n"
            "  python pipeline.py --select iris         # only iris\n"
            "  python pipeline.py --select members,iris # multiple chains\n"
            "  python pipeline.py --exclude lobbying    # everything except lobbying\n"
        ),
    )
    ap.add_argument("--list", action="store_true", help="print chains and exit")
    ap.add_argument("--select", metavar="CHAINS", help="comma-separated chains to run (default: all)")
    ap.add_argument("--exclude", metavar="CHAINS", help="comma-separated chains to skip")
    args = ap.parse_args()

    if args.list:
        _print_chain_list()
        return 0

    selected = _parse_csv_list(args.select)
    excluded = _parse_csv_list(args.exclude)
    chains = _filter_chains(selected, excluded)
    if not chains:
        print("No chains selected after --select/--exclude. Nothing to do.", file=sys.stderr)
        return 1

    run_id = make_run_id()
    setup_logging(run_id)

    # 60-day retention of per-run log dirs — uncomment to enable.
    # from services.run_paths import prune_old_runs
    # pruned = prune_old_runs(days=60)
    # if pruned:
    #     logging.info("Pruned %d run dir(s) older than 60 days", pruned)

    create_run_manifest(run_id)
    logging.info("Pipeline run id: %s — logs at %s", run_id, run_dir(run_id))
    logging.info("Running %d chain(s): %s", len(chains), ", ".join(n for n, _ in chains))

    succeeded: list[str] = []
    broken: list[tuple[str, str]] = []
    total = len(chains)

    for ordinal, (name, script) in enumerate(chains, start=1):
        status, exit_code, summary, error = _run_chain(run_id, ordinal, total, name, script)
        record_step_finished(run_id, name, status, exit_code, summary, error)
        if status == "ok":
            succeeded.append(name)
        else:
            broken.append((name, error or "unknown"))

    run_finished_at(run_id)

    print("\n=== Pipeline summary ===")
    print(f"Run id:  {run_id}")
    print(f"Log dir: {run_dir(run_id)}")
    print(f"Succeeded ({len(succeeded)}/{total}):")
    for name in succeeded:
        print(f"  + {name}")
    if broken:
        print(f"Failed ({len(broken)}/{total}):")
        for name, error in broken:
            print(f"  - {name}: {error}")
        print("\nData processing pipeline encountered errors.")
        return 1

    print("Data processing pipeline complete. All chains executed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
