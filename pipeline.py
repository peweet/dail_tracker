"""Dáil Tracker data pipeline orchestrator.

Runs each STEPS entry as a subprocess (or in-process for the Members API),
captures stdout+stderr to a per-step log file under
``logs/runs/<run_id>/steps/NN_<slug>.log``, and records every step in the
per-run manifest. The whole run is self-contained under
``logs/runs/<run_id>/`` so it can be uploaded as a single CI artifact.

Per-step failures are caught so a single flaky source doesn't poison the
rest of the run (DAIL-163). Exit code 1 if any step failed.
"""

from __future__ import annotations

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
from services.oireachtas_api_main import main as run_oireachtas_api
from services.run_paths import ENV_RUN_ID, make_run_id, run_dir, step_log_path

STEPS = [
    # ("PDF Endpoint Check", "pdf_endpoint_check.py"),
    # Poll the Oireachtas publications index for new PDFs across payments,
    # attendance, and interests. Anything new lands in the source's bronze
    # dir before PDF Downloader runs (which still covers the hard-coded
    # historical URL list and skips files already on disk).
    ("Poll new Oireachtas PDFs", "oireachtas_pdf_poller.py"),
    ("PDF Downloader", "pdf_downloader.py"),
    ("Members API", "dummy_value"),
    ("Flatten debate listings", "dbsect_listings_flatten.py"),
    ("Flatten members", "flatten_members_json_to_csv.py"),
    ("Process payments (full PSA)", "payments_full_psa_etl.py"),
    ("Attendance PDF", "attendance.py"),
    # Fetches YTD lobbying returns from the public CSV endpoint. Slow (~80s)
    # because the upstream assembles the response per-request. Hash-compare
    # in the poller skips bronze writes when nothing changed. Auto-switches
    # to wave mode on NEXT_WAVE_DATE for stricter size validation.
    ("Poll lobbying returns (YTD)", "lobbying_poller.py"),
    ("Lobbying PDF", "lobby_processing.py"),
    ("Extract embedded PDFs from lobbying returns", "lobbying_pdf_extract.py"),
    # CRO + Charities Tier-A resolution. Order matters: cro_normalise and
    # charity_normalise each consume an independent bronze file (CSV + xlsx);
    # charity_resolved joins their silver outputs together.
    ("CRO normalise", "cro_normalise.py"),
    ("Charity normalise", "charity_normalise.py"),
    ("Charity resolved (Tier A join)", "charity_resolved.py"),
    # Gold-layer enrichment of the Tier-A charity table: adds NACE sector
    # labels, CRO filing dates, and compliance flags. Purely additive —
    # reads silver, writes gold parquet only, does not modify any upstream
    # output. Dedups defensively on RCN.
    ("Charity enriched (gold)", "charity_enriched.py"),
    ("Process legislation", "legislation.py"),
    # Flattens bronze/questions/questions_results.json (written by Members API
    # in-process step above) to silver. Parallel structure to legislation.py.
    ("Flatten parliamentary questions", "questions.py"),
    ("Flatten bill amendments", "bill_amendments_flatten.py"),
    ("Member interests PDF conversion to Dataframe", "member_interests.py"),
    # Iris publishes Tue/Fri; this picks up new issues since the last run and
    # lands them in bronze before the Iris ETL globs *.pdf.
    ("Poll new Iris Oifigiuil PDFs", "iris_oifigiuil_poller.py"),
    ("Iris Oifigiuil ETL", "iris_oifigiuil_etl_polars.py"),
    ("Iris SI <-> bill enrichment", "iris_si_bill_enrichment.py"),
    # ministerial_tenure_build refreshes the Wikidata-sourced minister table
    # consumed by si_entity_enrichment. Network call; pipeline.py wraps each
    # step in try/except so a transient Wikidata failure can't poison the run.
    ("Ministerial tenure (Wikidata)", "ministerial_tenure_build.py"),
    # Wikidata socials + Wikipedia links per member — consumed by
    # member_overview hero chips. Network call to the same WDQS endpoint;
    # failure here leaves the previous parquet in place (or no parquet on
    # first ever run — the SQL view registration is wrapped in try/except,
    # so the hero just falls back to "no chips").
    ("Wikidata socials (member external links)", "wikidata_socials_etl.py"),
    ("SI entity enrichment", "si_entity_enrichment.py"),
    # transform_votes must precede enrich — enrich.py reads silver/pretty_votes.csv
    ("Transform vote data", "transform_votes.py"),
    ("Enrich", "enrich.py"),
]

_SUMMARY_SKIP_PREFIXES = ("warning:", "warn:", "[warn", "deprecation")


def _summarise_log(lines: list[str]) -> str | None:
    """Pick a useful summary line for the manifest.

    Walk from the end, skip blanks and obvious noise (warnings, deprecations),
    return the first remaining line. Pollers print a single-line summary like
    `[iris] poll done new=1 …` which lands here naturally.
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
    """Run a step script and tee its combined stdout/stderr to ``log_path``.

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


def _run_in_process(name: str, log_path: Path) -> tuple[int, str | None]:
    """Run the in-process step under a dedicated FileHandler so its log is captured."""
    root = logging.getLogger()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setLevel(logging.INFO)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    try:
        run_oireachtas_api()
        return 0, f"{name} completed"
    finally:
        root.removeHandler(handler)
        handler.close()


def _run_step(
    run_id: str, ordinal: int, total: int, name: str, script: str
) -> tuple[str, int | None, str | None, str | None]:
    """Returns (status, exit_code, summary, error)."""
    print(f"\n=== [{ordinal:02d}/{total}] {name} ===")
    logging.info("Pipeline step started: %s", name)

    log_path = step_log_path(run_id, ordinal, name)
    record_step_started(run_id, ordinal, name, script, log_path)

    try:
        if name == "Members API":
            exit_code, summary = _run_in_process(name, log_path)
        else:
            exit_code, summary = _run_subprocess(run_id, name, script, log_path)

        if exit_code != 0:
            err = f"exit code {exit_code}"
            logging.error("Pipeline step %s failed: %s", name, err)
            return "failed", exit_code, summary, err

        logging.info("Pipeline step finished: %s", name)
        return "ok", exit_code, summary, None
    except Exception as e:  # noqa: BLE001 — orchestrator must isolate every failure mode
        logging.error("Pipeline step %s failed: %s", name, e)
        return "failed", None, None, str(e)


def main() -> int:
    run_id = make_run_id()
    setup_logging(run_id)

    # 60-day retention of per-run log dirs — uncomment to enable.
    # from services.run_paths import prune_old_runs
    # pruned = prune_old_runs(days=60)
    # if pruned:
    #     logging.info("Pruned %d run dir(s) older than 60 days", pruned)

    create_run_manifest(run_id)
    logging.info("Pipeline run id: %s — logs at %s", run_id, run_dir(run_id))

    succeeded: list[str] = []
    broken_steps: list[tuple[str, str]] = []
    total = len(STEPS)

    for ordinal, (name, script) in enumerate(STEPS, start=1):
        status, exit_code, summary, error = _run_step(run_id, ordinal, total, name, script)
        record_step_finished(run_id, name, status, exit_code, summary, error)
        if status == "ok":
            succeeded.append(name)
        else:
            broken_steps.append((name, error or "unknown"))

    run_finished_at(run_id)

    print("\n=== Pipeline summary ===")
    print(f"Run id:  {run_id}")
    print(f"Log dir: {run_dir(run_id)}")
    print(f"Succeeded ({len(succeeded)}/{total}):")
    for name in succeeded:
        print(f"  + {name}")
    if broken_steps:
        print(f"Failed ({len(broken_steps)}/{total}):")
        for name, error in broken_steps:
            print(f"  - {name}: {error}")
        print("\nData processing pipeline encountered errors.")
        return 1

    print("Data processing pipeline complete. All steps executed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
