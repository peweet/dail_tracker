# pipeline.py
import logging
import subprocess
import sys

from manifest import create_run_manifest, run_finished_at
from services.logging_setup import setup_logging
from services.oireachtas_api_main import main as run_oireachtas_api

STEPS = [
    # ("PDF Endpoint Check", "pdf_endpoint_check.py"),
    ("PDF Downloader", "pdf_downloader.py"),
    ("Members API", "dummy_value"),
    ("Flatten members", "flatten_members_json_to_csv.py"),
    ("Process payments", "payments.py"),
    ("Attendance PDF", "attendance.py"),
    ("Lobbying PDF", "lobby_processing.py"),
    ("Process legislation", "legislation.py"),
    ("Member interests PDF conversion to Dataframe", "member_interests.py"),
    # ("Flatten bills", "flatten_service.py"),
    ("Enrich", "enrich.py"),
    ("Transform vote data", "transform_votes.py"),
    # ("Unit tests", "tests.py"),
    # ("Scrub unneeded files and intemediate data", "tear_down.py"),
]


def _run_step(name: str, script: str) -> None:
    """Run a single pipeline step. Raises on failure so the caller can record it.

    The ``Members API`` step is a Python-call special case; everything else is
    spawned as a subprocess so a crash in one source script can't take down
    the orchestrator process.
    """
    print(f"=== {name} ===")
    logging.info(f"Pipeline step started: {name}")
    print("Running script:", script)
    if name == "Members API":
        run_oireachtas_api()
        logging.info(f"Completed Oireachtas API step: {name}")
    else:
        subprocess.run([sys.executable, script], check=True)
    logging.info(f"Pipeline step finished: {name}")


def main() -> int:
    setup_logging()
    manifest = create_run_manifest()
    succeeded: list[str] = []
    broken_steps: list[tuple[str, str]] = []

    # Continue past per-step failures so a single flaky source doesn't poison
    # every downstream step (DAIL-163). The exit code at the end is what
    # surfaces failure to a cron / CI runner.
    for name, script in STEPS:
        try:
            _run_step(name, script)
            succeeded.append(name)
        except Exception as e:
            print(f"Pipeline step {name} failed: error {e}")
            logging.error(f"Pipeline step {name} failed: error {e}")
            broken_steps.append((name, str(e)))

    run_finished_at(manifest["run_id"])

    print("\n=== Pipeline summary ===")
    print(f"Succeeded ({len(succeeded)}/{len(STEPS)}):")
    for name in succeeded:
        print(f"  + {name}")
    if broken_steps:
        print(f"Failed ({len(broken_steps)}/{len(STEPS)}):")
        for name, error in broken_steps:
            print(f"  - {name}: {error}")
        print("\nData processing pipeline encountered errors.")
        return 1

    print("Data processing pipeline complete. All steps executed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
