# pipeline_sandbox/pipeline_with_tests.py
#
# Sandbox duplicate of pipeline.py with silver validation and SQL view
# contract checks added as pipeline steps.
#
# Motivation: pipeline.py is working and has incoming changes.
# This file lets you test the integrated pipeline+tests flow safely
# without touching the live pipeline.
#
# Two new steps added (after their respective dependencies):
#   "Silver schema validation"  — runs test/test_silver_*.py -m integration
#   "SQL view validation"       — runs test/test_sql_views.py -m sql
#
# Both steps exit non-zero on test failure, which breaks the pipeline
# at that point (fail-fast, same as every other step).
#
# To promote to pipeline.py once stable:
#   1. Copy the two new STEPS entries to pipeline.py
#   2. Verify run_silver_tests.py and run_sql_tests.py are at project root
#   3. Remove this file (or keep it as a reference)

import logging
import subprocess
import sys
from manifest import create_run_manifest, run_finished_at
from services.logging_setup import setup_logging
from services.oireachtas_api_main import main as run_oireachtas_api

STEPS = [
    # ("PDF Endpoint Check", "pdf_endpoint_check.py"),
    ("PDF Downloader",                                  "pdf_downloader.py"),
    ("Members API",                                     "dummy_value"),
    ("Flatten members",                                 "flatten_members_json_to_csv.py"),
    ("Process payments",                                "payments.py"),
    ("Attendance PDF",                                  "attendance.py"),
    ("Lobbying PDF",                                    "lobby_processing.py"),
    ("Process legislation",                             "legislation.py"),
    ("Member interests PDF conversion to Dataframe",    "member_interests.py"),
    ("Flatten bills",                                   "flatten_service.py"),
    ("Enrich",                                          "enrich.py"),
    ("Transform vote data",                             "transform_votes.py"),
    ("Silver schema validation",                        "run_silver_tests.py"),  # NEW
    ("SQL view validation",                             "run_sql_tests.py"),     # NEW
    # ("Scrub unneeded files and intermediate data",   "tear_down.py"),
]

pipeline_finished_without_errors = True
broken_steps = []
manifest = create_run_manifest()
silver_tests_passed = None
sql_tests_passed    = None

for name, script in STEPS:
    print(f"=== {name} ===")
    logging.info(f"Pipeline step started: {name}")
    try:
        print("Running script:", script)
        if name == "Members API":
            run_oireachtas_api()
            logging.info(f"Completed Oireachtas API step: {name}")
        else:
            result = subprocess.run([sys.executable, script], check=True)

        if name == "Silver schema validation":
            silver_tests_passed = True
        if name == "SQL view validation":
            sql_tests_passed = True

    except Exception as e:
        print(f"Pipeline step {name} failed: error {e}")
        logging.error(f"Pipeline step {name} failed: error {e}")

        if name == "Silver schema validation":
            silver_tests_passed = False
        if name == "SQL view validation":
            sql_tests_passed = False

        pipeline_finished_without_errors = False
        broken_steps.append((name, e))
        break
    logging.info(f"Pipeline step finished: {name}")

if __name__ == "__main__":
    if pipeline_finished_without_errors:
        print("Data processing pipeline complete. All steps executed successfully.")
    else:
        print("Data processing pipeline encountered errors.")
        print("Errors occurred on step:")
        for step, error in broken_steps:
            print(f"- {step}: {error}")
    run_finished_at(manifest["run_id"])
