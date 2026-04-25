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
    ("Flatten bills", "flatten_service.py"),
    ("Enrich", "enrich.py"),
    ("Transform vote data", "transform_votes.py"),
    # ("Unit tests", "tests.py"),
    # ("Scrub unneeded files and intemediate data", "tear_down.py"),
]
pipeline_finished_without_errors = True
broken_steps = []
manifest = create_run_manifest()  # Record manifest at pipeline start
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
    except Exception as e:
        print(
            f"Pipeline step {name} failed: error {e}",
        )
        logging.error(f"Pipeline step {name} failed: error {e}")
        pipeline_finished_without_errors = False
        broken_steps.append((name, e))
        break
    logging.info(f"Pipeline step finished: {name}")
if __name__ == "__main__":
    if pipeline_finished_without_errors:
        print("Data processing pipeline complete. All steps executed successfully.")
    else:
        print("Data processing pipeline encountered errors.")
        print("Errors occured on step:")
        for step, error in broken_steps:
            print(f"- {step}: {error}")
    run_finished_at(manifest["run_id"])  # Record finished time in manifest
