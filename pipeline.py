# pipeline.py
import subprocess, sys
import logging
STEPS = [
    #endpoint checker and downloader
    ("PDF Endpoint Check", "pdf_endpoint_check.py"),
    ("PDF Downloader", "pdf_downloader.py"),
    ("Members API", "oireachtas_api_service.py"),
    ("Flatten members", "flatten_members_json_to_csv.py"),
    ("Process payments", "payments.py"),
    ("Attendance PDF", "attendance.py"),
    ("Lobbying PDF", "lobby_processing.py"),
    ("Process legislation", "legislation.py"),
    ("Member interests PDF conversion to Dataframe", "interests.py"),
    ("Flatten bills", "flatten_service.py"),
    ("Bring in lobby information", "lobbying_processing.py"),
    ("Payments PDF conversion to Dataframe", "payments.py"),
    ("Ingest vote data", "votes.py"),
    ("Transform vote data", "transform_votes.py"),
    ("Enrich", "enrich.py"),
    ("Unit tests", "tests.py"),
    # ("Scrub unneeded files and intemediate data", "tear_down.py"),
]
pipeline_finished_without_errors = True
broken_steps = []
for name, script in STEPS:
    print(f"=== {name} ===")
    logging.info(f"Pipeline step started: {name}")
    try:
        result = subprocess.run([sys.executable, script], check=True)
    except Exception as e:
        print(f"Pipeline step {name} failed: error {e}", )
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
# enter python pipeline.py to run
# This script serves as the main entry point for the data processing pipeline. 
# It sequentially executes a series of scripts that perform various tasks such as fetching data from the Oireachtas API, flattening JSON data into CSV format, processing attendance PDFs, enriching the data by joining different datasets, and flattening bills data. Each step is clearly labeled in the output for easy tracking of the pipeline's progress. 
# By running this script, you can automate the entire data processing workflow with a single command.