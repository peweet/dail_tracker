# pipeline.py
import subprocess, sys

STEPS = [
    #endpoint checker and downloader
    # ("PDF Endpoint Check", "pdf_endpoint_check.py"),
    # ("PDF Downloader", "pdf_downloader.py"),
    ("Members API", "oireachtas_api_service.py"),
    ("Flatten members", "flatten_members_json_to_csv.py"),
    ("Attendance PDF", "attendance.py"),
    ("Enrich", "enrich.py"),
    ("Flatten bills", "flatten_service.py"),
    ("Bring in lobby information", "lobbying_processing.py"),
    ("Payments PDF", "payments.py"),
]

for name, script in STEPS:
    print(f"=== {name} ===")
    result = subprocess.run([sys.executable, script], check=True)

if __name__ == "__main__":
    print("Data processing pipeline complete. All steps executed successfully.")
# enter python pipeline.py to run
# This script serves as the main entry point for the data processing pipeline. It sequentially executes a series of scripts that perform various tasks such as fetching data from the Oireachtas API, flattening JSON data into CSV format, processing attendance PDFs, enriching the data by joining different datasets, and flattening bills data. Each step is clearly labeled in the output for easy tracking of the pipeline's progress. By running this script, you can automate the entire data processing workflow with a single command.