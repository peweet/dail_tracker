# pipeline.py
import subprocess, sys

STEPS = [
    ("Members API", "oireachtas_api_service.py"),
    ("Flatten members", "flatten_members_json_to_csv.py"),
    ("Attendance PDF", "attendance_2024.py"),
    ("Enrich", "enrich.py"),
    ("Flatten bills", "flatten_service.py"),
]

for name, script in STEPS:
    print(f"=== {name} ===")
    result = subprocess.run([sys.executable, script], check=True)

# enter python pipeline.py to run
# This script serves as the main entry point for the data processing pipeline. It sequentially executes a series of scripts that perform various tasks such as fetching data from the Oireachtas API, flattening JSON data into CSV format, processing attendance PDFs, enriching the data by joining different datasets, and flattening bills data. Each step is clearly labeled in the output for easy tracking of the pipeline's progress. By running this script, you can automate the entire data processing workflow with a single command.