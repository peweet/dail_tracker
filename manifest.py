from dataclasses import dataclass, asdict, field
from datetime import UTC, datetime
from pathlib import Path
import orjson
import uuid
import logging
import pdf_endpoint_check
MANIFEST_PATH = Path("C:\\Users\\pglyn\\PycharmProjects\\dail_extractor\\data\\manifests\\manifest.json")
# Write stage manifests with:
# input dependencies
# output files
# row counts
# run timestamps
# source version / fetch date

def create_run_manifest() -> dict:
    global endpoints_ok
    endpoints_ok = False
    endpoints_validation = pdf_endpoint_check.endpoint_checker()

    if endpoints_validation[1]:
        logging.info("PDF endpoint validation successful. All endpoints are valid.")
        endpoints_ok = True
    else:
        logging.warning("PDF endpoint validation found issues. Please review the broken endpoints.")
        for url in endpoints_validation[0]:
            logging.warning(f"Broken endpoint: {url}")
    ts = datetime.now(UTC).isoformat(timespec='seconds')
    time_stamp_record = {
        "run_id": f"{ts}-{uuid.uuid4().hex[:8]}",
        "started_at": ts
    }
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Read existing array or start new
    if MANIFEST_PATH.exists():
        with MANIFEST_PATH.open("rb") as f:
            try:
                manifest_list = orjson.loads(f.read())
            except Exception:
                manifest_list = []
    else:
        manifest_list = []
    manifest_list.append(time_stamp_record)
    MANIFEST_PATH.write_bytes(orjson.dumps(manifest_list, option=orjson.OPT_INDENT_2))
    return time_stamp_record

def run_finished_at():
    ts = datetime.now(UTC).isoformat(timespec='seconds')
    time_taken = None
    if MANIFEST_PATH.exists():
        with MANIFEST_PATH.open("rb") as f:
            try:
                manifest_list = orjson.loads(f.read())
            except Exception:
                manifest_list = []
    else:
        manifest_list = []
    if manifest_list:
        manifest_list[-1]["finished_at"] = ts
        # MANIFEST_PATH.write_bytes(orjson.dumps(manifest_list, option=orjson.OPT_INDENT_2))
        time_taken = datetime.fromisoformat(ts) - datetime.fromisoformat(manifest_list[-1]["started_at"])
        print(f"Pipeline run finished. Time taken: {time_taken}")
        manifest_list[-1]["time_to_run"] = str(time_taken)
        manifest_list[-1]["endpoints_ok"] = str(endpoints_ok)
        MANIFEST_PATH.write_bytes(orjson.dumps(manifest_list, option=orjson.OPT_INDENT_2))
if __name__ == "__main__":
    create_run_manifest()
    # Simulate some processing time
    import time
    time.sleep(2)
    run_finished_at()


