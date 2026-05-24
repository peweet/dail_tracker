import logging
import os
import uuid
from datetime import UTC, datetime

import orjson

from config import DATA_DIR

MANIFEST_PATH = DATA_DIR / "manifests" / "manifest.json"


def _read_manifest_list() -> list:
    if not MANIFEST_PATH.exists():
        return []
    try:
        return orjson.loads(MANIFEST_PATH.read_bytes())
    except Exception:
        return []


def _write_manifest_list(manifest_list: list) -> None:
    MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    MANIFEST_PATH.write_bytes(orjson.dumps(manifest_list, option=orjson.OPT_INDENT_2))


def _check_endpoints() -> bool | None:
    """HEAD-check every known PDF URL. ~90 requests, ~10-30s.

    Opt-in via env var DAIL_CHECK_ENDPOINTS=1 — otherwise returns None so the
    pipeline isn't blocked on flaky network at every run.
    """
    if os.environ.get("DAIL_CHECK_ENDPOINTS") != "1":
        return None
    # Imported lazily so the manifest module doesn't pull `requests` etc. on import.
    import pdf_endpoint_check

    broken, ok = pdf_endpoint_check.endpoint_checker()
    if ok:
        logging.info("PDF endpoint validation successful. All endpoints are valid.")
    else:
        logging.warning("PDF endpoint validation found issues. Please review the broken endpoints.")
        for url in broken:
            logging.warning(f"Broken endpoint: {url}")
    return bool(ok)


def create_run_manifest() -> dict:
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    record = {
        "run_id": f"{ts}-{uuid.uuid4().hex[:8]}",
        "started_at": ts,
        "endpoints_ok": _check_endpoints(),
    }
    manifest_list = _read_manifest_list()
    manifest_list.append(record)
    _write_manifest_list(manifest_list)
    return record


def run_finished_at(run_id: str | None = None) -> None:
    ts = datetime.now(UTC).isoformat(timespec="seconds")
    manifest_list = _read_manifest_list()
    if not manifest_list:
        return

    if run_id is not None:
        target = next((r for r in reversed(manifest_list) if r.get("run_id") == run_id), None)
        if target is None:
            return
    else:
        target = manifest_list[-1]

    target["finished_at"] = ts
    try:
        time_taken = datetime.fromisoformat(ts) - datetime.fromisoformat(target["started_at"])
        target["time_to_run"] = str(time_taken)
        print(f"Pipeline run finished. Time taken: {time_taken}")
    except (KeyError, ValueError):
        pass
    _write_manifest_list(manifest_list)


if __name__ == "__main__":
    import time

    record = create_run_manifest()
    time.sleep(2)
    run_finished_at(record["run_id"])
