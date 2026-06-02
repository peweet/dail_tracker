"""Pipeline run manifest.

Two-file model:
    logs/runs/<run_id>/manifest.json   — full per-run record with steps[]
    data/manifests/manifest.json       — rollup index, one summary row per run

The rollup stays compact and human-readable for quick grep/jq browsing.
Per-step diagnostics (durations, exit codes, log file paths, summary lines)
live in the per-run dir alongside the captured step logs they reference.

Pre-refactor entries in the rollup (those without `status` / `steps_total`)
are left untouched. New runs append the new schema.
"""

from __future__ import annotations

import logging
import os
import platform
import socket
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import orjson

from config import DATA_DIR, PROJECT_ROOT
from services.run_paths import get_git_sha, make_run_id, run_dir, write_latest_pointer

MANIFEST_PATH = DATA_DIR / "manifests" / "manifest.json"


def _read_json(path: Path, default):
    if not path.exists():
        return default
    try:
        return orjson.loads(path.read_bytes())
    except Exception:
        return default


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(orjson.dumps(payload, option=orjson.OPT_INDENT_2))


def _per_run_path(run_id: str) -> Path:
    return run_dir(run_id) / "manifest.json"


def _rel_to_project(path: Path) -> str:
    return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")


def _check_endpoints() -> bool | None:
    """HEAD-check every known PDF URL. Opt-in via DAIL_CHECK_ENDPOINTS=1."""
    if os.environ.get("DAIL_CHECK_ENDPOINTS") != "1":
        return None
    import pdf_endpoint_check

    broken, ok = pdf_endpoint_check.endpoint_checker()
    if ok:
        logging.info("PDF endpoint validation successful. All endpoints are valid.")
    else:
        logging.warning("PDF endpoint validation found issues. Please review the broken endpoints.")
        for url in broken:
            logging.warning(f"Broken endpoint: {url}")
    return bool(ok)


def create_run_manifest(run_id: str | None = None) -> dict:
    """Initialise the per-run manifest and append a stub to the rollup.

    Pass a `run_id` from services.run_paths.make_run_id() so the manifest id
    matches the on-disk log directory; one is generated if omitted.
    """
    if run_id is None:
        run_id = make_run_id()
    write_latest_pointer(run_id)

    started_at = datetime.now(UTC).isoformat(timespec="seconds")
    endpoints_ok = _check_endpoints()

    record = {
        "run_id": run_id,
        "started_at": started_at,
        "git_sha": get_git_sha(),
        "host": socket.gethostname(),
        "python": platform.python_version(),
        "platform": platform.system(),
        "endpoints_ok": endpoints_ok,
        "status": "running",
        "steps": [],
    }
    _write_json(_per_run_path(run_id), record)

    rollup = _read_json(MANIFEST_PATH, default=[])
    rollup.append(
        {
            "run_id": run_id,
            "started_at": started_at,
            "status": "running",
            "git_sha": record["git_sha"],
            "log_dir": _rel_to_project(run_dir(run_id)),
        }
    )
    _write_json(MANIFEST_PATH, rollup)

    return record


def record_step_started(run_id: str, ordinal: int, name: str, script: str, log_file: Path | None) -> None:
    record = _read_json(_per_run_path(run_id), default=None)
    if record is None:
        return
    record["steps"].append(
        {
            "ordinal": ordinal,
            "name": name,
            "script": script,
            "started_at": datetime.now(UTC).isoformat(timespec="seconds"),
            "status": "running",
            "log_file": (
                str(log_file.relative_to(run_dir(run_id))).replace("\\", "/") if log_file is not None else None
            ),
        }
    )
    _write_json(_per_run_path(run_id), record)


def record_step_finished(
    run_id: str,
    name: str,
    status: str,
    exit_code: int | None,
    summary: str | None,
    error: str | None = None,
) -> None:
    record = _read_json(_per_run_path(run_id), default=None)
    if record is None:
        return
    finished_at = datetime.now(UTC).isoformat(timespec="seconds")
    for step in reversed(record["steps"]):
        if step["name"] == name and step["status"] == "running":
            step["finished_at"] = finished_at
            step["status"] = status
            step["exit_code"] = exit_code
            step["summary"] = summary
            if error:
                step["error"] = error
            with suppress(KeyError, ValueError):
                step["duration_seconds"] = round(
                    (datetime.fromisoformat(finished_at) - datetime.fromisoformat(step["started_at"])).total_seconds(),
                    2,
                )
            break
    _write_json(_per_run_path(run_id), record)


def run_finished_at(run_id: str | None = None) -> None:
    """Finalise the per-run manifest and the matching rollup entry."""
    if run_id is None:
        return
    record = _read_json(_per_run_path(run_id), default=None)
    if record is None:
        return

    finished_at = datetime.now(UTC).isoformat(timespec="seconds")
    record["finished_at"] = finished_at
    duration: float | None
    try:
        duration = (datetime.fromisoformat(finished_at) - datetime.fromisoformat(record["started_at"])).total_seconds()
        record["duration_seconds"] = round(duration, 2)
    except (KeyError, ValueError):
        duration = None

    failed = [s for s in record["steps"] if s.get("status") == "failed"]
    ok_count = sum(1 for s in record["steps"] if s.get("status") == "ok")
    if not record["steps"] or not failed:
        status = "ok"
    elif ok_count == 0:
        status = "failed"
    else:
        status = "partial"
    record["status"] = status
    _write_json(_per_run_path(run_id), record)

    rollup = _read_json(MANIFEST_PATH, default=[])
    for entry in reversed(rollup):
        if entry.get("run_id") == run_id:
            entry["finished_at"] = finished_at
            if duration is not None:
                entry["duration_seconds"] = round(duration, 2)
            entry["status"] = status
            entry["steps_total"] = len(record["steps"])
            entry["steps_ok"] = ok_count
            entry["steps_failed"] = len(failed)
            if record.get("endpoints_ok") is not None:
                entry["endpoints_ok"] = record["endpoints_ok"]
            break
    _write_json(MANIFEST_PATH, rollup)

    if duration is not None:
        print(f"Pipeline run finished. Time taken: {duration:.1f}s ({status})")


if __name__ == "__main__":
    import time

    rid = make_run_id()
    create_run_manifest(rid)
    time.sleep(2)
    run_finished_at(rid)
