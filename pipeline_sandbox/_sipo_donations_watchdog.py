"""Watchdog for sipo_donations_paddle_etl.py CACHE stage (105pp, one PDF).

Same hang/segfault recovery as the expenses watchdogs: launch the cache stage,
watch its checkpoint dir (_ckpt_donations/c*.json); if no new page appears within
STALL seconds, kill the tree and relaunch (it resumes from checkpoints; the DPI
retry ladder skips a deterministically-bad page). Done when all pages are cached.

Run only when NO other OCR process is active (one PaddleOCR at a time):
  ./.venv/Scripts/python.exe pipeline_sandbox/_sipo_donations_watchdog.py
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / ".venv/Scripts/python.exe"
ETL = ROOT / "pipeline_sandbox/sipo_donations_paddle_etl.py"
CKPT = ROOT / "pipeline_sandbox/_sipo_output/_ckpt_donations"
LOG = ROOT / "pipeline_sandbox/_sipo_output/_log_donations.txt"

STALL = 200
POLL = 10
MAX_RESTARTS = 200


def page_count() -> int:
    return len(list(CKPT.glob("c*.json"))) if CKPT.exists() else 0


def kill_tree(pid: int) -> None:
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True, text=True)


def main() -> None:
    restarts = 0
    while restarts < MAX_RESTARTS:
        with open(LOG, "a", encoding="utf-8") as fh:
            fh.write(f"\n=== donations watchdog launch #{restarts + 1} ===\n")
            fh.flush()
            proc = subprocess.Popen([str(PY), str(ETL), "cache"], stdout=fh, stderr=subprocess.STDOUT)
        last, last_progress = page_count(), time.monotonic()
        hung = False
        while proc.poll() is None:
            time.sleep(POLL)
            c = page_count()
            if c > last:
                last, last_progress = c, time.monotonic()
            if time.monotonic() - last_progress > STALL:
                print(f"[don-wd] stalled {STALL}s at {c} pages -> KILL & resume", flush=True)
                kill_tree(proc.pid)
                hung = True
                break
        rc = proc.wait()
        print(f"[don-wd] launch #{restarts + 1} ended rc={rc} hung={hung} pages={page_count()}", flush=True)
        if rc == 0 and not hung:
            break
        restarts += 1
    # parse is cheap + safe; run it inline so the watchdog leaves a parquet behind
    print("[don-wd] cache done -> parsing", flush=True)
    subprocess.run([str(PY), str(ETL), "parse"])


if __name__ == "__main__":
    main()
