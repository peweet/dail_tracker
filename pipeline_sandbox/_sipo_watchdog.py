"""Watchdog driver for sipo_expenses_paddle_etl.py.

PaddleOCR on this paddle-3.3.1/Windows build is intermittently unstable — it both
SEGFAULTS (process dies) and HANGS (process sits forever on a page). A plain
restart-on-exit loop handles the crash but NOT the hang (the earlier bash driver
wasted 53 min on one stuck page). This driver bounds a hang:

  launch the ETL for ONE party -> watch its checkpoint dir -> if no new page
  checkpoint appears within STALL seconds, kill the whole process tree and relaunch
  (the ETL resumes from checkpoints; its per-page retry ladder eventually skips a
  page that keeps hanging). Done when the party's parquet exists or exit 0.

Run:  ./.venv/Scripts/python.exe pipeline_sandbox/_sipo_watchdog.py fg sf lab ...
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / ".venv/Scripts/python.exe"
ETL = ROOT / "pipeline_sandbox/sipo_expenses_paddle_etl.py"
CKPT = ROOT / "pipeline_sandbox/_sipo_output/by_party/_ckpt"
BY_PARTY = ROOT / "pipeline_sandbox/_sipo_output/by_party"

STALL = 200  # seconds with no new page checkpoint => assume hung, kill & restart
POLL = 10
MAX_RESTARTS = 120


def ckpt_count(key: str) -> int:
    d = CKPT / key
    return len(list(d.glob("c*.json"))) if d.exists() else 0  # ETL caches cNNN.json cells


def kill_tree(pid: int) -> None:
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)],
                   capture_output=True, text=True)


def run_party(key: str) -> bool:
    log = ROOT / f"pipeline_sandbox/_sipo_output/_log_{key}.txt"
    restarts = 0
    while restarts < MAX_RESTARTS:
        with open(log, "a", encoding="utf-8") as fh:
            fh.write(f"\n=== watchdog launch #{restarts + 1} for {key} ===\n")
            fh.flush()
            proc = subprocess.Popen([str(PY), str(ETL), key], stdout=fh, stderr=subprocess.STDOUT)
        last_count = ckpt_count(key)
        last_progress = time.monotonic()
        killed_hung = False
        while proc.poll() is None:
            time.sleep(POLL)
            c = ckpt_count(key)
            if c > last_count:
                last_count, last_progress = c, time.monotonic()
            if time.monotonic() - last_progress > STALL:
                print(f"[watchdog] {key}: stalled {STALL}s at {c} pages -> KILL & resume", flush=True)
                kill_tree(proc.pid)
                killed_hung = True
                break
        rc = proc.wait()
        done_parquet = (BY_PARTY / f"{key}.parquet").exists()
        print(f"[watchdog] {key}: launch #{restarts + 1} ended rc={rc} "
              f"hung={killed_hung} pages={ckpt_count(key)} parquet={done_parquet}", flush=True)
        if rc == 0 and done_parquet:
            return True
        if rc == 0 and not done_parquet:
            # ETL finished a pass but wrote no parquet (no rows?) — accept, stop looping
            return True
        restarts += 1
    print(f"[watchdog] {key}: GAVE UP after {MAX_RESTARTS} restarts", flush=True)
    return False


def main() -> None:
    keys = sys.argv[1:]
    if not keys:
        print("usage: _sipo_watchdog.py <party_key> [<party_key> ...]")
        return
    results = {}
    for key in keys:
        print(f"\n########## WATCHDOG: {key} ##########", flush=True)
        results[key] = run_party(key)
    print("\n=== watchdog summary ===", flush=True)
    for k, ok in results.items():
        print(f"  {k}: {'OK' if ok else 'FAILED'}  ({ckpt_count(k)} pages checkpointed)", flush=True)


if __name__ == "__main__":
    main()
