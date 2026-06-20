"""Watchdog driver for sipo_expense_items_paddle_etl.py (Part-4 itemised expenses).

Identical strategy to _sipo_watchdog.py (the Part-3 driver) but pointed at the
items ETL and its OWN checkpoint dir (_ckpt_items) so the two can never collide:
launch the items ETL for ONE party -> watch its per-page checkpoints -> if no new
page appears within STALL seconds, kill the tree and relaunch (it resumes from
checkpoints; the DPI retry ladder eventually skips a deterministically-bad page).

Run only when NO other OCR process is active (one PaddleOCR at a time — concurrent
runs thrash memory and crash the machine):
  ./.venv/Scripts/python.exe extractors/_sipo_items_watchdog.py ff fg sf lab green socdem pbp aontu
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / ".venv/Scripts/python.exe"
ETL = ROOT / "extractors/sipo_expense_items_paddle_etl.py"
CKPT = ROOT / "data/silver/sipo/by_party/_ckpt_items"
BY_PARTY = ROOT / "data/silver/sipo/by_party"

STALL = 200  # seconds with no new page checkpoint => assume hung, kill & restart
POLL = 10
MAX_RESTARTS = 120


def ckpt_count(key: str) -> int:
    d = CKPT / key
    return len(list(d.glob("p*.json"))) if d.exists() else 0


def kill_tree(pid: int) -> None:
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True, text=True)


def run_party(key: str) -> bool:
    log = ROOT / f"data/silver/sipo/_log_items_{key}.txt"
    restarts = 0
    while restarts < MAX_RESTARTS:
        with open(log, "a", encoding="utf-8") as fh:
            fh.write(f"\n=== items watchdog launch #{restarts + 1} for {key} ===\n")
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
                print(f"[items-wd] {key}: stalled {STALL}s at {c} pages -> KILL & resume", flush=True)
                kill_tree(proc.pid)
                killed_hung = True
                break
        rc = proc.wait()
        done_parquet = (BY_PARTY / f"{key}_items.parquet").exists()
        print(
            f"[items-wd] {key}: launch #{restarts + 1} ended rc={rc} "
            f"hung={killed_hung} pages={ckpt_count(key)} parquet={done_parquet}",
            flush=True,
        )
        if rc == 0:
            return True  # a clean pass (parquet written, or party legitimately had no items)
        restarts += 1
    print(f"[items-wd] {key}: GAVE UP after {MAX_RESTARTS} restarts", flush=True)
    return False


def main() -> None:
    keys = sys.argv[1:]
    if not keys:
        print("usage: _sipo_items_watchdog.py <party_key> [<party_key> ...]")
        return
    results = {}
    for key in keys:
        print(f"\n########## ITEMS WATCHDOG: {key} ##########", flush=True)
        results[key] = run_party(key)
    print("\n=== items watchdog summary ===", flush=True)
    for k, ok in results.items():
        print(f"  {k}: {'OK' if ok else 'FAILED'}  ({ckpt_count(k)} pages checkpointed)", flush=True)


if __name__ == "__main__":
    main()
