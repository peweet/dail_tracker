"""
_ocr_2012_watchdog.py  (SANDBOX driver)
---------------------------------------
PaddleOCR on Windows segfaults/hangs intermittently (documented in project_sipo_ocr).
ocr_2012_register.py checkpoints every page, so the cure is simply: relaunch it
until all pages are checkpointed. This driver does that — bounding each launch
with a hang timeout and stopping if a launch makes zero progress repeatedly.

Run:  python -m pipeline_sandbox.historic_members._ocr_2012_watchdog
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).parent
CKPT_DIR = HERE / "_ckpt_2012"
TARGET_PAGES = 102
LAUNCH_TIMEOUT = 600  # seconds per launch; a hung detector is killed and relaunched
MAX_STALLS = 4  # consecutive launches with no new page -> give up


def done() -> int:
    return len(list(CKPT_DIR.glob("p*.json")))


def main() -> None:
    py = sys.executable
    cmd = [py, "-m", "pipeline_sandbox.historic_members.ocr_2012_register"]
    stalls = 0
    launch = 0
    while True:
        n = done()
        if n >= TARGET_PAGES:
            print(f"[watchdog] all {n}/{TARGET_PAGES} pages checkpointed — done.")
            break
        if stalls >= MAX_STALLS:
            print(f"[watchdog] {stalls} stalled launches in a row at {n}/{TARGET_PAGES} — aborting.")
            sys.exit(2)
        launch += 1
        print(f"[watchdog] launch #{launch}: {n}/{TARGET_PAGES} done, starting OCR...", flush=True)
        try:
            subprocess.run(cmd, timeout=LAUNCH_TIMEOUT, check=False)
        except subprocess.TimeoutExpired:
            print("[watchdog] launch hit hang timeout — killed, relaunching.", flush=True)
        after = done()
        if after <= n:
            stalls += 1
            print(f"[watchdog] no progress this launch (still {after}); stall {stalls}/{MAX_STALLS}", flush=True)
            time.sleep(2)
        else:
            stalls = 0
            print(f"[watchdog] progressed {n} -> {after}", flush=True)


if __name__ == "__main__":
    main()
