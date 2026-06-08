"""Watchdog driver for the per-candidate OCR (`sipo_candidate_ocr.py`).

Same rationale as `_sipo_watchdog.py` but for the whole candidate corpus in one
resumable process. PaddleOCR on this build both SEGFAULTS and HANGS; the OCR
driver's DPI-attempt ladder handles a crash (re-run bumps DPI / skips the page),
but a HANG sits forever on one page — fatal for an unattended overnight run. This
driver bounds the hang:

  launch `python -m extractors.sipo_candidate_ocr [passthrough args]` -> watch the
  candidate checkpoint tree -> if no new page checkpoint appears within STALL
  seconds, kill the whole process tree and relaunch (the driver resumes from the
  cached pages + per-page attempt files). Repeat until the driver exits 0 (corpus
  complete) or MAX_RESTARTS is hit.

Run (whole expense corpus, overnight):
    ./.venv/Scripts/python.exe extractors/_sipo_candidate_watchdog.py
Pass-through to the OCR driver (e.g. donations, or a limit):
    ./.venv/Scripts/python.exe extractors/_sipo_candidate_watchdog.py --doc-types donation_statement
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = ROOT / ".venv/Scripts/python.exe"
CKPT = ROOT / "data/silver/sipo_candidate/_ckpt"

STALL = 600  # s with no new page checkpoint => assume hung, kill & resume. Generous so a
#              slow PaddleOCR import (minutes, under venv/disk load) can't false-kill before
#              the first page lands; a real page-hang wastes <=10min/cycle, fine overnight.
POLL = 20
MAX_RESTARTS = 400  # corpus is ~8k pages; allow many resume cycles


def ckpt_count() -> int:
    """Total cached pages across all candidate documents (progress signal)."""
    return sum(1 for _ in CKPT.glob("*/c*.json")) if CKPT.exists() else 0


def kill_tree(pid: int) -> None:
    subprocess.run(["taskkill", "/F", "/T", "/PID", str(pid)], capture_output=True, text=True)


def main() -> None:
    passthrough = sys.argv[1:]  # forwarded to sipo_candidate_ocr (e.g. --doc-types ...)
    log = ROOT / "data/silver/sipo_candidate/_log_candidate_ocr.txt"
    log.parent.mkdir(parents=True, exist_ok=True)
    restarts = 0
    fast_fails = 0  # consecutive crashes that OCR'd nothing (e.g. paddle can't init)
    while restarts < MAX_RESTARTS:
        start_count = ckpt_count()
        t_start = time.monotonic()
        with open(log, "a", encoding="utf-8") as fh:
            fh.write(f"\n=== watchdog launch #{restarts + 1} (args={passthrough}) ===\n")
            fh.flush()
            proc = subprocess.Popen(
                [str(PY), "-m", "extractors.sipo_candidate_ocr", *passthrough],
                stdout=fh, stderr=subprocess.STDOUT, cwd=str(ROOT),
            )
        last_count = start_count
        last_progress = time.monotonic()
        killed_hung = False
        while proc.poll() is None:
            time.sleep(POLL)
            c = ckpt_count()
            if c > last_count:
                last_count, last_progress = c, time.monotonic()
            if time.monotonic() - last_progress > STALL:
                print(f"[cand-watchdog] stalled {STALL}s at {c} pages -> KILL & resume", flush=True)
                kill_tree(proc.pid)
                killed_hung = True
                break
        rc = proc.wait()
        progressed = ckpt_count() > start_count
        ran = time.monotonic() - t_start
        print(f"[cand-watchdog] launch #{restarts + 1} ended rc={rc} hung={killed_hung} "
              f"progressed={progressed} ran={ran:.0f}s pages_cached={ckpt_count()}", flush=True)
        if rc == 0 and not killed_hung:
            print("[cand-watchdog] OCR driver completed the corpus.", flush=True)
            return
        # BACKOFF + ABORT so a crash-on-start can never become a spin-loop (the 228x bug):
        # a launch that crashed fast AND OCR'd nothing means paddle can't start — usually
        # a contended/exhausted machine (another OCR process running). Back off, and give up
        # rather than thrash if it persists.
        if progressed:
            fast_fails = 0
        elif not killed_hung and ran < 120:
            fast_fails += 1
            if fast_fails >= 6:
                print(f"[cand-watchdog] ABORT: {fast_fails} consecutive fast crashes, no progress "
                      f"(paddle can't start — machine contended/exhausted, or another OCR running). "
                      f"Stopping to avoid a spin-loop. pages_cached={ckpt_count()}", flush=True)
                return
            backoff = min(300, 30 * fast_fails)
            print(f"[cand-watchdog] fast crash #{fast_fails} (ran {ran:.0f}s, no progress) "
                  f"-> backoff {backoff}s", flush=True)
            time.sleep(backoff)
        restarts += 1
    print(f"[cand-watchdog] GAVE UP after {MAX_RESTARTS} restarts "
          f"(pages_cached={ckpt_count()})", flush=True)


if __name__ == "__main__":
    main()
