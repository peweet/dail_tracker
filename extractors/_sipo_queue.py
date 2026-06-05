"""Queued SIPO OCR runner — waits for the machine to be OCR-FREE, then runs the
remaining work sequentially (one PaddleOCR at a time, never concurrent):

  1. Social Democrats Part-3 candidate-summary   (_sipo_watchdog.py socdem)
  2. Part-4 itemised expenses for the scanned parties that still need it
     (_sipo_items_watchdog.py fg green lab pbp socdem)

FF/SF/Aontú Part-4 are already built (FF from OCR cache, SF/Aontú born-digital via
build_part4_no_ocr.py), so they're not re-run here.

Safe to launch WHILE another OCR is running: it BLOCKS on the OCR-free check before
starting anything, so it can be queued behind the in-flight FG run.
"""

from __future__ import annotations

import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PY = str(ROOT / ".venv/Scripts/python.exe")
WD_CAND = str(ROOT / "extractors/_sipo_watchdog.py")
WD_ITEMS = str(ROOT / "extractors/_sipo_items_watchdog.py")
try:
    sys.stdout.reconfigure(encoding="utf-8")
except Exception:
    pass

# count python workers running a given ETL script (matches the ETL filename, NOT the
# *_watchdog drivers — so the watchdogs themselves don't count as "OCR busy").
_PS = ("@(Get-CimInstance Win32_Process -Filter \"name='python.exe'\" | "
       "Where-Object {{ $_.CommandLine -match '{pat}' }}).Count")


def _count(pat: str) -> int:
    r = subprocess.run(["powershell", "-NoProfile", "-Command", _PS.format(pat=pat)],
                       capture_output=True, text=True)
    try:
        return int((r.stdout or "").strip().splitlines()[-1])
    except Exception:
        return -1  # detection failed -> treat as BUSY (safer: don't start OCR blind)


def ocr_busy() -> bool:
    cand = _count("sipo_expenses_paddle_etl")
    items = _count("sipo_expense_items_paddle_etl")
    if cand < 0 or items < 0:
        return True  # detection error -> assume busy, keep waiting
    return cand > 0 or items > 0


def main() -> None:
    print("[queue] waiting for the machine to be OCR-free before starting...", flush=True)
    waited = 0
    while ocr_busy():
        time.sleep(30)
        waited += 30
        if waited % 300 == 0:
            print(f"[queue] still OCR-busy after {waited // 60} min, waiting...", flush=True)
    print(f"[queue] OCR-free after {waited // 60} min -> starting queued work", flush=True)

    print("[queue] (1/2) Social Democrats candidate-summary (Part 3)...", flush=True)
    subprocess.run([PY, WD_CAND, "socdem"])

    print("[queue] (2/2) Part-4 items: fg green lab pbp socdem...", flush=True)
    subprocess.run([PY, WD_ITEMS, "fg", "green", "lab", "pbp", "socdem"])

    print("[queue] ALL QUEUED OCR COMPLETE", flush=True)


if __name__ == "__main__":
    main()
