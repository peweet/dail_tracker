#!/usr/bin/env python
"""PreToolUse hook — block heavy commands when free RAM is low (refuse to start a
memory-heavy command when RAM is already gone).

Written 2026-07-26 after two sessions died of OOM in one afternoon. The post-mortem:
this box has 15.7 GB of RAM, every open Claude session holds ~1.2 GB (client + its MCP
server), and `pytest test/siting` alone commits 5.4 GB. With ten sessions open there was
no headroom left, so the next heavy command tipped the machine into page-file thrash and
took the session with it. The root cause of the per-process bloat is fixed separately
(services/runtime_env.py caps the BLAS thread reservation); this hook is the backstop for
the part that config can't fix — too many heavy jobs running at once.

Guardrail tier (see MEMORY.md feedback_guardrail_determinism_tiers): this is a *hard*
check, because the consequence is a lost session rather than a style wobble. It blocks
only commands that have actually been measured as heavy, and only under the floor.

Cross-tool notes, same as guard_data_reads.py:
  * VS Code ignores matcher syntax and fires every hook under an event, so this script
    self-filters on the command text rather than trusting the matcher.
  * Tool-input keys differ across tools (snake_case vs camelCase) — try all.
  * stderr + exit 2 blocks in both tools. Exit 0 allows.

Escape hatch: set DAIL_SKIP_MEM_GUARD=1 when you genuinely mean to run it anyway.

Side effect: every heavy command is sampled into logs/memory_pressure.jsonl, which is
what makes the accumulation visible next time instead of being re-derived from scratch.
"""

from __future__ import annotations

import ctypes
import json
import os
import re
import sys
from ctypes import wintypes
from datetime import UTC, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
LEDGER = REPO / "logs" / "memory_pressure.jsonl"

#: Commands measured to commit >1 GB. Matched case-insensitively against the command text.
#: Add here only with a measurement — a pattern with no number behind it is superstition.
HEAVY_PATTERNS = (
    (r"pytest\s+test/siting", "siting test suite (~5.4 GB measured 2026-07-26)"),
    (
        r"pytest(?:\s+\S+)*\s+planning[\\/]+product[\\/]+test(?:\s|$)",
        "SpecPlan full test suite (~6.6 GB observed 2026-08-24)",
    ),
    (r"siting_engine_(regression|backtest|calibration)", "siting engine harness (~0.5-1 GB)"),
    (r"\bpipeline\.py", "full ETL pipeline (spawns per-chain subprocesses)"),
    (r"siting_report(_dryrun)?\.py", "siting report harness (loads the layer store)"),
    (r"streamlit\s+run", "Streamlit app (pandas + layer store)"),
    (r"paddleocr|ocr_run|run_ocr", "OCR run (documented RAM-exhaustion crash in MEMORY.md)"),
    (r"\bgit\s+(gc|repack)\b", "git repack over a parquet-heavy repo"),
)

#: Below this much free physical RAM, a heavy command is refused. 1.5 GB is under the
#: smallest measured heavy job (siting harness) and well under the biggest (5.4 GB), so
#: crossing it means the job cannot fit without paging.
FLOOR_MB = 1500


class _MemoryStatusEx(ctypes.Structure):
    _fields_ = [
        ("dwLength", wintypes.DWORD),
        ("dwMemoryLoad", wintypes.DWORD),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


def sample_memory() -> dict[str, int]:
    """Free/total physical and commit, in MB. Empty dict off-Windows or on any failure."""
    try:
        status = _MemoryStatusEx()
        status.dwLength = ctypes.sizeof(_MemoryStatusEx)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return {}
        mb = 1024 * 1024
        return {
            "free_mb": status.ullAvailPhys // mb,
            "total_mb": status.ullTotalPhys // mb,
            "load_pct": status.dwMemoryLoad,
            "commit_free_mb": status.ullAvailPageFile // mb,
            "commit_total_mb": status.ullTotalPageFile // mb,
        }
    except Exception:
        return {}


def _command(payload: dict) -> str:
    ti = payload.get("tool_input") or payload.get("toolInput") or payload.get("input") or {}
    if not isinstance(ti, dict):
        return ""
    for key in ("command", "cmd", "script"):
        value = ti.get(key)
        if isinstance(value, str) and value:
            return value
    return ""


def classify(command: str) -> tuple[str, str]:
    """Return (pattern, why) for the first heavy pattern the command matches."""
    for pattern, why in HEAVY_PATTERNS:
        if re.search(pattern, command, re.IGNORECASE):
            return pattern, why
    return "", ""


def record(entry: dict) -> None:
    """Append one sample. Never raises — a ledger write must not break a tool call."""
    try:
        LEDGER.parent.mkdir(parents=True, exist_ok=True)
        with LEDGER.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        pass


def main() -> int:
    try:
        payload = json.loads(sys.stdin.read() or "{}")
    except Exception:
        return 0  # never break the agent on a parse hiccup

    command = _command(payload)
    if not command:
        return 0
    pattern, why = classify(command)
    if not pattern:
        return 0

    mem = sample_memory()
    free = mem.get("free_mb")
    blocked = free is not None and free < FLOOR_MB and os.environ.get("DAIL_SKIP_MEM_GUARD") != "1"
    record(
        {
            "ts": datetime.now(UTC).isoformat(timespec="seconds"),
            "kind": why,
            "blocked": blocked,
            **mem,
        }
    )
    if not blocked:
        return 0

    sys.stderr.write(
        f"Blocked: {free} MB of physical RAM free (floor {FLOOR_MB} MB) and this command is "
        f"{why}. Starting it now would push the machine into page-file thrash — that is what "
        "killed two sessions on 2026-07-26.\n"
        "Free memory first: close surplus Claude/VS Code windows (each open session holds "
        "~1.2 GB: the client plus its own MCP server), or wait for the running job to finish. "
        "Check what is holding memory with:\n"
        "  Get-Process | Group-Object Name | Sort-Object @{e={($_.Group|Measure-Object "
        "PrivateMemorySize64 -Sum).Sum}} -Descending | Select-Object -First 6 Name, Count\n"
        "Override with DAIL_SKIP_MEM_GUARD=1 if you mean it."
    )
    return 2


if __name__ == "__main__":
    sys.exit(main())
