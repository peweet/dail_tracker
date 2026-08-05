"""Ratchet for the BLAS-thread cap that stops the box running out of RAM.

Background (measured 2026-07-26, 20-core Windows box): NumPy's bundled OpenBLAS sizes its
per-thread arenas by core count and Windows charges the reservation to the process commit,
so ``import pandas`` cost 656 MB per process — 46 MB when capped to one thread. With one
MCP server per open Claude session plus hooks and pytest, that reservation was the largest
single consumer on the machine and two sessions died of OOM.

These tests fail if the cap stops being applied, or if an entry point grows an import that
drags numpy in *before* the cap runs (at which point the setting is silently a no-op —
the failure mode that makes this worth a test rather than a comment).
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[1]

#: (file, module-or-marker that must appear AFTER the runtime_env import). Each of these
#: is a process entry point that goes on to import pandas/numpy, directly or via streamlit.
ENTRY_POINTS = (
    ("mcp_server/server.py", "dail_tracker_core"),
    ("pipeline.py", "manifest"),
    ("utility/app.py", None),
    ("utility/_bootstrap.py", None),
    ("test/conftest.py", None),
    # Added 2026-07-27. These are `python -m` entry points that import polars, and therefore
    # numpy, so they pay the full BLAS reservation if the cap import drifts below them.
    # Registering them here turns a silent regression into a failing test: `ruff --fix` moves a
    # bare `import services.runtime_env` down into the third-party block, which is why each of
    # these files fences it with `# isort: off` / `# isort: on` as pipeline.py does.
    ("extractors/npws_datasheets_extract.py", "polars"),
    ("extractors/ppr_sales_extract.py", "polars"),
    ("extractors/census_saps_2022_fetch.py", "polars"),
    # Added 2026-08-01: the API entry point imports dail_tracker_core (→ polars/numpy)
    # via its routers. Found missing the cap in the 07-31 scaling audit; fixed same sweep.
    ("api/main.py", "fastapi"),
)

#: Config files that pin the cap for processes spawned by Claude Code, which get env from
#: the parent rather than from an import.
CONFIG_CAPS = (
    (".mcp.json", "1"),
    (".claude/settings.json", "4"),
)


def _first_line_matching(text: str, pattern: str) -> int:
    for i, line in enumerate(text.splitlines()):
        if re.search(pattern, line):
            return i
    return -1


def test_cap_applies_in_a_fresh_process():
    """A bare `import services.runtime_env` must set the vars before numpy is loaded."""
    code = (
        "import services.runtime_env as r, os, sys;"
        "print(os.environ.get('OPENBLAS_NUM_THREADS'), r.applied_in_time,"
        " 'numpy' in sys.modules)"
    )
    out = subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert out.returncode == 0, out.stderr
    threads, in_time, numpy_loaded = out.stdout.split()
    assert int(threads) >= 1
    assert in_time == "True", "runtime_env ran after numpy was already imported — cap is a no-op"
    assert numpy_loaded == "False", "runtime_env must not import numpy itself"


def test_cap_actually_reduces_committed_memory():
    """The whole point: capped pandas must cost far less commit than uncapped.

    Guards against a future numpy/OpenBLAS release changing the variable names, which
    would leave the cap in place but doing nothing.
    """
    if sys.platform != "win32":
        pytest.skip("commit measurement uses the Win32 psapi API")
    probe = """
import ctypes
from ctypes import wintypes


class Counters(ctypes.Structure):
    _fields_ = [("cb", wintypes.DWORD), ("faults", wintypes.DWORD)] + [
        (name, ctypes.c_size_t) for name in
        ("peak_ws", "ws", "peak_paged", "paged", "peak_nonpaged", "nonpaged",
         "pagefile", "peak_pagefile", "private")
    ]


kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
psapi = ctypes.WinDLL("psapi", use_last_error=True)
kernel32.GetCurrentProcess.restype = wintypes.HANDLE
psapi.GetProcessMemoryInfo.argtypes = [
    wintypes.HANDLE, ctypes.POINTER(Counters), wintypes.DWORD
]
psapi.GetProcessMemoryInfo.restype = wintypes.BOOL


def private_mb():
    counters = Counters()
    counters.cb = ctypes.sizeof(Counters)
    psapi.GetProcessMemoryInfo(
        kernel32.GetCurrentProcess(), ctypes.byref(counters), counters.cb
    )
    return counters.private / 1048576


baseline = private_mb()
import pandas  # noqa: F401
print(int(private_mb() - baseline))
"""
    capped = subprocess.run(
        [sys.executable, "-c", "import services.runtime_env\n" + probe],
        cwd=str(REPO),
        capture_output=True,
        text=True,
        timeout=300,
    )
    assert capped.returncode == 0, capped.stderr
    cost_mb = int(capped.stdout.strip().splitlines()[-1])
    # Uncapped on a 20-core box this was 656 MB; capped to 4 threads, 142 MB. 400 MB is a
    # generous ceiling that still fails loudly if the cap stops working.
    assert cost_mb < 400, f"pandas import cost {cost_mb} MB — the BLAS cap is not taking effect"


@pytest.mark.parametrize("relpath,later_import", ENTRY_POINTS)
def test_entry_point_imports_runtime_env_before_anything_heavy(relpath, later_import):
    """Ordering is the whole contract — a cap set after numpy loads does nothing."""
    text = (REPO / relpath).read_text(encoding="utf-8")
    cap_line = _first_line_matching(text, r"runtime_env|_bootstrap")
    assert cap_line >= 0, f"{relpath} no longer caps BLAS threads"
    for pattern in (r"^\s*import (pandas|numpy|streamlit)", r"^\s*from (pandas|numpy|streamlit)"):
        heavy = _first_line_matching(text, pattern)
        if heavy >= 0:
            assert cap_line < heavy, f"{relpath} imports pandas/numpy/streamlit before the cap"
    if later_import:
        after = _first_line_matching(text, rf"^\s*(from|import) {later_import}")
        assert after > cap_line, f"{relpath} imports {later_import} before the cap"


@pytest.mark.parametrize("relpath,expected", CONFIG_CAPS)
def test_config_pins_thread_cap(relpath, expected):
    """Claude-spawned processes get the cap from env, not from an import."""
    cfg = json.loads((REPO / relpath).read_text(encoding="utf-8"))
    env = cfg["mcpServers"]["dail-tracker"]["env"] if relpath == ".mcp.json" else cfg["env"]
    assert env.get("OPENBLAS_NUM_THREADS") == expected
    assert env.get("OMP_NUM_THREADS") == expected


def test_memory_guard_blocks_heavy_command_under_the_floor(monkeypatch):
    """The backstop: a measured-heavy command must be refused when RAM is exhausted."""
    sys.path.insert(0, str(REPO / "tools" / "hooks"))
    import guard_memory  # type: ignore

    monkeypatch.setattr(
        guard_memory,
        "sample_memory",
        lambda: {
            "free_mb": 200,
            "total_mb": 16000,
            "load_pct": 98,
            "commit_free_mb": 1000,
            "commit_total_mb": 46000,
        },
    )
    monkeypatch.setattr(guard_memory, "record", lambda entry: None)
    monkeypatch.delenv("DAIL_SKIP_MEM_GUARD", raising=False)
    monkeypatch.setattr(
        "sys.stdin",
        type(
            "S",
            (),
            {"read": staticmethod(lambda: json.dumps({"tool_input": {"command": "python -m pytest test/siting -q"}}))},
        )(),
    )
    assert guard_memory.main() == 2, "heavy command was allowed with 200 MB free"


def test_memory_guard_allows_ordinary_commands(monkeypatch):
    sys.path.insert(0, str(REPO / "tools" / "hooks"))
    import guard_memory  # type: ignore

    monkeypatch.setattr(guard_memory, "sample_memory", lambda: {"free_mb": 100, "total_mb": 16000})
    monkeypatch.setattr(guard_memory, "record", lambda entry: None)
    monkeypatch.setattr(
        "sys.stdin",
        type("S", (), {"read": staticmethod(lambda: json.dumps({"tool_input": {"command": "git status"}}))})(),
    )
    assert guard_memory.main() == 0, "an ordinary command must never be blocked"
