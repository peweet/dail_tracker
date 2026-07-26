"""Process-level runtime caps — imported FIRST by every entry point.

Why this exists (measured 2026-07-26, 20-core box): NumPy's bundled OpenBLAS sizes
its per-thread scratch arenas by core count, and Windows charges the whole reservation
to the process's commit. ``import numpy`` cost **620 MB** of committed memory here;
``import pandas`` cost **656 MB**. Capping the BLAS thread count collapses it — the
cost is linear at roughly 32 MB per thread:

    threads   1     2     4     8    20 (uncapped)
    pandas   46MB  78MB 142MB 271MB  656MB

That mattered because this repo runs many python processes at once: one MCP server per
open Claude session, hook processes, pipeline chains, pytest. Eleven MCP servers were
holding 7.9 GB of commit on a 15.7 GB box, which is what pushed the machine into a
page-file thrash and killed sessions with OOM.

**Ordering rule: this must be imported before numpy/pandas/streamlit.** Once numpy has
loaded, OpenBLAS has already reserved its arenas and these variables do nothing. Import
it as the first statement in an entry point, above every other import. ``applied_in_time``
records whether that ordering actually held, so a test can assert it rather than trust it.

Every value is a ``setdefault``: an explicitly-exported environment variable always wins,
so ``.mcp.json`` and ``.claude/settings.json`` can pin a tighter cap (they use 1 — an MCP
server and a hook run no linear algebra at all), and a heavy scikit-learn job can raise it.

Nothing here prints. The MCP server speaks JSON-RPC over stdout; a stray write corrupts
the stream.
"""

from __future__ import annotations

import os
import sys

#: Environment variables read by the threading layers NumPy/SciPy/scikit-learn ship with.
#: OpenBLAS is the one that costs memory here; the rest are capped for consistency so a
#: different wheel (MKL, Accelerate) can't reintroduce the same reservation unnoticed.
THREAD_VARS = (
    "OPENBLAS_NUM_THREADS",
    "OMP_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "VECLIB_MAXIMUM_THREADS",
)

#: Four threads keeps useful BLAS parallelism for the scikit-learn work (the siting
#: works-type classifier) at 142 MB, versus 46 MB single-threaded and 656 MB uncapped.
DEFAULT_BLAS_THREADS = 4

#: Override knob for a job that genuinely wants wide BLAS parallelism.
OVERRIDE_VAR = "DAIL_BLAS_THREADS"

#: False if numpy was already imported when the cap ran — meaning the cap came too late
#: and this process still pays the full reservation. Asserted by test/test_runtime_env.py.
applied_in_time: bool = True


def cap_blas_threads(threads: int | str | None = None) -> int:
    """Pin the BLAS thread count for this process. Returns the value applied.

    Respects any variable already exported by the parent process, so config-level caps
    (.mcp.json, .claude/settings.json) and a caller's explicit export both win.
    """
    global applied_in_time
    applied_in_time = "numpy" not in sys.modules

    if threads is None:
        threads = os.environ.get(OVERRIDE_VAR) or DEFAULT_BLAS_THREADS
    try:
        value = max(1, int(threads))
    except (TypeError, ValueError):
        value = DEFAULT_BLAS_THREADS

    for var in THREAD_VARS:
        os.environ.setdefault(var, str(value))
    return value


cap_blas_threads()
