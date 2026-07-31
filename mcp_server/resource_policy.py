"""Per-process resource policy for the MCP server: DuckDB caps + idle release.

Why this exists. The stdio transport's process model is one server subprocess per
client session — the MCP spec has the client launch the server as a subprocess and
terminate it by closing stdin, so the lifecycle belongs to the client, not to us.
Every open Claude Code window therefore holds its own interpreter, and each one's
DuckDB connection took the library defaults: ``memory_limit`` = 80% of system RAM
(12.5 GiB on this box) and ``threads`` = core count (20). Measured 2026-07-27 with
seven concurrent sessions live, that is seven processes each believing it may take
12.5 GiB — the OOM path documented in ``services/runtime_env.py``, multiplied by the
session count. The BLAS caps in ``.mcp.json`` do NOT reach DuckDB's own thread pool.

Two policies, both scoped to THIS process. The Streamlit and API layers build their
own connections from ``dail_tracker_core.connections`` and keep the DuckDB defaults —
capping a 20-core box's analytics UI to 4 threads would be a real regression, so the
caps live here rather than in the shared connection builder.

  1. ``apply_caps`` at connection build — memory_limit, threads, and an ABSOLUTE
     temp_directory. DuckDB's default is the relative ``.tmp``, which spills into
     whatever directory the client happened to launch us from (the repo).
  2. Idle release — after ``DAIL_MCP_IDLE_SECONDS`` with no tool call in flight, the
     server closes its union connection and drops the per-process index caches. The
     next call rebuilds both lazily, which is what ``server._cur`` already does.

The server process itself NEVER exits on idle. Under stdio the client owns the
lifecycle; a self-terminating server presents to the user as a dead MCP connection
and an unexplained ``/mcp`` reconnect. Releasing state is the only legal spin-down.

Third policy, added after a call hung for 1801 s during a load test on 27 July: a
call deadline. Two layers, and both are needed for different reasons.

  * Client side — ``"timeout": 360000`` in ``.mcp.json``. Claude Code's own per-server
    wall-clock limit, whose default (``MCP_TOOL_TIMEOUT``) is about 28 HOURS, which is
    why a stuck call simply hangs. This makes the client give up in six minutes.
    ``.mcp.json`` is strict JSON and cannot hold a comment, so the reason lives here.
  * Server side — ``enforce_call_deadline`` at 300 s, deliberately UNDER the client's
    360 s. A client-side timeout alone stops the client waiting; it does NOT stop the
    query, which keeps consuming memory to completion. Interrupting at the database is
    what ends the work, and finishing first means the client receives a real error
    message instead of a blind timeout.

Every knob is an environment variable so a session can opt out without a code change
(``DAIL_MCP_IDLE_SECONDS=0`` disables the watchdog entirely).

Fourth policy, added 2026-07-31: adaptive tightening under live memory pressure.
Policies 1 and 2 above are a fixed budget sized for the worst case; ``effective_
memory_limit``/``effective_idle_seconds`` wrap them with a live free-RAM check
(``free_ram_mb``) so a process spawned — or still running — while RAM is already
tight claims less and lets go sooner, without changing the pure env-driven defaults
the tests pin. See the "Adaptive tightening" section below for the thresholds.
"""

from __future__ import annotations

import contextlib
import logging
import os
import sys
import tempfile
import threading
import time
import weakref
from collections.abc import Callable
from pathlib import Path

_log = logging.getLogger(__name__)

# Chosen against the measured profile (2026-07-27): a fresh server floors at ~90 MB of
# imports and the heaviest observed working state was 243 MB. 1 GB leaves ~4x headroom
# over anything a read tool has ever needed while making the seven-session worst case
# bounded instead of 7 x 12.5 GiB. 4 threads keeps a scan parallel without seven
# servers oversubscribing a 20-core box.
DEFAULT_MEMORY_LIMIT = "1GB"
DEFAULT_THREADS = 4
DEFAULT_IDLE_SECONDS = 900  # 15 min — longer than a pause in conversation, shorter than a lunch
POLL_SECONDS = 30

# A tool call that overruns this is aborted at the database. Measured 2026-07-27: normal
# reads land under 10 s and the ~77-view build takes ~3 s, so 300 s is ~30x the slowest
# ordinary call. The one legitimate long operation is a FIRST full-text corpus build —
# text_fts measured ~4 s, but precedent_fts reads ~14k inspector reports and its own
# docstring budgets ~5 minutes, so a machine building that corpus for the first time
# should raise DAIL_MCP_CALL_TIMEOUT_SECONDS rather than have the build cut short.
# An interrupted build is safe by construction: both modules write their freshness meta
# only AFTER a successful index build, so a killed build rebuilds rather than serving
# a half-made index.
DEFAULT_CALL_TIMEOUT_SECONDS = 300


def _env(name: str, default: str) -> str:
    return os.environ.get(name, "").strip() or default


def _env_int(name: str, default: int) -> int:
    try:
        return int(_env(name, str(default)))
    except ValueError:
        _log.warning("MCP resource policy: %s is not an integer — using %d", name, default)
        return default


def memory_limit() -> str:
    return _env("DAIL_MCP_DUCKDB_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT)


def threads() -> int:
    return max(1, _env_int("DAIL_MCP_DUCKDB_THREADS", DEFAULT_THREADS))


def idle_seconds() -> int:
    """Seconds of inactivity before the connection is released. 0 disables the watchdog."""
    return max(0, _env_int("DAIL_MCP_IDLE_SECONDS", DEFAULT_IDLE_SECONDS))


def call_timeout() -> int:
    """Seconds a single tool call may run before its queries are aborted. 0 disables."""
    return max(0, _env_int("DAIL_MCP_CALL_TIMEOUT_SECONDS", DEFAULT_CALL_TIMEOUT_SECONDS))


# ── Adaptive tightening under live memory pressure ─────────────────────────────
# The caps above are a fixed budget sized for the worst case (many sessions open at
# once). On a laptop that swings between "nothing else open" and "eleven sessions",
# a fixed budget either wastes headroom when RAM is plentiful or isn't tight enough
# when it's scarce. These wrap the configured values with a live free-RAM check so a
# process spawned (or still running) under pressure claims less and lets go sooner —
# without changing `idle_seconds()`/`memory_limit()` themselves, which stay pure and
# env-driven because test_resource_policy.py asserts their defaults deterministically.
#
# The floor and the two tightened values are a reasoned heuristic, not a measurement
# like the numbers above — retune PRESSURE_FLOOR_MB / PRESSURE_IDLE_SECONDS /
# PRESSURE_MEMORY_LIMIT if a future session profiles this and finds a better fit.

#: Below this much free physical RAM, tighten. Set above guard_memory.py's 1,500 MB
#: hard-block floor (tools/hooks/guard_memory.py) so this backs off before that
#: harder guard would refuse a heavy command outright.
PRESSURE_FLOOR_MB = 3000
PRESSURE_IDLE_SECONDS = 180
PRESSURE_MEMORY_LIMIT = "512MB"


def free_ram_mb() -> int | None:
    """Free physical RAM in MB, or None off-Windows or on any failure.

    Same GlobalMemoryStatusEx approach as tools/hooks/guard_memory.py's
    ``sample_memory``, duplicated rather than imported — a hook script and a server
    runtime module are different layers for what is one ctypes call.
    """
    if sys.platform != "win32":
        return None
    try:
        import ctypes
        from ctypes import wintypes

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

        status = _MemoryStatusEx()
        status.dwLength = ctypes.sizeof(_MemoryStatusEx)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return None
        return int(status.ullAvailPhys // (1024 * 1024))
    except Exception:
        return None


def under_pressure() -> bool:
    """True when this process's live free-RAM reading is below ``PRESSURE_FLOOR_MB``."""
    free = free_ram_mb()
    return free is not None and free < PRESSURE_FLOOR_MB


def effective_memory_limit() -> str:
    """``memory_limit()``, tightened to ``PRESSURE_MEMORY_LIMIT`` when RAM is tight.

    An explicit ``DAIL_MCP_DUCKDB_MEMORY_LIMIT`` always wins — this only adjusts the
    default, the same way runtime_env.py's BLAS caps let an explicit export win.
    """
    if os.environ.get("DAIL_MCP_DUCKDB_MEMORY_LIMIT", "").strip():
        return memory_limit()
    if under_pressure():
        return PRESSURE_MEMORY_LIMIT
    return memory_limit()


def effective_idle_seconds() -> int:
    """``idle_seconds()``, tightened to ``PRESSURE_IDLE_SECONDS`` when RAM is tight.

    An explicit ``DAIL_MCP_IDLE_SECONDS`` (including ``0`` to disable) always wins.
    Re-evaluated on every watchdog poll, so a process that starts with headroom and
    later runs low (more sessions opened after it) starts releasing sooner without a
    restart.
    """
    if os.environ.get("DAIL_MCP_IDLE_SECONDS", "").strip():
        return idle_seconds()
    configured = idle_seconds()
    if configured and under_pressure():
        return min(configured, PRESSURE_IDLE_SECONDS)
    return configured


def log_startup_pressure() -> None:
    """Log once if this process is starting under the pressure floor.

    Can't refuse to spawn — under stdio the client already launched the subprocess
    before this runs, and the server must never self-exit (see module docstring).
    This is the graceful substitute: a visible warning plus the tightened budget
    ``effective_memory_limit``/``effective_idle_seconds`` already apply for the rest
    of this process's life.
    """
    free = free_ram_mb()
    if free is not None and free < PRESSURE_FLOOR_MB:
        _log.warning(
            "MCP resource policy: starting with %d MB free RAM (floor %d MB) — "
            "running tightened (memory_limit=%s, idle_release=%ds) for this process's life",
            free,
            PRESSURE_FLOOR_MB,
            PRESSURE_MEMORY_LIMIT,
            PRESSURE_IDLE_SECONDS,
        )


def spill_dir() -> Path:
    """Absolute directory for DuckDB spill files, created if absent.

    Default is the OS temp dir, NOT the repo: DuckDB's stock ``temp_directory`` is the
    relative ``.tmp``, so a spilling query writes into the client's launch directory.
    """
    path = Path(_env("DAIL_MCP_SPILL_DIR", str(Path(tempfile.gettempdir()) / "dail_mcp_spill")))
    try:
        path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:  # read-only or missing volume — leave DuckDB on its default
        _log.warning("MCP resource policy: spill dir %s unusable (%s)", path, exc)
    return path


def apply_caps(conn) -> dict[str, str]:
    """Cap ``conn``'s memory, thread count and spill directory. Returns what took effect.

    Best-effort per setting: DuckDB raises ``CatalogException`` for a setting it doesn't
    know, so a renamed knob on a future version degrades that one cap to the default and
    logs, rather than taking the server down on a dependency bump.
    """
    wanted = {
        "memory_limit": effective_memory_limit(),
        "threads": str(threads()),
        "temp_directory": spill_dir().as_posix(),
    }
    applied: dict[str, str] = {}
    for setting, value in wanted.items():
        try:
            conn.execute(f"SET {setting} = ?", [value])
            applied[setting] = str(conn.execute("SELECT current_setting(?)", [setting]).fetchone()[0])
        except Exception as exc:  # noqa: BLE001 — a cap is an optimisation, never a hard dependency
            _log.warning("MCP resource policy: could not set %s=%s (%s)", setting, value, exc)
    return applied


def capped_connect(database: str = ":memory:", *, read_only: bool = False):
    """Open a DuckDB connection with this process's caps already applied.

    Every connection counts, and the union connection is NOT the only one the server
    opens — the FTS paths and the SQL-graph parser each open their own. Measured
    2026-07-27 on a fixed workload, that distinction is the whole game: a BM25 search
    over the speech corpus transiently commits ~1.4 GB, which is ~10x everything the
    union connection does and was completely unaffected by capping the union alone.
    Use this instead of ``duckdb.connect`` anywhere in ``mcp_server``.
    """
    import duckdb

    conn = duckdb.connect(database, read_only=read_only)
    apply_caps(conn)
    register_connection(conn)
    return conn


# Every live connection, weakly held so a closed one drops out on its own. Needed
# because a runaway query is not necessarily on the union connection — the FTS paths
# and the SQL-graph parser each open their own, and an interrupt only reaches the
# connection it is called on.
_LIVE_CONNECTIONS: weakref.WeakSet = weakref.WeakSet()

# Start time of the call the deadline last acted on, so one stuck call is interrupted
# once rather than on every poll.
_LAST_DEADLINE_EPISODE: float | None = None


def register_connection(conn) -> None:
    """Track a connection (or cursor) so ``interrupt_all`` can reach it.

    ``capped_connect`` calls this itself; the union connection is built by
    ``dail_tracker_core.connections.api_conn`` and is registered by the server.

    ⚠ CURSORS MUST BE REGISTERED TOO, and this is the trap that made the first version
    of the deadline useless. Verified 2026-07-31: ``interrupt()`` on a parent connection
    does NOT abort a query running on a cursor derived from it — the query ran to
    completion through 34 consecutive interrupts — while interrupting the cursor itself
    aborted in 2.0 s. Every tool in this server runs on ``_CONN.cursor()``, so
    registering only the connection would interrupt the one object no query uses.
    """
    # TypeError: a non-weakrefable stand-in (some test doubles) — tracking is best-effort.
    with contextlib.suppress(TypeError):
        _LIVE_CONNECTIONS.add(conn)


def interrupt_all() -> int:
    """Abort every in-flight query on every tracked connection. Returns how many were hit.

    Verified 2026-07-27 that ``interrupt()`` aborts a running query from another thread
    (the query raises ``InterruptException`` and the worker returns), which is what
    makes a deadline meaningful: FastMCP runs sync tool bodies in a worker thread, so
    cancelling the awaitable alone would leave the query burning memory to completion.

    Blunt on purpose — it cancels every query on every connection, not just the
    overrunning one. Under stdio there is a single client and calls are effectively
    serial, so the collateral risk is low; it would need revisiting before any shared
    multi-client daemon.
    """
    hit = 0
    for conn in list(_LIVE_CONNECTIONS):
        try:
            conn.interrupt()
            hit += 1
        except Exception:  # noqa: BLE001 — a closed or busy connection must not stop the sweep
            continue
    return hit


class _Activity:
    """Tool-call activity tracker — the watchdog's safety interlock and its deadline clock.

    ``idle_for`` returns None while any call is in flight, which is what stops the
    watchdog closing a connection out from under a running query. ``longest_inflight``
    is the other direction: how long the oldest running call has been going, which is
    what the deadline sweep tests.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inflight = 0
        self._last = time.monotonic()
        self._starts: dict[int, float] = {}
        self._seq = 0

    @contextlib.contextmanager
    def track(self):
        """Mark one call in flight for its duration.

        A per-call token, not a bare counter: concurrent calls finish out of order, and
        an ``__exit__`` that popped the oldest entry would let a fast call retire a slow
        call's start time and hide the exact overrun this is here to catch.
        """
        with self._lock:
            self._seq += 1
            token = self._seq
            self._inflight += 1
            self._starts[token] = self._last = time.monotonic()
        try:
            yield
        finally:
            with self._lock:
                self._inflight -= 1
                self._starts.pop(token, None)
                self._last = time.monotonic()

    @property
    def inflight(self) -> int:
        with self._lock:
            return self._inflight

    def idle_for(self) -> float | None:
        """Seconds since the last call ended, or None while a call is in flight."""
        with self._lock:
            if self._inflight > 0:
                return None
            return time.monotonic() - self._last

    def longest_inflight(self) -> float | None:
        """Age of the oldest running call, or None when nothing is running."""
        oldest = self.oldest()
        return None if oldest is None else oldest[1]

    def oldest(self) -> tuple[float, float] | None:
        """(start time, age) of the oldest running call, or None when idle.

        The start time doubles as an episode id: it lets the deadline sweep tell "the
        same overrunning call I already interrupted" from a new one, so a single stuck
        call produces one interrupt and one log line rather than one per poll.
        """
        with self._lock:
            if not self._starts:
                return None
            start = min(self._starts.values())
            return start, time.monotonic() - start


ACTIVITY = _Activity()


def instrument(mcp) -> bool:
    """Bracket every tool call for idle accounting. Returns True if the hook took.

    Wraps the FastMCP instance's ``ToolManager.call_tool`` — one interception point
    covering all tools, so no tool body changes and no per-tool decorator to forget.
    That attribute is private to the ``mcp`` SDK: if a future release renames it we log
    and return False, and the watchdog then never fires (heavier, but correct) instead
    of the server failing to start.
    """
    manager = getattr(mcp, "_tool_manager", None)
    inner = getattr(manager, "call_tool", None)
    if inner is None:
        _log.warning("MCP resource policy: no _tool_manager.call_tool to wrap — idle release disabled")
        return False

    async def tracked(*args, **kwargs):
        with ACTIVITY.track():
            return await inner(*args, **kwargs)

    manager.call_tool = tracked
    return True


def enforce_call_deadline() -> bool:
    """Abort the in-flight queries if the oldest call has outrun ``call_timeout``.

    Deliberately NOT an ``anyio.fail_after`` around the awaitable: FastMCP runs sync
    tool bodies through ``to_thread``, and cancelling the awaitable would leave the
    worker thread running the query to completion — the resource burn this exists to
    stop. Interrupting at the database is what actually ends the work; the tool body
    then raises ``InterruptException`` and the existing handler turns it into a normal
    tool error, so the client gets a message instead of a silent stall.

    Returns whether an interrupt was issued.
    """
    global _LAST_DEADLINE_EPISODE

    limit = call_timeout()
    if limit <= 0:
        return False
    oldest = ACTIVITY.oldest()
    if oldest is None:
        return False
    start, age = oldest
    if age < limit:
        return False
    if start == _LAST_DEADLINE_EPISODE:
        return False  # already interrupted this call — do not re-fire every poll
    _LAST_DEADLINE_EPISODE = start
    hit = interrupt_all()
    _log.warning(
        "MCP resource policy: tool call exceeded %ds (running %.0fs) — interrupted %d connection(s)",
        limit,
        age,
        hit,
    )
    return True


def drop_index_caches() -> None:
    """Clear the per-process index caches the tools rebuild lazily.

    Each ``reset_cache`` is cheap to undo: text/precedent FTS re-verify a fingerprint on
    next use, and the SQL graph re-parses ``sql_views/``.
    """
    from mcp_server import precedent_fts, sql_index, text_fts

    for module in (text_fts, precedent_fts, sql_index):
        reset = getattr(module, "reset_cache", None)
        if reset is None:
            continue
        try:
            reset()
        except Exception:  # noqa: BLE001 — a cache drop must never break a live server
            _log.exception("MCP resource policy: %s.reset_cache failed", module.__name__)


def start_watchdog(
    release: Callable[[], bool],
    *,
    poll_seconds: float = POLL_SECONDS,
    stop: threading.Event | None = None,
) -> threading.Thread | None:
    """Poll for idleness and call ``release`` when the process has gone quiet.

    ``release`` re-checks idleness under the connection lock and returns whether it
    actually released — this thread only decides *when to ask*. Returns None when the
    watchdog is disabled (``DAIL_MCP_IDLE_SECONDS=0``).

    ``stop`` lets a caller shut the loop down; the returned thread carries it as
    ``.stop_event``. Tests MUST set it — a daemon thread left polling past the end of a
    test run logs into an already-closed stderr at interpreter shutdown. Waiting on the
    event rather than sleeping also makes that shutdown immediate instead of one poll
    interval late.
    """
    idle_on, deadline_on = idle_seconds() > 0, call_timeout() > 0
    if not idle_on and not deadline_on:
        _log.info("MCP resource policy: idle release and call deadline both disabled")
        return None

    # The two duties are opposites — one fires only when nothing is running, the other
    # only when something is — so they share a thread without contending.
    if deadline_on:
        # Poll often enough that the overshoot past the deadline is a fraction of it,
        # not a second full poll interval.
        poll_seconds = min(poll_seconds, max(5.0, call_timeout() / 10))

    stop_event = stop or threading.Event()

    def loop() -> None:
        while not stop_event.wait(poll_seconds):
            try:
                if deadline_on:
                    enforce_call_deadline()
                if idle_on and release():
                    _log.info("MCP resource policy: released idle connection")
            except Exception:  # noqa: BLE001 — the watchdog must outlive any single failure
                _log.exception("MCP resource policy: watchdog sweep failed")

    thread = threading.Thread(target=loop, name="dail-mcp-watchdog", daemon=True)
    thread.stop_event = stop_event  # type: ignore[attr-defined]
    thread.start()
    return thread
