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

Every knob is an environment variable so a session can opt out without a code change
(``DAIL_MCP_IDLE_SECONDS=0`` disables the watchdog entirely).
"""

from __future__ import annotations

import logging
import os
import tempfile
import threading
import time
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
        "memory_limit": memory_limit(),
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
    return conn


class _Activity:
    """Tool-call activity counter — the watchdog's safety interlock.

    ``idle_for`` returns None while any call is in flight, which is what stops the
    watchdog closing a connection out from under a running query.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._inflight = 0
        self._last = time.monotonic()

    def __enter__(self) -> _Activity:
        with self._lock:
            self._inflight += 1
            self._last = time.monotonic()
        return self

    def __exit__(self, *exc_info: object) -> bool:
        with self._lock:
            self._inflight -= 1
            self._last = time.monotonic()
        return False

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
        with ACTIVITY:
            return await inner(*args, **kwargs)

    manager.call_tool = tracked
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
    if idle_seconds() <= 0:
        _log.info("MCP resource policy: idle release disabled")
        return None

    stop_event = stop or threading.Event()

    def loop() -> None:
        while not stop_event.wait(poll_seconds):
            try:
                if release():
                    _log.info("MCP resource policy: released idle connection")
            except Exception:  # noqa: BLE001 — the watchdog must outlive any single failure
                _log.exception("MCP resource policy: idle release failed")

    thread = threading.Thread(target=loop, name="dail-mcp-idle-release", daemon=True)
    thread.stop_event = stop_event  # type: ignore[attr-defined]
    thread.start()
    return thread
