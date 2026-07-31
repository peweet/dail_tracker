"""MCP server resource policy — DuckDB caps and idle release.

The defect these guard (measured 2026-07-27): every MCP server subprocess built its
DuckDB connection on the library defaults — ``memory_limit`` 12.5 GiB (80% of RAM),
``threads`` 20 (one per core), ``temp_directory`` the relative ``.tmp``. Because the
stdio transport gives every Claude session its own subprocess, seven sessions were
live at once, each with that ceiling. These tests pin the caps, and pin that the idle
watchdog can never close a connection while a tool call is in flight.
"""

from __future__ import annotations

import asyncio
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("mcp")

import duckdb  # noqa: E402

from mcp_server import resource_policy  # noqa: E402


@pytest.fixture
def fresh_activity(monkeypatch):
    """A private activity counter so tests never disturb the module-level one."""
    activity = resource_policy._Activity()
    monkeypatch.setattr(resource_policy, "ACTIVITY", activity)
    return activity


# ── caps ──────────────────────────────────────────────────────────────────────


def test_apply_caps_bounds_memory_threads_and_spill(tmp_path, monkeypatch):
    monkeypatch.setenv("DAIL_MCP_SPILL_DIR", str(tmp_path / "spill"))
    conn = duckdb.connect()
    try:
        uncapped_mem = conn.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        uncapped_threads = int(conn.execute("SELECT current_setting('threads')").fetchone()[0])

        applied = resource_policy.apply_caps(conn)

        assert set(applied) == {"memory_limit", "threads", "temp_directory"}
        # The whole point: strictly below whatever DuckDB would have taken on its own.
        assert applied["memory_limit"] != uncapped_mem
        assert int(applied["threads"]) == resource_policy.DEFAULT_THREADS
        assert int(applied["threads"]) <= uncapped_threads
        # Spill must be absolute and OUTSIDE the repo — the default '.tmp' is relative
        # to the client's launch directory, which is the repo working tree.
        spill = Path(applied["temp_directory"])
        assert spill.is_absolute()
        assert spill.is_dir()
        assert Path(__file__).resolve().parents[2] not in spill.parents
    finally:
        conn.close()


def test_caps_are_env_overridable(tmp_path, monkeypatch):
    monkeypatch.setenv("DAIL_MCP_DUCKDB_MEMORY_LIMIT", "256MB")
    monkeypatch.setenv("DAIL_MCP_DUCKDB_THREADS", "2")
    monkeypatch.setenv("DAIL_MCP_SPILL_DIR", str(tmp_path / "spill"))
    conn = duckdb.connect()
    try:
        applied = resource_policy.apply_caps(conn)
        assert int(applied["threads"]) == 2
        assert "MiB" in applied["memory_limit"] or "MB" in applied["memory_limit"]
    finally:
        conn.close()


def test_apply_caps_survives_a_renamed_setting():
    """A DuckDB version bump that renames a knob must degrade, not kill the server."""

    class RejectsThreads:
        """Accepts every SET except `threads` — the partial-failure case."""

        def __init__(self):
            self.set_ok: list[str] = []

        def execute(self, sql, params=None):
            if sql.startswith("SET "):
                setting = sql.split()[1]
                if setting == "threads":
                    raise duckdb.CatalogException("unrecognized configuration parameter")
                self.set_ok.append(setting)
            return self

        def fetchone(self):
            return ("applied",)

    conn = RejectsThreads()
    applied = resource_policy.apply_caps(conn)

    assert "threads" not in applied  # the one that raised is simply absent
    assert {"memory_limit", "temp_directory"} <= set(applied)  # the others still took


def test_env_int_falls_back_on_garbage(monkeypatch):
    monkeypatch.setenv("DAIL_MCP_DUCKDB_THREADS", "not-a-number")
    assert resource_policy.threads() == resource_policy.DEFAULT_THREADS
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "")
    assert resource_policy.idle_seconds() == resource_policy.DEFAULT_IDLE_SECONDS


# ── adaptive tightening under live memory pressure ─────────────────────────────


def test_effective_values_untightened_when_ram_is_plentiful(monkeypatch):
    monkeypatch.setattr(resource_policy, "free_ram_mb", lambda: resource_policy.PRESSURE_FLOOR_MB + 1)
    assert resource_policy.effective_memory_limit() == resource_policy.memory_limit()
    assert resource_policy.effective_idle_seconds() == resource_policy.idle_seconds()


def test_effective_values_tighten_under_pressure(monkeypatch):
    monkeypatch.setattr(resource_policy, "free_ram_mb", lambda: resource_policy.PRESSURE_FLOOR_MB - 1)
    assert resource_policy.effective_memory_limit() == resource_policy.PRESSURE_MEMORY_LIMIT
    assert resource_policy.effective_idle_seconds() == resource_policy.PRESSURE_IDLE_SECONDS


def test_explicit_env_wins_over_pressure(monkeypatch):
    monkeypatch.setattr(resource_policy, "free_ram_mb", lambda: resource_policy.PRESSURE_FLOOR_MB - 1)
    monkeypatch.setenv("DAIL_MCP_DUCKDB_MEMORY_LIMIT", "2GB")
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "9999")
    assert resource_policy.effective_memory_limit() == "2GB"
    assert resource_policy.effective_idle_seconds() == 9999


def test_effective_idle_seconds_never_exceeds_configured(monkeypatch):
    """Pressure only ever shortens the wait, never lengthens a tighter explicit default."""
    monkeypatch.setattr(resource_policy, "free_ram_mb", lambda: resource_policy.PRESSURE_FLOOR_MB - 1)
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "")  # blank counts as unset -> falls through
    assert resource_policy.effective_idle_seconds() <= resource_policy.idle_seconds()


def test_under_pressure_none_when_free_ram_unreadable(monkeypatch):
    monkeypatch.setattr(resource_policy, "free_ram_mb", lambda: None)
    assert resource_policy.under_pressure() is False


def test_free_ram_mb_off_windows_returns_none(monkeypatch):
    monkeypatch.setattr(resource_policy.sys, "platform", "linux")
    assert resource_policy.free_ram_mb() is None


# ── activity accounting (the watchdog's safety interlock) ─────────────────────


def test_idle_for_is_none_while_a_call_is_in_flight(fresh_activity):
    assert fresh_activity.idle_for() is not None
    with fresh_activity.track():
        assert fresh_activity.inflight == 1
        assert fresh_activity.idle_for() is None  # <- what stops a release mid-query
    assert fresh_activity.inflight == 0
    assert fresh_activity.idle_for() is not None


def test_activity_counter_is_thread_safe(fresh_activity):
    def worker():
        for _ in range(200):
            with fresh_activity.track():
                pass

    threads = [threading.Thread(target=worker) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert fresh_activity.inflight == 0


def test_instrument_counts_calls_through_the_tool_manager(fresh_activity):
    seen: list[int | None] = []

    class FakeManager:
        async def call_tool(self, name, arguments, **kwargs):
            seen.append(fresh_activity.idle_for())
            return f"{name}:{arguments}"

    class FakeMCP:
        _tool_manager = FakeManager()

    fake = FakeMCP()
    assert resource_policy.instrument(fake) is True
    result = asyncio.run(fake._tool_manager.call_tool("search_members", {"q": "x"}))

    assert result == "search_members:{'q': 'x'}"
    assert seen == [None]  # the wrapper had already marked the call in flight
    assert fresh_activity.inflight == 0  # and unwound it afterwards


def test_instrument_reports_false_when_the_sdk_shape_changes():
    """Private SDK attribute: a rename must disable idle release, not break startup."""

    class NoManager:
        pass

    assert resource_policy.instrument(NoManager()) is False


# ── watchdog wiring ──────────────────────────────────────────────────────────


def test_watchdog_runs_only_when_it_has_a_duty(monkeypatch):
    """The thread serves two duties; it stands down only when BOTH are switched off."""
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "0")
    monkeypatch.setenv("DAIL_MCP_CALL_TIMEOUT_SECONDS", "0")
    assert resource_policy.start_watchdog(lambda: False) is None


def test_watchdog_still_starts_for_the_deadline_alone(monkeypatch):
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "0")
    monkeypatch.setenv("DAIL_MCP_CALL_TIMEOUT_SECONDS", "300")
    stop = threading.Event()
    thread = resource_policy.start_watchdog(lambda: False, stop=stop)
    try:
        assert thread is not None, "disabling idle release must not disable the deadline"
    finally:
        stop.set()
        thread.join(timeout=5)


# ── call deadline ────────────────────────────────────────────────────────────


def test_longest_inflight_tracks_the_oldest_call(fresh_activity):
    assert fresh_activity.longest_inflight() is None
    with fresh_activity.track():
        first = fresh_activity.longest_inflight()
        assert first is not None
        with fresh_activity.track():
            # A second, younger call must not reset the clock on the first.
            assert fresh_activity.longest_inflight() >= first
        assert fresh_activity.longest_inflight() >= first
    assert fresh_activity.longest_inflight() is None


def test_a_fast_call_does_not_retire_a_slow_calls_clock(fresh_activity):
    """The bug a bare counter would hide: out-of-order completion losing the start time."""
    outer = fresh_activity.track()
    outer.__enter__()
    try:
        with fresh_activity.track():
            pass  # a fast call starts and finishes inside the slow one
        assert fresh_activity.inflight == 1
        assert fresh_activity.longest_inflight() is not None
    finally:
        outer.__exit__(None, None, None)
    assert fresh_activity.longest_inflight() is None


def test_deadline_does_not_fire_below_the_limit(fresh_activity, monkeypatch):
    monkeypatch.setenv("DAIL_MCP_CALL_TIMEOUT_SECONDS", "3600")
    hits = []
    monkeypatch.setattr(resource_policy, "interrupt_all", lambda: hits.append(1) or 1)
    with fresh_activity.track():
        assert resource_policy.enforce_call_deadline() is False
    assert hits == []


def test_deadline_does_not_fire_when_nothing_is_running(fresh_activity, monkeypatch):
    monkeypatch.setenv("DAIL_MCP_CALL_TIMEOUT_SECONDS", "1")
    monkeypatch.setattr(resource_policy, "interrupt_all", lambda: 1)
    assert resource_policy.enforce_call_deadline() is False


def test_deadline_interrupts_an_overrunning_call(fresh_activity, monkeypatch):
    monkeypatch.setenv("DAIL_MCP_CALL_TIMEOUT_SECONDS", "1")
    hits = []
    monkeypatch.setattr(resource_policy, "interrupt_all", lambda: (hits.append(1), 2)[1])
    with fresh_activity.track():
        time.sleep(1.05)
        assert resource_policy.enforce_call_deadline() is True
    assert hits == [1]


def test_deadline_can_be_switched_off(fresh_activity, monkeypatch):
    monkeypatch.setenv("DAIL_MCP_CALL_TIMEOUT_SECONDS", "0")
    monkeypatch.setattr(resource_policy, "interrupt_all", lambda: 1)
    with fresh_activity.track():
        time.sleep(0.05)
        assert resource_policy.enforce_call_deadline() is False


def test_interrupt_all_reaches_registered_connections_and_survives_bad_ones():
    class Good:
        def __init__(self):
            self.hit = 0

        def interrupt(self):
            self.hit += 1

    class Broken:
        def interrupt(self):
            raise RuntimeError("already closed")

    good, broken = Good(), Broken()
    resource_policy.register_connection(good)
    resource_policy.register_connection(broken)
    try:
        hit = resource_policy.interrupt_all()
        assert good.hit == 1, "a live connection must be interrupted"
        assert hit >= 1, "a broken connection must not abort the sweep"
    finally:
        resource_policy._LIVE_CONNECTIONS.discard(good)
        resource_policy._LIVE_CONNECTIONS.discard(broken)


def test_interrupting_a_connection_does_not_reach_its_cursor():
    """The defect that made the first deadline useless, pinned as a property of DuckDB.

    Every tool runs on ``_CONN.cursor()``. Interrupting the parent left the query
    running through 34 consecutive sweeps. If a future DuckDB makes interrupt
    propagate, this test fails and `server._cur` can stop registering cursors — but
    until then, registering only the connection interrupts nothing that matters.
    """
    parent = duckdb.connect()
    cur = parent.cursor()
    out = {}

    def run():
        try:
            cur.execute("SELECT count(*) FROM range(20000000000) t(i) WHERE i % 97 = 3").fetchone()
            out["r"] = "COMPLETED"
        except Exception as exc:  # noqa: BLE001
            out["r"] = type(exc).__name__

    t = threading.Thread(target=run, daemon=True)
    t.start()
    time.sleep(1.0)
    parent.interrupt()  # the WRONG object
    t.join(timeout=6)
    assert t.is_alive(), "interrupt now propagates to cursors — _cur() need not register them"
    cur.interrupt()  # the right one
    t.join(timeout=30)
    assert not t.is_alive()
    assert out["r"] == "InterruptException"


def test_cur_registers_the_cursor_not_only_the_connection(monkeypatch):
    """Guards the wiring: a cursor handed to a tool must be reachable by interrupt_all."""
    from mcp_server import server

    class FakeCursor:
        def __init__(self):
            self.interrupted = 0

        def interrupt(self):
            self.interrupted += 1

    class FakeConn:
        def __init__(self):
            self.cursors: list[FakeCursor] = []

        def cursor(self):
            c = FakeCursor()
            self.cursors.append(c)
            return c

        def interrupt(self):
            pass

    conn = FakeConn()
    monkeypatch.setattr(server, "_CONN", conn, raising=False)
    cursor = server._cur()
    try:
        assert cursor in list(resource_policy._LIVE_CONNECTIONS), "cursor was not registered"
        resource_policy.interrupt_all()
        assert cursor.interrupted == 1
    finally:
        resource_policy._LIVE_CONNECTIONS.discard(cursor)
        monkeypatch.setattr(server, "_CONN", None, raising=False)


def test_deadline_interrupts_once_per_call_not_once_per_poll(fresh_activity, monkeypatch):
    """34 log lines for one stuck call was the first version's behaviour."""
    monkeypatch.setenv("DAIL_MCP_CALL_TIMEOUT_SECONDS", "1")
    monkeypatch.setattr(resource_policy, "_LAST_DEADLINE_EPISODE", None, raising=False)
    hits = []
    monkeypatch.setattr(resource_policy, "interrupt_all", lambda: (hits.append(1), 1)[1])
    with fresh_activity.track():
        time.sleep(1.05)
        assert resource_policy.enforce_call_deadline() is True
        for _ in range(5):  # further sweeps while the same call is still stuck
            assert resource_policy.enforce_call_deadline() is False
    assert hits == [1], "one stuck call must produce exactly one interrupt"


def test_a_real_duckdb_query_is_actually_abortable():
    """The mechanism the whole deadline rests on: interrupt from another thread ends
    the query, so the worker thread returns instead of running to completion."""
    conn = duckdb.connect()
    out = {}

    def run():
        try:
            conn.execute("SELECT count(*) FROM range(20000000000) t(i) WHERE i % 97 = 3").fetchone()
            out["r"] = "COMPLETED"
        except Exception as exc:  # noqa: BLE001
            out["r"] = type(exc).__name__

    t = threading.Thread(target=run, daemon=True)
    t.start()
    time.sleep(1.0)
    conn.interrupt()
    t.join(timeout=30)
    assert not t.is_alive(), "interrupt did not end the query"
    assert out["r"] == "InterruptException"


def test_watchdog_survives_a_failing_release(monkeypatch):
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "1")
    calls: list[int] = []
    done = threading.Event()
    stop = threading.Event()

    def release() -> bool:
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("boom")
        done.set()
        return True

    thread = resource_policy.start_watchdog(release, poll_seconds=0.01, stop=stop)
    try:
        assert thread is not None
        assert done.wait(timeout=5), "watchdog died on the first failing release"
        assert len(calls) >= 2
    finally:
        stop.set()
        thread.join(timeout=5)
    assert not thread.is_alive()


def test_watchdog_stops_promptly_when_asked():
    """Left running, a daemon poll loop logs into a closed stderr at interpreter exit."""
    stop = threading.Event()
    thread = resource_policy.start_watchdog(lambda: False, poll_seconds=30, stop=stop)
    assert thread is not None
    stop.set()
    thread.join(timeout=5)  # must not wait out the 30s poll interval
    assert not thread.is_alive()


def test_drop_index_caches_clears_every_module():
    from mcp_server import precedent_fts, sql_index, text_fts

    text_fts._CHECKED["speeches"] = True
    precedent_fts._CHECKED["precedents"] = True
    sql_index._GRAPH = {"sentinel": True}
    sql_index._BODIES = {"sentinel": "x"}

    resource_policy.drop_index_caches()

    assert text_fts._CHECKED == {}
    assert precedent_fts._CHECKED == {}
    assert sql_index._GRAPH is None
    assert sql_index._BODIES is None
