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


# ── activity accounting (the watchdog's safety interlock) ─────────────────────


def test_idle_for_is_none_while_a_call_is_in_flight(fresh_activity):
    assert fresh_activity.idle_for() is not None
    with fresh_activity:
        assert fresh_activity.inflight == 1
        assert fresh_activity.idle_for() is None  # <- what stops a release mid-query
    assert fresh_activity.inflight == 0
    assert fresh_activity.idle_for() is not None


def test_activity_counter_is_thread_safe(fresh_activity):
    def worker():
        for _ in range(200):
            with fresh_activity:
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


def test_watchdog_disabled_by_zero_idle_seconds(monkeypatch):
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "0")
    assert resource_policy.start_watchdog(lambda: False) is None


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
