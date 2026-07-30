"""The MCP server's idle release: it must free state, never close a live query.

Uses a stand-in connection object rather than a real one so this runs in CI with no
parquet present (same constraint as test_mcp_server_smoke.py).
"""

from __future__ import annotations

import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytest.importorskip("mcp")

from mcp_server import resource_policy, server  # noqa: E402


class FakeConn:
    def __init__(self) -> None:
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def cursor(self) -> str:
        return "cursor"


@pytest.fixture
def isolated_conn(monkeypatch):
    """Swap in a fake _CONN and a private activity counter, always restoring after."""
    activity = resource_policy._Activity()
    monkeypatch.setattr(resource_policy, "ACTIVITY", activity)
    conn = FakeConn()
    monkeypatch.setattr(server, "_CONN", conn, raising=False)
    yield conn, activity
    monkeypatch.setattr(server, "_CONN", None, raising=False)


def test_no_connection_is_a_no_op(monkeypatch):
    monkeypatch.setattr(server, "_CONN", None, raising=False)
    assert server._release_if_idle() is False


def test_never_releases_while_a_call_is_in_flight(isolated_conn, monkeypatch):
    """The regression that would hurt: closing DuckDB under a running tool call."""
    conn, activity = isolated_conn
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "0")  # maximally eager — still must not fire
    with activity:
        assert server._release_if_idle() is False
    assert conn.closed is False
    assert server._CONN is conn


def test_does_not_release_before_the_idle_threshold(isolated_conn, monkeypatch):
    conn, _ = isolated_conn
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "3600")
    assert server._release_if_idle() is False
    assert conn.closed is False


def test_releases_when_idle_and_rebuilds_lazily(isolated_conn, monkeypatch):
    conn, _ = isolated_conn
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "0")
    dropped: list[bool] = []
    monkeypatch.setattr(resource_policy, "drop_index_caches", lambda: dropped.append(True))

    assert server._release_if_idle() is True
    assert conn.closed is True
    assert server._CONN is None
    assert dropped == [True]

    # Idempotent: a second sweep finds nothing to do rather than double-closing.
    assert server._release_if_idle() is False


def test_release_survives_a_connection_that_fails_to_close(isolated_conn, monkeypatch):
    _, _ = isolated_conn
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "0")
    monkeypatch.setattr(resource_policy, "drop_index_caches", lambda: None)

    class Stubborn(FakeConn):
        def close(self) -> None:
            raise RuntimeError("duckdb refused")

    monkeypatch.setattr(server, "_CONN", Stubborn(), raising=False)
    assert server._release_if_idle() is True  # state still dropped
    assert server._CONN is None


def test_release_races_are_serialised_by_the_lock(isolated_conn, monkeypatch):
    """Concurrent sweeps must produce exactly one release, not N closes."""
    conn, _ = isolated_conn
    monkeypatch.setenv("DAIL_MCP_IDLE_SECONDS", "0")
    monkeypatch.setattr(resource_policy, "drop_index_caches", lambda: None)
    results: list[bool] = []
    lock = threading.Lock()

    def sweep() -> None:
        got = server._release_if_idle()
        with lock:
            results.append(got)

    threads = [threading.Thread(target=sweep) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(True) == 1
    assert conn.closed is True


def test_importing_the_server_starts_no_background_thread():
    """The watchdog is wired in __main__ only — importing must stay side-effect free,
    or every test process and every tool that imports the module inherits a thread."""
    assert not any(t.name == "dail-mcp-idle-release" for t in threading.enumerate())
