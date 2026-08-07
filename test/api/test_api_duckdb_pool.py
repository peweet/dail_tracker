from __future__ import annotations

import json

import duckdb
import pytest

from api.duckdb_pool import DuckDBConnectionPool, PoolExhausted, configured_pool_size
from api.snapshot import DataSnapshot, is_not_modified, load_data_snapshot, representation_etag


def _bootstrap(database: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(database)
    conn.execute("CREATE TABLE probe (value INTEGER)")
    conn.execute("INSERT INTO probe VALUES (1)")
    return conn


def test_pool_leases_independent_connections_sharing_the_registered_catalog():
    pool = DuckDBConnectionPool.open(size=2, bootstrap_connection=_bootstrap)
    try:
        with pool.connection() as first, pool.connection() as second:
            assert first is not second
            assert first.execute("SELECT count(*) FROM probe").fetchone() == (1,)
            assert second.execute("SELECT count(*) FROM probe").fetchone() == (1,)
    finally:
        pool.close()


def test_pool_is_bounded_and_releases_a_connection_after_each_request():
    pool = DuckDBConnectionPool.open(size=1, bootstrap_connection=_bootstrap)
    try:
        with pool.connection(), pytest.raises(PoolExhausted), pool.connection(timeout=0):
            pass
        with pool.connection() as conn:
            assert conn.execute("SELECT 1").fetchone() == (1,)
    finally:
        pool.close()


@pytest.mark.parametrize("raw", ["0", "9", "not-a-number"])
def test_configured_pool_size_rejects_invalid_bounds(monkeypatch, raw):
    monkeypatch.setenv("DAIL_API_CONNECTION_POOL_SIZE", raw)
    with pytest.raises(ValueError):
        configured_pool_size()


def test_snapshot_is_deterministic_for_runtime_hashes_not_manifest_order(tmp_path):
    first = {
        "generated_at": "2026-08-07T08:10:07Z",
        "files": [
            {"path": "data/gold/b.parquet", "sha256": "b", "read_at_runtime": True},
            {"path": "data/gold/a.parquet", "sha256": "a", "read_at_runtime": True},
            {"path": "data/gold/dead.parquet", "sha256": "ignored", "read_at_runtime": False},
        ],
    }
    path = tmp_path / "runtime_data_manifest.json"
    path.write_text(json.dumps(first), encoding="utf-8")
    snapshot = load_data_snapshot(path)

    first["files"].reverse()
    path.write_text(json.dumps(first), encoding="utf-8")
    assert load_data_snapshot(path).identifier == snapshot.identifier


def test_etag_is_query_order_independent_and_supports_conditional_requests():
    snapshot = DataSnapshot(identifier="a" * 64, generated_at="2026-08-07T08:10:07Z")
    first = representation_etag(snapshot, path="/v1/votes", query_items=[("limit", "50"), ("house", "Dail")])
    second = representation_etag(snapshot, path="/v1/votes", query_items=[("house", "Dail"), ("limit", "50")])

    assert first == second
    assert is_not_modified(first, first)
    assert is_not_modified(f'W/{first}, "other"', first)
    assert not is_not_modified('"other"', first)
