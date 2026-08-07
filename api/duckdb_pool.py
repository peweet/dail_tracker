"""Bounded, request-safe access to the API's named in-memory DuckDB database."""

from __future__ import annotations

import os
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from queue import Empty, Queue

import duckdb

_DEFAULT_POOL_SIZE = 2
_MAX_POOL_SIZE = 8


class PoolExhausted(RuntimeError):
    """No independent DuckDB connection became available before the request deadline."""


def configured_pool_size() -> int:
    """Return the deliberately small, bounded per-process connection-pool size."""
    raw = os.getenv("DAIL_API_CONNECTION_POOL_SIZE", str(_DEFAULT_POOL_SIZE))
    try:
        size = int(raw)
    except ValueError as exc:
        raise ValueError("DAIL_API_CONNECTION_POOL_SIZE must be an integer") from exc
    if not 1 <= size <= _MAX_POOL_SIZE:
        raise ValueError(f"DAIL_API_CONNECTION_POOL_SIZE must be between 1 and {_MAX_POOL_SIZE}")
    return size


class DuckDBConnectionPool:
    """Independent connections sharing one named in-memory DuckDB catalogue.

    ``cursor()`` only creates another handle on the same connection, so it cannot
    make concurrent request queries safe. This pool instead opens independent
    connections against a named in-memory database after the bootstrap connection
    has registered every API view.
    """

    def __init__(
        self,
        *,
        bootstrap: duckdb.DuckDBPyConnection,
        connections: list[duckdb.DuckDBPyConnection],
    ) -> None:
        self._bootstrap = bootstrap
        self._connections = connections
        self._available: Queue[duckdb.DuckDBPyConnection] = Queue(maxsize=len(connections))
        for conn in connections:
            self._available.put(conn)

    @classmethod
    def open(
        cls,
        *,
        size: int,
        bootstrap_connection: Callable[[str], duckdb.DuckDBPyConnection],
    ) -> DuckDBConnectionPool:
        """Register the view set once, then open ``size`` independent handles."""
        if size < 1:
            raise ValueError("DuckDB connection-pool size must be positive")
        database = f":memory:dail_api_{uuid.uuid4().hex}"
        bootstrap = bootstrap_connection(database)
        connections: list[duckdb.DuckDBPyConnection] = []
        try:
            for _ in range(size):
                connections.append(duckdb.connect(database))
            return cls(bootstrap=bootstrap, connections=connections)
        except Exception:
            for conn in connections:
                conn.close()
            bootstrap.close()
            raise

    @property
    def size(self) -> int:
        return len(self._connections)

    @contextmanager
    def connection(self, *, timeout: float = 1.0) -> Iterator[duckdb.DuckDBPyConnection]:
        """Lease one connection exclusively for a synchronous API request."""
        try:
            conn = self._available.get(timeout=timeout)
        except Empty as exc:
            raise PoolExhausted("DuckDB connection pool is busy") from exc
        try:
            yield conn
        finally:
            self._available.put(conn)

    def close(self) -> None:
        """Close request handles before the bootstrap that keeps the DB alive."""
        for conn in self._connections:
            conn.close()
        self._bootstrap.close()
