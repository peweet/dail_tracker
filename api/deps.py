"""Request-scoped DuckDB access.

The app holds ONE read-only in-memory connection (views registered once at
startup — see api/main.py lifespan). Each request gets an independent
``conn.cursor()``: in DuckDB a cursor is a separate connection sharing the same
database catalog, so it sees the registered views and gives concurrent reads
isolation for fetching, without rebuilding the (expensive) view set per request.

If a single connection ever bottlenecks under load, swap this provider for a
small fixed pool — the route signatures (``cur = Depends(get_cursor)``) don't change.
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
from fastapi import Request


def get_cursor(request: Request) -> Iterator[duckdb.DuckDBPyConnection]:
    """An independent cursor off the single read-only union connection.

    DuckDB cursors share the database catalog (so they see all registered views)
    while isolating fetch state — safe for concurrent reads off one connection.
    """
    cur = request.app.state.conn.cursor()
    try:
        yield cur
    finally:
        cur.close()
