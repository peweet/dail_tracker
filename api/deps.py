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

from collections.abc import Callable, Iterator
from dataclasses import dataclass

import duckdb
from fastapi import Query, Request


@dataclass(frozen=True)
class Page:
    """Resolved pagination for one request."""

    skip: int
    limit: int


def pagination(default: int = 50, cap: int = 500) -> Callable[..., Page]:
    """THE pagination convention: ``skip``/``limit`` with a 50-default, 500-cap floor.

    Endpoints that deliberately deviate declare it at the call site —
    ``Depends(pagination(default=200, cap=2000))`` — instead of re-typing Query
    params (pre-2026-07-18 the defaults sprawled 20/25/30/40/50/100/200 and caps
    500 vs 2000 with no signal for which spreads were intentional).
    """

    def _page(
        skip: int = Query(0, ge=0),
        limit: int = Query(default, ge=1, le=cap),
    ) -> Page:
        return Page(skip=skip, limit=limit)

    return _page


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
