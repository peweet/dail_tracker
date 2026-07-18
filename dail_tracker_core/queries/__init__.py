"""dail_tracker_core.queries — per-domain data-retrieval functions.

Each module here exposes pure ``(conn, *params) -> QueryResult`` functions
containing ONLY retrieval SQL (SELECT / WHERE / ORDER BY / LIMIT). All joins,
aggregation, and value-gating live in the registered ``sql_views/*.sql`` (the
firewall). These functions take an explicit DuckDB connection so they are unit-
testable and free of any Streamlit/interface dependency.

``run_query`` is the single home for the read-layer's error policy: a DuckDB
failure (missing view/parquet, bad column) becomes an *unavailable* QueryResult
(not a swallow-to-empty-DataFrame), so callers can tell "source down" from "no
rows". Every ``queries/<domain>.py`` used to copy this try/except; they now keep
a one-line ``_run`` shim that binds their domain ``label`` (preserved verbatim in
the message + log) and their module ``log``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def run_query(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    params: list | None = None,
    *,
    label: str,
    log: logging.Logger | None = None,
) -> QueryResult:
    """Execute retrieval SQL and wrap the outcome as a ``QueryResult``.

    A DuckDB error (missing view, missing parquet, bad column) becomes an
    ``unavailable`` result rather than a silent empty DataFrame, and is logged for
    the server-side trail. ``label`` is the domain prefix kept verbatim in the
    unavailable reason and log line; ``log`` lets the caller keep its own module
    logger name.
    """
    lg = log or _log
    try:
        # Cursor per call: the conn is a process-wide @st.cache_resource singleton,
        # and concurrent sessions interleaving execute()/df() on the same connection
        # corrupt each other's pending result (None, or the other query's frame).
        # A cursor shares the catalog (registered views) but owns its result state.
        with conn.cursor() as cur:
            frame = cur.execute(sql, params or []).df()
        if frame is None:
            lg.warning("%s query returned no result frame", label)
            return QueryResult.unavailable(f"{label} query returned no result frame")
        return QueryResult.success(frame)
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        # WARNING, not exception(): a missing optional view is a handled "source
        # unavailable" state, not a crash — a full traceback per failure would be
        # noise. The exception text is kept inline for the server-side trail.
        lg.warning("%s query failed: %s", label, exc)
        return QueryResult.unavailable(f"{label} query failed: {exc}")


def make_runner(label: str, log: logging.Logger):
    """The per-domain ``_run`` shim, made once.

    Every ``queries/<domain>.py`` used to carry an identical two-line ``_run``
    copy binding its label + module logger (25 verbatim copies — audit
    2026-07-17). Modules now do ``_run = make_runner("domain", _log)``.
    ``member_overview`` keeps its own variant (it adds a ``conn is None`` guard).
    """

    def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
        return run_query(conn, sql, params, label=label, log=log)

    return _run
