"""FastAPI application — read-only JSON API over dail_tracker_core.

A named in-memory DuckDB catalogue is built at startup (the member-overview
view set, registered once). Requests lease independent connections from a bounded
pool (see api/deps.py), and all handles close at shutdown. All retrieval/composition lives in the Streamlit-free core —
routers are thin (parse → core → serialize → envelope).

Licence: data served under CC-BY 4.0 (mirrors the Oireachtas PSI upstream).
"""

from __future__ import annotations

# isort: off
import services.runtime_env  # noqa: F401  — BLAS-thread cap; MUST stay the first repo import
# isort: on

import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from api import __version__
from api.duckdb_pool import DuckDBConnectionPool, configured_pool_size
from api.routers import (
    analytics,
    appointments,
    attendance,
    catalog,
    charities,
    committees,
    constituencies,
    corporate,
    councillors,
    exports,
    health,
    housing,
    judiciary,
    legislation,
    lobbying,
    local_government,
    members,
    ministerial,
    payments,
    political_finance,
    procurement,
    public_finance,
    public_payments,
    votes,
)
from api.snapshot import DataSnapshot, is_not_modified, load_data_snapshot, representation_etag
from dail_tracker_core.connections import api_conn
from dail_tracker_core.models.responses import RootMetadataResponse
from dail_tracker_core.results import SourceUnavailable
from services.logging_cloud import (
    configure_logging,
    get_logger,
    get_request_id,
    new_request_id,
    reset_request_id,
    set_request_id,
)

log = get_logger(__name__)

# Header a proxy/CDN may already have set; reuse it so one id spans the hop.
_REQUEST_ID_HEADER = "X-Request-ID"
_DATA_SNAPSHOT_HEADER = "X-Data-Snapshot"
_CACHE_CONTROL = "public, max-age=60, stale-while-revalidate=300"
_NON_CACHEABLE_PATHS = frozenset({"/v1/health", "/v1/readiness"})

_DESCRIPTION = (
    "Read-only JSON API over Irish parliamentary accountability data "
    "(attendance, votes, payments, lobbying, legislation, statutory instruments). "
    "Open, no key required. Data under CC-BY 4.0."
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Configure logging FIRST — before anything that can fail. Under uvicorn the
    # root logger already has handlers by now, which is precisely why this uses
    # logging_cloud (unguarded dictConfig) and not logging_setup (early-returns).
    configure_logging()

    # Register views once on a named in-memory connection, then lease separate
    # connections to requests. DuckDB cursors are not independent connections.
    started = time.perf_counter()
    pool: DuckDBConnectionPool | None = None
    try:
        snapshot = load_data_snapshot()
        pool = DuckDBConnectionPool.open(
            size=configured_pool_size(),
            bootstrap_connection=api_conn,
        )
        app.state.duckdb_pool = pool
        app.state.data_snapshot = snapshot
    except Exception:
        if pool is not None:
            pool.close()
        log.exception("API DuckDB startup failed - no request connections available")
        raise
    log.info(
        "startup complete",
        extra={
            "event": "startup",
            "duration_ms": round((time.perf_counter() - started) * 1000, 1),
            "duckdb_pool_size": pool.size,
            "data_snapshot": snapshot.identifier,
        },
    )
    try:
        yield
    finally:
        pool.close()
        log.info("shutdown complete", extra={"event": "shutdown"})


app = FastAPI(
    title="Dáil Tracker API",
    version=__version__,
    description=_DESCRIPTION,
    lifespan=lifespan,
)


def _apply_snapshot_cache(request: Request, response: Response) -> Response:
    """Attach a snapshot validator after a successful JSON response is computed."""
    if (
        request.method != "GET"
        or request.url.path in _NON_CACHEABLE_PATHS
        or not request.url.path.startswith("/v1/")
        or response.status_code != 200
        or "application/json" not in response.headers.get("content-type", "")
    ):
        return response
    snapshot: DataSnapshot | None = getattr(request.app.state, "data_snapshot", None)
    if snapshot is None:
        return response
    etag = representation_etag(
        snapshot,
        path=request.url.path,
        query_items=list(request.query_params.multi_items()),
    )
    headers = {
        "ETag": etag,
        "Cache-Control": _CACHE_CONTROL,
        _DATA_SNAPSHOT_HEADER: snapshot.identifier,
    }
    if is_not_modified(request.headers.get("if-none-match"), etag):
        return Response(status_code=304, headers=headers)
    response.headers.update(headers)
    return response


@app.middleware("http")
async def _request_context(request: Request, call_next):
    """Bind a request id, emit one structured line per request, surface 500s.

    Replaces uvicorn.access (silenced in logging_cloud) so the access line carries
    the same request id as everything logged during the request.
    """
    token = set_request_id(request.headers.get(_REQUEST_ID_HEADER) or new_request_id())
    started = time.perf_counter()
    try:
        try:
            response = await call_next(request)
        except Exception:
            # Without this the traceback goes only to uvicorn's own logger, in a
            # different format and with no request id — unusable in aggregation.
            log.exception(
                "unhandled exception",
                extra={
                    "event": "request_failed",
                    "method": request.method,
                    "path": request.url.path,
                    "duration_ms": round((time.perf_counter() - started) * 1000, 1),
                },
            )
            raise

        response = _apply_snapshot_cache(request, response)
        duration_ms = round((time.perf_counter() - started) * 1000, 1)
        # Slow reads matter here: the DuckDB path is the one that degrades under
        # load, and a p50 in the hundreds of ms is already known on some routes.
        level = logging.WARNING if duration_ms > 1000 else logging.INFO
        log.log(
            level,
            "%s %s -> %s",
            request.method,
            request.url.path,
            response.status_code,
            extra={
                "event": "request",
                "method": request.method,
                "path": request.url.path,
                "status": response.status_code,
                "duration_ms": duration_ms,
            },
        )
        response.headers[_REQUEST_ID_HEADER] = get_request_id()
        return response
    finally:
        reset_request_id(token)


@app.exception_handler(SourceUnavailable)
async def _source_unavailable(_request: Request, exc: SourceUnavailable) -> JSONResponse:
    # A REQUIRED view/parquet could not be queried. 503 — never let an outage
    # masquerade as 404/"no data" (the distinction QueryResult exists to keep).
    # Logged at ERROR: this is an outage, not a client mistake.
    log.error("source unavailable: %s", exc, extra={"event": "source_unavailable"})
    return JSONResponse(
        status_code=503,
        content={"detail": "A required data source is temporarily unavailable.", "kind": "unavailable"},
    )


# Machine-readable error kinds: clients branch on `kind`, never on parsing the
# detail string (the string-match disease the votes router once had). Applied
# centrally so the ~45 endpoints' HTTPException raises need no edits.
_ERROR_KINDS = {400: "bad_request", 404: "not_found", 422: "bad_request", 503: "unavailable"}
_PUBLIC_UNAVAILABLE_DETAIL = "A required data source is temporarily unavailable."


@app.exception_handler(StarletteHTTPException)
async def _http_error(_request: Request, exc: StarletteHTTPException) -> JSONResponse:
    kind = _ERROR_KINDS.get(exc.status_code, "error")
    if exc.status_code == 503:
        log.error("source unavailable: %s", exc.detail, extra={"event": "source_unavailable"})
        detail = _PUBLIC_UNAVAILABLE_DETAIL
    else:
        detail = exc.detail
    return JSONResponse(status_code=exc.status_code, content={"detail": detail, "kind": kind})


@app.exception_handler(RequestValidationError)
async def _validation_error(_request: Request, exc: RequestValidationError) -> JSONResponse:
    """Keep FastAPI's validation detail while applying our stable error envelope."""
    return JSONResponse(
        status_code=422,
        content={"detail": jsonable_encoder(exc.errors()), "kind": "bad_request"},
    )


app.include_router(health.router, prefix="/v1")
app.include_router(catalog.router, prefix="/v1")
app.include_router(members.router, prefix="/v1")
app.include_router(legislation.router, prefix="/v1")
app.include_router(votes.router, prefix="/v1")
app.include_router(payments.router, prefix="/v1")
app.include_router(lobbying.router, prefix="/v1")
app.include_router(procurement.router, prefix="/v1")
app.include_router(committees.router, prefix="/v1")
app.include_router(ministerial.router, prefix="/v1")
app.include_router(political_finance.router, prefix="/v1")
app.include_router(judiciary.router, prefix="/v1")
app.include_router(charities.router, prefix="/v1")
app.include_router(public_payments.router, prefix="/v1")
app.include_router(appointments.router, prefix="/v1")
app.include_router(corporate.router, prefix="/v1")
app.include_router(attendance.router, prefix="/v1")
app.include_router(housing.router, prefix="/v1")
app.include_router(public_finance.router, prefix="/v1")
app.include_router(local_government.router, prefix="/v1")
app.include_router(constituencies.router, prefix="/v1")
app.include_router(councillors.router, prefix="/v1")
app.include_router(analytics.router, prefix="/v1")
app.include_router(exports.router, prefix="/v1")


def _public_get_resources() -> list[str]:
    """Derive the discovery list from registered routes so it cannot drift."""
    return sorted(
        {
            path
            for route in app.routes
            if isinstance((path := getattr(route, "path", None)), str)
            and path.startswith("/v1")
            and getattr(route, "include_in_schema", True)
            and "GET" in (getattr(route, "methods", None) or set())
        }
    )


@app.get("/", tags=["meta"], response_model=RootMetadataResponse)
def root() -> dict:
    return {
        "name": "Dáil Tracker API",
        "version": "v1",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "licence": "CC-BY-4.0",
        "attribution": "Data via Dáil Tracker",
        "catalog": "/v1/catalog",
        "resources": _public_get_resources(),
    }
