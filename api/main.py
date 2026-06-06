"""FastAPI application — read-only JSON API over dail_tracker_core.

One read-only in-memory DuckDB connection is built at startup (the member-overview
view set, registered once) and closed at shutdown; requests get a cursor off it
(see api/deps.py). All retrieval/composition lives in the Streamlit-free core —
routers are thin (parse → core → serialize → envelope).

Licence: data served under CC-BY 4.0 (mirrors the Oireachtas PSI upstream).
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI

from api import __version__
from api.routers import catalog, health, legislation, members
from dail_tracker_core.connections import legislation_conn, member_overview_conn

_DESCRIPTION = (
    "Read-only JSON API over Irish parliamentary accountability data "
    "(attendance, votes, payments, lobbying, legislation, statutory instruments). "
    "Open, no key required. Data under CC-BY 4.0."
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Build the view sets ONCE (expensive); share via cursors per request.
    app.state.conn = member_overview_conn()
    app.state.leg_conn = legislation_conn()
    try:
        yield
    finally:
        app.state.conn.close()
        app.state.leg_conn.close()


app = FastAPI(
    title="Dáil Tracker API",
    version=__version__,
    description=_DESCRIPTION,
    lifespan=lifespan,
)

app.include_router(health.router, prefix="/v1")
app.include_router(catalog.router, prefix="/v1")
app.include_router(members.router, prefix="/v1")
app.include_router(legislation.router, prefix="/v1")


@app.get("/", tags=["meta"])
def root() -> dict:
    return {
        "name": "Dáil Tracker API",
        "version": "v1",
        "docs": "/docs",
        "openapi": "/openapi.json",
        "licence": "CC-BY-4.0",
        "attribution": "Data via Dáil Tracker",
        "catalog": "/v1/catalog",
        "resources": [
            "/v1/health",
            "/v1/catalog",
            "/v1/members",
            "/v1/members/{code}/dossier",
            "/v1/legislation",
            "/v1/legislation/{bill_id}",
            "/v1/statutory-instruments",
        ],
    }
