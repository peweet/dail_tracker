"""FastAPI application — read-only JSON API over dail_tracker_core.

One read-only in-memory DuckDB connection is built at startup (the member-overview
view set, registered once) and closed at shutdown; requests get a cursor off it
(see api/deps.py). All retrieval/composition lives in the Streamlit-free core —
routers are thin (parse → core → serialize → envelope).

Licence: data served under CC-BY 4.0 (mirrors the Oireachtas PSI upstream).
"""

from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from api import __version__
from api.routers import (
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
from dail_tracker_core.connections import api_conn
from dail_tracker_core.results import SourceUnavailable

_DESCRIPTION = (
    "Read-only JSON API over Irish parliamentary accountability data "
    "(attendance, votes, payments, lobbying, legislation, statutory instruments). "
    "Open, no key required. Data under CC-BY 4.0."
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # One read-only union connection with every served view set, built ONCE
    # (expensive); requests get a cursor off it (see api/deps.py).
    app.state.conn = api_conn()
    try:
        yield
    finally:
        app.state.conn.close()


app = FastAPI(
    title="Dáil Tracker API",
    version=__version__,
    description=_DESCRIPTION,
    lifespan=lifespan,
)


@app.exception_handler(SourceUnavailable)
async def _source_unavailable(_request: Request, exc: SourceUnavailable) -> JSONResponse:
    # A REQUIRED view/parquet could not be queried. 503 — never let an outage
    # masquerade as 404/"no data" (the distinction QueryResult exists to keep).
    return JSONResponse(status_code=503, content={"detail": str(exc)})

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
app.include_router(exports.router, prefix="/v1")


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
            "/v1/members/{code}/questions",
            "/v1/members/{code}/interests",
            "/v1/members/{code}/speeches",
            "/v1/legislation",
            "/v1/legislation/{bill_id}",
            "/v1/statutory-instruments",
            "/v1/votes",
            "/v1/votes/{vote_id}",
            "/v1/votes/{vote_id}/interest-breakdown",
            "/v1/cross-reference/votes-interests",
            "/v1/payments",
            "/v1/payments/{year}",
            "/v1/lobbying/organisations",
            "/v1/lobbying/revolving-door",
            "/v1/procurement/suppliers",
            "/v1/procurement/suppliers/{supplier_norm}/dossier",
            "/v1/procurement/competition",
            "/v1/procurement/lobbying-overlap",
            "/v1/procurement/authorities",
            "/v1/procurement/cpv",
            "/v1/procurement/open-tenders",
            "/v1/committees",
            "/v1/committees/{committee}",
            "/v1/ministers",
            "/v1/cabinet",
            "/v1/ministerial/diary/organisations",
            "/v1/ministerial/diary/organisations/{name}",
            "/v1/ministerial/diary/meetings",
            "/v1/political-finance/donations",
            "/v1/political-finance/election-spend",
            "/v1/judiciary/appointments",
            "/v1/judiciary/courts-health",
            "/v1/charities",
            "/v1/corporate/notices",
            "/v1/corporate/repeat-distress",
            "/v1/corporate/receivers",
            "/v1/public-body-payments",
            "/v1/public-appointments",
            "/v1/lobbying/dpo/{individual_name}",
            "/v1/search/votes-by-topic",
            "/v1/attendance/turnout",
            "/v1/attendance/absences",
            "/v1/attendance/taa-compliance",
            "/v1/attendance/missing-members",
            "/v1/attendance/years",
            "/v1/housing/waiting-list",
            "/v1/housing/supply",
            "/v1/housing/accommodation-spend",
            "/v1/public-finance/government-finance",
            "/v1/local-government/councils",
            "/v1/local-government/councils/{local_authority}",
            "/v1/constituencies",
            "/v1/constituencies/{name}/dossier",
            "/v1/councillors",
            "/v1/councillors/councils",
            "/v1/coverage",
            "/v1/data",
            "/v1/data/{resource}",
        ],
    }
