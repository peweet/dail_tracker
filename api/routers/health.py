"""Liveness/readiness probe."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException

from api.contracts import ERROR_RESPONSES
from api.deps import get_cursor
from api.routers.catalog import required_catalog_views
from dail_tracker_core.models.responses import HealthResponse

router = APIRouter(tags=["meta"], responses=ERROR_RESPONSES)

# Every resource advertised by /v1/catalog needs its backing count view. Keep the
# direct payments-base sentinel as well: it is the data-backed surface beneath
# the public-payments summary, rather than a metadata-only registration.
_LIVENESS_VIEWS = frozenset({"v_payments_base"})
_REQUIRED_VIEWS = required_catalog_views() | _LIVENESS_VIEWS


def _probe(conn: duckdb.DuckDBPyConnection, required_views: frozenset[str]) -> dict:
    try:
        rows = conn.execute("SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'").fetchall()
        registered = {str(row[0]) for row in rows}
        missing = sorted(required_views - registered)
        if missing:
            raise HTTPException(
                status_code=503,
                detail=f"database missing required data views: {', '.join(missing)}",
            )
        views_registered = len(registered)
        return {"status": "ok", "views_registered": views_registered}
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc


@router.get("/health", response_model=HealthResponse, summary="Process liveness + core data seam")
def health(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    """Cheap liveness probe used by the installed-wheel delivery smoke."""
    return _probe(cur, _LIVENESS_VIEWS)


@router.get("/readiness", response_model=HealthResponse, summary="All catalogued resources are registered")
def readiness(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    """Strict serving probe: every resource promised by /v1/catalog must bind."""
    return _probe(cur, _REQUIRED_VIEWS)
