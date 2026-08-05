"""Liveness/readiness probe."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from api.contracts import ERROR_RESPONSES
from api.routers.catalog import required_catalog_views
from dail_tracker_core.models.responses import HealthResponse

router = APIRouter(tags=["meta"], responses=ERROR_RESPONSES)

# Every resource advertised by /v1/catalog needs its backing count view. Keep the
# direct payments-base sentinel as well: it is the data-backed surface beneath
# the public-payments summary, rather than a metadata-only registration.
_LIVENESS_VIEWS = frozenset({"v_payments_base"})
_REQUIRED_VIEWS = required_catalog_views() | _LIVENESS_VIEWS


def _probe(request: Request, required_views: frozenset[str]) -> dict:
    conn = getattr(request.app.state, "conn", None)
    if conn is None:
        raise HTTPException(status_code=503, detail="connection not initialised")
    # Probe on a request-scoped cursor, never the shared base connection — a
    # DuckDBPyConnection object is not safe for concurrent .execute() across
    # the anyio worker threads FastAPI runs sync handlers on.
    cur = conn.cursor()
    try:
        rows = cur.execute("SELECT table_name FROM information_schema.tables WHERE table_type='VIEW'").fetchall()
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
    finally:
        cur.close()


@router.get("/health", response_model=HealthResponse, summary="Process liveness + core data seam")
def health(request: Request) -> dict:
    """Cheap liveness probe used by the installed-wheel delivery smoke."""
    return _probe(request, _LIVENESS_VIEWS)


@router.get("/readiness", response_model=HealthResponse, summary="All catalogued resources are registered")
def readiness(request: Request) -> dict:
    """Strict serving probe: every resource promised by /v1/catalog must bind."""
    return _probe(request, _REQUIRED_VIEWS)
