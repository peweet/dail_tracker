"""Liveness/readiness probe."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

from api.contracts import ERROR_RESPONSES
from dail_tracker_core.models.responses import HealthResponse

router = APIRouter(tags=["meta"], responses=ERROR_RESPONSES)

# At least one real, data-backed public surface must have registered. A static
# metadata-only view (for example v_payments_sources) must never turn an empty
# mounted data directory into a false-ready service.
_REQUIRED_VIEWS = frozenset({"v_payments_base"})


@router.get("/health", response_model=HealthResponse)
def health(request: Request) -> dict:
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
        missing = sorted(_REQUIRED_VIEWS - registered)
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
