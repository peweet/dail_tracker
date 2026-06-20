"""Liveness/readiness probe."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request

router = APIRouter(tags=["meta"])


@router.get("/health")
def health(request: Request) -> dict:
    conn = getattr(request.app.state, "conn", None)
    if conn is None:
        raise HTTPException(status_code=503, detail="connection not initialised")
    try:
        n = conn.execute("SELECT count(*) FROM information_schema.tables WHERE table_type='VIEW'").fetchone()
        return {"status": "ok", "views_registered": int(n[0]) if n else 0}
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(status_code=503, detail=f"database unavailable: {exc}") from exc
