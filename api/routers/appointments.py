"""Public appointments resource — state-board and similar appointment notices."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers, serialize

router = APIRouter(tags=["public-appointments"])


@router.get("/public-appointments", summary="Public-appointment notices (state boards etc.)")
def list_public_appointments(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_public_appointments(cur, skip=skip, limit=limit)
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)
