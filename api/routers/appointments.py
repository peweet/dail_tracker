"""Public appointments resource — state-board and similar appointment notices."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends

from api.deps import Page, get_cursor, pagination
from dail_tracker_core import dossiers, serialize

router = APIRouter(tags=["public-appointments"])


@router.get("/public-appointments", summary="Public-appointment notices (state boards etc.)")
def list_public_appointments(
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_public_appointments(cur, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)
