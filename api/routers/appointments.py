"""Public appointments resource — state-board and similar appointment notices."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends

from api.contracts import ERROR_RESPONSES
from api.deps import Page, get_cursor, pagination
from dail_tracker_core import dossiers, serialize
from dail_tracker_core.models.envelope import ListEnvelope

router = APIRouter(tags=["public-appointments"], responses=ERROR_RESPONSES)


@router.get(
    "/public-appointments",
    response_model=ListEnvelope,
    response_model_exclude_unset=True,
    summary="Public-appointment notices (state boards etc.)",
)
def list_public_appointments(
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_public_appointments(cur, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)
