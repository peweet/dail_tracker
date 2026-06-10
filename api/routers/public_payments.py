"""Public-body payments resource — payments / POs over €20k, the realised-SPEND grain.

⚠️ What public bodies actually PAID — distinct from procurement AWARD ceilings.
NEVER add this spend to eTenders/TED award values (different value_kind); only the
sum-safe column is addable.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["public-payments"])


@router.get("/public-body-payments", summary="Public-body payments/POs over €20k (realised spend)")
def public_body_payments(
    side: str = Query("publisher", description="'publisher' (paying body) or 'supplier' (who was paid)"),
    order_by: str = Query("value", description="'value' (sum-safe €) or 'lines' (record count)"),
    limit: int = Query(25, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.public_body_payments(cur, side=side, order_by=order_by, limit=limit)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
