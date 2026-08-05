"""Public-body payments resource — payments / POs over €20k, the realised-SPEND grain.

⚠️ What public bodies actually PAID — distinct from procurement AWARD ceilings.
NEVER add this spend to eTenders/TED award values (different value_kind); only the
sum-safe column is addable.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query, Request

from api.contracts import ERROR_RESPONSES
from api.deps import get_cursor
from dail_tracker_core import dossiers
from dail_tracker_core.models.responses import PublicBodyPaymentsResponse

_SELECTORS = {"side": {"publisher", "supplier"}, "order_by": {"value", "lines"}}


def _validate_selectors(request: Request) -> None:
    """Reject selector typos rather than silently defaulting a public response."""
    for name, allowed in _SELECTORS.items():
        value = request.query_params.get(name)
        if value is not None and value not in allowed:
            choices = ", ".join(sorted(allowed))
            raise HTTPException(status_code=422, detail=f"{name} must be one of: {choices}")


router = APIRouter(tags=["public-payments"], responses=ERROR_RESPONSES, dependencies=[Depends(_validate_selectors)])


@router.get(
    "/public-body-payments",
    response_model=PublicBodyPaymentsResponse,
    summary="Public-body payments/POs over €20k (realised spend)",
)
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
