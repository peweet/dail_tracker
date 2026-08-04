"""Charities resource — register-wide totals, or one charity's filed financial series.

Figures are AS FILED: some filers submit data-entry errors (implausible billions);
a single charity's row is never a sector fact.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.contracts import ERROR_RESPONSES
from api.deps import get_cursor
from dail_tracker_core import dossiers
from dail_tracker_core.models.responses import CharityFinancialsResponse

router = APIRouter(tags=["charities"], responses=ERROR_RESPONSES)


@router.get(
    "/charities",
    response_model=CharityFinancialsResponse,
    summary="Charity finances — sector totals per year, or one charity by RCN",
)
def charity_financials(
    rcn: int | None = Query(
        None, description="Registered Charity Number for one charity's full series; omit for sector totals"
    ),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.charity_financials(cur, rcn=rcn)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
