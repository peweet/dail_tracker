"""Housing resource — national social-housing demand (waiting list), supply &
affordability, and the state asylum/Ukraine accommodation spend.

Accommodation spend is drawn from the published over-€20k purchase-order registers
(a realised-spend grain); its response carries the never-sum caveat.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, Query

from api.deps import Page, get_cursor, pagination
from dail_tracker_core import dossiers

router = APIRouter(tags=["housing"])


@router.get("/housing/waiting-list", summary="Social-housing waiting-list league table by area")
def housing_waiting_list(
    grain: str = Query("county", description="'county' | 'la' | 'national'"),
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.housing_waiting_list(cur, grain=grain, skip=page.skip, limit=page.limit)


@router.get("/housing/supply", summary="National supply & affordability headline + completions trend")
def housing_supply(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return dossiers.housing_supply(cur)


@router.get("/housing/accommodation-spend", summary="State asylum/Ukraine accommodation spend (over-€20k POs)")
def housing_accommodation_spend(
    limit: int = Query(40, ge=1, le=500, description="number of top providers"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.housing_accommodation_spend(cur, limit=limit)
