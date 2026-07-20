"""Lobbying resource — registered organisations + the revolving-door register."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import Page, get_cursor, pagination
from dail_tracker_core import dossiers, serialize

router = APIRouter(tags=["lobbying"])


@router.get("/lobbying/organisations", summary="Lobbying organisations index (enriched)")
def list_organisations(
    name: str | None = Query(None, description="case-insensitive substring on organisation name"),
    exclude_state_adjacent: bool = Query(False, description="drop HSE/hospital-type public bodies"),
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_lobbying_orgs(
        cur, name=name, exclude_state_adjacent=exclude_state_adjacent, skip=page.skip, limit=page.limit
    )
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)


@router.get("/lobbying/revolving-door", summary="Former office-holders now lobbying (DPO register)")
def list_revolving_door(
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_revolving_door(cur, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)


@router.get(
    "/lobbying/dpo/{individual_name}",
    summary="One designated public official's revolving-door footprint (firms, clients, targets)",
)
def dpo_profile(
    individual_name: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.dpo_lobbying_profile(cur, individual_name)
    if data is None:
        raise HTTPException(status_code=404, detail=f"no DPO named '{individual_name}' on the register")
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
