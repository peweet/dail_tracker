"""Lobbying resource — registered organisations + the revolving-door register."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers, serialize

router = APIRouter(tags=["lobbying"])


@router.get("/lobbying/organisations", summary="Lobbying organisations index (enriched)")
def list_organisations(
    name: str | None = Query(None, description="case-insensitive substring on organisation name"),
    exclude_state_adjacent: bool = Query(False, description="drop HSE/hospital-type public bodies"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_lobbying_orgs(
        cur, name=name, exclude_state_adjacent=exclude_state_adjacent, skip=skip, limit=limit
    )
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)


@router.get("/lobbying/revolving-door", summary="Former office-holders now lobbying (DPO register)")
def list_revolving_door(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_revolving_door(cur, skip=skip, limit=limit)
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)


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
