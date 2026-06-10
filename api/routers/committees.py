"""Committees resource — per-chamber rollup + one committee's party-seat breakdown."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["committees"])


@router.get("/committees", summary="Committees for a chamber (chair, member/party counts)")
def list_committees(
    chamber: str = Query("Dáil", description="Dáil or Seanad"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return {"chamber": chamber, "committees": dossiers.list_committees(cur, chamber=chamber)}


@router.get(
    "/committees/{committee}",
    summary="One committee's rollup + its long-format party-seat breakdown",
)
def get_committee(
    committee: str,
    chamber: str = Query("Dáil", description="Dáil or Seanad"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.get_committee(cur, chamber, committee)
    if data is None:
        raise HTTPException(status_code=404, detail=f"committee '{committee}' not found in {chamber}")
    return data
