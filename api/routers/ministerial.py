"""Ministerial accountability resource — who held a department on a given date."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["ministerial"])


@router.get("/ministers", summary="Who held a department on a given date")
def who_was_minister(
    department: str = Query(..., description="Fuzzy department label, e.g. 'Health', 'Finance'"),
    on_date: str = Query(..., description="ISO date (YYYY-MM-DD)"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    # The core helper returns either the holder, a disambiguation list, or the
    # department picker when nothing matches — all useful 200 responses. Only a
    # genuinely empty source is a 503.
    data = dossiers.who_was_minister(cur, department, on_date)
    return data


@router.get("/cabinet", summary="Current ministerial line-up + the department list")
def current_cabinet(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    data = dossiers.current_cabinet(cur)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
