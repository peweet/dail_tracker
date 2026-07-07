"""Your-councillors resource — elected local-authority members, meeting coverage, and
the (sparse) recorded roll-call votes.

Roll-call vote coverage is sparse (Carlow only so far); the roster + meeting-coverage
data-state are the broadly-populated surfaces.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["councillors"])


@router.get("/councillors/councils", summary="Councils with a published councillor roster")
def councillor_councils(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return dossiers.list_councillor_councils(cur)


@router.get("/councillors/votes", summary="A councillor's recorded roll-call votes (sparse coverage)")
def councillor_votes(
    council: str = Query(..., description="local authority name"),
    member: str = Query(..., description="councillor name"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.councillor_votes(cur, council=council, member=member)


@router.get("/councillors", summary="Councillor roster for a council (optionally one LEA) + coverage + CE")
def councillors_roster(
    council: str = Query(..., description="local authority name"),
    lea: str | None = Query(None, description="optional local electoral area"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.councillors_roster(cur, council=council, lea=lea)
    if data is None:
        raise HTTPException(status_code=404, detail=f"council '{council}' not found")
    return data
