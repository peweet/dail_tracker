"""Your-councillors resource — elected local-authority members, meeting coverage, the
(sparse) recorded roll-call votes, and the motion events parsed from minute prose.

Roll-call vote coverage is sparse: 6 of 31 councils as of 2026-08-01 (Carlow, Cork City,
Fingal, Galway City, Kilkenny, Laois — read the live list from the coverage tier, never
hardcode it). The roster + meeting-coverage data-state are the broadly-populated surfaces.
Vote and decision payloads carry their own provenance/coverage block; a consumer that
renders a count without it will overstate what the parse found.
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


@router.get("/councillors/decisions", summary="Motion events parsed from a council's minutes (Extracted band)")
def council_decisions(
    council: str = Query(..., description="local authority name"),
    limit: int = Query(200, ge=1, le=2000, description="max rows"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    """Proposer/seconder motion events. ~90% of rows carry no outcome word — see the
    `coverage` block returned with the rows."""
    return dossiers.council_decisions(cur, council=council, limit=limit)


@router.get("/councillors/powers", summary="Reserved vs executive decisions for a council (Extracted band)")
def council_powers(
    council: str = Query(..., description="local authority name"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    """Where the elected members decide (s.183 disposals, Part 8, LPT, budget, development
    plan) versus where they noted a Chief Executive decision. Document grain — counts, not
    quotable events."""
    return dossiers.council_powers(cur, council=council)


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
