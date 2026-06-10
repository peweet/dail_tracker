"""Votes (divisions) resource — list + composed division dossier."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers, serialize
from dail_tracker_core.models.votes import DivisionDossier

router = APIRouter(tags=["votes"])


@router.get("/votes", summary="List divisions (votes), filterable")
def list_votes(
    house: str = Query("Dáil", description="Dáil or Seanad"),
    date_from: str | None = Query(None, description="vote_date >= (YYYY-MM-DD)"),
    date_to: str | None = Query(None, description="vote_date <= (YYYY-MM-DD)"),
    outcome: str | None = Query(None, description="e.g. 'Carried', 'Lost'"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_votes(
        cur, date_from=date_from, date_to=date_to, outcome=outcome, house=house, skip=skip, limit=limit
    )
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)


@router.get(
    "/votes/{vote_id}",
    response_model=DivisionDossier,
    summary="A division's full record (vote + party breakdown + each member's vote + sources)",
)
def division_dossier(
    vote_id: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> DivisionDossier:
    data = dossiers.build_division_dossier(cur, vote_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"division '{vote_id}' not found")
    return DivisionDossier(**data)


@router.get(
    "/votes/{vote_id}/interest-breakdown",
    summary="A division's Yes/Níl/Abstain tally split by its voters' declared interests",
)
def division_interest_breakdown(
    vote_id: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.build_division_interest_breakdown(cur, vote_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"division '{vote_id}' not found")
    return data


@router.get(
    "/cross-reference/votes-interests",
    tags=["cross-reference"],
    summary="Members who voted a given way on a division AND declare a given interest",
)
def cross_reference_votes_interests(
    vote_id: str | None = Query(None, description="Division id (or use keyword)"),
    keyword: str | None = Query(None, description="Debate-title substring (or use vote_id)"),
    vote_type: str = Query("Voted No", description="'Voted Yes' | 'Voted No' | 'Abstained'"),
    interest: str = Query("landlord", description="landlord | property | director | shareholder"),
    house: str = Query("Dáil", description="Dáil or Seanad"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.cross_reference_votes_interests(
        cur, vote_id=vote_id, keyword=keyword, vote_type=vote_type, interest=interest, house=house
    )
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
