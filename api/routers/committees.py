"""Committees resource — per-chamber rollup + one committee's party-seat breakdown."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.contracts import ERROR_RESPONSES
from api.deps import get_cursor
from dail_tracker_core import dossiers, serialize
from dail_tracker_core.models.envelope import ListEnvelope
from dail_tracker_core.models.responses import CommitteeResponse

router = APIRouter(tags=["committees"], responses=ERROR_RESPONSES)


@router.get(
    "/committees",
    response_model=ListEnvelope,
    response_model_exclude_unset=True,
    summary="Committees for a chamber (chair, member/party counts)",
)
def list_committees(
    chamber: str = Query("Dáil", description="Dáil or Seanad"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    # Standard list envelope (was a bespoke {chamber, committees} wrapper).
    rows = dossiers.list_committees(cur, chamber=chamber)
    return serialize.envelope(rows, total=len(rows), meta={"chamber": chamber})


@router.get(
    "/committees/{committee}",
    response_model=CommitteeResponse,
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
