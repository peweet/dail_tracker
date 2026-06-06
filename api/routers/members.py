"""Member resource — the registry list + the composed dossier (the differentiator)."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers, serialize
from dail_tracker_core.models.member import MemberDossier, MemberSummary

router = APIRouter(tags=["members"])


@router.get("/members", summary="List members (TDs + Senators), filterable")
def list_members(
    house: str | None = Query(None, description="Dáil or Seanad"),
    party: str | None = Query(None),
    constituency: str | None = Query(None),
    fuzzy_name: str | None = Query(None, description="case-insensitive substring on member_name"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_members(
        cur, house=house, party=party, constituency=constituency, fuzzy_name=fuzzy_name, skip=skip, limit=limit
    )
    members = [MemberSummary(**r).model_dump() for r in records]
    return serialize.envelope(members, limit=limit, offset=skip, total=total, truncated=truncated)


@router.get(
    "/members/{code}/dossier",
    response_model=MemberDossier,
    summary="A member's full cross-dataset accountability record, composed server-side",
)
def member_dossier(
    code: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> MemberDossier:
    data = dossiers.build_member_dossier(cur, code)
    if data is None:
        raise HTTPException(status_code=404, detail=f"member '{code}' not found")
    return MemberDossier(**data)
