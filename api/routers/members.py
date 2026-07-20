"""Member resource — the registry list + the composed dossier (the differentiator)."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import Page, get_cursor, pagination
from dail_tracker_core import dossiers, serialize
from dail_tracker_core.models.member import MemberDossier, MemberSummary

router = APIRouter(tags=["members"])


@router.get("/members", summary="List members (TDs + Senators), filterable")
def list_members(
    house: str | None = Query(None, description="Dáil or Seanad"),
    party: str | None = Query(None),
    constituency: str | None = Query(None),
    fuzzy_name: str | None = Query(None, description="case-insensitive substring on member_name"),
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_members(
        cur, house=house, party=party, constituency=constituency, fuzzy_name=fuzzy_name, skip=page.skip, limit=page.limit
    )
    members = [MemberSummary(**r).model_dump() for r in records]
    return serialize.envelope(members, limit=page.limit, offset=page.skip, total=total, truncated=truncated)


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


@router.get(
    "/members/{name_or_code}/questions",
    summary="A member's parliamentary-question feed, filterable",
)
def member_questions(
    name_or_code: str,
    year: int | None = Query(None, description="Calendar year filter"),
    qtype: str | None = Query(None, description="Question type, e.g. 'Oral', 'Written'"),
    ministry: str | None = Query(None, description="Addressed ministry/department"),
    topic: str | None = Query(None, description="Topic label"),
    text: str | None = Query(None, description="Free-text search of the question"),
    limit: int = Query(200, ge=1, le=2000),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.build_member_questions(
        cur, name_or_code, year=year, qtype=qtype, ministry=ministry, topic=topic, text=text, limit=limit
    )
    if data is None:
        raise HTTPException(status_code=404, detail=f"member '{name_or_code}' not found")
    return data


@router.get(
    "/members/{name_or_code}/interests",
    summary="A member's declared Register of Members' Interests (per-year summary + every declaration)",
)
def member_interests(
    name_or_code: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.build_member_interests(cur, name_or_code)
    if data is None:
        raise HTTPException(status_code=404, detail=f"member '{name_or_code}' not found")
    return data


@router.get(
    "/members/{name_or_code}/speeches",
    summary="A member's floor-contribution feed (speeches + oral questions) from the debate record",
)
def member_speeches(
    name_or_code: str,
    year: int | None = Query(None, description="Calendar year filter"),
    contribution_type: str | None = Query(None, description="speech | question | answer"),
    business: str | None = Query(None, description="Item of business, e.g. 'Commencement Matters'"),
    irish_only: bool = Query(False, description="Only contributions delivered in Irish"),
    text: str | None = Query(None, description="Free-text search of the spoken words"),
    limit: int = Query(200, ge=1, le=2000),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.build_member_speeches(
        cur,
        name_or_code,
        year=year,
        contribution_type=contribution_type,
        business=business,
        irish_only=irish_only,
        text=text,
        limit=limit,
    )
    if data is None:
        raise HTTPException(status_code=404, detail=f"member '{name_or_code}' not found")
    return data
