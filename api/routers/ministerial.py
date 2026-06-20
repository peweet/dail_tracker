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


# ── Ministerial diaries (who ministers meet) ───────────────────────────────────
# Co-occurrence ACCESS record, never proof of influence — diaries are self-curated,
# non-exhaustive and quarterly-in-arrears. The no-inference caveat is attached by the
# core composition layer and carried through verbatim.


@router.get(
    "/ministerial/diary/organisations",
    summary="Organisations ranked by logged ministerial meetings (with lobbying-register corroboration)",
)
def diary_top_organisations(
    outside_only: bool = Query(True, description="Drop state/semi-state bodies (lead with outside interests)"),
    limit: int = Query(25, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.ministerial_diary_top_organisations(cur, limit=limit, outside_only=outside_only)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data


@router.get(
    "/ministerial/diary/organisations/{name}",
    summary="One organisation's ministerial-access record (summary + every logged meeting)",
)
def diary_organisation(
    name: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.ministerial_diary_organisation(cur, name)
    if data is None:
        raise HTTPException(status_code=404, detail=f"no logged ministerial meeting names '{name}'")
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data


@router.get(
    "/ministerial/diary/meetings",
    summary="Search logged external ministerial meetings by minister surname and/or subject keyword",
)
def diary_meetings(
    minister: str = Query("", description="Minister surname substring (case-insensitive)"),
    topic: str = Query("", description="Subject keyword substring (case-insensitive)"),
    limit: int = Query(30, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.ministerial_diary_meetings(cur, minister=minister, topic=topic, limit=limit)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
