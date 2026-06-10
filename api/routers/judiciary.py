"""Judiciary resources — the bench (appointments) + court-system health.

Appointment / office / rank / assignment data only — NO performance, conduct or
ranking data exists by design. courts-health names no judge: waiting times and
clearance are system-capacity signals, never a verdict on any individual.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["judiciary"])


@router.get("/judiciary/appointments", summary="Judicial appointments + elevation ladder + sitting-bench roster")
def judicial_appointments(
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.judicial_appointments(cur, limit=limit)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data


@router.get("/judiciary/courts-health", summary="Court-system health (clearance, waiting times, courthouses)")
def courts_health(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    data = dossiers.courts_health(cur)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
