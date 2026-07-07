"""Attendance resource — participation turnout, absence runs, TAA compliance.

The participation model (turnout = divisions voted in ÷ divisions held) is the honest
replacement for the censored TAA "sitting days" ranking. Office-holders are FLAGGED,
not hidden — a low rate is context, not a verdict; the qualifier rides in ``head.caveat``.
Year-scoped lists default to the latest reporting year, echoed in ``head.year``.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["attendance"])


@router.get("/attendance/years", summary="Reporting years available for a house")
def attendance_years(
    house: str = Query("Dáil", description="Dáil or Seanad"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_years(cur, house=house)


@router.get("/attendance/turnout", summary="Division turnout for a year (worst-first)")
def attendance_turnout(
    year: int | None = Query(None, description="defaults to the latest reporting year"),
    house: str = Query("Dáil", description="Dáil or Seanad"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_turnout(cur, year=year, house=house, skip=skip, limit=limit)


@router.get("/attendance/absences", summary="Longest physical-absence runs for a year")
def attendance_absences(
    year: int | None = Query(None, description="defaults to the latest reporting year"),
    house: str = Query("Dáil", description="Dáil or Seanad"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_absences(cur, year=year, house=house, skip=skip, limit=limit)


@router.get("/attendance/taa-compliance", summary="Members below the statutory 120-day TAA threshold")
def attendance_taa(
    year: int | None = Query(None, description="defaults to the latest reporting year"),
    house: str = Query("Dáil", description="Dáil or Seanad"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_taa_compliance(cur, year=year, house=house, skip=skip, limit=limit)


@router.get("/attendance/missing-members", summary="Roster members with no attendance record")
def attendance_missing(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_missing_members(cur, skip=skip, limit=limit)
