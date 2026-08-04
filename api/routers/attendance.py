"""Attendance resource — participation turnout, absence runs, TAA compliance.

The participation model (turnout = divisions voted in ÷ divisions held) is the honest
replacement for the censored TAA "sitting days" ranking. Office-holders are FLAGGED,
not hidden — a low rate is context, not a verdict; the qualifier rides in ``head.caveat``.
Year-scoped lists default to the latest reporting year, echoed in ``head.year``.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, Query

from api.contracts import ERROR_RESPONSES
from api.deps import Page, get_cursor, pagination
from dail_tracker_core import dossiers
from dail_tracker_core.models.envelope import ListEnvelope
from dail_tracker_core.models.responses import AttendanceYearsResponse

router = APIRouter(tags=["attendance"], responses=ERROR_RESPONSES)


@router.get(
    "/attendance/years", response_model=AttendanceYearsResponse, summary="Reporting years available for a house"
)
def attendance_years(
    house: str = Query("Dáil", description="Dáil or Seanad"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_years(cur, house=house)


@router.get(
    "/attendance/turnout",
    response_model=ListEnvelope,
    response_model_exclude_unset=True,
    summary="Division turnout for a year (worst-first)",
)
def attendance_turnout(
    year: int | None = Query(None, description="defaults to the latest reporting year"),
    house: str = Query("Dáil", description="Dáil or Seanad"),
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_turnout(cur, year=year, house=house, skip=page.skip, limit=page.limit)


@router.get(
    "/attendance/absences",
    response_model=ListEnvelope,
    response_model_exclude_unset=True,
    summary="Longest physical-absence runs for a year",
)
def attendance_absences(
    year: int | None = Query(None, description="defaults to the latest reporting year"),
    house: str = Query("Dáil", description="Dáil or Seanad"),
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_absences(cur, year=year, house=house, skip=page.skip, limit=page.limit)


@router.get(
    "/attendance/taa-compliance",
    response_model=ListEnvelope,
    response_model_exclude_unset=True,
    summary="Members below the statutory 120-day TAA threshold",
)
def attendance_taa(
    year: int | None = Query(None, description="defaults to the latest reporting year"),
    house: str = Query("Dáil", description="Dáil or Seanad"),
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_taa_compliance(cur, year=year, house=house, skip=page.skip, limit=page.limit)


@router.get(
    "/attendance/missing-members",
    response_model=ListEnvelope,
    response_model_exclude_unset=True,
    summary="Roster members with no attendance record",
)
def attendance_missing(
    page: Page = Depends(pagination(default=100)),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return dossiers.attendance_missing_members(cur, skip=page.skip, limit=page.limit)
