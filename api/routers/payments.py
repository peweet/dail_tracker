"""Payments resource — the all-time Travel & Accommodation Allowance ranking.

(Per-member payment detail is on the member dossier; this is the cross-member
league table.)
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers, serialize

router = APIRouter(tags=["payments"])


@router.get("/payments", summary="All-time TAA payment ranking by member")
def list_payments(
    house: str = Query("Dáil", description="Dáil or Seanad"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_payments_ranking(cur, house=house, skip=skip, limit=limit)
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)


@router.get("/payments/{year}", summary="TAA payment ranking for one calendar year")
def list_payments_for_year(
    year: int,
    house: str = Query("Dáil", description="Dáil or Seanad"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_payments_year_ranking(cur, year=year, house=house, skip=skip, limit=limit)
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)
