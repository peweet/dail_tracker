"""Payments resource — the all-time Travel & Accommodation Allowance ranking.

(Per-member payment detail is on the member dossier; this is the cross-member
league table.)
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, Query

from api.deps import Page, get_cursor, pagination
from dail_tracker_core import dossiers, serialize

router = APIRouter(tags=["payments"])


@router.get("/payments", summary="All-time TAA payment ranking by member")
def list_payments(
    house: str = Query("Dáil", description="Dáil or Seanad"),
    page: Page = Depends(pagination()),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_payments_ranking(cur, house=house, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)


@router.get("/payments/{year}", summary="TAA payment ranking for one calendar year")
def list_payments_for_year(
    year: int,
    house: str = Query("Dáil", description="Dáil or Seanad"),
    page: Page = Depends(pagination(default=20)),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_payments_year_ranking(cur, year=year, house=house, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)
