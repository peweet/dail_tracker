"""Legislation + statutory-instruments resources."""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers, serialize
from dail_tracker_core.models.legislation import BillDossier

router = APIRouter(tags=["legislation"])


@router.get("/legislation", summary="List bills, filterable")
def list_bills(
    status: str | None = Query(None, description="bill_status, e.g. 'Current'"),
    title_search: str | None = Query(None, description="case-insensitive substring on bill_title"),
    start_date: str | None = Query(None, description="introduced_date >= (YYYY-MM-DD)"),
    end_date: str | None = Query(None, description="introduced_date <= (YYYY-MM-DD)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_bills(
        cur, status=status, title_search=title_search, start_date=start_date, end_date=end_date, skip=skip, limit=limit
    )
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)


@router.get(
    "/legislation/{bill_id}",
    response_model=BillDossier,
    summary="A bill's composed record (detail + lifecycle + amendments + SIs made under it)",
)
def bill_dossier(
    bill_id: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> BillDossier:
    data = dossiers.build_bill_dossier(cur, bill_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"bill '{bill_id}' not found")
    return BillDossier(**data)


@router.get("/statutory-instruments", summary="List statutory instruments, filterable")
def list_statutory_instruments(
    year: int | None = Query(None),
    operation: str | None = Query(None, description="e.g. 'made', 'revoked'"),
    department: str | None = Query(None, description="si_department_label"),
    eu_only: bool = Query(False, description="EU-derived instruments only"),
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_statutory_instruments(
        cur, year=year, operation=operation, department=department, eu_only=eu_only, skip=skip, limit=limit
    )
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)
