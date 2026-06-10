"""Procurement (eTenders/TED) resource — the supplier dossier is the wedge.

Suppliers list + a composed per-supplier dossier (summary + every award), plus
the two whole-corpus analytical lenses (buyer competition quality, and the
procurement×lobbying co-occurrence overlap). Each analytical response carries the
no-inference caveat the core composition layer attaches — never re-derived here.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers, serialize

router = APIRouter(tags=["procurement"])


@router.get("/procurement/suppliers", summary="Supplier ranking (CRO + lobbying-overlap enriched)")
def list_suppliers(
    year: int | None = Query(None, description="Scope to one calendar year; omit for all-time"),
    order_by: str = Query("awards", description="'awards' (contract count) or 'value' (sum-safe €)"),
    skip: int = Query(0, ge=0),
    limit: int = Query(20, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_suppliers(
        cur, year=year, order_by=order_by, skip=skip, limit=limit
    )
    return serialize.envelope(records, limit=limit, offset=skip, total=total, truncated=truncated)


@router.get(
    "/procurement/suppliers/{supplier_norm}/dossier",
    summary="One supplier's composed record (ranking summary + every award, newest first)",
)
def supplier_dossier(
    supplier_norm: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.build_supplier_dossier(cur, supplier_norm)
    if data is None:
        raise HTTPException(status_code=404, detail=f"supplier '{supplier_norm}' not found")
    return data


@router.get(
    "/procurement/competition",
    summary="Per-buyer competition quality (single-bidder rate, TED 2024+) — a signal, never a verdict",
)
def competition(
    min_lots: int = Query(40, ge=0, description="Drop buyers with fewer lots-with-a-bid-count (noisy)"),
    order_by: str = Query("single_bid", description="'single_bid' (rate) or 'lots' (volume)"),
    limit: int = Query(20, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.list_procurement_competition(cur, min_lots=min_lots, order_by=order_by, limit=limit)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data


@router.get(
    "/procurement/lobbying-overlap",
    summary="Companies on BOTH the procurement and lobbying registers (co-occurrence only)",
)
def lobbying_overlap(
    order_by: str = Query("award_value", description="award_value | award_rows | lobby_returns | authorities"),
    side: str | None = Query(None, description="Filter to 'registrant' or 'client'"),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.list_procurement_lobbying_overlap(cur, limit=limit, order_by=order_by, side=side)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
