"""Corporate notices resource — distress / register notices from Iris Oifigiúil.

CORPORATE ONLY: personal/individual insolvency is excluded upstream by policy, so no
person is named here. A wind-up / receivership notice is a FACT about a company's legal
status on a date — never a verdict on a director or a finding of wrongdoing, and a
Members' Voluntary Liquidation is a SOLVENT wind-up (routine lifecycle), not distress.
Each response carries that caveat verbatim from the core composition layer.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["corporate"])


@router.get("/corporate/notices", summary="Corporate distress / register notices (Iris Oifigiúil)")
def corporate_notices(
    query: str = Query("", description="Entity-name substring (case-insensitive)"),
    subtype: str = Query("", description="Notice subtype, e.g. 'receivership', 'examinership'"),
    year: int = Query(0, ge=0, description="Issue year; 0 = all years"),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.corporate_distress_notices(cur, query=query, subtype=subtype, year=year, limit=limit)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data


@router.get(
    "/corporate/repeat-distress",
    summary="CBI-authorised firms in repeat corporate distress (experimental) — regulatory provenance only",
)
def repeat_distress(
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.corporate_repeat_distress(cur, limit=limit)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data


@router.get(
    "/corporate/receivers",
    summary="Receivership lens: top appointers, operator firms, type-mix + notices-by-year (whole corpus)",
)
def receivers(
    limit: int = Query(25, ge=1, le=500, description="Caps each ranking (appointers, firms)"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.corporate_receivers(cur, limit=limit)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
