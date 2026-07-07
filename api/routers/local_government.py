"""Local-government resource — council accountability (NOAC scorecard, cash signals,
collection rates, planning-overturn, over-€20k procurement scale).

Each council figure is its OWN reported amount shown beside the national benchmark —
never apportioned, never summed across measures (the dossier carries the caveat).
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["local-government"])


@router.get("/local-government/councils", summary="31-council index + map layers + national headline")
def list_councils(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return dossiers.list_councils(cur)


@router.get("/local-government/councils/{local_authority}", summary="One council's accountability dossier")
def council_dossier(
    local_authority: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.build_council_dossier(cur, local_authority)
    if data is None:
        raise HTTPException(status_code=404, detail=f"council '{local_authority}' not found")
    return data


@router.get(
    "/local-government/councils/{local_authority}/noac-indicators",
    summary="Full NOAC 2024 indicator set for one council (~125 series)",
)
def council_noac_indicators(
    local_authority: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.council_noac_indicators(cur, local_authority)
    if data is None:
        raise HTTPException(status_code=404, detail=f"council '{local_authority}' not found")
    return data
