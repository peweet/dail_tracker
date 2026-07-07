"""Constituencies resource — the per-constituency dossier.

Composes one constituency's demographics, current Dáil TDs, party breakdown, the Dáil
work done since GE2024, housing context (supply + waiting list) and the serving councils'
money (each council figure stands alone — the dossier carries the never-sum caveat).
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["constituencies"])


@router.get("/constituencies", summary="All 43 constituencies (demographics + current TD count)")
def list_constituencies(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return dossiers.list_constituencies(cur)


@router.get("/constituencies/{name}/dossier", summary="One constituency's composed record")
def constituency_dossier(
    name: str,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.build_constituency_dossier(cur, name)
    if data is None:
        raise HTTPException(status_code=404, detail=f"constituency '{name}' not found")
    return data
