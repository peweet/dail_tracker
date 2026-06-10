"""SIPO political-finance resources — party donations + GE2024 election expenses.

Two DISTINCT money grains: donations and election expenses are NEVER added
together. Over-cap / under-threshold figures are the REAL disclosed values (the
API states them verbatim, never "corrects" them); some are OCR-derived and carry
a verify flag. No donor-address field is exposed.
"""

from __future__ import annotations

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import get_cursor
from dail_tracker_core import dossiers

router = APIRouter(tags=["political-finance"])


@router.get("/political-finance/donations", summary="Party donations disclosed to SIPO")
def party_donations(
    party: str | None = Query(
        None, description="Exact party label for its individual donor receipts; omit for the ranking"
    ),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.party_donations(cur, party=party)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data


@router.get("/political-finance/election-spend", summary="GE2024 candidate election expenses disclosed to SIPO")
def party_election_spend(
    party: str | None = Query(
        None, description="Exact party label for its per-candidate breakdown; omit for the ranking"
    ),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    data = dossiers.party_election_spend(cur, party=party)
    if "error" in data:
        raise HTTPException(status_code=503, detail=data["error"])
    return data
