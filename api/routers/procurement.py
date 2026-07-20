"""Procurement (eTenders/TED) resource — the supplier dossier is the wedge.

Suppliers list + a composed per-supplier dossier (summary + every award), plus
the two whole-corpus analytical lenses (buyer competition quality, and the
procurement×lobbying co-occurrence overlap). Each analytical response carries the
no-inference caveat the core composition layer attaches — never re-derived here.
"""

from __future__ import annotations

import os

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.deps import Page, get_cursor, pagination
from dail_tracker_core import dossiers, serialize
from dail_tracker_core.queries import procurement as _q
from services.deflator import list_indices

router = APIRouter(tags=["procurement"])

# EXPERIMENTAL real-terms (inflation-adjusted) endpoints. Gated to the same DAIL_EXPERIMENTAL flag
# as the Streamlit lens so the feature stays local until vetted: routes are hidden from the public
# OpenAPI schema AND return 404 on the deployed API (the flag is unset there). All deflation lives
# in the views + services/deflator.py; these are retrieval pass-throughs with the caveat attached.
_EXPERIMENTAL = os.getenv("DAIL_EXPERIMENTAL") == "1"
_REAL_CAVEAT = (
    "EXPERIMENTAL real-terms lens. Re-expresses past disclosed values in today's money (purchasing "
    "power) — NOT a current cost and NOT a recommended bid price. General CPI is not construction, "
    "materials, labour-rate or tender-price inflation; public spend uses the government-consumption "
    "deflator. Each figure names the index it used. See /procurement/inflation/indices."
)


def _require_experimental() -> None:
    if not _EXPERIMENTAL:
        raise HTTPException(status_code=404, detail="experimental endpoint not enabled")


@router.get("/procurement/suppliers", summary="Supplier ranking (CRO + lobbying-overlap enriched)")
def list_suppliers(
    year: int | None = Query(None, description="Scope to one calendar year; omit for all-time"),
    order_by: str = Query("awards", description="'awards' (contract count) or 'value' (sum-safe €)"),
    page: Page = Depends(pagination(default=20)),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_suppliers(cur, year=year, order_by=order_by, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)


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


@router.get(
    "/procurement/authorities",
    summary="Award activity by contracting authority (buyer) — counts + sum-safe value (CEILINGS)",
)
def authorities(
    page: Page = Depends(pagination(default=25)),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_procurement_authorities(cur, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)


@router.get(
    "/procurement/cpv",
    summary="Award activity by CPV code (what was bought) — counts + sum-safe value (CEILINGS)",
)
def cpv(
    page: Page = Depends(pagination(default=25)),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_procurement_cpv(cur, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)


@router.get(
    "/procurement/open-tenders",
    summary="Live TED (EU OJ) Irish tender opportunities — the forward pipeline (estimates, never summed)",
)
def open_tenders(
    only_open: bool = Query(True, description="Keep only notices still open for bids"),
    page: Page = Depends(pagination(default=40)),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    records, total, truncated = dossiers.list_open_tenders(cur, only_open=only_open, skip=page.skip, limit=page.limit)
    return serialize.envelope(records, limit=page.limit, offset=page.skip, total=total, truncated=truncated)


# ── EXPERIMENTAL real-terms (inflation-adjusted) endpoints (gated) ────────────────────────────
@router.get(
    "/procurement/inflation/indices",
    summary="EXPERIMENTAL — the deflation index registry (CPI / gov-consumption / construction TPI / materials)",
    include_in_schema=_EXPERIMENTAL,
)
def inflation_indices() -> dict:
    """The registered price indices, each with its label, what it applies to, source and caveat —
    so any adjusted figure can be traced to the index that produced it."""
    _require_experimental()
    return serialize.envelope(list_indices(), caveat=_REAL_CAVEAT)


@router.get(
    "/procurement/inflation/cpv",
    summary="EXPERIMENTAL — per-CPV award benchmark, nominal + inflation-adjusted band "
    "(construction categories use tender prices, others CPI)",
    include_in_schema=_EXPERIMENTAL,
)
def inflation_cpv(
    min_valued: int = Query(8, ge=1, description="Drop categories with fewer sum-safe valued awards"),
    limit: int = Query(100, ge=1, le=1000),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    _require_experimental()
    res = _q.cpv_summary_real(cur, min_valued=min_valued, limit=limit)
    if not res.ok:
        raise HTTPException(status_code=503, detail=res.unavailable_reason or "real-terms view unavailable")
    return serialize.envelope(serialize.to_records(res.data), limit=limit, caveat=_REAL_CAVEAT)


@router.get(
    "/procurement/inflation/spend-trend",
    summary="EXPERIMENTAL — per-year public spend, nominal vs real (government-consumption deflator) + uplift",
    include_in_schema=_EXPERIMENTAL,
)
def inflation_spend_trend(
    tier: str = Query("SPENT", description="'SPENT' (paid) or 'COMMITTED' (ordered) — never blended"),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    _require_experimental()
    res = _q.payments_real_trend(cur, tier=tier)
    if not res.ok:
        raise HTTPException(status_code=503, detail=res.unavailable_reason or "real-terms view unavailable")
    return serialize.envelope(serialize.to_records(res.data), caveat=_REAL_CAVEAT)
