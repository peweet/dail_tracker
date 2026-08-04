"""Focused analytical reads that complement the composed resource dossiers.

Every handler is a thin adapter over ``dail_tracker_core.queries``. Keeping the
query in core makes the same read reusable by the API, MCP, and Streamlit while
the response model gives API clients one stable ``{head, results}`` envelope.
"""

from __future__ import annotations

from typing import Literal

import duckdb
from fastapi import APIRouter, Depends, HTTPException, Query

from api.contracts import ERROR_RESPONSES
from api.deps import get_cursor
from dail_tracker_core import serialize
from dail_tracker_core.models.envelope import ListEnvelope
from dail_tracker_core.queries import (
    attendance,
    entity,
    interests,
    judiciary,
    legislation,
    lobbying,
    local_government,
    ministerial_diary,
    nphdb_bam,
    payments,
    votes,
)
from dail_tracker_core.queries.procurement import afs
from dail_tracker_core.results import QueryResult

router = APIRouter(prefix="/analytics", tags=["analytics"], responses=ERROR_RESPONSES)


def _response(result: QueryResult) -> dict:
    """Require a successful query and wrap its rows in the API list contract."""
    records = serialize.to_records(result.require())
    return serialize.envelope(records, total=len(records))


@router.get("/attendance/taa-compliance", response_model=ListEnvelope, response_model_exclude_unset=True)
def attendance_taa_summary(
    year: int,
    house: str = "Dáil",
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(attendance.taa_compliance_summary(cur, year=year, house=house))


@router.get("/attendance/year-ranking", response_model=ListEnvelope, response_model_exclude_unset=True)
def attendance_year_ranking(
    year: int,
    house: str = "Dáil",
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(attendance.year_ranking(cur, year=year, house=house))


@router.get("/entities/cross-register", response_model=ListEnvelope, response_model_exclude_unset=True)
def entity_cross_register(
    min_registers: int = Query(2, ge=2),
    limit: int = Query(50, ge=1, le=500),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(entity.top_cross_register(cur, min_registers=min_registers, limit=limit))


@router.get("/interests", response_model=ListEnvelope, response_model_exclude_unset=True)
def interests_detail(
    house: str,
    name: str = "",
    years: tuple[int, ...] = Query(()),
    landlord_only: bool = False,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(interests.detail(cur, house=house, name_q=name, years=years, landlord_only=landlord_only))


@router.get("/judiciary/appointing-authorities", response_model=ListEnvelope, response_model_exclude_unset=True)
def judiciary_authorities(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return _response(judiciary.authority_summary(cur))


@router.get("/legislation/circular-si-crosswalk", response_model=ListEnvelope, response_model_exclude_unset=True)
def circular_si_crosswalk(
    si_year: int = Query(0, ge=0),
    si_number: int = Query(0, ge=0),
    resolved_only: bool = False,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    if bool(si_year) != bool(si_number):
        raise HTTPException(status_code=422, detail="si_year and si_number must be supplied together")
    return _response(
        legislation.circular_si_crosswalk(
            cur,
            si_year=si_year,
            si_number=si_number,
            resolved_only=resolved_only,
        )
    )


@router.get("/legislation/introduced-year-counts", response_model=ListEnvelope, response_model_exclude_unset=True)
def legislation_year_counts(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return _response(legislation.introduced_year_counts(cur))


@router.get("/lobbying/summary", response_model=ListEnvelope, response_model_exclude_unset=True)
def lobbying_summary(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return _response(lobbying.summary(cur))


@router.get("/local-government/derelict-levy", response_model=ListEnvelope, response_model_exclude_unset=True)
def derelict_levy_ranking(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return _response(local_government.derelict_levy_ranking(cur))


@router.get("/ministerial/access-to-contracts", response_model=ListEnvelope, response_model_exclude_unset=True)
def ministerial_access_to_contracts(
    limit: int = Query(25, ge=1, le=500),
    order_by: Literal["awards_eur", "paid_eur", "meetings", "total_lobbying_returns"] = "awards_eur",
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(ministerial_diary.access_to_contracts(cur, limit=limit, order_by=order_by))


@router.get("/ministerial/company-influence", response_model=ListEnvelope, response_model_exclude_unset=True)
def ministerial_company_influence(
    name: str = Query(min_length=1),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(ministerial_diary.company_influence(cur, name=name))


@router.get("/procurement/nphdb-bam-disclosures", response_model=ListEnvelope, response_model_exclude_unset=True)
def nphdb_bam_disclosures(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return _response(nphdb_bam.disclosures(cur))


@router.get("/payments/member-year", response_model=ListEnvelope, response_model_exclude_unset=True)
def payment_member_year(
    member_name: str = Query(min_length=1),
    year: int = Query(ge=1900, le=2100),
    unique_member_code: str | None = None,
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(
        payments.member_year_summary(
            cur,
            member_name=member_name,
            year=year,
            unique_member_code=unique_member_code,
        )
    )


@router.get("/payments/summary", response_model=ListEnvelope, response_model_exclude_unset=True)
def payment_summary(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return _response(payments.summary(cur))


@router.get("/procurement/afs/coverage", response_model=ListEnvelope, response_model_exclude_unset=True)
def procurement_afs_coverage(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return _response(afs.afs_coverage_by_council(cur))


@router.get("/procurement/afs/national-by-year", response_model=ListEnvelope, response_model_exclude_unset=True)
def procurement_afs_national(cur: duckdb.DuckDBPyConnection = Depends(get_cursor)) -> dict:
    return _response(afs.afs_national_by_year(cur))


@router.get("/procurement/afs/council-by-year", response_model=ListEnvelope, response_model_exclude_unset=True)
def procurement_afs_council(
    council: str = Query(min_length=1),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(afs.afs_total_by_year(cur, council=council))


@router.get("/procurement/afs/po-coverage", response_model=ListEnvelope, response_model_exclude_unset=True)
def procurement_afs_po_coverage(
    council: str = Query(min_length=1),
    year: int | None = Query(None, ge=1900, le=2100),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(afs.afs_vs_po_coverage(cur, council=council, year=year))


@router.get("/votes/member-year", response_model=ListEnvelope, response_model_exclude_unset=True)
def vote_member_year(
    member_id: str = Query(min_length=1),
    cur: duckdb.DuckDBPyConnection = Depends(get_cursor),
) -> dict:
    return _response(votes.member_year_summary(cur, member_id=member_id))
