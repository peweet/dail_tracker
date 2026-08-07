"""Pre-notice procurement lead retrieval.

These observations are kept separate from live tenders, awards, payments and
budgets. The query layer filters the registered gold contract without adding
scores, current-status claims, or money aggregation.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_run = make_runner("procurement", logging.getLogger(__name__))


def _lead_filters(*, area: str | None, sector: str | None, stage: str | None) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if area:
        clauses.append("reporting_area ILIKE ?")
        params.append(f"%{area.strip()}%")
    if sector:
        clauses.append("sector = ?")
        params.append(sector.strip())
    if stage:
        clauses.append("normalized_stage = ?")
        params.append(stage.strip())
    return (f" WHERE {' AND '.join(clauses)}" if clauses else "", params)


def _work_package_filters(
    *, lead_id: str | None, package_code: str | None, package_group: str | None
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if lead_id:
        clauses.append("lead_id = ?")
        params.append(lead_id.strip())
    if package_code:
        clauses.append("package_code = ?")
        params.append(package_code.strip())
    if package_group:
        clauses.append("package_group = ?")
        params.append(package_group.strip())
    return (f" WHERE {' AND '.join(clauses)}" if clauses else "", params)


def pre_tender_leads(
    conn: duckdb.DuckDBPyConnection,
    *,
    area: str | None = None,
    sector: str | None = None,
    stage: str | None = None,
    limit: int = 200,
    offset: int = 0,
) -> QueryResult:
    """Return a bounded list of dated, source-linked pre-tender observations."""
    where, params = _lead_filters(area=area, sector=sector, stage=stage)
    params.append(max(1, min(int(limit), 500)))
    params.append(max(0, int(offset)))
    return _run(
        conn,
        "SELECT lead_id, source_corpus, source_record_id, source_date, date_precision,"
        " buyer_or_sponsor, reporting_area, area_basis, project_name, sector,"
        " likely_work_package, published_stage, normalized_stage, stage_display_order,"
        " amount_text, amount_is_not_aggregable, source_url, source_review_required,"
        " current_status_verified, tender_notice_status, report_as_of, classification_schema"
        ", school_roll_number, snapshot_freshness"
        " FROM v_procurement_pre_tender_leads"
        + where
        + " ORDER BY stage_display_order, source_date DESC NULLS LAST, project_name LIMIT ? OFFSET ?",
        params,
    )


def pre_tender_lead_count(
    conn: duckdb.DuckDBPyConnection,
    *,
    area: str | None = None,
    sector: str | None = None,
    stage: str | None = None,
) -> QueryResult:
    """Count pre-tender lead observations under the same filters as the list."""
    where, params = _lead_filters(area=area, sector=sector, stage=stage)
    return _run(conn, "SELECT count(*) AS total FROM v_procurement_pre_tender_leads" + where, params)


def pre_tender_lead_by_id(conn: duckdb.DuckDBPyConnection, lead_id: str) -> QueryResult:
    """Return one lead by its stable observation identifier."""
    return _run(
        conn,
        "SELECT * FROM v_procurement_pre_tender_leads WHERE lead_id = ? LIMIT 1",
        [lead_id],
    )


def pre_tender_work_packages(
    conn: duckdb.DuckDBPyConnection,
    *,
    lead_id: str | None = None,
    package_code: str | None = None,
    package_group: str | None = None,
    limit: int = 2_000,
    offset: int = 0,
) -> QueryResult:
    """Return deterministic work-package classifications without changing lead grain."""
    where, params = _work_package_filters(lead_id=lead_id, package_code=package_code, package_group=package_group)
    params.append(max(1, min(int(limit), 5_000)))
    params.append(max(0, int(offset)))
    return _run(
        conn,
        "SELECT package_row_id, lead_id, source_corpus, source_record_id, source_date,"
        " reporting_area, project_name, package_code, package_label, package_group,"
        " evidence_phrase, matched_field, classification_basis, source_url,"
        " source_review_required, current_status_verified, amount_is_not_aggregable,"
        " classification_schema FROM v_procurement_pre_tender_work_packages"
        + where
        + " ORDER BY package_group, package_label, source_date DESC NULLS LAST, project_name LIMIT ? OFFSET ?",
        params,
    )


def pre_tender_work_package_count(
    conn: duckdb.DuckDBPyConnection,
    *,
    lead_id: str | None = None,
    package_code: str | None = None,
    package_group: str | None = None,
) -> QueryResult:
    """Count package classifications under the same filters as the list."""
    where, params = _work_package_filters(lead_id=lead_id, package_code=package_code, package_group=package_group)
    return _run(
        conn,
        "SELECT count(*) AS total FROM v_procurement_pre_tender_work_packages" + where,
        params,
    )
