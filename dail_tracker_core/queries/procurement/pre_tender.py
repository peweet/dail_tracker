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
        ", stage_1_start_date, stage_2a_start_date, stage_2b_start_date, stage_3_start_date"
        ", stage_4_start_date, start_on_site_date, start_on_site_note"
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


def pre_tender_sectors(
    conn: duckdb.DuckDBPyConnection, *, area: str | None = None, stage: str | None = None
) -> QueryResult:
    """Distinct sectors with a per-sector count — the sector facet's option list. Counts move
    with the other active filters so the facet agrees with the listing beside it."""
    where, params = _lead_filters(area=area, sector=None, stage=stage)
    clause = f"{where} AND" if where else " WHERE"
    return _run(
        conn,
        "SELECT sector, COUNT(*) AS n FROM v_procurement_pre_tender_leads"
        + clause
        + " sector IS NOT NULL AND sector <> ''"
        " GROUP BY sector ORDER BY n DESC, sector ASC",
        params,
    )


def pre_tender_stages(
    conn: duckdb.DuckDBPyConnection, *, area: str | None = None, sector: str | None = None
) -> QueryResult:
    """Normalised stages present, with counts, ordered by the corpus's own stage_display_order.

    The facet keys on ``normalized_stage`` rather than ``published_stage`` because the published
    wording is source-specific and collides across corpora — the Department's "Stage 3" (tender
    documents) and "Stage 4" (construction) mean nothing to a council Part 8 row. The published
    wording is still shown per card as the source's own words.
    """
    where, params = _lead_filters(area=area, sector=sector, stage=None)
    clause = f"{where} AND" if where else " WHERE"
    return _run(
        conn,
        "SELECT normalized_stage AS stage, MIN(stage_display_order) AS stage_display_order,"
        " COUNT(*) AS n FROM v_procurement_pre_tender_leads"
        + clause
        + " normalized_stage IS NOT NULL AND normalized_stage <> ''"
        " GROUP BY normalized_stage ORDER BY stage_display_order, stage ASC",
        params,
    )


def pre_tender_areas(
    conn: duckdb.DuckDBPyConnection, *, sector: str | None = None, stage: str | None = None
) -> QueryResult:
    """Reporting areas present, with counts — the area facet's option list. ``reporting_area`` is
    the source's own geography wording (a county, an ETB, or a town), never a derived boundary."""
    where, params = _lead_filters(area=None, sector=sector, stage=stage)
    clause = f"{where} AND" if where else " WHERE"
    return _run(
        conn,
        "SELECT reporting_area AS area, COUNT(*) AS n FROM v_procurement_pre_tender_leads"
        + clause
        + " reporting_area IS NOT NULL AND reporting_area <> ''"
        " GROUP BY reporting_area ORDER BY n DESC, area ASC",
        params,
    )


def pre_tender_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row corpus summary for the section headline and its verification caveat.

    Carries the counts the caveat is built from rather than letting the page assert them:
    how many observations are held, how many distinct areas and sources, the newest and oldest
    observation date, and — the load-bearing pair — how many rows are status-verified and how
    many are stale. Every row in this lane is currently unverified against live tenders, so the
    headline must be able to say so from the data instead of from a hard-coded sentence.

    ``n_past_tender`` exists because the section's own framing — projects described but not yet
    advertised — is not true of every row in it. A row reported as in construction, or carrying a
    start-on-site date already in the past, had demonstrably been advertised and awarded by the
    observation date. The page has to be able to state that count instead of a blanket claim.
    ``n_undated`` is the rows no age band can be derived for, so "how many are stale" is never
    read as covering the whole corpus.
    """
    return _run(
        conn,
        "SELECT"
        "  COUNT(*) AS n_leads,"
        "  COUNT(DISTINCT reporting_area) AS n_areas,"
        "  COUNT(DISTINCT source_url) AS n_sources,"
        "  MAX(source_date) AS newest_observation,"
        "  MIN(source_date) AS oldest_observation,"
        "  MAX(report_as_of) AS report_as_of,"
        "  COUNT(*) FILTER (WHERE current_status_verified) AS n_status_verified,"
        "  COUNT(*) FILTER (WHERE snapshot_freshness = 'stale_over_365_days') AS n_stale,"
        "  COUNT(*) FILTER (WHERE snapshot_freshness = 'recent_180_days') AS n_recent,"
        "  COUNT(*) FILTER (WHERE source_date IS NULL) AS n_undated,"
        "  COUNT(*) FILTER (WHERE normalized_stage = 'active_delivery'"
        "    OR TRY_CAST(start_on_site_date AS DATE) <= CURRENT_DATE) AS n_past_tender"
        " FROM v_procurement_pre_tender_leads",
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
