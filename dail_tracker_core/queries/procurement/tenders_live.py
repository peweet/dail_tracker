"""Procurement retrieval — national LIVE tender pipeline (etenders.gov.ie) — planned/open competitions, not money.

Split from the single 1,6xx-line queries/procurement.py by MONEY GRAIN
(2026-07-18) so the never-sum boundaries are module boundaries. Import surface is
unchanged: ``from dail_tracker_core.queries import procurement`` re-exports every
function; grain-shared constants live in ``._shared``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_run = make_runner("procurement", _log)

# ── NATIONAL live tender pipeline (etenders.gov.ie) — the PLANNED tier, open NOW ──
# Forward end of the pipeline TED (EU-threshold only) and the OGP quarterly CSV cannot give us:
# sub-threshold national opportunities (schools, councils, water schemes). estimated_value_eur is a
# buyer estimate at the PLANNED stage — value_kind='estimate_advertised', NEVER summed with awards or
# payments. The view already keeps only the genuinely-open set (deadline in the future) and orders
# soonest-closing first; this wrapper is retrieval-only.


def live_tenders(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = 80,
    within_days: int | None = None,
    sector: str | None = None,
) -> QueryResult:
    """Open national tenders accepting bids now (soonest-closing first). estimated_value_eur is a
    PLANNED-tier buyer estimate shown for context — never summed with award/payment figures.

    ``within_days`` narrows to opportunities closing within that many days (a forward date facet over
    the view's pre-computed days_to_deadline); None keeps the full open set. ``sector`` narrows to one
    CPV division — available only once the snapshot has been enriched with a CPV from the detail page
    (cpv_division is referenced ONLY when a sector is passed, so an un-enriched snapshot is unaffected)."""
    sql = (
        "SELECT title, buyer, published_date, submission_deadline, days_to_deadline,"
        " procedure, status, estimated_value_eur, realisation_tier, value_kind,"
        " resource_id, detail_url, retrieved_utc"
        " FROM v_procurement_live_tenders"
    )
    where: list[str] = []
    params: list = []
    if within_days is not None:
        where.append("days_to_deadline <= ?")
        params.append(int(within_days))
    if sector:
        where.append("cpv_division = ?")
        params.append(sector)
    if where:
        sql += " WHERE " + " AND ".join(where)
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def live_tender_sectors(conn: duckdb.DuckDBPyConnection, *, within_days: int | None = None) -> QueryResult:
    """Distinct CPV divisions in the open national pipeline with a per-division count — the sector
    facet's option list. Returns ``unavailable`` (so the page simply omits the facet) until the
    snapshot has been enriched with a CPV division from the detail page. ``within_days`` keeps the
    facet counts in step with the listing's closing-date window."""
    sql = (
        "SELECT cpv_division AS sector, COUNT(*) AS n"
        " FROM v_procurement_live_tenders"
        " WHERE cpv_division IS NOT NULL AND cpv_division <> ''"
    )
    params: list = []
    if within_days is not None:
        sql += " AND days_to_deadline <= ?"
        params.append(int(within_days))
    sql += " GROUP BY cpv_division ORDER BY n DESC, sector ASC"
    return _run(conn, sql, params)


def live_tenders_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row summary of the open national pipeline for the section headline + freshness gate:
    open count, distinct buyers, how many close within 14 days, the next closing date, and the
    snapshot timestamp (retrieved_utc) so the page can show 'as of …' and guard staleness. No euro
    total beyond an indicative PLANNED-tier estimate floor — never presented as committed/paid."""
    return _run(
        conn,
        "SELECT"
        "  COUNT(*) AS n_open,"
        "  COUNT(DISTINCT buyer) AS n_buyers,"
        "  COUNT(*) FILTER (WHERE days_to_deadline <= 14) AS closing_within_14d,"
        "  MIN(submission_deadline) AS next_closing,"
        "  MAX(submission_deadline) AS last_closing,"  # furthest deadline in the open set (the data's horizon)
        "  MAX(days_to_deadline)::INT AS max_days,"
        "  MAX(retrieved_utc) AS retrieved_utc"
        " FROM v_procurement_live_tenders",
    )


def expiring_contracts_etenders(
    conn: duckdb.DuckDBPyConnection, *, months_ahead: int = 24, limit: int | None = 60
) -> QueryResult:
    """NATIONAL (eTenders) contracts whose ADVERTISED term ends within the window (soonest first).

    The end date is award/created date + advertised duration — a term, never a verified event;
    renewals are deliberately not folded in. Frameworks/DPS are excluded by the view. award_value_eur
    is award/ceiling grade: display-only, never summed. Likely-personal winner names are withheld by
    the view (the contract itself stays listed — public record)."""
    sql = (
        "SELECT buyer_name, contract_name, cpv_code, spend_category, winner_display,"
        " supplier_norm, supplier_class, award_date, duration_months, est_end_date,"
        " est_end_basis, award_value_eur, value_kind"
        " FROM v_procurement_expiring_contracts_etenders"
        " WHERE est_end_date BETWEEN CURRENT_DATE AND CURRENT_DATE + (? * INTERVAL 1 MONTH)"
        " ORDER BY est_end_date ASC"
    )
    params: list = [int(months_ahead)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)
