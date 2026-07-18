"""Procurement retrieval — TED (EU Official Journal) notices — a SEPARATE award register (never-sum with national).

Split from the single 1,6xx-line queries/procurement.py by MONEY GRAIN
(2026-07-18) so the never-sum boundaries are module boundaries. Import surface is
unchanged: ``from dail_tracker_core.queries import procurement`` re-exports every
function; grain-shared constants live in ``._shared``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.queries.procurement._shared import _TED_ORDER
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_run = make_runner("procurement", _log)

def ted_corpus_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row TED corpus summary for the tab headline + the pan-EU toggle. The sum-safe
    value already EXCLUDES pan-EU outliers (those vast research-framework ceilings are never
    value_safe_to_sum), so the toggle does not change the real total — it only adds the 375
    pan-EU notices back to the count and *reveals* their headline ceiling (the TED echo of
    the eTenders €570bn mirage). Also the page's TED source-state gate."""
    return _run(
        conn,
        "SELECT"
        "  COUNT(*) AS n_notices,"
        "  COUNT(*) FILTER (WHERE NOT is_pan_eu_outlier) AS n_notices_ex_pan_eu,"
        "  MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        "  COUNT(DISTINCT winner_join_norm) FILTER (WHERE NOT is_pan_eu_outlier) AS n_winners,"
        "  COUNT(DISTINCT buyer_name) AS n_buyers,"
        "  COUNT(*) FILTER (WHERE is_pan_eu_outlier) AS n_pan_eu,"
        "  COALESCE(SUM(award_value_eur) FILTER (WHERE value_safe_to_sum), 0) AS value_safe_eur,"
        "  COALESCE(SUM(award_value_eur) FILTER (WHERE is_pan_eu_outlier), 0) AS pan_eu_ceiling_eur"
        " FROM v_procurement_ted_winner_history",  # full 2016-2026 history (api + per-notice-XML lanes)
    )


def ted_competition_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row competition-intensity summary for the TED tab, on a DISTINCT-NOTICE basis
    (the history view is notice x winner, so we dedup by publication_number first — the
    competition fields are identical across a notice's winner rows). All neutral facts:
    how many award notices received only one tender, ran without an open call, or were
    awarded on lowest price alone. NEVER framed as a verdict in the UI.

    Restricted to source_lane='api' (2024+ eForms): the competition fields only exist from
    2024 — the 2016-2023 per-notice-XML backfill has none, so including it would silently
    deflate every rate. The page labels this strip 'eForms, 2024+' to match."""
    return _run(
        conn,
        "SELECT"
        "  COUNT(*) AS n_notices,"
        "  COUNT(*) FILTER (WHERE n_tenders_received IS NOT NULL) AS notices_with_tenders,"
        "  COUNT(*) FILTER (WHERE is_single_bid) AS single_bid_notices,"
        "  COUNT(*) FILTER (WHERE is_uncompetitive_procedure) AS uncompetitive_notices,"
        "  COUNT(*) FILTER (WHERE is_price_only) AS price_only_notices"
        " FROM ("
        "   SELECT DISTINCT publication_number, n_tenders_received, is_single_bid,"
        "          is_uncompetitive_procedure, is_price_only"
        "   FROM v_procurement_ted_winner_history"
        "   WHERE NOT is_pan_eu_outlier AND source_lane = 'api'"
        " )",
    )


def ted_awards_by_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """TED award NOTICES per year (2016-2026), pan-EU excluded — feeds the TED tab's
    'EU awards over time' trend. DISTINCT publication_number so a multi-supplier framework
    counts once (the notice, not its winner rows). Pre-aggregated here; the page only renders."""
    return _run(
        conn,
        "SELECT year, COUNT(DISTINCT publication_number) AS n_awards"
        " FROM v_procurement_ted_winner_history"
        " WHERE NOT is_pan_eu_outlier AND year IS NOT NULL"
        " GROUP BY year ORDER BY year",
    )


def ted_supplier_summary(
    conn: duckdb.DuckDBPyConnection, *, limit: int | None = 60, order_by: str = "awards"
) -> QueryResult:
    """Top TED winners (company-class), ranked by award-notice count (trustworthy) or
    sum-safe value (excl. pan-EU). Carries both value columns so the page's pan-EU toggle
    needs no second query."""
    order = _TED_ORDER.get(order_by, _TED_ORDER["awards"])
    sql = f"SELECT * FROM v_procurement_ted_supplier_summary ORDER BY {order}"
    params: list = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def ted_for_supplier(conn: duckdb.DuckDBPyConnection, join_norm: str) -> QueryResult:
    """One TED winner's footprint for the cross-reference panel on an eTenders supplier
    profile (matched on the normalised name). Returns the single summary row, or empty if
    the firm has no TED notices. Never summed with the firm's eTenders total."""
    return _run(
        conn,
        "SELECT * FROM v_procurement_ted_supplier_summary WHERE winner_join_norm = ?",
        [join_norm],
    )


def ted_notices_for_supplier(conn: duckdb.DuckDBPyConnection, join_norm: str) -> QueryResult:
    """One winner's individual TED award notices — the CONDUIT to the authoritative EU
    source. Each row carries the publication number, buyer, date, the value-kind tag and the
    ``notice_url`` that opens the full Official Journal notice (where the deliverable, the real
    framework ceiling and the award criteria live — detail the thin gold slice omits).

    Rolls up CLOSELY-NAMED winners that share the first-two-token brand STEM (e.g.
    'Vision Built Manufacturing' + 'Vision Built Structures'; 'John Sisk & Son' / 'Sons' /
    '… Holding') so a renamed or merged entity's notices surface on one profile — while
    'John Sisk' stays separate from 'Sisk Healthcare' (different first token). ``is_exact_name``
    flags the rows whose normalised name matches exactly; ``winner_name`` is carried so the UI
    can show — never hide — which variant each notice belongs to. A name-similarity grouping,
    NOT a verified corporate link: the consumer must label variants as "may be related". One
    row per notice; exact names first, then newest. Award notices, never summed."""
    tokens = (join_norm or "").split()
    stem = " ".join(tokens[:2]) if len(tokens) >= 2 else (join_norm or "")
    return _run(
        conn,
        "SELECT publication_number, buyer_name, dispatch_date, value_kind,"
        " is_multi_supplier_framework, n_winners, notice_url, winner_name, winner_join_norm,"
        " (winner_join_norm = ?) AS is_exact_name"
        " FROM v_procurement_ted_winner_history"
        " WHERE winner_join_norm = ? OR winner_join_norm = ? OR winner_join_norm LIKE ?"
        " ORDER BY is_exact_name DESC, dispatch_date DESC NULLS LAST",
        [join_norm, join_norm, stem, f"{stem} %"],
    )


# ── TED COMPETITION / TENDER notices (cn-standard) — a THIRD grain, the pre-award pipeline ──
# estimated_value is a buyer estimate (value_safe_to_sum always FALSE); NEVER summed with awards
# or payments. One row per notice.
def ted_tenders_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row summary of the TED tender pipeline for the tab headline + source-state gate:
    notice count, year span, how many are still open by deadline, and how many ran without an
    open competitive call. No euro total (estimates are never summed)."""
    return _run(
        conn,
        "SELECT"
        "  COUNT(*) AS n_notices,"
        "  COUNT(DISTINCT buyer_name) AS n_buyers,"
        "  MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        "  COUNT(*) FILTER (WHERE is_still_open) AS n_still_open,"
        "  COUNT(*) FILTER (WHERE is_uncompetitive_procedure) AS n_uncompetitive"
        " FROM v_procurement_ted_tenders",
    )


def ted_tenders(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = 60,
    only_open: bool = False,
    sector: str | None = None,
) -> QueryResult:
    """The tender-pipeline listing (most recent first). ``only_open`` keeps notices whose
    submission deadline has not yet passed. ``sector`` narrows to one CPV division (the TED
    feed's sector facet — unlike the national eTenders feed, TED notices carry a CPV code).
    estimated_value_eur is a pre-award estimate shown for context — never summed with
    award/payment figures."""
    sql = (
        "SELECT publication_number, notice_url, buyer_name, cpv_code, cpv_division, procedure_type,"
        " is_uncompetitive_procedure, submission_deadline, is_still_open, estimated_value_eur, currency,"
        " dispatch_date, year"
        " FROM v_procurement_ted_tenders"
    )
    where: list[str] = []
    params: list = []
    if only_open:
        where.append("is_still_open")
    if sector:
        where.append("cpv_division = ?")
        params.append(sector)
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY dispatch_date DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def ted_tender_sectors(conn: duckdb.DuckDBPyConnection, *, only_open: bool = False) -> QueryResult:
    """Distinct CPV divisions in the TED tender pipeline with a per-division notice count, busiest
    first — the option list (with counts) for the sector facet. ``only_open`` matches the listing's
    open-by-deadline toggle so the facet counts agree with what the filtered list will show."""
    sql = (
        "SELECT cpv_division AS sector, COUNT(*) AS n"
        " FROM v_procurement_ted_tenders"
        " WHERE cpv_division IS NOT NULL AND cpv_division <> ''"
    )
    if only_open:
        sql += " AND is_still_open"
    sql += " GROUP BY cpv_division ORDER BY n DESC, sector ASC"
    return _run(conn, sql, [])


def expiring_contracts_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row summary of the advertised-term corpus: how many notices carry an end
    estimate, how many fall due in the next 12 months, and the basis mix (explicit end
    date vs projected from duration). No euro total — award/ceiling values never sum."""
    return _run(
        conn,
        "SELECT"
        "  COUNT(*) AS n_with_estimate,"
        "  COUNT(*) FILTER (WHERE contract_end_date_est BETWEEN CURRENT_DATE"
        "    AND CURRENT_DATE + INTERVAL 12 MONTH) AS n_ending_12m,"
        "  COUNT(*) FILTER (WHERE contract_end_basis = 'explicit_end_date') AS n_explicit,"
        "  MIN(contract_end_date_est) AS earliest_end, MAX(contract_end_date_est) AS latest_end"
        " FROM v_procurement_expiring_contracts",
    )


def expiring_contracts(
    conn: duckdb.DuckDBPyConnection, *, months_ahead: int = 12, limit: int | None = 60
) -> QueryResult:
    """Contracts whose ADVERTISED term ends within the window (soonest first).

    The end date is the term advertised on the award notice (explicit end date, or
    start/conclusion date + duration) — an advertised term, never a verified end event;
    renewals may extend it. award_value_eur is award/ceiling grade: display-only."""
    sql = (
        "SELECT publication_number, notice_url, buyer_name, winners_display, n_winners,"
        " cpv_division, award_value_eur, value_kind, is_multi_supplier_framework,"
        " contract_conclusion_date, contract_duration_months, renewal_max,"
        " contract_end_date_est, contract_end_basis, year"
        " FROM v_procurement_expiring_contracts"
        " WHERE contract_end_date_est BETWEEN CURRENT_DATE"
        "   AND CURRENT_DATE + (? * INTERVAL 1 MONTH)"
        " ORDER BY contract_end_date_est ASC"
    )
    params: list = [int(months_ahead)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)
