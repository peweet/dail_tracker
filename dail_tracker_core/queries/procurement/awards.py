"""Procurement retrieval — eTenders AWARD notices — the award-ceiling grain (never summed with payments/TED).

Split from the single 1,6xx-line queries/procurement.py by MONEY GRAIN
(2026-07-18) so the never-sum boundaries are module boundaries. Import surface is
unchanged: ``from dail_tracker_core.queries import procurement`` re-exports every
function; grain-shared constants live in ``._shared``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.queries.procurement._shared import (
    _COMPETITION_ORDER,
    _RANK_ORDER,
    _SUPPLIER_COLS,
    _SUPPLIER_ORDER,
)
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

_run = make_runner("procurement", _log)

def supplier_summary(
    conn: duckdb.DuckDBPyConnection,
    *,
    limit: int | None = None,
    order_by: str = "awards",
    year: int | None = None,
) -> QueryResult:
    """Supplier ranking — one row per distinct supplier (company-class). ``order_by``
    is ``"awards"`` (contract count, the trustworthy default) or ``"value"`` (sum-safe
    awarded value, surfacing the money leaders). ``year`` (a calendar year) scopes the
    ranking to that year via the per-year view; ``None`` is the all-time ranking.
    Carries CRO match + lobbying flags (entity-level — identical in both views)."""
    order = _SUPPLIER_ORDER.get(order_by, _SUPPLIER_ORDER["awards"])
    params: list = []
    if year is None:
        # has_epa_licence is folded into the all-time view only (the Companies landing
        # uses it for the EPA filter/count); the per-year view does not carry it.
        sql = f"SELECT {_SUPPLIER_COLS}, has_epa_licence FROM v_procurement_supplier_summary ORDER BY {order}"
    else:
        sql = f"SELECT {_SUPPLIER_COLS} FROM v_procurement_supplier_year_summary WHERE year = ? ORDER BY {order}"
        params.append(int(year))
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def available_years(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Distinct award years present in the company-class slice, newest first — the
    option list behind the page's year pills."""
    return _run(
        conn,
        "SELECT DISTINCT year FROM v_procurement_supplier_year_summary WHERE year IS NOT NULL ORDER BY year DESC",
    )


def coverage_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row corpus summary for the page hero / scale anchor: the true distinct
    counts, the date span, and the sum-safe awarded-value total — computed live over
    the company-class, non-truncated slice (same gate as the rankings) so the badges
    never under- or over-count. No GROUP BY: a single aggregate row, not a rollup."""
    return _run(
        conn,
        "SELECT"
        " MIN(EXTRACT(year FROM award_date))::INT AS min_year,"
        " MAX(EXTRACT(year FROM award_date))::INT AS max_year,"
        " COUNT(*) AS n_award_rows,"
        " COUNT(*) FILTER (WHERE value_safe_to_sum) AS n_safe_rows,"
        " COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS value_safe_total_eur,"
        " COUNT(DISTINCT supplier_norm) AS n_suppliers,"
        " COUNT(DISTINCT contracting_authority) AS n_authorities,"
        " COUNT(DISTINCT cpv_code) AS n_categories"
        " FROM v_procurement_awards"
        " WHERE supplier_class = 'company' AND NOT name_truncated AND length(supplier_norm) >= 4",
    )


def supplier_concentration(conn: duckdb.DuckDBPyConnection, *, top_n: int = 10) -> QueryResult:
    """How concentrated is contract-winning? Returns the share of all company-class awards
    held by the top-N firms (by award count), plus the totals behind it. The percentage is
    computed in SQL (a metric belongs in the query layer, not the page). Answers a
    journalist's first question — 'how few firms hold how much'."""
    return _run(
        conn,
        "WITH ranked AS ("
        "  SELECT n_awards, ROW_NUMBER() OVER (ORDER BY n_awards DESC) AS rn,"
        "         SUM(n_awards) OVER () AS total_awards"
        "  FROM v_procurement_supplier_summary)"
        " SELECT"
        "  ? AS top_n,"
        "  COUNT(*) AS n_suppliers,"
        "  MAX(total_awards) AS total_awards,"
        "  COALESCE(SUM(n_awards) FILTER (WHERE rn <= ?), 0) AS top_n_awards,"
        "  ROUND(100.0 * COALESCE(SUM(n_awards) FILTER (WHERE rn <= ?), 0)"
        "        / NULLIF(MAX(total_awards), 0), 1) AS top_n_share_pct"
        " FROM ranked",
        [int(top_n), int(top_n), int(top_n)],
    )


def competition(
    conn: duckdb.DuckDBPyConnection,
    *,
    min_lots: int = 0,
    order_by: str = "single_bid",
    limit: int | None = None,
) -> QueryResult:
    """Per-buyer procurement competition signals from ``v_procurement_competition`` (TED
    2024+). ``single_bid_lot_pct`` = single-bid LOTS / lots-with-a-bid-count — each contract
    part counted once (the honest lot-level rate, not the inflated notice-level one).
    ``min_lots`` drops small, noisy samples; ``order_by`` is ``"single_bid"`` (rate, default)
    or ``"lots"`` (volume). A factual competition signal, never a verdict — the dossier layer
    attaches the caveat."""
    order = _COMPETITION_ORDER.get(order_by, _COMPETITION_ORDER["single_bid"])
    sql = (
        "SELECT buyer_name, n_notices, n_lots_with_bidcount, n_single_bid_lots,"
        " single_bid_lot_pct, n_uncompetitive_notices, n_price_only_notices, first_year, last_year"
        " FROM v_procurement_competition WHERE n_lots_with_bidcount >= ?"
        f" ORDER BY {order}"
    )
    params: list = [int(min_lots)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def bid_signal(
    conn: duckdb.DuckDBPyConnection,
    *,
    trade_code: str | None = None,
    sector_code: str | None = None,
    min_awards: int = 20,
    limit: int | None = None,
) -> QueryResult:
    """EXPERIMENTAL "Should I bid?" signals per CPV trade from ``v_procurement_bid_signal``,
    grouped by CPV sector (division) — applies the same logic to EVERY sector, not just
    construction.

    NOT a price, and known LOW-VALUE: the pricing-by-comparable investigation proved this data
    cannot quote a job (4.5x–15x intra-trade spread; headline value mixes framework ceilings
    14x–79x above real awards) and there is NO size/area (e.g. m²/GFA) anywhere to normalise it,
    so two contracts in the same trade can differ purely by project size. This returns FACTS for
    a bidder to reason from, each with its own n so a thin sample is visible: the contract-award
    band (p25/median/p75, ceilings excluded), the ceiling context shown separately, competition
    (median bids + single-bid rate), and SME win rate. All aggregation lives in the view; the
    page renders, never computes. A high single-bid rate is a prompt to look, never a verdict.

    ``trade_code`` filters to one 4-digit CPV trade; ``sector_code`` to one 2-digit CPV sector;
    ``min_awards`` drops noisy small trades (ignored when a specific ``trade_code`` is asked)."""
    cols = (
        "trade_code, sector_code, sector_label, trade_label, n_awards_total, n_contract_awards,"
        " award_p25_eur, award_median_eur, award_p75_eur, n_recent_contract_awards,"
        " n_framework_ceilings, ceiling_p25_eur, ceiling_median_eur, ceiling_p75_eur,"
        " n_with_bid_data, median_bids, n_single_bid,"
        " single_bid_pct, n_with_sme_data, n_sme_won, sme_win_pct"
    )
    where = []
    params: list = []
    if trade_code:
        where.append("trade_code = ?")
        params.append(str(trade_code))
    else:
        where.append("n_awards_total >= ?")
        params.append(int(min_awards))
    if sector_code:
        where.append("sector_code = ?")
        params.append(str(sector_code))
    sql = f"SELECT {cols} FROM v_procurement_bid_signal WHERE {' AND '.join(where)}"
    # Group sectors together, biggest trade first within each (the page renders sector headers).
    sql += " ORDER BY sector_label, n_awards_total DESC"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def awards_by_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Company-class award counts per calendar year (the trend lens — 'is contract activity
    rising?'). Counts only, pre-aggregated; the page renders, never computes."""
    return _run(
        conn,
        "SELECT year, SUM(n_awards)::BIGINT AS n_awards"
        " FROM v_procurement_supplier_year_summary"
        " WHERE year IS NOT NULL GROUP BY year ORDER BY year",
    )


def value_contrast(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Whole-corpus naive-vs-safe value contrast for the "€570bn that isn't" panel.

    UNGATED on purpose (every award row, all supplier classes) — this is the open-data
    literacy story about the *dataset*, distinct from the company-class rankings slice.
    Returns one row: the naive Σ of every reported value (a ~24× overstatement driven by
    multi-supplier framework ceilings repeated across rows), the only summable figure
    (`value_safe_to_sum` Σ), and the framework ceiling counted *once per notice* (which
    shows how much of the naive total is pure repetition). No metric leaves the view/core
    layer — the page only renders these numbers."""
    return _run(
        conn,
        "WITH per_framework AS ("
        "  SELECT tender_id, MAX(value_eur) AS v FROM v_procurement_awards"
        "  WHERE is_framework_or_dps GROUP BY tender_id)"
        " SELECT"
        "  COUNT(*) AS n_rows,"
        "  COUNT(*) FILTER (WHERE is_framework_or_dps) AS n_framework_rows,"
        "  COUNT(*) FILTER (WHERE value_safe_to_sum) AS n_safe_rows,"
        "  COALESCE(SUM(value_eur), 0) AS naive_total_eur,"
        "  COALESCE(SUM(value_eur) FILTER (WHERE value_safe_to_sum), 0) AS safe_total_eur,"
        "  COALESCE(SUM(value_eur) FILTER (WHERE is_framework_or_dps), 0) AS framework_naive_eur,"
        "  (SELECT COALESCE(SUM(v), 0) FROM per_framework) AS framework_once_eur"
        " FROM v_procurement_awards",
    )


def cpv_summary_real(conn: duckdb.DuckDBPyConnection, *, min_valued: int = 1, limit: int | None = None) -> QueryResult:
    """Per-CPV award benchmark carrying BOTH the nominal band and the inflation-adjusted (CPI,
    today's-prices) band, from ``v_procurement_cpv_summary_real``. ``n_real_excluded`` is the
    honest count of sum-safe awards that could not be adjusted (year outside the index), so the
    two bands are over slightly different samples — the page shows that, never hides it. All
    aggregation + deflation is in the view; this only filters/orders/limits."""
    sql = (
        "SELECT cpv_code, cpv_description, n_awards_valued, median_award_eur, p25_award_eur, p75_award_eur,"
        " n_awards_valued_real, median_award_real_eur, p25_award_real_eur, p75_award_real_eur,"
        " min_award_real_eur, max_award_real_eur, n_real_excluded, real_base_year, deflator_index,"
        # sector-aware band: construction CPVs use the SCSI tender-price index, others CPI
        " n_awards_valued_real_sector, median_award_real_sector_eur, p25_award_real_sector_eur,"
        " p75_award_real_sector_eur, deflator_index_sector"
        " FROM v_procurement_cpv_summary_real WHERE n_awards_valued >= ?"
        " ORDER BY n_awards_valued DESC"
    )
    params: list = [int(min_valued)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def awards_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """Every award row for one supplier (detail view), most recent first.

    Sole traders / natural persons are excluded here: row-level naming follows
    the published source, but composing one person's full award history is
    profile-building on an individual. Both the API supplier dossier and the
    Streamlit drill-down call this query, so the quarantine lives in this one
    place (rankings were already company-only in their views)."""
    return _run(
        conn,
        "SELECT tender_id, contracting_authority, cpv_code, cpv_description,"
        " tender_title, category_label, procedure_type, contract_duration_months,"
        " n_bids_received, ted_can_link, ted_notice_link, etenders_notice_url,"
        " competition_type, award_date, value_eur, value_kind, value_safe_to_sum,"
        " is_call_off"
        " FROM v_procurement_awards WHERE supplier_norm = ?"
        " AND supplier_class <> 'sole_trader_or_individual'"
        " ORDER BY award_date DESC NULLS LAST",
        [supplier_norm],
    )


def supplier_year_trend(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """One firm's public-sector work SECURED per calendar year — award count + sum-safe
    awarded value — straight from the per-(supplier, year) view (same company-class, non-
    truncated gate as the rankings; only value_safe_to_sum is summed).

    ⚠️ This is the PUBLIC procurement register only (eTenders national awards): contracts the
    firm *won*, NOT its turnover, and an *awarded* contract value, never money paid. A private
    company may earn most of its income outside the public sector — none of that appears here.
    Pre-aggregated/value-gated in the view; the consumer renders the trend, computing nothing.
    Oldest year first so a time axis reads left-to-right."""
    return _run(
        conn,
        "SELECT year, n_awards, awarded_value_safe_eur, n_value_safe_awards"
        " FROM v_procurement_supplier_year_summary"
        " WHERE supplier_norm = ? AND year IS NOT NULL"
        " ORDER BY year",
        [supplier_norm],
    )


def authority_summary(
    conn: duckdb.DuckDBPyConnection, *, limit: int | None = 50, order_by: str = "awards", year: int | None = None
) -> QueryResult:
    """Contracting authorities ranked by number of awards (or sum-safe value).
    ``year`` scopes to one calendar year via the per-year view; ``None`` is all-time."""
    order = _RANK_ORDER.get(order_by, _RANK_ORDER["awards"])
    cols = "contracting_authority, n_awards, n_suppliers, awarded_value_safe_eur"
    params: list = []
    if year is None:
        sql = f"SELECT {cols} FROM v_procurement_authority_summary ORDER BY {order}"
    else:
        sql = f"SELECT {cols} FROM v_procurement_authority_year_summary WHERE year = ? ORDER BY {order}"
        params.append(int(year))
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def cpv_summary(
    conn: duckdb.DuckDBPyConnection, *, limit: int | None = 50, order_by: str = "awards", year: int | None = None
) -> QueryResult:
    """CPV categories ranked by number of awards (or sum-safe value).
    ``year`` scopes to one calendar year via the per-year view; ``None`` is all-time."""
    order = _RANK_ORDER.get(order_by, _RANK_ORDER["awards"])
    cols = (
        "cpv_code, cpv_description, n_awards, n_suppliers, awarded_value_safe_eur, "
        "n_awards_valued, median_award_eur, p25_award_eur, p75_award_eur"
    )
    params: list = []
    if year is None:
        sql = f"SELECT {cols} FROM v_procurement_cpv_summary ORDER BY {order}"
    else:
        sql = f"SELECT {cols} FROM v_procurement_cpv_year_summary WHERE year = ? ORDER BY {order}"
        params.append(int(year))
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


# Drill-down award lists for an authority / category show EVERY award class (so the
# row count matches the card's all-class total). The supplier_class / name_truncated
# flags ride along so the page can mask non-company / individual names (privacy) while
# still disclosing that the award happened. ``year`` optionally scopes the list.
def awards_for_authority(
    conn: duckdb.DuckDBPyConnection, contracting_authority: str, *, year: int | None = None
) -> QueryResult:
    """Every award made BY one contracting authority, newest first."""
    sql = (
        "SELECT tender_id, supplier, supplier_norm, supplier_class, name_truncated,"
        " cpv_code, cpv_description, tender_title, category_label, procedure_type,"
        " contract_duration_months, n_bids_received, ted_can_link, ted_notice_link, etenders_notice_url,"
        " competition_type, award_date, value_eur, value_kind, value_safe_to_sum"
        " FROM v_procurement_awards WHERE contracting_authority = ?"
    )
    params: list = [contracting_authority]
    if year is not None:
        sql += " AND EXTRACT(year FROM award_date) = ?"
        params.append(int(year))
    return _run(conn, sql + " ORDER BY award_date DESC NULLS LAST", params)


def awards_for_cpv(conn: duckdb.DuckDBPyConnection, cpv_code: str, *, year: int | None = None) -> QueryResult:
    """Every award in one CPV category, newest first."""
    sql = (
        "SELECT tender_id, supplier, supplier_norm, supplier_class, name_truncated,"
        " contracting_authority, cpv_description, tender_title, procedure_type,"
        " contract_duration_months, n_bids_received, ted_can_link, ted_notice_link, etenders_notice_url,"
        " competition_type, award_date, value_eur, value_kind, value_safe_to_sum"
        " FROM v_procurement_awards WHERE cpv_code = ?"
    )
    params: list = [cpv_code]
    if year is not None:
        sql += " AND EXTRACT(year FROM award_date) = ?"
        params.append(int(year))
    return _run(conn, sql + " ORDER BY award_date DESC NULLS LAST", params)
