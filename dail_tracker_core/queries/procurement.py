"""Procurement (eTenders) retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/procurement_data.py`` (the exemplar
thin wrapper). Every function is retrieval-only SQL against the registered
``procurement_*`` views; all aggregation/joins/value-gating already live in the
views (see e.g. ``sql_views/procurement/procurement_supplier_summary.sql``). The SQL strings
are byte-for-byte the same as the old wrapper so output is unchanged — the only
difference is the return type (``QueryResult`` instead of a bare DataFrame, with
DuckDB failures surfaced as ``unavailable`` instead of a silent empty frame).

Build a connection with ``dail_tracker_core.db.connect_with_views(["procurement_*.sql"])``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

# Display-ordering options exposed to the page. The page never builds SQL — it
# passes one of these keys and the safe ORDER BY fragment is chosen here, so a
# raw string can never reach the query. "awards" is the trustworthy default
# (counts); "value" surfaces the money leaders (sum-safe awarded value only,
# ties broken by award count).
_SUPPLIER_ORDER = {
    "awards": "n_awards DESC",
    "value": "awarded_value_safe_eur DESC, n_awards DESC",
}
_RANK_ORDER = {  # authority + cpv summaries share the same column shape
    "awards": "n_awards DESC",
    "value": "awarded_value_safe_eur DESC, n_awards DESC",
}
_COMPETITION_ORDER = {  # buyer competition ranking
    "single_bid": "single_bid_lot_pct DESC NULLS LAST, n_lots_with_bidcount DESC",
    "lots": "n_lots_with_bidcount DESC",
}


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    """Execute retrieval SQL and wrap the outcome.

    A DuckDB error (missing view, missing parquet, bad column) becomes an
    ``unavailable`` result rather than the old swallow-to-empty-DataFrame, so the
    caller can tell "source down" from "no rows". The exception is logged for the
    server-side trail exactly as the old ``_safe`` did.
    """
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.exception("procurement query failed")
        return QueryResult.unavailable(f"procurement query failed: {exc}")


_SUPPLIER_COLS = (
    "supplier, supplier_norm, n_awards, n_authorities, awarded_value_safe_eur,"
    " n_value_safe_awards, n_ceiling_notices,"
    " company_num, company_status, cro_match_method,"
    " on_lobbying_register, lobbying_returns, is_lobbying_registrant, is_lobbying_client"
)


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
        sql = f"SELECT {_SUPPLIER_COLS} FROM v_procurement_supplier_summary ORDER BY {order}"
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
        f"  {int(top_n)} AS top_n,"
        "  COUNT(*) AS n_suppliers,"
        "  MAX(total_awards) AS total_awards,"
        f"  COALESCE(SUM(n_awards) FILTER (WHERE rn <= {int(top_n)}), 0) AS top_n_awards,"
        f"  ROUND(100.0 * COALESCE(SUM(n_awards) FILTER (WHERE rn <= {int(top_n)}), 0)"
        "        / NULLIF(MAX(total_awards), 0), 1) AS top_n_share_pct"
        " FROM ranked",
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


def awards_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """Every award row for one supplier (detail view), most recent first."""
    return _run(
        conn,
        "SELECT tender_id, contracting_authority, cpv_code, cpv_description,"
        " competition_type, award_date, value_eur, value_kind, value_safe_to_sum"
        " FROM v_procurement_awards WHERE supplier_norm = ?"
        " ORDER BY award_date DESC NULLS LAST",
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
    cols = "cpv_code, cpv_description, n_awards, n_suppliers, awarded_value_safe_eur"
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
        " cpv_code, cpv_description, competition_type, award_date, value_eur, value_kind, value_safe_to_sum"
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
        " contracting_authority, cpv_description, competition_type, award_date,"
        " value_eur, value_kind, value_safe_to_sum"
        " FROM v_procurement_awards WHERE cpv_code = ?"
    )
    params: list = [cpv_code]
    if year is not None:
        sql += " AND EXTRACT(year FROM award_date) = ?"
        params.append(int(year))
    return _run(conn, sql + " ORDER BY award_date DESC NULLS LAST", params)


# ── TED (EU Official Journal award notices) — a SEPARATE award register ───────────
# Award grain, never summed with eTenders. pan-EU outliers (GÉANT-type frameworks) are
# excluded from value totals by default; the page's toggle re-includes them.
_TED_ORDER = {
    "awards": "n_awards DESC",
    "value": "ted_value_safe_eur DESC, n_awards DESC",
}


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
    framework ceiling and the award criteria live — detail the thin gold slice omits). One row
    per notice, newest first. Award notices, never summed."""
    return _run(
        conn,
        "SELECT publication_number, buyer_name, dispatch_date, value_kind,"
        " is_multi_supplier_framework, n_winners, notice_url"
        " FROM v_procurement_ted_winner_history WHERE winner_join_norm = ?"
        " ORDER BY dispatch_date DESC NULLS LAST",
        [join_norm],
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


def ted_tenders(conn: duckdb.DuckDBPyConnection, *, limit: int | None = 60, only_open: bool = False) -> QueryResult:
    """The tender-pipeline listing (most recent first). ``only_open`` keeps notices whose
    submission deadline has not yet passed. estimated_value_eur is a pre-award estimate shown
    for context — never summed with award/payment figures."""
    sql = (
        "SELECT publication_number, notice_url, buyer_name, cpv_code, cpv_division, procedure_type,"
        " is_uncompetitive_procedure, submission_deadline, is_still_open, estimated_value_eur, currency,"
        " dispatch_date, year"
        " FROM v_procurement_ted_tenders"
    )
    if only_open:
        sql += " WHERE is_still_open"
    sql += " ORDER BY dispatch_date DESC"
    params: list = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


# ── Public-body PAYMENTS (the SPENT / COMMITTED tiers) — a DIFFERENT grain from awards ──
# Never summed with eTenders/TED. One lifecycle tier at a time; only value_safe_to_sum sums,
# never across vat_status. Suppliers named per published source (see the view headers).
_PAYMENT_TIERS = {"SPENT": "SPENT", "COMMITTED": "COMMITTED"}  # whitelist (no raw string in SQL)


def _tier(tier: str) -> str:
    return _PAYMENT_TIERS.get((tier or "").upper(), "SPENT")


def payments_corpus_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """One-row summary of the public-body payment corpus for the section headline + source-state
    gate: distinct publishers/suppliers, year span, and the sum-safe total for EACH tier shown
    separately (paid vs ordered are never added). Totals span mixed vat_status, so the page must
    label them indicative floors, not audited totals."""
    return _run(
        conn,
        "SELECT"
        "  COUNT(*) AS n_payments,"
        "  COUNT(DISTINCT publisher_name) AS n_publishers,"
        "  COUNT(DISTINCT supplier_normalised) AS n_suppliers,"
        "  MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        "  COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier='SPENT'), 0)"
        "    AS spent_safe_eur,"
        "  COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier='COMMITTED'), 0)"
        "    AS committed_safe_eur"
        " FROM v_procurement_payments",
    )


def payments_publisher_summary(
    conn: duckdb.DuckDBPyConnection, *, tier: str = "SPENT", limit: int | None = 60
) -> QueryResult:
    """Public bodies ranked by sum-safe amount for one lifecycle tier (paid / ordered)."""
    sql = (
        "SELECT * FROM v_procurement_payments_publisher_summary"
        " WHERE realisation_tier = ? ORDER BY total_safe_eur DESC"
    )
    params: list = [_tier(tier)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def payments_supplier_summary(
    conn: duckdb.DuckDBPyConnection, *, tier: str = "SPENT", limit: int | None = 60
) -> QueryResult:
    """Suppliers ranked by sum-safe amount the State paid (SPENT) or ordered (COMMITTED)."""
    sql = (
        "SELECT * FROM v_procurement_payments_supplier_summary"
        " WHERE realisation_tier = ? ORDER BY total_safe_eur DESC"
    )
    params: list = [_tier(tier)]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def payments_for_publisher(
    conn: duckdb.DuckDBPyConnection, publisher_name: str, *, tier: str = "SPENT", limit: int = 200
) -> QueryResult:
    """Top suppliers paid/ordered by one public body (drill-down), sum-safe within that body."""
    return _run(
        conn,
        "SELECT mode(supplier) AS supplier, supplier_normalised, supplier_class,"
        " COUNT(*) AS n_payments, MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur,"
        " mode(cro_company_num) AS cro_company_num"
        " FROM v_procurement_payments WHERE publisher_name = ? AND realisation_tier = ?"
        " GROUP BY supplier_normalised, supplier_class"
        " ORDER BY total_safe_eur DESC LIMIT ?",
        [publisher_name, _tier(tier), int(limit)],
    )


def payments_by_year(
    conn: duckdb.DuckDBPyConnection, publisher_name: str, *, tier: str = "SPENT"
) -> QueryResult:
    """One public body's sum-safe spend per calendar year, for ONE lifecycle tier (the body
    dossier's spend-over-time spine — now meaningful with the 2016–2026 council backfill).
    One tier only by design: ordered and paid are never charted on one stacked axis (that would
    read as a sum). Counts + euros pre-aggregated here; the page renders, never computes."""
    return _run(
        conn,
        "SELECT year, COUNT(*) AS n_payments,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur"
        " FROM v_procurement_payments"
        " WHERE publisher_name = ? AND realisation_tier = ? AND year IS NOT NULL"
        " GROUP BY year ORDER BY year",
        [publisher_name, _tier(tier)],
    )


def payments_publisher_profile(conn: duckdb.DuckDBPyConnection, publisher_name: str) -> QueryResult:
    """Single-row buyer dossier header for one public body (the per-council profile anchor).

    Carries BOTH lifecycle tiers side by side so the page can show "€X ordered" and "€Y paid"
    without ever summing them (they are different stages of public money). Also returns
    publisher_type so the page can badge a local authority, and the supplier/line/year spans.
    Sum-safe euro only (public_body transfers already excluded upstream by value_safe_to_sum)."""
    return _run(
        conn,
        "SELECT mode(publisher_name) AS publisher_name, mode(publisher_type) AS publisher_type,"
        " mode(sector) AS sector,"
        " COUNT(DISTINCT supplier_normalised) AS n_suppliers,"
        " MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'SPENT')     AS n_paid_lines,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'SPENT'), 0)"
        "   AS paid_safe_eur,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'COMMITTED'), 0)"
        "   AS ordered_safe_eur"
        " FROM v_procurement_payments WHERE publisher_name = ?",
        [publisher_name],
    )


def payments_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """One firm's public-body payment footprint for the cross-reference on an eTenders supplier
    profile: paid (SPENT) and ordered (COMMITTED) totals + publisher count. Indicative floor
    (mixed vat_status); never summed with the firm's award totals."""
    return _run(
        conn,
        "SELECT realisation_tier, COUNT(*) AS n_payments,"
        " COUNT(DISTINCT publisher_name) AS n_publishers,"
        " MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " (COUNT(DISTINCT vat_status) > 1) AS vat_mixed,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur"
        " FROM v_procurement_payments WHERE supplier_normalised = ?"
        " GROUP BY realisation_tier ORDER BY total_safe_eur DESC",
        [supplier_norm],
    )


# ── AFS (per-LA audited Annual Financial Statement) — the BUDGET/accounts grain ──────────
# A SIBLING context fact for the local-authority dossier: the council's total audited revenue
# spend by service division (the denominator the named-supplier PO/payment slice sits inside).
# NEVER summed or unioned with PO/payment or award euros — different grain (see the view headers).
def afs_total_by_year(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's audited REVENUE-account spend per year (2016–2025 where filed) — the
    "council accounts, all spending" spine of the local-authority dossier. gross_expenditure_eur
    is Σ operating expenditure by service (a budget actual, never the PO/award grain). Pre-
    aggregated in the view; the page renders, never computes."""
    return _run(
        conn,
        "SELECT year, gross_expenditure_eur, income_eur, net_expenditure_eur,"
        " n_divisions, printed_total_eur, reconciled, parser"
        " FROM v_procurement_afs_total_by_year WHERE council = ? ORDER BY year",
        [council],
    )


def afs_by_division(conn: duckdb.DuckDBPyConnection, council: str, year: int) -> QueryResult:
    """One council-year's revenue spending by service division (Housing / Roads / …), largest
    gross first — the by-function breakdown panel. Display passthrough of the reconcile-gated
    fact; gross is operating expenditure by division, never the council's headline total."""
    return _run(
        conn,
        "SELECT division, gross_expenditure_eur, income_eur, net_expenditure_eur, reconciled"
        " FROM v_procurement_afs_by_division WHERE council = ? AND year = ?"
        " ORDER BY gross_expenditure_eur DESC",
        [council, int(year)],
    )


def afs_vs_po_coverage(conn: duckdb.DuckDBPyConnection, council: str, *, year: int | None = None) -> QueryResult:
    """Audited revenue spend (AFS) vs the slice traceable to named >€20k suppliers (POs), per
    year. Carries both tiers' PO totals and both pct_* ratios (the page reads the tier the
    council publishes). INDICATIVE ratio only — different thresholds/stages/grain, not a
    reconciliation (see the view header). ``year=None`` returns every year for the council."""
    sql = (
        "SELECT year, afs_gross_eur, afs_net_eur, po_spent_safe_eur, po_committed_safe_eur,"
        " n_spent_lines, n_committed_lines, n_named_suppliers, pct_spent_of_gross, pct_committed_of_gross"
        " FROM v_procurement_afs_vs_po_coverage WHERE council = ?"
    )
    params: list = [council]
    if year is not None:
        sql += " AND year = ?"
        params.append(int(year))
    return _run(conn, sql + " ORDER BY year", params)


def lobbying_overlap(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Companies on BOTH the procurement and lobbying registers (co-occurrence
    disclosure only — never causation; see the view header)."""
    return _run(
        conn,
        "SELECT lobby_name, lobby_side, supplier, supplier_norm, n_lobby_returns,"
        " n_award_rows, n_authorities, awarded_value_safe_eur"
        " FROM v_procurement_lobbying_overlap ORDER BY n_award_rows DESC",
    )


def charity_overlap(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Registered charities that ALSO appear on the procurement award register,
    linked by a shared CRO company number (a hard identifier — the charity's
    declared cro_number equals the supplier's CRO match). Co-occurrence disclosure
    only: the same legal entity is on both registers — NOT a claim about either.
    gov_funded_share_latest (0–1) is the charity's own latest-return figure, shown
    as context. awarded_value_safe_eur is already the money-grain-safe sum (ceiling
    notices excluded) inside the view — display only, never re-aggregated here."""
    return _run(
        conn,
        "SELECT rcn, registered_charity_name, company_num, company_status,"
        " charity_classification, state_adjacent_flag, funding_profile,"
        " gov_funded_share_latest, gross_income_latest_eur,"
        " supplier_norm, matched_supplier_name, n_awards, n_authorities,"
        " awarded_value_safe_eur, n_value_safe_awards, n_ceiling_notices"
        " FROM v_procurement_charity_overlap ORDER BY awarded_value_safe_eur DESC, n_awards DESC",
    )
