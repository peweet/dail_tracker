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


def supplier_payments_by_year(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """One firm's public-body PAYMENTS RECEIVED per year — the supplier-side mirror of
    ``payments_by_year`` (which is per publisher). Paid (SPENT) and ordered (COMMITTED) come back
    as SEPARATE sum-safe columns so the consumer charts them on their own axes.

    ⚠️ Three never-cross rules ride on the column split: paid and ordered are different lifecycle
    stages (never added to each other, never stacked — that reads as a sum), and BOTH are a
    different grain from the award totals (realised/committed spend vs an award ceiling — never
    added to the awards trend either). Indicative floor only: amounts span mixed VAT bases.
    Sum-safe euro only (public-body transfers already excluded upstream). Oldest year first."""
    return _run(
        conn,
        "SELECT year,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'SPENT'), 0)"
        "   AS paid_safe_eur,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'COMMITTED'), 0)"
        "   AS ordered_safe_eur,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'SPENT')     AS n_paid_lines,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines"
        " FROM v_procurement_payments"
        " WHERE supplier_normalised = ? AND year IS NOT NULL"
        " GROUP BY year ORDER BY year",
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


def epa_compliance_for_supplier(conn: duckdb.DuckDBPyConnection, company_num: int) -> QueryResult:
    """One CRO company's EPA environmental-licence + enforcement record for the dossier panel,
    matched on CRO ``company_num``. Returns the single summary row, or empty if the firm holds no EPA
    licence. Licence portfolio + compliance counts only (a separate public register) — carries no money
    and is never summed with the firm's award/payment figures."""
    return _run(
        conn,
        "SELECT * FROM v_procurement_epa_compliance WHERE company_num = ?",
        [company_num],
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
        "SELECT * FROM v_procurement_payments_publisher_summary WHERE realisation_tier = ? ORDER BY total_safe_eur DESC"
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
        "SELECT * FROM v_procurement_payments_supplier_summary WHERE realisation_tier = ? ORDER BY total_safe_eur DESC"
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


def payments_by_year(conn: duckdb.DuckDBPyConnection, publisher_name: str, *, tier: str = "SPENT") -> QueryResult:
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


def council_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Publishing local authorities for the "Your council" index — one row per council,
    pre-grouped North->South by province (province_order) then by scale within province.

    Carries BOTH lifecycle totals as separate columns (ordered_safe_eur / paid_safe_eur)
    so the page renders one labelled pill per council WITHOUT pivoting or summing — paid
    (SPENT) and ordered (COMMITTED) are different stages and never added. The view already
    orders the rows; the page selects and renders, computing nothing."""
    return _run(conn, "SELECT * FROM v_procurement_council_summary")


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


def payments_supplier_header(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """Single-row header for a paid-supplier drill-down (the mirror of
    ``payments_publisher_profile``): the firm's published display name, both lifecycle
    tiers' sum-safe totals side by side (never summed), its distinct-publisher / line / year
    spans and its CRO match. Sum-safe euro only; mixed vat_status flagged so the page can
    label the totals indicative floors."""
    return _run(
        conn,
        "SELECT mode(supplier) AS supplier, supplier_normalised, mode(supplier_class) AS supplier_class,"
        " COUNT(DISTINCT publisher_name) AS n_publishers,"
        " MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'SPENT')     AS n_paid_lines,"
        " COUNT(*) FILTER (WHERE realisation_tier = 'COMMITTED') AS n_ordered_lines,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'SPENT'), 0)"
        "   AS paid_safe_eur,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum AND realisation_tier = 'COMMITTED'), 0)"
        "   AS ordered_safe_eur,"
        " (COUNT(DISTINCT vat_status) > 1) AS vat_mixed,"
        " mode(cro_company_num) AS cro_company_num, mode(cro_company_status) AS cro_company_status"
        " FROM v_procurement_payments WHERE supplier_normalised = ?"
        " GROUP BY supplier_normalised",
        [supplier_norm],
    )


def payments_publishers_for_supplier(
    conn: duckdb.DuckDBPyConnection, supplier_norm: str, *, tier: str = "SPENT", limit: int = 200
) -> QueryResult:
    """The public bodies that paid (SPENT) or ordered (COMMITTED) from one supplier — the
    drill-down line items behind a paid-supplier card, and the exact mirror of
    ``payments_for_publisher`` (which lists a body's suppliers). Sum-safe within each body;
    bodies named per their own published lists. One row per publisher, biggest first."""
    return _run(
        conn,
        "SELECT publisher_name, mode(publisher_type) AS publisher_type, mode(sector) AS sector,"
        " COUNT(*) AS n_payments, MIN(year)::INT AS min_year, MAX(year)::INT AS max_year,"
        " COALESCE(SUM(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur"
        " FROM v_procurement_payments WHERE supplier_normalised = ? AND realisation_tier = ?"
        " GROUP BY publisher_name ORDER BY total_safe_eur DESC LIMIT ?",
        [supplier_norm, _tier(tier), int(limit)],
    )


def payment_lines_for_pair(
    conn: duckdb.DuckDBPyConnection, supplier_norm: str, publisher_name: str, *, tier: str = "SPENT", limit: int = 500
) -> QueryResult:
    """The actual published payment LINE ITEMS for one (supplier × public body × tier) pair —
    the LEAF of the payments drill-down. The supplier↔body cards above are aggregates that
    link to each other; this is the terminus that finally shows the individual records a body
    published (period, description, PO number, amount), with a link to the body's own source
    file. One row per published line, biggest first. Sum-safe euro flag rides along so the page
    can mark a line that is not safe to total; never summed across vat_status."""
    return _run(
        conn,
        "SELECT period, year, description, po_number, amount_eur, value_kind,"
        " value_safe_to_sum, vat_status, source_file_url"
        " FROM v_procurement_payments"
        " WHERE supplier_normalised = ? AND publisher_name = ? AND realisation_tier = ?"
        " ORDER BY amount_eur DESC NULLS LAST LIMIT ?",
        [supplier_norm, publisher_name, _tier(tier), int(limit)],
    )


def entity_chain_for_company(conn: duckdb.DuckDBPyConnection, company_num: str) -> QueryResult:
    """One CRO-matched firm's cross-register footprint: which of the three public-money
    registers it appears in (eTenders awards, TED awards, public-body payments) and each
    register's own headline number, side by side. Hard CRO company-number match only.

    ⚠️ The money columns are DIFFERENT GRAINS (award ceilings vs realised payments) and the
    page MUST label them separately and NEVER sum them. Absence from a register is coverage,
    not missing money (only ~7% of State spend is in the payments corpus). Single-row select
    over the pre-joined view — the page renders, never computes."""
    return _run(
        conn,
        "SELECT company_num, display_name, in_etenders, in_ted, in_payments, n_registers,"
        " etenders_award_rows, etenders_n_authorities, etenders_awarded_value_safe_eur,"
        " ted_awards, ted_n_buyers, ted_value_safe_eur,"
        " payment_lines, payments_n_publishers, paid_safe_eur, committed_safe_eur"
        " FROM v_procurement_entity_chain WHERE company_num = ?",
        [company_num],
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
    NET COST first — the "where your money goes" breakdown. Net cost (gross minus the service's
    own income/grants) is what the local taxpayer actually funds, so it leads; gross + the
    pipeline-computed pct_self_funded ride alongside. Display passthrough of the reconcile-gated
    fact; gross is operating expenditure by division, never the council's headline total."""
    return _run(
        conn,
        "SELECT division, gross_expenditure_eur, income_eur, net_expenditure_eur,"
        " pct_self_funded, reconciled"
        " FROM v_procurement_afs_by_division WHERE council = ? AND year = ?"
        " ORDER BY net_expenditure_eur DESC",
        [council, int(year)],
    )


def afs_capital_by_year(conn: duckdb.DuckDBPyConnection, council: str) -> QueryResult:
    """One council's audited CAPITAL-account investment per year — the "what your council is
    building / acquiring" spine of the dossier's BUILDING lane. capital_expenditure_eur is Σ
    investment by service that year (a distinct fact: never summed with revenue net cost, PO/
    payment or award euros). Pre-aggregated in the view; the page renders, never computes."""
    return _run(
        conn,
        "SELECT year, capital_expenditure_eur, capital_income_eur, n_divisions, reconciled, parser"
        " FROM v_procurement_afs_capital_by_year WHERE council = ? ORDER BY year",
        [council],
    )


def afs_capital_by_division(conn: duckdb.DuckDBPyConnection, council: str, year: int) -> QueryResult:
    """One council-year's CAPITAL investment by service division (largest first) — the build/
    acquire programme, dominated by central housing grants. Display passthrough of the reconcile-
    gated fact; a distinct grain, never summed with the revenue or PO/award euros."""
    return _run(
        conn,
        "SELECT division, capital_expenditure_eur, capital_income_eur, reconciled"
        " FROM v_procurement_afs_capital_by_division WHERE council = ? AND year = ?"
        " ORDER BY capital_expenditure_eur DESC",
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


# ── Entity search + derived-signal views (doc/PROCUREMENT_NUGGETS.md, 2026-06-11) ────────
# All factual structure signals — the consuming page must keep the no-inference posture
# (every view header states its caveat family). Pre-aggregated in the views; retrieval only.
def entity_search(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The unified typeahead corpus (suppliers + authorities + CPV categories in one list)
    for the page's search-first hero. The page applies a DISPLAY-ONLY name filter over this
    frame (same pattern as the supplier search). The two money hint columns are different
    grains (award ceilings vs realised payments) — shown with their own labels, never added."""
    return _run(
        conn,
        "SELECT entity_kind, display_name, url_key, n_records, n_counterparties,"
        " awarded_value_safe_eur, paid_safe_eur, cro_company_num, on_lobbying_register"
        " FROM v_procurement_entity_search",
    )


def supplier_single_bid(conn: duckdb.DuckDBPyConnection, join_norm: str) -> QueryResult:
    """One firm's lot-level single-bid context (TED 2024+, sole-winner notices only) for the
    competition block on a company dossier. Factual signal, never a verdict — the page must
    carry the 'often wholly legitimate' caveat. Matched on the TED winner join norm (the same
    key as ted_for_supplier)."""
    return _run(
        conn,
        "SELECT * FROM v_procurement_supplier_single_bid WHERE winner_join_norm = ?",
        [join_norm],
    )


def single_bid_baseline(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """The national lot-level single-bid baseline (one row) the per-firm/per-market rates are
    read against. Computed over the by-CPV view so the denominator matches the market table."""
    return _run(
        conn,
        "SELECT COALESCE(SUM(n_lots_with_bidcount), 0) AS n_lots_with_bidcount,"
        " COALESCE(SUM(n_single_bid_lots), 0) AS n_single_bid_lots,"
        " ROUND(100.0 * SUM(n_single_bid_lots) / NULLIF(SUM(n_lots_with_bidcount), 0), 1)"
        "   AS single_bid_lot_pct"
        " FROM v_procurement_competition_by_cpv",
    )


def competition_by_cpv(conn: duckdb.DuckDBPyConnection, *, min_lots: int = 100) -> QueryResult:
    """Lot-level single-bid rate per CPV division (market), highest first — the market-spread
    panel. ``min_lots`` drops small noisy markets."""
    return _run(
        conn,
        "SELECT cpv_division, n_notices, n_lots_with_bidcount, n_single_bid_lots,"
        " single_bid_lot_pct, n_uncompetitive_notices, n_buyers, first_year, last_year"
        " FROM v_procurement_competition_by_cpv WHERE n_lots_with_bidcount >= ?"
        " ORDER BY single_bid_lot_pct DESC NULLS LAST",
        [int(min_lots)],
    )


def single_bid_notices_for_cpv(
    conn: duckdb.DuckDBPyConnection, cpv_division: str, *, limit: int = 80
) -> QueryResult:
    """The individual single-bid award NOTICES within one CPV division (market) — the drill-down
    behind a single-bid market card. Each row opens the authoritative EU Official Journal notice
    (notice_url). Restricted to source_lane='api' (the single-bid field only exists for 2024+
    eForms, matching the market panel's denominator) and pan-EU outliers excluded. DISTINCT by
    publication number so a notice counts once. A single bid is a recorded fact, often wholly
    legitimate — the page carries that caveat; this query only surfaces the notices, never a verdict."""
    return _run(
        conn,
        "SELECT DISTINCT publication_number, notice_url, buyer_name, winner_name,"
        " dispatch_date, year, n_tenders_received, value_kind, cpv_code"
        " FROM v_procurement_ted_winner_history"
        " WHERE cpv_division = ? AND is_single_bid AND NOT is_pan_eu_outlier AND source_lane = 'api'"
        " ORDER BY dispatch_date DESC NULLS LAST"
        " LIMIT ?",
        [cpv_division, int(limit)],
    )


def new_entrants_by_year(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Share of eTenders awards going to first-time suppliers, per year. Years before 2016
    are flagged is_left_censored (the corpus starts 2013, so 'first award' is first IN THE
    CORPUS) — the page must de-emphasise or exclude them."""
    return _run(
        conn,
        "SELECT year, n_awards, n_suppliers, n_new_suppliers, n_awards_to_new,"
        " pct_awards_to_new_entrants, is_left_censored"
        " FROM v_procurement_new_entrants ORDER BY year",
    )


def incumbency_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str, *, limit: int = 8) -> QueryResult:
    """One firm's repeat-winner relationships (its top buyers by award count, with the
    distinct-years span) for the dossier's relationships block. Central-purchasing rows are
    flagged so the page can badge framework mechanics vs bilateral relationships."""
    return _run(
        conn,
        "SELECT contracting_authority, authority_is_central_purchasing, n_awards,"
        " n_distinct_years, first_year, last_year, awarded_value_safe_eur"
        " FROM v_procurement_incumbency WHERE supplier_norm = ?"
        " ORDER BY n_awards DESC LIMIT ?",
        [supplier_norm, int(limit)],
    )


def incumbency_top(conn: duckdb.DuckDBPyConnection, *, min_years: int = 6, limit: int = 24) -> QueryResult:
    """The longest-running supplier×buyer award relationships (≥min_years distinct years),
    for the patterns panel. A long streak is a structure fact, not a finding — the page
    carries the caveat and the central-purchasing badge."""
    return _run(
        conn,
        "SELECT supplier, supplier_norm, contracting_authority, authority_is_central_purchasing,"
        " n_awards, n_distinct_years, first_year, last_year"
        " FROM v_procurement_incumbency WHERE n_distinct_years >= ?"
        " ORDER BY n_distinct_years DESC, n_awards DESC LIMIT ?",
        [int(min_years), int(limit)],
    )


def dependency_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str) -> QueryResult:
    """One firm's top-buyer dependency share (only present when the firm has ≥5 awards) for
    the dossier's relationships block. 'X won N of its M awards from Y' — a structure fact,
    no concentration-risk language."""
    return _run(
        conn,
        "SELECT top_authority, top_authority_is_central_purchasing, awards_from_top_authority,"
        " total_awards, n_authorities, top_authority_share_pct"
        " FROM v_procurement_supplier_dependency WHERE supplier_norm = ?",
        [supplier_norm],
    )


def dependency_top(
    conn: duckdb.DuckDBPyConnection, *, min_awards: int = 10, min_share_pct: float = 80.0, limit: int = 24
) -> QueryResult:
    """Suppliers winning ≥min_share_pct of their ≥min_awards awards from one buyer, for the
    patterns panel. Central-purchasing rows excluded (an OGP top-buyer is framework
    mechanics, not a bilateral relationship — the panel would otherwise be all OGP)."""
    return _run(
        conn,
        "SELECT supplier, supplier_norm, top_authority, awards_from_top_authority,"
        " total_awards, n_authorities, top_authority_share_pct"
        " FROM v_procurement_supplier_dependency"
        " WHERE total_awards >= ? AND top_authority_share_pct >= ?"
        "   AND NOT top_authority_is_central_purchasing"
        " ORDER BY top_authority_share_pct DESC, total_awards DESC LIMIT ?",
        [int(min_awards), float(min_share_pct), int(limit)],
    )


def quarter_profile_top(conn: duckdb.DuckDBPyConnection, *, quarter: int = 4, limit: int = 12) -> QueryResult:
    """Publishers most skewed toward one quarter (default Q4 — the year-end ordering spike),
    by share of their own COMMITTED lines. Seasonality is a shape fact, never a motive."""
    return _run(
        conn,
        "SELECT publisher_name, quarter, n_lines, committed_safe_eur, pct_of_publisher_lines"
        " FROM v_procurement_quarter_profile WHERE quarter = ?"
        " ORDER BY pct_of_publisher_lines DESC LIMIT ?",
        [int(quarter), int(limit)],
    )


def quarter_totals(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Corpus-wide COMMITTED line counts + sum-safe euros per quarter (the Q4-spike headline).
    Aggregated over the per-publisher view so the denominators match the skew panel."""
    return _run(
        conn,
        "SELECT quarter, SUM(n_lines) AS n_lines,"
        " COALESCE(SUM(committed_safe_eur), 0) AS committed_safe_eur"
        " FROM v_procurement_quarter_profile GROUP BY quarter ORDER BY quarter",
    )


def sector_breadth_top(conn: duckdb.DuckDBPyConnection, *, min_sectors: int = 5, limit: int = 12) -> QueryResult:
    """Paid suppliers reaching the most public-service SECTORS, for the patterns panel.
    Name-collision guard: a short generic normalised name aggregating many distinct raw
    names (e.g. 'ELECTRIC') is many firms, not one — those buckets are excluded here
    (≥3 raw variants behind a single-token name), and n_raw_variants rides along so the
    page can disclose the grouping basis."""
    return _run(
        conn,
        "SELECT supplier_normalised, n_raw_variants, n_sectors, n_publishers, n_lines,"
        " paid_safe_eur, committed_safe_eur, vat_mixed, sectors, first_year, last_year"
        " FROM v_procurement_supplier_sector_breadth"
        " WHERE n_sectors >= ?"
        "   AND NOT (n_raw_variants >= 3 AND NOT contains(trim(supplier_normalised), ' '))"
        " ORDER BY n_sectors DESC, paid_safe_eur DESC NULLS LAST LIMIT ?",
        [int(min_sectors), int(limit)],
    )


def call_offs_for_supplier(conn: duckdb.DuckDBPyConnection, supplier_norm: str, *, limit: int = 25) -> QueryResult:
    """One firm's call-off awards (drawdowns under a framework/DPS) with the parent
    agreement resolved where it exists in the corpus — the framework nesting, made visible.
    An unresolved parent is itself a transparency fact ('parent not in the published
    corpus'), never hidden. The parent ceiling is context, never added to the call-off."""
    return _run(
        conn,
        "SELECT tender_id, contracting_authority, award_date, value_eur, value_kind,"
        " value_safe_to_sum, parent_agreement_id, parent_in_corpus, parent_authority,"
        " parent_value_eur, parent_value_kind, parent_n_suppliers"
        " FROM v_procurement_call_off_links WHERE supplier_norm = ?"
        " ORDER BY award_date DESC NULLS LAST LIMIT ?",
        [supplier_norm, int(limit)],
    )
