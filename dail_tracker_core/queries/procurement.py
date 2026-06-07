"""Procurement (eTenders) retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/procurement_data.py`` (the exemplar
thin wrapper). Every function is retrieval-only SQL against the registered
``procurement_*`` views; all aggregation/joins/value-gating already live in the
views (see e.g. ``sql_views/procurement_supplier_summary.sql``). The SQL strings
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


def lobbying_overlap(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Companies on BOTH the procurement and lobbying registers (co-occurrence
    disclosure only — never causation; see the view header)."""
    return _run(
        conn,
        "SELECT lobby_name, lobby_side, supplier, supplier_norm, n_lobby_returns,"
        " n_award_rows, n_authorities, awarded_value_safe_eur"
        " FROM v_procurement_lobbying_overlap ORDER BY n_award_rows DESC",
    )
