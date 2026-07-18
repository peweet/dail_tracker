"""Public-body payments retrieval — Streamlit-free.

Retrieval-only SQL against the registered ``public_payments`` views (defined in
``sql_views/procurement/procurement_public_payments.sql``). The privacy gate (public_display)
and the value-gating (value_safe_to_sum, excluding intergovernmental transfers)
already live in those views — this layer only reads. DuckDB failures surface as
``unavailable`` QueryResults rather than silent empty frames.

Two registers are unioned behind ``v_public_payments``: the generic public-body
fact and the HSE/Tusla fact. Rankings come from the rollup views
(``v_public_payments_publisher_summary`` / ``v_public_payments_supplier_summary``);
single-row corpus stats + drill-downs read the base view directly.

Two registers note above is historical: the HSE/Tusla fact is already concatenated
into procurement_payments_fact.parquet by the consolidate step, so v_public_payments
reads that single gold fact.

Build a connection with
``dail_tracker_core.db.connect_with_views(["procurement_public_payments.sql"])``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import make_runner
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)

# Display-ordering options exposed to the page. The page passes one of these keys;
# the safe ORDER BY fragment is chosen here so a raw string can never reach SQL.
# "value" (sum-safe spend) is the headline; "lines" (record count) is the neutral
# default that does not privilege the big-ticket bodies.
_PUBLISHER_ORDER = {
    "value": "total_safe_eur DESC NULLS LAST, n_lines DESC",
    "lines": "n_lines DESC",
}
_SUPPLIER_ORDER = {
    "value": "total_safe_eur DESC NULLS LAST, n_lines DESC",
    "lines": "n_lines DESC",
}
_LINE_ORDER = {
    "value": "amount_eur DESC NULLS LAST",
    # Group a drill-down by period, newest first, with the bigger figures leading inside
    # each quarter — far easier to read than a value-only sort that interleaves quarters.
    "recent": "year DESC NULLS LAST, quarter DESC NULLS LAST, amount_eur DESC NULLS LAST",
}


_run = make_runner("public_payments", _log)


def coverage_stats(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Single-aggregate-row corpus summary for the hero scale-anchor AND the page's
    source-state gate (if unavailable, the page renders the source-down state). Counts
    the served slice (public_display already applied by the view); sum is sum-safe only
    so it is never an over-stated 'spend'. No GROUP BY — one row, not a rollup."""
    return _run(
        conn,
        "SELECT"
        "  count(*)                                         AS n_lines,"
        "  count(*) FILTER (WHERE value_safe_to_sum)        AS n_safe_lines,"
        "  count(DISTINCT publisher_id)                     AS n_publishers,"
        "  count(DISTINCT supplier_normalised)              AS n_suppliers,"
        "  sum(amount_eur) FILTER (WHERE value_safe_to_sum) AS total_safe_eur,"
        "  min(year)                                        AS first_year,"
        "  max(year)                                        AS last_year"
        " FROM v_public_payments",
    )


def available_years(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Distinct payment years present, newest first — the page's year-pill options."""
    return _run(
        conn,
        "SELECT DISTINCT year FROM v_public_payments WHERE year IS NOT NULL ORDER BY year DESC",
    )


def publisher_summary(
    conn: duckdb.DuckDBPyConnection, *, order_by: str = "value", limit: int | None = None
) -> QueryResult:
    """Per-publisher ranking (one row per publisher × amount_semantics). ``order_by`` is
    ``"value"`` (sum-safe spend) or ``"lines"`` (record count)."""
    order = _PUBLISHER_ORDER.get(order_by, _PUBLISHER_ORDER["value"])
    sql = f"SELECT * FROM v_public_payments_publisher_summary ORDER BY {order}"
    params: list = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def supplier_summary(
    conn: duckdb.DuckDBPyConnection, *, order_by: str = "value", limit: int | None = None
) -> QueryResult:
    """Per-supplier ranking, entity-level across publishers. Personal suppliers are already
    excluded by the base view's public_display gate."""
    order = _SUPPLIER_ORDER.get(order_by, _SUPPLIER_ORDER["value"])
    sql = f"SELECT * FROM v_public_payments_supplier_summary ORDER BY {order}"
    params: list = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


_LINE_COLS = (
    "publisher_id, publisher_name, sector, supplier, supplier_class,"
    " amount_eur, amount_semantics, value_safe_to_sum, description, year, quarter,"
    " period, source_file_url, extraction_confidence"
)


def publisher_lines(
    conn: duckdb.DuckDBPyConnection,
    publisher_id: str,
    *,
    year: int | None = None,
    order_by: str = "value",
    limit: int | None = None,
) -> QueryResult:
    """Payment/PO lines for one publisher (drill-down). Optional calendar-year scope."""
    order = _LINE_ORDER.get(order_by, _LINE_ORDER["value"])
    params: list = [publisher_id]
    sql = f"SELECT {_LINE_COLS} FROM v_public_payments WHERE publisher_id = ?"
    if year is not None:
        sql += " AND year = ?"
        params.append(int(year))
    sql += f" ORDER BY {order}"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def supplier_lines(
    conn: duckdb.DuckDBPyConnection,
    supplier_normalised: str,
    *,
    order_by: str = "value",
    limit: int | None = None,
) -> QueryResult:
    """Every line for one supplier across all publishers (drill-down), keyed on the
    normalised name (the join key the supplier ranking exposes)."""
    order = _LINE_ORDER.get(order_by, _LINE_ORDER["value"])
    params: list = [supplier_normalised]
    sql = f"SELECT {_LINE_COLS} FROM v_public_payments WHERE supplier_normalised = ? ORDER BY {order}"
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


# ──────────────────────────────────────────────────────────────────────────────
# "What the money buys" — category lens (doc/PAYMENTS_CATEGORY_LENS_DESIGN.md).
# All three views are pre-aggregated per (spend_category[, publisher|supplier],
# realisation_tier); this layer only reads + orders. One row per tier, NEVER blended.
# ──────────────────────────────────────────────────────────────────────────────
_CATEGORY_ORDER = {
    "value": "total_safe_eur DESC NULLS LAST, n_lines DESC",
    "lines": "n_lines DESC",
}

_CATEGORY_COLS = (
    "spend_category, realisation_tier, n_lines, n_bodies, n_suppliers, first_year, last_year, total_safe_eur"
)


def categories(conn: duckdb.DuckDBPyConnection, *, order_by: str = "value") -> QueryResult:
    """The spend-category overview: one row per (category × realisation_tier), with the
    sum-safe total and counts. ``realisation_tier`` is SPENT ('paid') or COMMITTED ('ordered')
    — the page renders one tier at a time and never blends them. Pre-aggregated in
    ``v_payments_by_category``; this only orders."""
    order = _CATEGORY_ORDER.get(order_by, _CATEGORY_ORDER["value"])
    return _run(conn, f"SELECT {_CATEGORY_COLS} FROM v_payments_by_category ORDER BY {order}")


def category_suppliers(
    conn: duckdb.DuckDBPyConnection, spend_category: str, *, limit: int | None = None
) -> QueryResult:
    """The named vendors paid/ordered within one spend category (the drill). ``cro_company_num``
    is surfaced for the optional Company-dossier link, NOT operator-merged — 'Mosney' and
    'Mosney Holidays' stay distinct. One row per (supplier × tier)."""
    sql = (
        "SELECT spend_category, supplier, supplier_normalised, cro_company_num, realisation_tier,"
        " n_lines, n_bodies, first_year, last_year, total_safe_eur"
        " FROM v_payments_category_suppliers WHERE spend_category = ?"
        " ORDER BY total_safe_eur DESC NULLS LAST, n_lines DESC"
    )
    params: list = [spend_category]
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def category_publishers(conn: duckdb.DuckDBPyConnection, spend_category: str) -> QueryResult:
    """The bodies whose published spend drives one category (the attribution block). One row
    per (publisher × tier), sum-safe value DESC."""
    return _run(
        conn,
        "SELECT publisher_id, publisher_name, publisher_type, spend_category, realisation_tier,"
        " n_lines, n_suppliers, first_year, last_year, total_safe_eur"
        " FROM v_payments_by_category_publisher WHERE spend_category = ?"
        " ORDER BY total_safe_eur DESC NULLS LAST, n_lines DESC",
        [spend_category],
    )


def supplier_quarter_totals(
    conn: duckdb.DuckDBPyConnection,
    supplier_normalised: str,
    *,
    limit: int | None = None,
) -> QueryResult:
    """Per-quarter rollup for one supplier's drill-down: one row per (year, quarter)
    with the sum-safe subtotal and line count, newest quarter first. The GROUP BY lives
    here so the page can render quarter sections without aggregating (logic firewall).
    period is functionally determined by (year, quarter) so it groups 1:1 and is carried
    through as the display label / join key the page filters its line list on."""
    params: list = [supplier_normalised]
    sql = (
        "SELECT year, quarter, period,"
        "  count(*)                                          AS n_lines,"
        "  coalesce(sum(amount_eur) FILTER (WHERE value_safe_to_sum), 0) AS total_safe_eur"
        " FROM v_public_payments WHERE supplier_normalised = ?"
        " GROUP BY year, quarter, period"
        " ORDER BY year DESC NULLS LAST, quarter DESC NULLS LAST"
    )
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)
