"""Payments retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/payments_data.py``. Every function
returns a ``QueryResult`` wrapping the raw retrieval frame. The Series/dict
shaping the page consumes (summary row, filter-option lists, the since-2020
summary dict) stays in the thin Streamlit wrapper — it is UI-adjacent formatting,
and keeping it there lets core stay uniformly QueryResult-of-DataFrame.

Several functions take an optional ``unique_member_code``: when supplied it is the
post-enrichment join key (preferred); otherwise the legacy ``member_name`` string
match is used (the stand-alone /rankings-payments picker populates from the
parquet's native "Last, First" format). The branch is retrieval plumbing (which
WHERE clause), so it lives here.

Build with ``connect_with_views(["payments_*.sql"], swallow_errors=False)`` —
payments registers loud (a missing view is a real break).
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    try:
        return QueryResult.success(conn.execute(sql, params or []).df())
    except Exception as exc:  # noqa: BLE001 — any DuckDB failure is "source unavailable"
        _log.exception("payments query failed")
        return QueryResult.unavailable(f"payments query failed: {exc}")


def summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    return _run(
        conn,
        "SELECT members_count, payment_count, total_paid,"
        " first_payment_date, last_payment_date, first_year, last_year,"
        " source_summary, latest_fetch_timestamp_utc, mart_version, code_version"
        " FROM v_payments_summary LIMIT 1",
    )


def member_options(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT member_name FROM v_payments_member_detail WHERE house = ? ORDER BY member_name LIMIT 2000",
        [house],
    )


def year_options(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT DISTINCT payment_year FROM v_payments_yearly_evolution"
        " WHERE house = ? ORDER BY payment_year DESC LIMIT 50",
        [house],
    )


def year_ranking(conn: duckdb.DuckDBPyConnection, year: int, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT member_name, unique_member_code, position, party_name,"
        " constituency, taa_band_label, total_paid, payment_count, rank_high,"
        " year_total_paid, year_member_count, year_avg_per_td"
        " FROM v_payments_yearly_evolution"
        " WHERE payment_year = ? AND house = ?"
        " ORDER BY rank_high ASC",
        [year, house],
    )


def member_all_years(
    conn: duckdb.DuckDBPyConnection, member_name: str, unique_member_code: str | None = None
) -> QueryResult:
    select = (
        "SELECT payment_year, total_paid, payment_count, rank_high,"
        " taa_band_label, position, party_name, constituency, member_alltime_total"
        " FROM v_payments_yearly_evolution"
    )
    if unique_member_code:
        return _run(conn, f"{select} WHERE unique_member_code = ? ORDER BY payment_year DESC", [unique_member_code])
    return _run(conn, f"{select} WHERE member_name = ? ORDER BY payment_year DESC", [member_name])


def member_year_summary(
    conn: duckdb.DuckDBPyConnection, member_name: str, year: int, unique_member_code: str | None = None
) -> QueryResult:
    select = (
        "SELECT member_name, position, party_name, constituency,"
        " taa_band_label, total_paid, payment_count, rank_high"
        " FROM v_payments_yearly_evolution"
    )
    if unique_member_code:
        return _run(
            conn, f"{select} WHERE unique_member_code = ? AND payment_year = ? LIMIT 1", [unique_member_code, year]
        )
    return _run(conn, f"{select} WHERE member_name = ? AND payment_year = ? LIMIT 1", [member_name, year])


def member_payments(
    conn: duckdb.DuckDBPyConnection, member_name: str, year: int, unique_member_code: str | None = None
) -> QueryResult:
    select = "SELECT date_paid, narrative, amount_num, taa_band_label FROM v_payments_member_detail"
    order = " ORDER BY date_paid ASC, narrative ASC"
    if unique_member_code:
        return _run(
            conn, f"{select} WHERE unique_member_code = ? AND payment_year = ?{order}", [unique_member_code, year]
        )
    return _run(conn, f"{select} WHERE member_name = ? AND payment_year = ?{order}", [member_name, year])


def alltime_ranking(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT member_name, unique_member_code, position, party_name,"
        " constituency, taa_band_label, total_paid_since_2020,"
        " payment_count_since_2020, earliest_year, latest_year, rank_high"
        " FROM v_payments_alltime_ranking"
        " WHERE house = ?"
        " ORDER BY rank_high ASC",
        [house],
    )


def alltime_summary(conn: duckdb.DuckDBPyConnection, house: str = "Dáil") -> QueryResult:
    return _run(
        conn,
        "SELECT total_paid_since_2020, member_count, avg_per_td_since_2020"
        " FROM v_payments_alltime_summary WHERE house = ? LIMIT 1",
        [house],
    )
