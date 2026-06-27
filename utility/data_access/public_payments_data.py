"""Public-body payments data access — thin Streamlit wrapper over dail_tracker_core.

The retrieval SQL + QueryResult handling live in
``dail_tracker_core.queries.public_payments``; this file owns only the Streamlit
caching (``st.cache_resource`` for the connection, ``st.cache_data`` per query)
and unwraps ``QueryResult`` for the page.

Forbidden here (logic firewall — the checker scans this file): JOIN / GROUP BY /
HAVING / WINDOW in SQL, CREATE VIEW, read_parquet, pandas merge/pivot,
business-metric definitions — all of which live in sql_views/ and
dail_tracker_core. The privacy gate (public_display) lives in the view.
"""

from __future__ import annotations

import contextlib
import json
from pathlib import Path

import duckdb
import streamlit as st

from dail_tracker_core.db import connect_with_views
from dail_tracker_core.queries import public_payments as _q
from dail_tracker_core.results import QueryResult

_ROOT = Path(__file__).resolve().parents[2]


@st.cache_resource
def get_public_payments_conn() -> duckdb.DuckDBPyConnection:
    # procurement_payments_by_category.sql carries the "What the money buys" lens views
    # (v_payments_by_category[_publisher] / v_payments_category_suppliers); both files read
    # the same gold payment fact, so registering them together is the whole dependency set.
    return connect_with_views(
        ["procurement_public_payments.sql", "procurement_payments_by_category.sql"],
        swallow_errors=True,
    )


@st.cache_data(ttl=600)
def fetch_coverage() -> dict:
    """Committed coverage metadata for the two registers (publisher counts, quarantined
    personal-row count, value taxonomy). Display only — no aggregation. Merges the two
    coverage JSONs; returns {} if both absent (page degrades gracefully)."""
    out: dict = {}
    for name in ("public_payments_coverage.json", "hse_tusla_payments_coverage.json"):
        # missing/garbled metadata must not break the page
        with contextlib.suppress(Exception):
            out[name.replace("_coverage.json", "")] = json.loads(
                (_ROOT / "data" / "_meta" / name).read_text(encoding="utf-8")
            )
    return out


@st.cache_data(ttl=300)
def fetch_coverage_stats_result() -> QueryResult:
    """One-row corpus summary for the hero scale anchor + the page's source-state gate
    (if unavailable, the page shows the source-down state)."""
    return _q.coverage_stats(get_public_payments_conn())


@st.cache_data(ttl=600)
def fetch_available_years_result() -> QueryResult:
    return _q.available_years(get_public_payments_conn())


@st.cache_data(ttl=300)
def fetch_publisher_summary_result(order_by: str = "value", limit: int | None = None) -> QueryResult:
    return _q.publisher_summary(get_public_payments_conn(), order_by=order_by, limit=limit)


@st.cache_data(ttl=300)
def fetch_supplier_summary_result(order_by: str = "value", limit: int | None = None) -> QueryResult:
    return _q.supplier_summary(get_public_payments_conn(), order_by=order_by, limit=limit)


@st.cache_data(ttl=300)
def fetch_publisher_lines_result(
    publisher_id: str, year: int | None = None, order_by: str = "value", limit: int | None = None
) -> QueryResult:
    return _q.publisher_lines(get_public_payments_conn(), publisher_id, year=year, order_by=order_by, limit=limit)


@st.cache_data(ttl=300)
def fetch_supplier_lines_result(
    supplier_normalised: str, order_by: str = "value", limit: int | None = None
) -> QueryResult:
    return _q.supplier_lines(get_public_payments_conn(), supplier_normalised, order_by=order_by, limit=limit)


@st.cache_data(ttl=300)
def fetch_supplier_quarter_totals_result(supplier_normalised: str, limit: int | None = None) -> QueryResult:
    return _q.supplier_quarter_totals(get_public_payments_conn(), supplier_normalised, limit=limit)


# "What the money buys" — category lens (doc/PAYMENTS_CATEGORY_LENS_DESIGN.md).
@st.cache_data(ttl=300)
def fetch_categories_result(order_by: str = "value") -> QueryResult:
    """Spend-category overview (category × tier). Tier is never blended on the page."""
    return _q.categories(get_public_payments_conn(), order_by=order_by)


@st.cache_data(ttl=300)
def fetch_category_suppliers_result(spend_category: str, limit: int | None = None) -> QueryResult:
    """Named vendors paid/ordered within one category (CRO surfaced, not merged)."""
    return _q.category_suppliers(get_public_payments_conn(), spend_category, limit=limit)


@st.cache_data(ttl=300)
def fetch_category_publishers_result(spend_category: str) -> QueryResult:
    """Bodies whose published spend drives one category (attribution block)."""
    return _q.category_publishers(get_public_payments_conn(), spend_category)
