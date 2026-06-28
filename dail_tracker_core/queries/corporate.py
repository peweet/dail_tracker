"""Corporate notices retrieval — Streamlit-free.

Moved verbatim from ``utility/data_access/corporate_data.py``. Retrieval-only
SELECTs against the registered ``corporate_*`` views; the page does its own
faceting/search/aggregation in pandas off these frames (unchanged). Build a
connection with ``connect_with_views(["corporate_*.sql"], swallow_errors=True)``.
"""

from __future__ import annotations

import logging

import duckdb

from dail_tracker_core.queries import run_query
from dail_tracker_core.results import QueryResult

_log = logging.getLogger(__name__)


def _run(conn: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> QueryResult:
    return run_query(conn, sql, params, label="corporate", log=_log)


def corporate_notices(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Every in-scope corporate notice (personal insolvency excluded upstream)."""
    return _run(conn, "SELECT * FROM v_corporate_notices")


def corporate_notices_for_company(conn: duckdb.DuckDBPyConnection, company_num) -> QueryResult:
    """Every CRO-matched corporate notice (Iris Oifigiúil) for ONE company, keyed on the
    hard CRO ``company_num`` — the per-firm slice of ``v_corporate_cro_notice_match`` that
    the company dossier's corporate-register panel renders. Newest first.

    Display-only public-record annotation: appearing on a statutory notice (insolvency /
    liquidation / receivership / register change) is a public record, NOT a finding of
    wrongdoing, and a firm may appear as the SUBJECT of a notice or as a named insolvency
    practitioner — the firm's own ``company_status`` (carried on every row) is what states
    whether THIS legal entity is itself in distress."""
    return _run(
        conn,
        "SELECT notice_ref, entity_name, issue_date, notice_category, notice_subtype,"
        " company_num, company_status, company_reg_date, comp_dissolved_date, status_pill_value"
        " FROM v_corporate_cro_notice_match WHERE company_num = ?"
        " ORDER BY issue_date DESC NULLS LAST",
        [company_num],
    )


def cbi_notice_matches(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-notice CBI authorisation lookup (EXPERIMENTAL — sandbox source)."""
    return _run(conn, "SELECT * FROM v_corporate_cbi_notice_match")


def cbi_repeat_distress(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Per-firm repeat-distress aggregate (EXPERIMENTAL — sandbox source)."""
    return _run(conn, "SELECT * FROM v_corporate_cbi_repeat_distress")


def cbi_enforcement(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Central Bank of Ireland enforcement actions — settlements / sanctions, one row per
    published notice, biggest disclosed fine first. ``fine_amount_eur`` is the single fine on
    that notice (NOT summable across notices — settlements differ in basis; value_safe_to_sum
    is False on the view). Summary/aggregate notices carry a null fine. The page lists named
    actions; it never totals them."""
    return _run(
        conn,
        "SELECT notice_date, title, party_name, doc_type, fine_amount_eur, pdf_url, source_url"
        " FROM v_corporate_cbi_enforcement"
        " ORDER BY fine_amount_eur DESC NULLS LAST, notice_date DESC NULLS LAST",
    )


def brand_aliases(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Brand -> parent_fund -> fund_type curated alias map."""
    return _run(conn, "SELECT * FROM v_corporate_brand_aliases")


def isif_portfolio(conn: duckdb.DuckDBPyConnection, *, limit: int | None = None) -> QueryResult:
    """ISIF (Ireland Strategic Investment Fund) sovereign-fund investment commitments — the
    State putting money INTO companies, one row per investee, newest commitment first.

    NOT summable: amounts are in mixed currencies (EUR/USD), some are 'up to' ceilings, and
    value_safe_to_sum is False on the view. The page lists named commitments; it never totals
    them. A commitment is a public investment record, not evidence of anything else."""
    sql = (
        "SELECT investee_name, commitment_date, commitment_year_label, description,"
        " amount_stated, amount_currency, amount_is_up_to, source_url"
        " FROM v_corporate_isif_portfolio"
        " ORDER BY commitment_date DESC NULLS LAST"
    )
    params: list = []
    if limit is not None:
        sql += " LIMIT ?"
        params.append(int(limit))
    return _run(conn, sql, params)


def receiver_appointers(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Funds/banks ranked by receivership notices naming them (precomputed)."""
    return _run(conn, "SELECT parent, n_notices, dominant_fund_type, type_bucket FROM v_corporate_receiver_appointers")


def receiver_bucket_mix(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Appointer type-mix headline (mention-weighted) by bucket."""
    return _run(conn, "SELECT type_bucket, n FROM v_corporate_receiver_bucket_mix")


def receiver_firms(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Professional firms named AS receiver, by notice presence (precomputed)."""
    return _run(conn, "SELECT firm, n_notices, is_big6 FROM v_corporate_receiver_firms")


def receiver_year_counts(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Receivership notices by year — the featured-panel sparkline series."""
    return _run(conn, "SELECT year, n FROM v_corporate_receiver_year_counts")


def receiver_summary(conn: duckdb.DuckDBPyConnection) -> QueryResult:
    """Featured/operator headline scalar counts (one row)."""
    return _run(conn, "SELECT n_recv, n_spv, n_tagged, n_any_tagged FROM v_corporate_receiver_summary")
