"""Advertised-term (expiring contracts) retrieval — the framework filter and its headline count.

``is_multi_supplier_framework`` was selected by the listing query and reaching the render frame
long before it was shown, so these tests pin the two behaviours that make it visible: the filter
narrows to frameworks without touching the window or the ordering, and the summary reports how
many exist so the headline states the count instead of asserting it.
"""

from __future__ import annotations

import duckdb

from dail_tracker_core.queries.procurement.ted import expiring_contracts, expiring_contracts_stats


def _connection() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect()
    conn.execute(
        """
        CREATE TABLE v_procurement_expiring_contracts (
            publication_number VARCHAR, notice_url VARCHAR, buyer_name VARCHAR,
            winners_display VARCHAR, n_winners BIGINT, cpv_division VARCHAR,
            award_value_eur DOUBLE, value_kind VARCHAR,
            is_multi_supplier_framework BOOLEAN, contract_conclusion_date DATE,
            contract_duration_months DOUBLE, renewal_max BIGINT,
            contract_end_date_est DATE, contract_end_basis VARCHAR, year BIGINT
        )
        """
    )
    rows = [
        # A single-winner contract ending soonest — must lead the default ordering.
        (
            "1-2025",
            "u1",
            "UCC",
            "Acme Ltd",
            1,
            "Construction",
            100.0,
            "contract_award_value",
            False,
            "2024-01-01",
            24.0,
            None,
            "2026-09-01",
            "conclusion_plus_duration",
            2024,
        ),
        # A multi-supplier framework ending later — only this one survives frameworks_only.
        (
            "2-2025",
            "u2",
            "HSE",
            "Alpha Ltd, Beta Ltd",
            4,
            "Health/Social",
            500.0,
            "framework_or_dps_ceiling",
            True,
            "2024-01-01",
            36.0,
            2,
            "2026-11-01",
            "explicit_end_date",
            2024,
        ),
        # A framework far outside the window — excluded by months_ahead, not by the flag.
        (
            "3-2025",
            "u3",
            "TCD",
            "Gamma Ltd",
            3,
            "IT services",
            900.0,
            "framework_or_dps_ceiling",
            True,
            "2024-01-01",
            120.0,
            None,
            "2034-01-01",
            "conclusion_plus_duration",
            2024,
        ),
    ]
    conn.executemany(
        "INSERT INTO v_procurement_expiring_contracts VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )
    return conn


def test_frameworks_only_narrows_without_changing_the_window_or_order():
    conn = _connection()
    try:
        every = expiring_contracts(conn, months_ahead=120)
        assert every.data["publication_number"].tolist() == ["1-2025", "2-2025", "3-2025"]

        frameworks = expiring_contracts(conn, months_ahead=120, frameworks_only=True)
        assert frameworks.data["publication_number"].tolist() == ["2-2025", "3-2025"]
        assert frameworks.data["is_multi_supplier_framework"].all()
        # Soonest-first ordering survives the filter.
        assert frameworks.data["contract_end_date_est"].is_monotonic_increasing
    finally:
        conn.close()


def test_the_window_still_bounds_the_framework_filter():
    """frameworks_only must AND with months_ahead, never replace it — otherwise a framework
    ending in 2034 would appear in a '24 months' list."""
    conn = _connection()
    try:
        near = expiring_contracts(conn, months_ahead=24, frameworks_only=True)
        assert near.data["publication_number"].tolist() == ["2-2025"]
    finally:
        conn.close()


def test_limit_applies_after_the_framework_filter():
    conn = _connection()
    try:
        capped = expiring_contracts(conn, months_ahead=120, frameworks_only=True, limit=1)
        assert capped.data["publication_number"].tolist() == ["2-2025"]
    finally:
        conn.close()


def test_summary_counts_frameworks_across_the_whole_corpus():
    """The headline count is corpus-wide, not window-scoped — it describes the register, while
    the list below it describes the chosen window."""
    conn = _connection()
    try:
        s = expiring_contracts_stats(conn).data.iloc[0]
        assert s["n_with_estimate"] == 3
        assert s["n_frameworks"] == 2
        assert s["n_explicit"] == 1
    finally:
        conn.close()


def test_missing_view_reports_unavailable_rather_than_empty():
    empty_conn = duckdb.connect()
    try:
        assert expiring_contracts(empty_conn, frameworks_only=True).ok is False
        assert expiring_contracts_stats(empty_conn).ok is False
    finally:
        empty_conn.close()
