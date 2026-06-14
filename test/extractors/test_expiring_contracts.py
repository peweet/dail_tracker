"""Contract for v_procurement_expiring_contracts_etenders — the national (eTenders) "expiring
contracts / re-tender pipeline" view reconstructed from award date + advertised duration.

Skips if the gold awards parquet isn't built. Guards the honesty rails: frameworks excluded,
value stays AWARD/ceiling grade (display-only, never summed), the estimated end is date+duration,
and likely-personal winners are withheld.
"""

from __future__ import annotations

import duckdb
import pytest

from dail_tracker_core.db import connect_with_views


@pytest.fixture(scope="module")
def con():
    c = connect_with_views(["procurement_expiring_contracts_etenders.sql"])
    try:
        c.execute("SELECT 1 FROM v_procurement_expiring_contracts_etenders LIMIT 1")
    except duckdb.Error:
        pytest.skip("awards parquet / view not available")
    yield c
    c.close()


def _q(con, sql):
    return con.execute(sql).fetchone()[0]


def test_no_framework_rows(con):
    # A framework's "duration" is a ceiling window, not a single contract — must be excluded.
    assert _q(con, "SELECT COUNT(*) FROM v_procurement_expiring_contracts_etenders WHERE value_kind='framework_or_dps_ceiling'") == 0


def test_value_is_award_grade_display_only(con):
    # Every row carries an AWARD/ceiling value_kind — never a payment/spend grade (those must not
    # leak into this register, which is never summed and never added to payments).
    bad = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_expiring_contracts_etenders "
        "WHERE value_kind IN ('payment_actual','po_committed')",
    )
    assert bad == 0


def test_estimated_end_after_award(con):
    # est_end_date is award_date + a positive duration → always strictly after the award date.
    bad = _q(con, "SELECT COUNT(*) FROM v_procurement_expiring_contracts_etenders WHERE est_end_date <= award_date")
    assert bad == 0


def test_duration_within_guard(con):
    bad = _q(con, "SELECT COUNT(*) FROM v_procurement_expiring_contracts_etenders WHERE duration_months NOT BETWEEN 1 AND 240")
    assert bad == 0


def test_personal_winners_withheld(con):
    # A sole_trader_or_individual winner name is withheld (NULL); the contract row still lists.
    leaked = _q(
        con,
        "SELECT COUNT(*) FROM v_procurement_expiring_contracts_etenders "
        "WHERE supplier_class='sole_trader_or_individual' AND winner_display IS NOT NULL",
    )
    assert leaked == 0
