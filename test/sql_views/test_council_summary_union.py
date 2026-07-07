"""Tripwire for v_procurement_council_summary — the "Your council" directory.

The view was rebuilt 2026-06-26 from a payments-only SELECT into a UNION across the three council
money lanes (purchase orders + audited revenue AFS + audited capital AFS), so that councils which
publish audited accounts but NO purchase-order list — Dublin City (the largest LA in the State),
Dún Laoghaire-Rathdown, Louth, Tipperary — appear in the directory and are reachable in the dossier
instead of being silently dropped. These guards lock that contract:

  * the four audited-accounts-only councils are present, flagged has_paying = false but
    has_running / has_building true (so the page renders their accounts lanes, not "No payments");
  * the never-sum tiers stay separate (ordered_safe_eur / paid_safe_eur are distinct columns);
  * province is assigned for every council (no NULL band would drop a card);
  * a known payer (Mayo) keeps has_paying = true.

Reads the three facts the view reads, so it skips cleanly when those parquets are absent (CI).
"""

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).parents[2]
SQL = ROOT / "sql_views" / "procurement" / "procurement_council_summary.sql"
FACTS = [
    ROOT / "data" / "gold" / "parquet" / "procurement_payments_fact.parquet",
    ROOT / "data" / "silver" / "parquet" / "la_afs_divisions.parquet",
    ROOT / "data" / "silver" / "parquet" / "la_afs_capital_divisions.parquet",
]

pytestmark = pytest.mark.skipif(not all(f.exists() for f in FACTS), reason="council money facts absent")

AFS_ONLY = ["Dublin City", "Dun Laoghaire-Rathdown", "Louth", "Tipperary"]


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute(SQL.read_text(encoding="utf-8"))
    return c


def test_afs_only_councils_present_and_flagged(con):
    rows = con.execute(
        "SELECT council, has_paying, has_running, has_building FROM v_procurement_council_summary WHERE council IN ?",
        [AFS_ONLY],
    ).fetchall()
    found = {r[0] for r in rows}
    assert found == set(AFS_ONLY), f"missing audited-accounts-only councils: {set(AFS_ONLY) - found}"
    for council, has_paying, has_running, has_building in rows:
        assert not has_paying, f"{council} should have no purchase-order lane"
        assert has_running or has_building, f"{council} should carry an audited-accounts lane"


def test_every_council_has_a_province(con):
    n_null = con.execute(
        "SELECT COUNT(*) FROM v_procurement_council_summary WHERE province IS NULL OR province_order IS NULL"
    ).fetchone()[0]
    assert n_null == 0


def test_lifecycle_tiers_are_separate_columns(con):
    # Meath publishes 'SPENT' (paid); the COMMITTED payers publish 'ordered'. Never one summed col.
    cols = {d[0] for d in con.execute("SELECT * FROM v_procurement_council_summary LIMIT 0").description}
    assert {"ordered_safe_eur", "paid_safe_eur"} <= cols
    assert "total_eur" not in cols  # a merged total would invite a never-sum violation


def test_known_payer_still_flagged(con):
    has_paying = con.execute("SELECT has_paying FROM v_procurement_council_summary WHERE council = 'Mayo'").fetchone()
    assert has_paying is not None and has_paying[0]
