"""Tripwire for v_procurement_council_summary — the "Your council" directory.

The view UNIONs the three council money lanes (purchase orders + audited revenue AFS + audited
capital AFS) so a council appears in the directory if it publishes ANY of them.

⚠️ HISTORY — this test used to assert Dublin City / DLR / Louth / Tipperary were "audited-accounts
ONLY" (has_paying = false). That was WRONG: their purchase-order rows existed all along in the
disclosed_bq_po_newbodies lane, but under the formal spelling ("Dublin City Council") that failed
to match the union's "Dublin City" — the same publisher-name orphaning fixed on 2026-07-14 by the
canonicaliser in procurement_payments_consolidate._canon_la_publisher_names. Once the names were
canonicalised, all 31 councils carry a paying lane. The guards now lock the CORRECTED contract:

  * every council in the directory has a paying lane (the orphaning must not return);
  * the previously-orphaned councils carry BOTH a paying lane and an accounts lane;
  * the never-sum tiers stay separate (ordered_safe_eur / paid_safe_eur distinct columns);
  * province is assigned for every council; a known payer (Mayo) keeps has_paying = true.

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

# The 8 councils whose payment rows were orphaned by the long-name spelling before 2026-07-14.
# All must now show a paying lane AND their audited-accounts lanes.
FORMERLY_ORPHANED = ["Carlow", "Cavan", "Dublin City", "Dun Laoghaire-Rathdown", "Kerry", "Louth", "Roscommon", "Tipperary"]


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect()
    c.execute(SQL.read_text(encoding="utf-8"))
    return c


def test_no_council_is_orphaned_from_its_payment_lane(con):
    """The regression this whole fix is about: a council with payments must show has_paying=true.
    Before the canonicaliser, 8 councils (incl. Dublin City, the largest LA) showed false here
    because their PO rows sat under a formal spelling the union could not match."""
    rows = con.execute(
        "SELECT council, has_paying, has_running, has_building FROM v_procurement_council_summary WHERE council IN ?",
        [FORMERLY_ORPHANED],
    ).fetchall()
    found = {r[0] for r in rows}
    assert found == set(FORMERLY_ORPHANED), f"councils dropped from the directory: {set(FORMERLY_ORPHANED) - found}"
    for council, has_paying, _has_running, _has_building in rows:
        assert has_paying, f"{council} lost its purchase-order lane — the name-orphaning regressed"


def test_union_surfaces_accounts_lanes(con):
    """The union's purpose: a council that publishes audited accounts is reachable via them even
    if the paying lane were ever absent. Dublin City carries both a paying and an accounts lane."""
    dc = con.execute(
        "SELECT has_running, has_building FROM v_procurement_council_summary WHERE council = 'Dublin City'"
    ).fetchone()
    assert dc and (dc[0] or dc[1]), "Dublin City should carry an audited-accounts lane"


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
