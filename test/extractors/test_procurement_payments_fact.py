"""Gold-quality contract for the consolidated public-body payment fact
(extractors/procurement_payments_consolidate.py → procurement_payments_fact.parquet).

Skips if the gold parquet hasn't been built. Guards the invariants the page relies on:
the canonical 2-axis taxonomy is present, tiers map correctly, VAT bases are tagged, and the
owner privacy decision (suppliers named, incl. individuals) holds with no address/PII column.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

ROOT = Path(__file__).resolve().parents[2]
FACT = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"


@pytest.fixture(scope="module")
def con():
    if not FACT.exists():
        pytest.skip("procurement_payments_fact.parquet not built")
    c = duckdb.connect()
    yield c
    c.close()


def _q(con, sql):
    return con.execute(sql.replace("FACT", f"read_parquet('{FACT.as_posix()}')")).fetchall()


def test_canonical_taxonomy_present(con):
    cols = {r[0] for r in _q(con, "DESCRIBE SELECT * FROM FACT")}
    assert {"value_kind", "realisation_tier", "vat_status"}.issubset(cols)


def test_tier_mapping_is_consistent(con):
    # Every payment_actual row is SPENT; every po_committed row is COMMITTED. No blend.
    bad = _q(
        con,
        "SELECT COUNT(*) FROM FACT WHERE (value_kind='payment_actual' AND realisation_tier<>'SPENT')"
        " OR (value_kind='po_committed' AND realisation_tier<>'COMMITTED')",
    )[0][0]
    assert bad == 0


def test_vat_inclusive_publishers_tagged(con):
    # HSE / Tusla are VAT-inclusive; the rest are left 'unknown' (never falsely 'exclusive').
    hse = _q(con, "SELECT DISTINCT vat_status FROM FACT WHERE publisher_name LIKE '%Health Service%'")
    assert hse == [("incl_vat",)]
    assert {"incl_vat", "unknown"}.issuperset({r[0] for r in _q(con, "SELECT DISTINCT vat_status FROM FACT")})


def test_suppliers_named_including_individuals(con):
    # Owner decision: individuals/sole traders ARE named (published-source data).
    n = _q(con, "SELECT COUNT(*) FROM FACT WHERE supplier_class='sole_trader_or_individual'")[0][0]
    assert n > 0  # they are present, not quarantined out


def test_no_address_or_pii_column(con):
    # Guardrail: only published name/amount/description — never an address/PII field.
    cols = {r[0].lower() for r in _q(con, "DESCRIBE SELECT * FROM FACT")}
    assert not (cols & {"address", "supplier_address", "eircode", "ppsn", "dob", "home_address"})


def test_public_body_transfers_mostly_excluded_from_sum(con):
    # Intergovernmental transfers (public_body recipients) must not inflate spend totals:
    # the overwhelming majority are value_safe_to_sum = false.
    safe, total = _q(
        con,
        "SELECT COUNT(*) FILTER (WHERE value_safe_to_sum), COUNT(*) FROM FACT WHERE supplier_class='public_body'",
    )[0]
    assert total > 0
    assert safe / total < 0.05  # <5% leak (the rest correctly non-summable)
