"""Data-integrity contract for v_supplier_entity_xref — the organisation-360 spine.

Runs in the @sql lane against committed gold (data/gold/parquet/supplier_entity_xref.parquet,
built by extractors/entity_xref_build.py). Pins the invariants the company dossier page and
the org-360 composition rely on: one row per supplier, the presence flags agree with their
counts, the cross-register tally is internally consistent, and counts never go negative.

The anchor was extended 2026-07-18 from AWARD-only to AWARD ∪ PAYMENT suppliers (10,017 →
28,357), so `procurement_award_rows >= 1` no longer holds — a payment-only entity carries 0
award rows by design. The union brings its own invariants, pinned below: no orphan rows, the
source flags agree with their counts, and neither money grain leaks into an entity that has
no rows in it.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

pytestmark = pytest.mark.sql

_PARQUET = Path("data/gold/parquet/supplier_entity_xref.parquet")


@pytest.fixture(scope="module")
def rel() -> str:
    if not _PARQUET.exists():
        pytest.skip("supplier_entity_xref gold not built (run extractors/entity_xref_build.py)")
    return f"read_parquet('{_PARQUET.as_posix()}')"


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect()
    yield c
    c.close()


def test_one_row_per_supplier_norm(conn, rel):
    total, distinct = conn.execute(f"SELECT count(*), count(DISTINCT supplier_norm) FROM {rel}").fetchone()
    assert total == distinct, "supplier_norm must be unique (the join key)"
    assert total >= 1000, "anchor is thousands of suppliers; a tiny frame means a broken input"


def test_presence_flags_match_counts(conn, rel):
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} "
        f"WHERE has_corporate_notice != (corporate_notices > 0) "
        f"   OR on_lobbying_register != (lobby_returns > 0)"
    ).fetchone()[0]
    assert bad == 0, "a presence flag disagrees with its count"


def test_cross_register_count_consistent(conn, rel):
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} WHERE cross_register_count != ("
        f"CAST(on_lobbying_register AS INT) + CAST(has_corporate_notice AS INT) "
        f"+ CAST(is_charity AS INT) + CAST(has_epa_licence AS INT))"
    ).fetchone()[0]
    assert bad == 0, "cross_register_count must equal the sum of the four extra-register flags"


def test_has_cro_matches_company_num(conn, rel):
    bad = conn.execute(f"SELECT count(*) FROM {rel} WHERE has_cro != (company_num IS NOT NULL)").fetchone()[0]
    assert bad == 0


def test_counts_nonneg(conn, rel):
    # NOTE: procurement_award_rows >= 1 no longer holds. The anchor was extended
    # 2026-07-18 from award-only to award ∪ payment suppliers, so a payment-only
    # entity legitimately carries 0 award rows (that was the point — BAM's paid
    # vehicles were invisible before). Every row must still be in ONE of the two.
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} WHERE lobby_returns < 0 OR corporate_notices < 0 "
        f"OR procurement_award_rows < 0 OR payment_rows < 0"
    ).fetchone()[0]
    assert bad == 0


def test_every_row_is_in_at_least_one_procurement_source(conn, rel):
    """The anchor is a UNION — an entity in neither source has no business being here."""
    orphans = conn.execute(f"SELECT count(*) FROM {rel} WHERE NOT in_awards AND NOT in_payments").fetchone()[0]
    assert orphans == 0, f"{orphans} spine rows are in neither the award nor the payment fact"


def test_source_flags_match_their_counts(conn, rel):
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} "
        f"WHERE in_awards != (procurement_award_rows > 0) OR in_payments != (payment_rows > 0)"
    ).fetchone()[0]
    assert bad == 0, "a source flag disagrees with its row count"


def test_money_grains_are_carried_separately_and_nonneg(conn, rel):
    """awarded (contracted) and paid (disbursed) are DIFFERENT money grains — both columns
    must exist independently so a consumer can show them side by side. This test does NOT
    add them; adding them would itself breach the three-grain rule (reference_data_map)."""
    bad = conn.execute(
        f"SELECT count(*) FROM {rel} "
        f"WHERE COALESCE(awarded_value_safe_eur, 0) < 0 OR COALESCE(paid_value_safe_eur, 0) < 0"
    ).fetchone()[0]
    assert bad == 0, "negative money in the spine"
    # A payment-only entity must not carry award money, and vice versa — that would mean
    # the union mis-assigned a grain.
    leaked = conn.execute(
        f"SELECT count(*) FROM {rel} WHERE (NOT in_awards AND COALESCE(awarded_value_safe_eur,0) > 0) "
        f"OR (NOT in_payments AND COALESCE(paid_value_safe_eur,0) > 0)"
    ).fetchone()[0]
    assert leaked == 0, "money attributed to a grain the entity has no rows in"
