"""Data-integrity contracts for high-value gold tables that were previously UNGUARDED.

Uses the shared invariant vocabulary (test/contracts/_invariants.py). Runs in the @sql lane
against committed gold. Focus = the money / cross-reference tables whose numbers get quoted,
plus the documented sum-traps that today only have a human-readable warning, not a test.

Every invariant below was verified to currently HOLD on real gold before being asserted.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent))
import _invariants as inv  # noqa: E402

pytestmark = pytest.mark.sql

_GOLD = Path("data/gold/parquet")


def _rel(table: str) -> str:
    return f"read_parquet('{(_GOLD / (table + '.parquet')).as_posix()}')"


@pytest.fixture(scope="module")
def conn():
    c = duckdb.connect()
    yield c
    c.close()


# ── procurement_lobbying_overlap — pin the famous dedup-SUM trap ─────────────────────────
# Rows are exploded per lobby-name, so awarded_value_safe_eur REPEATS per supplier. Summing
# rows double-counts (measured: ~€2.0bn naive vs ~€1.34bn honest). The structural guarantee
# that makes the honest read possible is: one award value per supplier.
def test_plo_award_value_functionally_determined_by_supplier(conn):
    inv.functionally_determined(conn, _rel("procurement_lobbying_overlap"), "supplier_norm", "awarded_value_safe_eur")


def test_plo_nonneg_and_side_vocab(conn):
    r = _rel("procurement_lobbying_overlap")
    inv.nonneg(conn, r, "awarded_value_safe_eur", "n_lobby_returns", "n_award_rows", "n_authorities")
    inv.in_vocab(conn, r, "lobby_side", {"client", "registrant"})


def test_plo_row_sum_overcounts_distinct_supplier_sum(conn):
    # documents (and locks) the trap: the naive row-sum MUST exceed the distinct-supplier sum,
    # i.e. duplicate rows exist → never sum awarded_value_safe_eur across rows.
    r = _rel("procurement_lobbying_overlap")
    naive = conn.execute(f"SELECT sum(awarded_value_safe_eur) FROM {r}").fetchone()[0]
    honest = conn.execute(
        f"SELECT sum(v) FROM (SELECT supplier_norm, any_value(awarded_value_safe_eur) v FROM {r} GROUP BY 1)"
    ).fetchone()[0]
    assert honest < naive, "expected duplicated supplier rows (the dedup-sum trap) — has the grain changed?"


# ── charities_enriched ──────────────────────────────────────────────────────────────────
def test_charities_rcn_unique_and_money_nonneg(conn):
    r = _rel("charities_enriched")
    inv.unique_key(conn, r, "rcn")
    inv.nonneg(conn, r, "gross_income_latest_eur", "gross_expenditure_latest_eur", "total_assets_latest_eur")
    inv.no_sentinels(conn, r, "registered_charity_name")


# ── corporate_notices_enriched ──────────────────────────────────────────────────────────
def test_corporate_notices_flags_vocab_and_year(conn):
    r = _rel("corporate_notices_enriched")
    inv.flag_consistent(conn, r, "has_receiver_firm", "receiver_firms IS NOT NULL AND len(receiver_firms) > 0")
    inv.in_vocab(
        conn,
        r,
        "notice_category",
        {"corporate_insolvency", "corporate_notice", "corporate_rescue", "investment_vehicle_register_notice"},
    )
    bad_year = conn.execute(
        f"SELECT count(*) FROM {r} WHERE year IS NOT NULL AND (year < 2010 OR year > 2030)"
    ).fetchone()[0]
    assert bad_year == 0, f"{bad_year} corporate notices with an implausible year"
    inv.no_sentinels(conn, r, "entity_name")
