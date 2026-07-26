"""Contract for the AFS Balance-Sheet facts — financial position (stocks) + Note 7 loan movement.

Added 2026-07-21. Widened from the narrow loans-payable fact into the full Statement of
Financial Position (la_afs_balance_sheet, long) plus the Note 7 borrow/repay FLOW
(la_afs_loan_movement). NOAC publishes none of the balance sheet, so this is the only source of
council financial position.

Guards, by fact:
  balance sheet — directly-printed audited lines, so validation is structural: no duplicate
  (council, year, item); loans_payable non-negative; fixed-asset components sum to the printed
  total; the Crown Square ground truth survives (Galway 51,417,617 -> 94,483,841).
  loan movement — a reconstructed table, so it is reconcile-GATED at extract: every stored
  (council, year) must satisfy opening + flows == closing. This test re-proves that invariant on
  gold and pins that repayments/redemptions are signed negative.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

_BS = Path("data/silver/parquet/la_afs_balance_sheet.parquet")
_MOVE = Path("data/silver/parquet/la_afs_loan_movement.parquet")


@pytest.fixture(scope="module")
def conn():
    if not _BS.exists() or not _MOVE.exists():
        pytest.skip(
            "AFS balance-sheet facts not built (run extractors/la_afs_balancesheet_extract.py + la_afs_loanmovement_extract.py)"
        )
    c = duckdb.connect()
    yield c
    c.close()


def _bs(c) -> str:
    return f"read_parquet('{_BS.as_posix()}')"


def _mv(c) -> str:
    return f"read_parquet('{_MOVE.as_posix()}')"


# --------------------------------------------------------------------------- balance sheet
@pytest.mark.sql
def test_bs_columns_exist(conn):
    cols = {d[0] for d in conn.execute(f"SELECT * FROM {_bs(conn)} LIMIT 0").description}
    assert {"council", "year", "section", "item", "value_eur", "is_statement_year", "value_kind"} <= cols


@pytest.mark.sql
def test_bs_one_row_per_council_year_item(conn):
    dupes = conn.execute(
        f"SELECT count(*) FROM (SELECT council, year, item FROM {_bs(conn)} "
        f"GROUP BY council, year, item HAVING count(*) > 1)"
    ).fetchone()[0]
    assert dupes == 0, f"{dupes} (council, year, item) triples appear more than once"


@pytest.mark.sql
def test_bs_loans_payable_present_and_nonnegative(conn):
    bad = conn.execute(
        f"SELECT count(*) FROM {_bs(conn)} WHERE item='loans_payable' AND (value_eur IS NULL OR value_eur < 0)"
    ).fetchone()[0]
    assert bad == 0, f"{bad} loans_payable rows null/negative"


@pytest.mark.sql
def test_bs_fixed_asset_components_sum_to_printed_total(conn):
    """Galway 2022 fixed-asset components (fa_*) must sum to the printed €1,276,189,993 (±€2)."""
    total = conn.execute(
        f"SELECT sum(value_eur) FROM {_bs(conn)} WHERE slug='galway_city' AND year=2022 AND section='fixed_assets'"
    ).fetchone()[0]
    assert total == pytest.approx(1276189993, abs=2)


@pytest.mark.sql
def test_bs_ground_truth_crown_square_jump(conn):
    rows = dict(
        conn.execute(
            f"SELECT year, value_eur FROM {_bs(conn)} WHERE slug='galway_city' AND item='loans_payable' "
            f"AND year IN (2021, 2022)"
        ).fetchall()
    )
    assert rows.get(2021) == pytest.approx(51417617.0)
    assert rows.get(2022) == pytest.approx(94483841.0)


@pytest.mark.sql
def test_bs_coverage_has_not_regressed(conn):
    """30 of 31 councils at build time (only Wexford, scanned-image, misses)."""
    n = conn.execute(f"SELECT count(DISTINCT slug) FROM {_bs(conn)}").fetchone()[0]
    assert n >= 28, f"only {n} councils in balance sheet — parser regression?"


# --------------------------------------------------------------------------- loan movement
@pytest.mark.sql
def test_move_every_stored_year_reconciles(conn):
    """The gate that makes the movement trustworthy: opening + Σflows == closing, per year.
    A non-reconciling year is dropped at extract, so gold must contain ZERO violations."""
    bad = conn.execute(
        f"""WITH p AS (
              SELECT council, year,
                sum(CASE WHEN item='opening_balance' THEN value_eur ELSE 0 END) AS o,
                sum(CASE WHEN item='closing_balance' THEN value_eur ELSE 0 END) AS c,
                sum(CASE WHEN is_flow THEN value_eur ELSE 0 END)                AS f
              FROM {_mv(conn)} GROUP BY council, year)
            SELECT count(*) FROM p WHERE abs(o + f - c) >= 1000"""
    ).fetchone()[0]
    assert bad == 0, f"{bad} council-years fail opening + flows == closing"


@pytest.mark.sql
def test_move_repayments_are_negative(conn):
    """Repayments and redemptions are outflows — stored with their printed negative sign."""
    pos = conn.execute(
        f"SELECT count(*) FROM {_mv(conn)} WHERE item IN ('repayment_of_principal','early_redemptions') AND value_eur > 0"
    ).fetchone()[0]
    assert pos == 0, f"{pos} repayment/redemption rows are positive"


@pytest.mark.sql
def test_move_ground_truth_galway_borrowings(conn):
    """Galway 2022 new borrowings = €49,710,995 (the Crown Square loan)."""
    v = conn.execute(
        f"SELECT value_eur FROM {_mv(conn)} WHERE slug='galway_city' AND year=2022 AND item='borrowings'"
    ).fetchone()
    assert v is not None and v[0] == pytest.approx(49710995.0)
