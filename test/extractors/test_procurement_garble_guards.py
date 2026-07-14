"""Regression guards for the 2026-06 procurement garble fixes (doc/archive/DATA_QUALITY_AUDIT.md §6):
  * _drop_unattributable — period/section TOTAL rows (blank supplier+desc+po) must not survive to gold
  * _strip_bled_amount   — a leading amount duplicated into description is stripped ONLY when it == amount_eur
  * courts reading-order reader — no empty suppliers; period-total rows excluded
Unit tests run always; the integration checks assert the real gold (DAIL_INTEGRATION_TESTS=1).
"""

from __future__ import annotations

import os
from pathlib import Path

import duckdb
import polars as pl
import pytest

from extractors.procurement_payments_consolidate import _drop_unattributable, _strip_bled_amount

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data/gold/parquet/procurement_payments_fact.parquet"


def test_drop_unattributable_removes_total_rows_only():
    df = pl.DataFrame(
        {
            "supplier_raw": ["ACME LTD", "", None, "  "],
            "description": ["Repairs", "", None, ""],
            "po_number": ["123", "", None, ""],
            "amount_eur": [1000.0, 155_000_000.0, 74_000_000.0, 50.0],
        }
    )
    out = _drop_unattributable(df, "test")
    assert out.height == 1  # only the real ACME row survives
    assert out["supplier_raw"].to_list() == ["ACME LTD"]


def test_strip_bled_amount_only_when_equals_amount():
    df = pl.DataFrame(
        {
            "description": [
                "€80,000,000.00 Third Level Building",  # leading == amount -> strip
                "70% Bitumen Emulsion",  # spec, leading != amount -> keep
                "240350384 Minor Contracts",  # code prefix != amount -> keep
                "Repairs",  # no leading number -> keep
            ],
            "amount_eur": [80_000_000.0, 30_985.78, 132_619.15, 500.0],
        }
    )
    out = _strip_bled_amount(df)
    assert out["description"].to_list() == [
        "Third Level Building",
        "70% Bitumen Emulsion",
        "240350384 Minor Contracts",
        "Repairs",
    ]


def test_strip_bled_amount_preserves_amount_column():
    df = pl.DataFrame({"description": ["€1,234.56 Stuff"], "amount_eur": [1234.56]})
    out = _strip_bled_amount(df)
    assert out["amount_eur"].to_list() == [1234.56]
    assert out["description"].to_list() == ["Stuff"]


@pytest.mark.skipif(os.environ.get("DAIL_INTEGRATION_TESTS") != "1", reason="needs real gold fact")
def test_gold_has_no_unattributable_rows():
    if not GOLD.exists():
        pytest.skip("gold fact not built")
    n = (
        duckdb.connect()
        .execute(
            f"SELECT count(*) FROM read_parquet('{GOLD.as_posix()}') "
            "WHERE coalesce(trim(supplier_raw),'')='' AND coalesce(trim(description),'')='' "
            "AND coalesce(trim(po_number),'')=''"
        )
        .fetchone()[0]
    )
    assert n == 0, f"{n} unattributable (blank supplier+desc+po) rows leaked into gold"


@pytest.mark.skipif(os.environ.get("DAIL_INTEGRATION_TESTS") != "1", reason="needs real gold fact")
def test_courts_supplier_attribution_recovered():
    if not GOLD.exists():
        pytest.skip("gold fact not built")
    c = duckdb.connect()
    empty, mx = c.execute(
        f"SELECT count(*) FILTER (WHERE coalesce(trim(supplier_normalised),'')=''), max(amount_eur) "
        f"FROM read_parquet('{GOLD.as_posix()}') WHERE publisher_id='ie_courts'"
    ).fetchone()
    assert empty == 0, f"ie_courts has {empty} empty-supplier rows — courts reader regressed"
    assert mx < 5_000_000, f"ie_courts max single amount €{mx:,.0f} — a period-total row may have leaked back"
