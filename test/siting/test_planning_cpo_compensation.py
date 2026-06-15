"""Privacy + classification contract for planning_cpo_compensation.py (sandbox).

The single load-bearing guarantee: the anonymized land-acquisition layer NEVER carries a payee
identity, and the acquisition_type is the source-grounded keyword derivation (no inference).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
pl = pytest.importorskip("polars")

from extractors.planning_cpo_compensation import _ACQ_MATCH, OUT, _acq_type, build  # noqa: E402

_NAME_COLS = {"supplier", "supplier_raw", "supplier_normalised", "payee", "name"}


@pytest.mark.parametrize("desc,expect", [
    ("PURCHASE OF DWELLING ASSET", "dwelling"),
    ("LAND BANK ASSET PURCHASE", "land_bank"),
    ("LAND PURCHASE-NEW ROAD WORKS", "road_land"),
    ("COMPULSORY PURCHASE ORDER", "cpo"),
    ("LAND PURCHASE - CPO INTEREST", "cpo"),
    ("Land - purchase", "land_general"),
])
def test_acquisition_type_is_source_grounded(desc, expect):
    out = pl.DataFrame({"d": [desc]}).select(_acq_type(pl.col("d")).alias("t"))
    assert out["t"][0] == expect, desc


def test_match_pattern_catches_land_excludes_rent():
    df = pl.DataFrame({"d": [
        "Purchase of dwelling asset", "Compulsory Purchase Order", "Land bank asset purchase",
        "Rent of office building", "Operating lease of buildings", "Cleaning services",
    ]})
    hit = df.select(pl.col("d").str.to_lowercase().str.contains(_ACQ_MATCH).alias("m"))["m"].to_list()
    assert hit == [True, True, True, False, False, False]


@pytest.mark.skipif(not (ROOT / "data/gold/parquet/procurement_payments_fact.parquet").exists(),
                    reason="gold fact not built")
def test_output_carries_no_payee_identity():
    agg = build()
    assert _NAME_COLS.isdisjoint(set(agg.columns)), f"identity leak: {_NAME_COLS & set(agg.columns)}"
    # aggregate grain: figure x year x location, never an individual row
    assert {"acquiring_body", "year", "acquisition_type", "total_compensation_eur"}.issubset(agg.columns)
    assert agg["total_compensation_eur"].min() > 0
    assert agg["year"].min() >= 2000  # no garbage years


@pytest.mark.skipif(not OUT.exists(), reason="output not built yet")
def test_written_parquet_has_no_identity_columns():
    cols = set(pl.read_parquet(OUT).columns)
    assert _NAME_COLS.isdisjoint(cols), f"identity leak in written parquet: {_NAME_COLS & cols}"
