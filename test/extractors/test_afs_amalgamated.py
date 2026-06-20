"""Tests for the amalgamated AFS extractor (extractors/afs_amalgamated_extract.py).

Three layers:
  1. Pure number-parsing (to_num) — incl. the 2019 "€ millions + M suffix" notation
     and parenthesised negatives that earlier broke the parser.
  2. Golden parse of a committed I&E page text fixture — proves the table parser still
     yields 8 reconciling divisions if the gov.ie PDF layout drifts.
  3. Data-integrity invariants on the committed full-series golden parquet (2016–2023):
     accounting identity (net = gross − income), structure, sign sanity, and the
     cross-year prior-year-column consistency check (independent cross-document validation).

Regenerate fixtures: re-run afs_amalgamated_extract.py, then
  test/fixtures/afs/_generate.py  (copies the page text + golden parquet).
"""

from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "extractors"))
from afs_amalgamated_extract import DIVISIONS, parse_ie, to_num  # noqa: E402

FX = Path(__file__).resolve().parents[1] / "fixtures" / "afs"
PAGE = FX / "afs_2020_ie_page.txt"
GOLDEN = FX / "afs_amalgamated_divisions.parquet"
EUR = 1_000_000  # tolerance: 2019 reports in € millions (rounded to €0.01m = €10k)


# ---- 1. pure number parsing -------------------------------------------------
@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("6,750,822,110", 6_750_822_110.0),  # full euros
        ("(13,737,809)", -13_737_809.0),  # parenthesised negative
        ("1,630.75 M", 1_630_750_000.0),  # 2019 € millions notation
        ("27.31 M", 27_310_000.0),
        ("0", 0.0),
        ("-", 0.0),
        ("", 0.0),
        # comma-/dot-only cells satisfy the NUM regex but strip to a non-number — these used to
        # crash to_num with float('') (ValueError) and abort a whole multi-year AFS run.
        ("(,)", 0.0),
        (",", 0.0),
        (".", 0.0),
    ],
)
def test_to_num(raw: str, expected: float):
    assert to_num(raw) == pytest.approx(expected)


# ---- 2. golden parse of the I&E page ---------------------------------------
def test_parse_ie_yields_eight_reconciling_divisions():
    text = PAGE.read_text(encoding="utf-8")
    ie, total = parse_ie(text)
    assert len(ie) == 8, f"expected 8 divisions, got {len(ie)}: {list(ie)}"
    assert set(ie) == {c for c, _ in DIVISIONS}
    # printed total found and Σ gross reconciles to it
    assert total is not None, "printed Total line not detected"
    gross_sum = sum(v[0] for v in ie.values())
    assert abs(gross_sum - total[0]) <= 2, f"gross Σ {gross_sum} != printed {total[0]}"


def test_parse_ie_accounting_identity():
    ie, _ = parse_ie(PAGE.read_text(encoding="utf-8"))
    for div, (gross, income, net, _prior) in ie.items():
        assert abs((gross - income) - net) <= EUR, f"{div}: gross-income != net"


# ---- 3. full-series golden parquet invariants ------------------------------
@pytest.fixture(scope="module")
def golden() -> pl.DataFrame:
    return pl.read_parquet(GOLDEN)


def test_structure_8_years_8_divisions(golden: pl.DataFrame):
    assert golden.height == 64
    assert sorted(golden["year"].unique().to_list()) == [2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]
    per_year = golden.group_by("year").agg(pl.col("division").n_unique().alias("n"))
    assert per_year["n"].to_list() == [8] * 8


def test_accounting_identity_all_rows(golden: pl.DataFrame):
    resid = golden.with_columns(
        (pl.col("gross_expenditure") - pl.col("income") - pl.col("net_expenditure")).abs().alias("r")
    )
    assert resid["r"].max() <= EUR, "net != gross - income beyond €1m tolerance"


def test_no_negative_gross_or_income(golden: pl.DataFrame):
    assert golden.filter(pl.col("gross_expenditure") < 0).height == 0
    assert golden.filter(pl.col("income") < 0).height == 0


def test_cross_year_prior_consistency(golden: pl.DataFrame):
    """Year N's restated prior-year column must equal year N-1's reported net —
    an independent cross-document validation that the 8 PDFs were parsed consistently."""
    cur = golden.select(["year", "division", "net_expenditure"])
    pri = golden.select(
        [
            (pl.col("year") - 1).alias("year"),
            "division",
            pl.col("net_expenditure_prior_yr").alias("prior"),
        ]
    )
    joined = cur.join(pri, on=["year", "division"], how="inner").with_columns(
        (pl.col("net_expenditure") - pl.col("prior")).abs().alias("d")
    )
    assert joined.height == 56, f"expected 56 cross-year pairs, got {joined.height}"
    assert joined["d"].max() <= EUR, "prior-year column disagrees with previous year's actual"


def test_taxonomy_tags_present(golden: pl.DataFrame):
    assert (golden["realisation_tier"] == "SPENT").all()
    assert (golden["value_kind"] == "net_expenditure_actual").all()
    assert golden["scope"].unique().to_list() == ["all-31-LAs (amalgamated)"]
    assert golden["source"].str.contains("AFS").all()
