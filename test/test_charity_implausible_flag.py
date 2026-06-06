"""Tests for the within-entity plausibility gate in charity.charity_normalise.

The Charities Regulator's public register carries filer data-entry errors where a
single annual return's gross income/expenditure lands many orders of magnitude
beyond the same charity's every other filing (real case: RCN 20026691 South West
Mayo Development Company filed gross_expenditure = €299,304,304,680 for 2024 vs
~€3-5m every other year). add_implausible_flag must catch those without rewriting
the raw value, and must leave legitimately-large, smoothly-growing bodies (HSE
scale) alone.
"""

from __future__ import annotations

import polars as pl

from charity.charity_normalise import add_implausible_flag


def _flag(df: pl.DataFrame) -> pl.DataFrame:
    return add_implausible_flag(df).sort("period_year")


def test_flags_single_garbage_year_preserving_raw_value() -> None:
    # South West Mayo shape: one €299bn expenditure year among ~€3m peers.
    years = list(range(2014, 2025))
    exp = [3_000_000.0] * 10 + [299_304_304_680.0]
    df = pl.DataFrame(
        {
            "rcn": [20026691] * 11,
            "period_year": years,
            "gross_income": [3_000_000.0] * 11,
            "gross_expenditure": exp,
        }
    )
    out = _flag(df)
    flagged = out.filter(pl.col("amount_implausible_flag"))
    assert flagged.height == 1
    assert flagged["period_year"].item() == 2024
    # Raw value is preserved, never rewritten.
    assert flagged["gross_expenditure"].item() == 299_304_304_680.0


def test_flags_garbage_gross_income_independently() -> None:
    # Claddagh shape: garbage gross_income, fine expenditure in the same row.
    df = pl.DataFrame(
        {
            "rcn": [20204981] * 5,
            "period_year": [2020, 2021, 2022, 2023, 2024],
            "gross_income": [40_000.0, 90_000.0, 115_000.0, 250_311_568_654.0, 83_000.0],
            "gross_expenditure": [18_000.0, 29_000.0, 47_000.0, 106_332.0, 98_000.0],
        }
    )
    out = _flag(df)
    flagged = out.filter(pl.col("amount_implausible_flag"))
    assert flagged["period_year"].to_list() == [2023]


def test_does_not_flag_smoothly_growing_large_body() -> None:
    # HSE shape: €13.6bn -> €28.3bn over 11 years. Max/median ~1.5x — never flagged.
    inc = [13.6, 14.3, 15.0, 15.1, 16.0, 18.1, 21.3, 22.5, 24.4, 25.3, 27.6]
    df = pl.DataFrame(
        {
            "rcn": [20059064] * 11,
            "period_year": list(range(2014, 2025)),
            "gross_income": [v * 1e9 for v in inc],
            "gross_expenditure": [v * 1e9 for v in inc],
        }
    )
    out = add_implausible_flag(df)
    assert out["amount_implausible_flag"].sum() == 0


def test_floor_spares_small_charities() -> None:
    # A 100x jump that stays under €100m is below the absolute floor — not flagged.
    df = pl.DataFrame(
        {
            "rcn": [1] * 4,
            "period_year": [2021, 2022, 2023, 2024],
            "gross_income": [10_000.0, 10_000.0, 10_000.0, 5_000_000.0],
            "gross_expenditure": [10_000.0, 10_000.0, 10_000.0, 10_000.0],
        }
    )
    out = add_implausible_flag(df)
    assert out["amount_implausible_flag"].sum() == 0


def test_requires_three_filings() -> None:
    # With only two filings the median is unreliable, so we never flag (faithful).
    df = pl.DataFrame(
        {
            "rcn": [1, 1],
            "period_year": [2023, 2024],
            "gross_income": [50_000.0, 250_000_000_000.0],
            "gross_expenditure": [50_000.0, 60_000.0],
        }
    )
    out = add_implausible_flag(df)
    assert out["amount_implausible_flag"].sum() == 0
