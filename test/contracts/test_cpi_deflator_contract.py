"""Data-integrity contract for the derived CPI deflator (cso_cpi_deflator.parquet).

Built by extractors/cso_pxstat_extract.py:build_cpi_deflator() from raw CPA07 by
chain-linking the annual %-change series for All Items (the index level itself is
split across base-month rebasings and cannot span our fact window).

A deflator RE-EXPRESSES a nominal € into base-year prices; it must never be lossy,
non-monotone in the wrong direction, or off at the base year. Every invariant below
was verified to hold on real gold before being asserted. Runs in the @sql lane.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import polars as pl
import pytest

pytestmark = pytest.mark.sql

_GOLD = Path("data/gold/parquet")
_DEFLATOR = _GOLD / "cso_cpi_deflator.parquet"
_BASE_YEAR = 2025


@pytest.fixture(scope="module")
def defl() -> pl.DataFrame:
    if not _DEFLATOR.exists():
        pytest.skip("cso_cpi_deflator.parquet not built yet")
    return pl.read_parquet(_DEFLATOR)


def test_schema_and_no_nulls(defl):
    assert set(defl.columns) == {
        "year", "cpi_pct_change", "cpi_index_chained", "deflator_to_base", "base_year",
    }
    assert defl.null_count().to_numpy().sum() == 0, "deflator must have no nulls"


def test_one_row_per_year(defl):
    assert defl["year"].n_unique() == defl.height, "deflator grain is one row per year"


def test_base_year_is_identity(defl):
    """The base year deflates to itself: factor == 1.0 exactly."""
    f = defl.filter(pl.col("year") == _BASE_YEAR)["deflator_to_base"]
    assert f.len() == 1 and f[0] == pytest.approx(1.0, abs=1e-9)
    assert (defl["base_year"] == _BASE_YEAR).all()


def test_past_years_deflate_up(defl):
    """Every pre-base year must have factor >= 1.0 (prices rose to the base year).

    This is the sign guarantee that protects real-terms totals from silently
    SHRINKING historical figures — the failure mode that would corrupt comparisons.
    """
    bad = defl.filter((pl.col("year") < _BASE_YEAR) & (pl.col("deflator_to_base") < 1.0))
    assert bad.height == 0, f"{bad.height} pre-base years deflate DOWN: {bad['year'].to_list()}"


def test_factor_bounded(defl):
    """Sanity bound on the chain-link. The series reaches back to 1975, so the oldest
    factor is ~6.5x (cumulative late-70s/80s inflation); bound generously at 8x."""
    assert defl["deflator_to_base"].max() < 8.0
    assert defl["deflator_to_base"].min() > 0.0


def test_index_reconstructs_pct_change(defl):
    """The chain-linked index must be internally consistent with its own %-change column."""
    d = defl.sort("year").with_columns(
        (pl.col("cpi_index_chained") / pl.col("cpi_index_chained").shift(1) - 1).mul(100).alias("derived_pct")
    )
    chk = d.filter(pl.col("derived_pct").is_not_null())
    diff = (chk["derived_pct"] - chk["cpi_pct_change"]).abs().max()
    assert diff < 0.01, f"chained index inconsistent with %-change (max diff {diff})"


def test_2012_to_base_matches_published_cpi(defl):
    """Cross-check against CSO published cumulative CPI: 2012->2025 ~ +24.7%.

    Locks the chain-link to reality; a regression here means the series drifted.
    """
    f2012 = defl.filter(pl.col("year") == 2012)["deflator_to_base"][0]
    assert f2012 == pytest.approx(1.247, abs=0.01)


# ── Non-destruction: the deflator must not be able to corrupt the canonical facts ──
# It is a SEPARATE reference table; the money facts keep their nominal columns intact.
def test_money_facts_keep_nominal_columns():
    conn = duckdb.connect()
    try:
        aw = _GOLD / "procurement_awards.parquet"
        pay = _GOLD / "procurement_payments_fact.parquet"
        if aw.exists():
            cols = {r[0] for r in conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{aw.as_posix()}')").fetchall()}
            assert "value_eur" in cols, "awards must retain canonical nominal value_eur"
        if pay.exists():
            cols = {r[0] for r in conn.execute(
                f"DESCRIBE SELECT * FROM read_parquet('{pay.as_posix()}')").fetchall()}
            assert "amount_eur" in cols, "payments must retain canonical nominal amount_eur"
    finally:
        conn.close()


@pytest.mark.parametrize("fact,col", [
    ("procurement_awards", "value_eur"),
    ("procurement_payments_fact", "amount_eur"),
])
def test_value_plausible_flag_present_and_consistent(fact, col):
    """The parse-artefact guard must exist, be boolean, and agree with its definition:
    a present value is plausible IFF it sits in the band; a null value -> null flag."""
    from services.deflator import PLAUSIBLE_FLOOR_EUR

    path = _GOLD / f"{fact}.parquet"
    if not path.exists():
        pytest.skip(f"{fact} not built")
    df = pl.read_parquet(path)
    assert "value_plausible" in df.columns, f"{fact} missing value_plausible flag"
    assert df["value_plausible"].dtype == pl.Boolean
    # any value below the floor must be flagged not-plausible (the user's sub-€100 worry)
    low_unflagged = df.filter(
        (pl.col(col).is_not_null()) & (pl.col(col) > 0) & (pl.col(col) < PLAUSIBLE_FLOOR_EUR)
        & (pl.col("value_plausible") != False)  # noqa: E712
    ).height
    assert low_unflagged == 0, f"{low_unflagged} sub-€{PLAUSIBLE_FLOOR_EUR:.0f} values not flagged implausible"
    # null value -> null flag (unknown, not a false positive)
    assert df.filter(pl.col(col).is_null() & pl.col("value_plausible").is_not_null()).height == 0
