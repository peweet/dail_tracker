"""Contract + view tests for the per-LA social-housing construction pipeline.

Covers the committed gold (``housing_construction_pipeline.parquet``) and the
registered ``v_housing_construction_pipeline`` view that the Housing page's
"Where social homes are being built" table renders from.

Bounds are deliberately TOLERANT (the CSR is refreshed quarterly, so exact unit
counts move) — the tests pin the structural invariants that must hold every
quarter, not this quarter's numbers:
  * 31 local authorities, one row each;
  * on-site units are a subset of the not-yet-completed pipeline (never exceed it);
  * the never-sum flag is set (a snapshot; the same scheme recurs each quarter);
  * the view's national totals equal the sum of the per-LA rows;
  * ranks are dense-ordered by pipeline size and shares are ~100% in aggregate.

Marked ``@sql`` — runs against committed gold with no pipeline build.
"""

import sys
from pathlib import Path

import polars as pl
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))

from config import GOLD_PARQUET_DIR  # noqa: E402

_GOLD = GOLD_PARQUET_DIR / "housing_construction_pipeline.parquet"

_EXPECTED_COLS = {
    "local_authority", "pipeline_units", "pipeline_schemes", "units_on_site",
    "schemes_on_site", "units_completed", "report_period", "source_name",
    "source_url", "fetched_at", "extraction_method", "privacy_tier", "value_safe_to_sum",
}


@pytest.mark.sql
def test_construction_gold_exists_and_shape():
    assert _GOLD.exists(), "housing_construction_pipeline.parquet not committed"
    df = pl.read_parquet(_GOLD)
    assert _EXPECTED_COLS.issubset(set(df.columns)), f"missing cols: {_EXPECTED_COLS - set(df.columns)}"
    assert df["local_authority"].n_unique() == 31, "expected 31 local authorities"
    assert df.height == 31


@pytest.mark.sql
def test_construction_gold_invariants():
    df = pl.read_parquet(_GOLD)
    # counts are non-negative
    for c in ("pipeline_units", "pipeline_schemes", "units_on_site", "units_completed"):
        assert df[c].drop_nulls().min() >= 0, f"{c} has a negative"
    # on-site is a subset of the not-yet-completed pipeline
    bad = df.filter(pl.col("units_on_site") > pl.col("pipeline_units"))
    assert bad.height == 0, f"on-site exceeds pipeline for: {bad['local_authority'].to_list()}"
    # never-sum flag set (snapshot grain)
    assert not df["value_safe_to_sum"].any(), "value_safe_to_sum must be False for a quarterly snapshot"
    # plausible national scale (guards a parser wipe/blowup without pinning a quarter)
    assert 8_000 <= int(df["pipeline_units"].sum()) <= 80_000


@pytest.mark.sql
def test_construction_view_matches_gold_and_ranks():
    from dail_tracker_core.connections import housing_conn
    from dail_tracker_core.queries import housing as q

    res = q.construction_pipeline(housing_conn())
    assert res.ok and len(res.data) == 31
    d = res.data

    # national totals in the view equal the per-LA sums
    assert int(d["national_pipeline_units"].iloc[0]) == int(d["pipeline_units"].sum())
    assert int(d["national_units_on_site"].iloc[0]) == int(d["units_on_site"].sum())

    # rank 1 is the largest pipeline and carries the largest national share
    top = d.sort_values("pipeline_rank").iloc[0]
    assert top["pipeline_units"] == d["pipeline_units"].max()
    assert top["pct_of_national"] == d["pct_of_national"].max()

    # shares sum to ~100 (rounding tolerance)
    assert abs(float(d["pct_of_national"].sum()) - 100.0) <= 1.5
