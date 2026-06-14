"""Contract for the authoritative appeal-outcomes join (planning_appeal_outcomes.py).

Guards the reason this exists: the per-council overturn rate must be CREDIBLE — no 100% artifacts like
the self-reported AppealDecision field produced — and the national rate must sit in the known band.
"""

from __future__ import annotations

from pathlib import Path

import pytest

pl = pytest.importorskip("polars")

OUT = Path(__file__).resolve().parents[1] / "pipeline_sandbox/_planning_output/planning_appeal_outcomes.parquet"
PII = {"ApplicantForename", "ApplicantSurname", "ApplicantAddress", "applicant"}

pytestmark = pytest.mark.skipif(not OUT.exists(), reason="appeal-outcomes parquet not built")


def _df():
    return pl.read_parquet(OUT)


def test_no_applicant_pii():
    assert PII.isdisjoint(set(_df().columns))


def test_national_overturn_rate_is_in_known_band():
    df = _df()
    rate = 100 * df["overturned"].sum() / df.height
    assert 15 <= rate <= 45, f"overturn {rate:.1f}% outside the plausible ~1/3 band"


def test_no_council_shows_implausible_100pct():
    # the whole point: the authoritative ACP join must not reproduce the self-reported 100% artifacts.
    rank = (_df().group_by("PlanningAuthority")
            .agg(pl.len().alias("n"), pl.col("overturned").sum().alias("ov"))
            .filter(pl.col("n") >= 25)
            .with_columns((100 * pl.col("ov") / pl.col("n")).alias("pct")))
    assert rank.filter(pl.col("pct") >= 95).height == 0, "a council at >=95% overturn — artifact likely back"


def test_decisions_are_clear_grant_or_refuse():
    df = _df()
    assert set(df["council_decision"].unique()) <= {"GRANT", "REFUSE"}
    assert set(df["abp_decision"].unique()) <= {"GRANT", "REFUSE"}
    # overturned == decisions differ
    assert df.filter(pl.col("overturned") != (pl.col("council_decision") != pl.col("abp_decision"))).height == 0
