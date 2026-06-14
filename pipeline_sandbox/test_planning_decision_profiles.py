"""Contract for the national decision-profile output (planning_decision_profiles.py).

Integration-style: validates the written parquet if present (skips if the sandbox build hasn't run).
Guards the two things that bit during the build: applicant-PII must never appear, and the `appealed`
flag must be sane (the AppealDecision empty-string trap inflated it to 96% before the fix).
"""

from __future__ import annotations

from pathlib import Path

import pytest

pl = pytest.importorskip("polars")

OUT = Path(__file__).resolve().parents[1] / "pipeline_sandbox/_planning_output/planning_decision_profiles.parquet"
PII = {"ApplicantForename", "ApplicantSurname", "ApplicantAddress", "applicant", "ApplicantName"}
FLAGS = ["in_sac", "in_spa", "in_nha", "in_pnha", "in_natura2000"]

pytestmark = pytest.mark.skipif(not OUT.exists(), reason="decision-profile parquet not built")


def _df():
    return pl.read_parquet(OUT)


def test_no_applicant_pii():
    assert PII.isdisjoint(set(_df().columns))


def test_designation_flags_present_and_boolean():
    df = _df()
    for c in FLAGS:
        assert c in df.columns, c
        assert df.schema[c] == pl.Boolean, c


def test_appeal_rate_is_sane_not_the_empty_string_bug():
    # the empty-string AppealDecision trap inflated this to ~96%; real appeal rates are single digit.
    df = _df().filter(pl.col("decided"))
    rate = 100 * df["appealed"].sum() / df.height
    assert rate < 15, f"appeal rate {rate:.1f}% — empty-string AppealDecision bug likely back"


def test_decision_logic_consistent():
    df = _df()
    # refused and granted are mutually exclusive; both imply decided
    assert df.filter(pl.col("refused") & pl.col("granted")).height == 0
    assert df.filter((pl.col("refused") | pl.col("granted")) & ~pl.col("decided")).height == 0


def test_natura_is_union_of_sac_spa():
    df = _df()
    bad = df.filter(pl.col("in_natura2000") != (pl.col("in_sac") | pl.col("in_spa")))
    assert bad.height == 0
