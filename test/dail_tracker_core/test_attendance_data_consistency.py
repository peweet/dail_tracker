"""Real-data consistency guards for attendance denominators.

This is the test that would have caught the recurring "the page says 82 sitting
days but a member has 94 recorded" bug. It asserts, against the ACTUAL built
data, the one invariant that makes that contradiction impossible:

    the denominator the page shows (distinct sitting dates the chamber sat)
    is NEVER smaller than any member's own recorded sitting-day count.

Because the denominator is now data-derived (v_attendance_chamber_sitting_days /
the same fact table), this holds by construction — and these tests pin it so a
future "optimisation" that swaps back to a hand-curated (and drift-prone)
denominator fails immediately.

Skips cleanly when the pipeline output isn't present (CI without data), matching
the repo's integration-test convention.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
from config import SILVER_DIR, SITTING_DAYS_BY_YEAR

_FACT_CSV = SILVER_DIR / "td_attendance_fact_table.csv"

pytestmark = pytest.mark.integration

# Official scheduled sitting days (config) may legitimately differ a little from
# the count of distinct dates that actually appear in the TAA record (a sitting
# day where no member's attendance was captured won't appear). A gap larger than
# this — or the WRONG SIGN (official < recorded) — signals a stale/placeholder
# config value like the old 2025 = 82.
_RECONCILE_TOLERANCE_DAYS = 8


def _load_fact() -> pd.DataFrame:
    if not _FACT_CSV.is_file():
        pytest.skip(f"attendance fact table not built: {_FACT_CSV}")
    df = pd.read_csv(_FACT_CSV)
    # Keep only real calendar years (the parser can emit a 'Missing' bucket).
    df = df[pd.to_numeric(df["year"], errors="coerce").notna()].copy()
    df["year"] = df["year"].astype(int)
    return df


def _chamber_sitting_days(fact: pd.DataFrame) -> dict[int, int]:
    """Distinct sitting dates per year — the data-derived denominator the UI uses."""
    return (
        fact.dropna(subset=["iso_sitting_days_attendance"])
        .groupby("year")["iso_sitting_days_attendance"]
        .nunique()
        .to_dict()
    )


def _max_member_sitting(fact: pd.DataFrame) -> dict[int, int]:
    return fact.groupby("year")["sitting_days_count"].max().to_dict()


# ── The core guard ────────────────────────────────────────────────────────────


def test_no_member_has_more_sitting_days_than_the_chamber_sat():
    """The denominator can never be smaller than the numerator.

    This is the exact failure mode of the old bug (member sitting count 94 shown
    against an 82-day denominator). With the data-derived denominator it is
    structurally impossible; the test makes any regression loud.
    """
    fact = _load_fact()
    chamber = _chamber_sitting_days(fact)
    max_member = _max_member_sitting(fact)
    for year, denom in chamber.items():
        assert max_member[year] <= denom, (
            f"{year}: a member recorded {max_member[year]} sitting days but the "
            f"chamber only sat {denom} distinct days — denominator is too small."
        )


def test_2025_chamber_sitting_days_exceed_the_old_stale_config_value():
    """Regression pin for the specific reported case: 2025 has ~94 recorded
    sitting dates, far above the removed config placeholder of 82."""
    fact = _load_fact()
    chamber = _chamber_sitting_days(fact)
    if 2025 not in chamber:
        pytest.skip("no 2025 attendance rows present")
    assert chamber[2025] > 82, (
        f"2025 recorded sitting days = {chamber[2025]}; the old config 82 would "
        "have under-stated the denominator (the reported bug)."
    )
    # And the stale value must not have been re-added to config.
    assert SITTING_DAYS_BY_YEAR.get(2025) != 82


# ── Config reconciliation ─────────────────────────────────────────────────────


def test_config_official_days_reconcile_with_recorded_dates():
    """For every year present in BOTH config and the data, the official scheduled
    figure must be >= the recorded distinct-date count and within tolerance.

    A config value SMALLER than the recorded count (official < recorded) is the
    stale-placeholder signature and fails here regardless of which year it is.
    """
    fact = _load_fact()
    chamber = _chamber_sitting_days(fact)
    overlap = sorted(set(SITTING_DAYS_BY_YEAR) & set(chamber))
    if not overlap:
        pytest.skip("no overlapping years between config and built data")
    for year in overlap:
        official = SITTING_DAYS_BY_YEAR[year]
        recorded = chamber[year]
        assert official >= recorded, (
            f"{year}: config official sitting days ({official}) is LESS than the "
            f"recorded distinct dates ({recorded}) — stale/placeholder config."
        )
        assert official - recorded <= _RECONCILE_TOLERANCE_DAYS, (
            f"{year}: config ({official}) and recorded ({recorded}) diverge by "
            f"more than {_RECONCILE_TOLERANCE_DAYS} days — check the config."
        )


def test_config_has_no_year_below_its_recorded_sitting_dates():
    """Whole-config sweep: no SITTING_DAYS_BY_YEAR entry may be below the data's
    distinct sitting-date count for that year (the invariant that, if violated,
    re-creates the original 82-vs-94 contradiction)."""
    fact = _load_fact()
    chamber = _chamber_sitting_days(fact)
    offenders = {y: (SITTING_DAYS_BY_YEAR[y], chamber[y]) for y in SITTING_DAYS_BY_YEAR if y in chamber and SITTING_DAYS_BY_YEAR[y] < chamber[y]}
    assert not offenders, f"config sitting days below recorded distinct dates: {offenders}"


# ── End-to-end view wiring (skips if views can't register) ───────────────────


def test_chamber_view_matches_fact_table_for_dail():
    """The data-derived denominator the page actually queries
    (v_attendance_chamber_sitting_days) agrees with the fact table.

    Exercises the real SQL wiring; skips if the views can't register (e.g. the
    Seanad parquet/CSV the UNION needs isn't present in this checkout)."""
    fact = _load_fact()
    expected = _chamber_sitting_days(fact)

    try:
        from dail_tracker_core.db import connect_with_views

        conn = connect_with_views(["attendance_chamber_sitting_days.sql"], swallow_errors=False)
        got = conn.execute(
            "SELECT year, sitting_days FROM v_attendance_chamber_sitting_days WHERE house = 'Dáil'"
        ).df()
    except Exception as exc:  # noqa: BLE001 — view needs both chambers' source files
        pytest.skip(f"chamber view did not register: {exc}")

    got_map = {int(y): int(s) for y, s in zip(got["year"], got["sitting_days"], strict=True)}
    for year, denom in got_map.items():
        if year in expected:
            assert denom == expected[year], f"{year}: view {denom} != fact {expected[year]}"
