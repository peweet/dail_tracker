"""Guard: the live attendance pipeline must match the curated OFFICIAL sitting days.

The number of days the chamber sat each year is the denominator of every
attendance rate, and it has been a repeat source of bugs (a hand-typed config
that drifted; an extractor that truncated member sitting days; the "82 vs 94"
mismatch). ``data/_meta/official_sitting_days.csv`` is the committed, source-
derived ground truth (built by ``tools/curate_official_sitting_days.py`` from the
TAA Verification-of-Attendance PDFs — the union of distinct sitting dates).

These tests pin the live pipeline to that reference so the bug cannot recur
silently:

  1. For every COMPLETE year, the data-derived distinct sitting-date count (what
     the UI denominator uses, per chamber) equals the curated official figure.
  2. No member's recorded sitting days exceed the curated official figure (the
     numerator can never beat the denominator — the truncation/“82 vs 94” class
     of bug fails here).
  3. The curated reference still carries its known anchor values, so an accidental
     wipe/regeneration that zeroed it is caught.

Skips cleanly when the built fact tables aren't present (CI without data).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parents[2]))
from config import SILVER_DIR

pytestmark = pytest.mark.integration

_OFFICIAL_CSV = Path("data/_meta/official_sitting_days.csv")
_FACT_BY_HOUSE = {
    "Dáil": SILVER_DIR / "td_attendance_fact_table.csv",
    "Seanad": SILVER_DIR / "seanad_attendance_fact_table.csv",
}

# Known committed anchors — a wipe/regression that zeroed the reference fails here.
_ANCHORS = {("Dáil", 2023): 97, ("Dáil", 2024): 83, ("Dáil", 2025): 94}


def _official() -> pd.DataFrame:
    if not _OFFICIAL_CSV.is_file():
        pytest.skip(f"curated official sitting days not present: {_OFFICIAL_CSV}")
    return pd.read_csv(_OFFICIAL_CSV)


def _fact(house: str) -> pd.DataFrame:
    path = _FACT_BY_HOUSE[house]
    if not path.is_file():
        pytest.skip(f"attendance fact table not built: {path}")
    df = pd.read_csv(path)
    df = df[pd.to_numeric(df["year"], errors="coerce").notna()].copy()
    df["year"] = df["year"].astype(int)
    return df


def _chamber_distinct_sitting(house: str) -> dict[int, int]:
    fact = _fact(house)
    return (
        fact.dropna(subset=["iso_sitting_days_attendance"])
        .groupby("year")["iso_sitting_days_attendance"]
        .nunique()
        .astype(int)
        .to_dict()
    )


def test_curated_reference_keeps_its_anchor_values():
    """The committed reference still holds its known figures (not wiped/zeroed)."""
    off = _official()
    for (house, year), expected in _ANCHORS.items():
        row = off[(off["house"] == house) & (off["year"] == year)]
        assert not row.empty, f"curated official_sitting_days missing {house} {year}"
        got = int(row["official_sitting_days"].iloc[0])
        assert got == expected, f"{house} {year}: curated official {got} != known anchor {expected}"


@pytest.mark.parametrize("house", ["Dáil", "Seanad"])
def test_data_derived_matches_curated_official_for_complete_years(house: str):
    """The denominator the UI computes equals the curated official figure.

    Only COMPLETE years are checked: an in-progress year's running count is below
    the eventual total and would (correctly) differ.
    """
    off = _official()
    off = off[(off["house"] == house) & (off["complete_year"])]
    if off.empty:
        pytest.skip(f"no complete-year official rows for {house}")
    derived = _chamber_distinct_sitting(house)
    for _, r in off.iterrows():
        year, official = int(r["year"]), int(r["official_sitting_days"])
        if year not in derived:
            continue
        assert derived[year] == official, (
            f"{house} {year}: live data-derived sitting days {derived[year]} != "
            f"curated official {official}. Attendance extraction has drifted — "
            "re-run tools/curate_official_sitting_days.py only if the SOURCE PDFs changed."
        )


@pytest.mark.parametrize("house", ["Dáil", "Seanad"])
def test_no_member_sitting_days_exceed_official(house: str):
    """Numerator never beats the denominator (the truncation / 82-vs-94 bug class)."""
    off = _official()
    official_by_year = {
        int(r["year"]): int(r["official_sitting_days"]) for _, r in off[off["house"] == house].iterrows()
    }
    fact = _fact(house)
    max_member = fact.groupby("year")["sitting_days_count"].max().astype(int).to_dict()
    for year, mx in max_member.items():
        if year not in official_by_year:
            continue
        assert mx <= official_by_year[year], (
            f"{house} {year}: a member recorded {mx} sitting days but the chamber "
            f"officially sat only {official_by_year[year]} — denominator too small."
        )
