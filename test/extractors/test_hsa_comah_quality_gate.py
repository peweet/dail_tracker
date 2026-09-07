"""The COMAH quality ratchet must be able to FAIL (gate-trap rule: prove it before trusting
it). Synthetic frames only — no data files, CI-safe. Added 2026-09-04 with the ratchet."""

import polars as pl
import pytest

from extractors.hsa_comah_extract import _MAX_UNGEOCODED, _MAX_UPPER_TIER_TOWN, enforce_quality


def _frame(upper_town: int, ungeocoded: int) -> pl.DataFrame:
    rows = []
    for i in range(upper_town):
        rows.append({"establishment": f"UT{i}", "tier": "upper", "geocode_precision": "town", "lat": 53.0})
    for i in range(ungeocoded):
        rows.append({"establishment": f"UG{i}", "tier": "lower", "geocode_precision": None, "lat": None})
    rows.append({"establishment": "OK", "tier": "upper", "geocode_precision": "site", "lat": 53.0})
    return pl.DataFrame(rows)


def test_passes_at_the_committed_baseline():
    enforce_quality(_frame(_MAX_UPPER_TIER_TOWN, _MAX_UNGEOCODED))


def test_fails_when_upper_tier_town_rows_exceed_baseline():
    with pytest.raises(SystemExit, match="upper-tier town-precision"):
        enforce_quality(_frame(_MAX_UPPER_TIER_TOWN + 1, 0))


def test_fails_when_ungeocoded_rows_exceed_baseline():
    with pytest.raises(SystemExit, match="ungeocoded"):
        enforce_quality(_frame(0, _MAX_UNGEOCODED + 1))
