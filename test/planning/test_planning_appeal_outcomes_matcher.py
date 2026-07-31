"""Pure-function tests for _spatial_temporal_matches (planning_appeal_outcomes.py).

Locks in the 2026-08 fix: an LRD cross-check found ABP-322540 (Castlepark, Mallow, a 469-unit
scheme) falsely matched to an unrelated 2007 permission on the same site, because the fallback
had no plausibility bound on how old a "pre-dating" decision could be, and the true 2025
application sat just outside the tight spatial radius. Real coordinates/dates from that case are
used below so the regression is anchored to the actual failure, not an invented one.
"""

from __future__ import annotations

import datetime as dt

import pytest

pl = pytest.importorskip("polars")

from planning.civic.extractors.planning_appeal_outcomes import _auth_key, _spatial_temporal_matches  # noqa: E402

_RESIDUAL_COLS = ["abp_case", "lon", "lat", "auth_key", "lodged_date", "abp_decision", "PLANINGATY", "CATEGORY", "DECIDED_ON"]
_APPS_COLS = ["ApplicationNumber", "PlanningAuthority", "decision_normalised", "DecisionDate", "lon", "lat"]


def _residual(*, abp_case, lon, lat, lodged_date, authority="Cork County Council"):
    return pl.DataFrame(
        [[abp_case, lon, lat, _auth_key(authority), lodged_date, "GRANT", authority, "PERMISSION", None]],
        schema=_RESIDUAL_COLS,
        orient="row",
    )


def _apps(rows):
    return pl.DataFrame(rows, schema=_APPS_COLS, orient="row")


def test_prefers_a_wider_but_recent_match_over_a_close_but_ancient_one():
    # Real coordinates: ACP centroid for ABP-322540 vs. its true application (24/6036, ~89 m away,
    # decided 2025-04-23) vs. three unrelated 2007 permissions on the same land (~52-63 m away).
    residual = _residual(abp_case="322540", lon=-8.628061, lat=52.136956, lodged_date=dt.date(2025, 5, 15))
    apps = _apps(
        [
            ["24/6036", "Cork County Council", "Granted-Conditional", dt.date(2025, 4, 23), -8.626905, 52.137364],
            ["07/55057", "Cork County Council", "Granted-Conditional", dt.date(2007, 9, 12), -8.627539, 52.137335],
            ["07/55006", "Cork County Council", "Granted-Conditional", None, -8.627539, 52.137335],
            ["07/55079", "Cork County Council", "Granted-Conditional", dt.date(2007, 12, 13), -8.627539, 52.137335],
        ]
    )
    out = _spatial_temporal_matches(residual, apps)
    assert out.height == 1
    assert out["ApplicationNumber"][0] == "24/6036"
    assert out["match_method"][0] == "spatial_temporal"


def test_returns_no_match_rather_than_an_implausibly_old_one():
    # Only a stale, decades-old decision sits nearby — no plausible candidate for a 2025 appeal, so
    # the case should come back UNMATCHED, not silently pinned to the old permission.
    residual = _residual(abp_case="999999", lon=-8.628061, lat=52.136956, lodged_date=dt.date(2025, 5, 15))
    apps = _apps([["95/1", "Cork County Council", "Granted-Conditional", dt.date(1995, 1, 1), -8.628061, 52.136956]])
    out = _spatial_temporal_matches(residual, apps)
    assert out.height == 0


def test_still_matches_within_the_tight_radius_when_recent():
    # The common case (unchanged by the fix): a genuinely close, recently-decided application still
    # matches on the tight (validated) radius without needing the wide fallback.
    residual = _residual(
        abp_case="321970", lon=-9.120162, lat=53.273865, lodged_date=dt.date(2025, 2, 25), authority="Galway City Council"
    )
    apps = _apps([["2460270", "Galway City Council", "Refused", dt.date(2025, 1, 29), -9.120500, 53.273900]])
    out = _spatial_temporal_matches(residual, apps)
    assert out.height == 1
    assert out["ApplicationNumber"][0] == "2460270"
