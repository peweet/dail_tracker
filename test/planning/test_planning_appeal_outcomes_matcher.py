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

_RESIDUAL_COLS = [
    "abp_case",
    "lon",
    "lat",
    "auth_key",
    "lodged_date",
    "abp_decision",
    "PLANINGATY",
    "CATEGORY",
    "DECIDED_ON",
]
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
        abp_case="321970",
        lon=-9.120162,
        lat=53.273865,
        lodged_date=dt.date(2025, 2, 25),
        authority="Galway City Council",
    )
    apps = _apps([["2460270", "Galway City Council", "Refused", dt.date(2025, 1, 29), -9.120500, 53.273900]])
    out = _spatial_temporal_matches(residual, apps)
    assert out.height == 1
    assert out["ApplicationNumber"][0] == "2460270"


def test_tight_candidate_wins_before_a_newer_wide_candidate():
    residual = _residual(abp_case="400001", lon=-8.0, lat=53.0, lodged_date=dt.date(2025, 6, 1))
    apps = _apps(
        [
            ["TIGHT", "Cork County Council", "Refused", dt.date(2025, 4, 1), -8.0003, 53.0003],
            ["WIDE", "Cork County Council", "Granted", dt.date(2025, 5, 20), -8.0010, 53.0010],
        ]
    )
    assert _spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "TIGHT"


def test_same_date_tie_keeps_legacy_neighbour_then_source_order():
    residual = _residual(abp_case="400002", lon=-8.0008, lat=53.0008, lodged_date=dt.date(2025, 6, 1))
    apps = _apps(
        [
            # Source order alone would prefer CENTER, but the legacy grid traversal sees the
            # south-west neighbour first and therefore selects NEIGHBOUR.
            ["CENTER", "Cork County Council", "Granted", dt.date(2025, 5, 1), -8.0007, 53.0007],
            ["NEIGHBOUR", "Cork County Council", "Refused", dt.date(2025, 5, 1), -8.0013, 53.0003],
        ]
    )
    assert _spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "NEIGHBOUR"


def test_different_authority_and_post_lodgement_rows_are_excluded():
    residual = _residual(abp_case="400003", lon=-8.0, lat=53.0, lodged_date=dt.date(2025, 6, 1))
    apps = _apps(
        [
            ["WRONG-AUTH", "Galway City Council", "Granted", dt.date(2025, 5, 1), -8.0, 53.0],
            ["TOO-LATE", "Cork County Council", "Granted", dt.date(2025, 6, 2), -8.0, 53.0],
        ]
    )
    assert _spatial_temporal_matches(residual, apps).is_empty()


def test_undated_appeal_selects_geometrically_nearest_tight_candidate():
    residual = _residual(abp_case="400004", lon=-8.0, lat=53.0, lodged_date=None)
    apps = _apps(
        [
            ["FARTHER", "Cork County Council", "Granted", dt.date(2025, 5, 1), -8.0004, 53.0004],
            ["NEAREST", "Cork County Council", "Refused", dt.date(1990, 1, 1), -8.0001, 53.0001],
        ]
    )
    assert _spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "NEAREST"


def test_exact_date_and_radius_boundaries_are_inclusive():
    lodged = dt.date(2025, 6, 1)
    cutoff = lodged - dt.timedelta(days=365 * 5)
    residual = _residual(abp_case="400005", lon=0.0, lat=0.0, lodged_date=lodged)
    apps_at_cutoff = _apps([["AT-LIMITS", "Cork County Council", "Granted", cutoff, 0.0015, 0.0015]])
    assert _spatial_temporal_matches(residual, apps_at_cutoff)["ApplicationNumber"].item() == "AT-LIMITS"

    apps_on_lodgement = _apps([["ON-LODGEMENT", "Cork County Council", "Refused", lodged, 0.0006, 0.0006]])
    assert _spatial_temporal_matches(residual, apps_on_lodgement)["ApplicationNumber"].item() == "ON-LODGEMENT"


def test_one_step_outside_date_or_wide_radius_is_excluded():
    lodged = dt.date(2025, 6, 1)
    residual = _residual(abp_case="400006", lon=0.0, lat=0.0, lodged_date=lodged)
    rows = _apps(
        [
            ["TOO-OLD", "Cork County Council", "Granted", lodged - dt.timedelta(days=365 * 5 + 1), 0.0, 0.0],
            ["TOO-WIDE", "Cork County Council", "Granted", lodged, 0.001501, 0.0],
        ]
    )
    assert _spatial_temporal_matches(residual, rows).is_empty()


def test_undated_nearest_uses_both_coordinate_deltas():
    residual = _residual(abp_case="400007", lon=-8.0, lat=53.0, lodged_date=None)
    rows = _apps(
        [
            ["LON-HEAVY", "Cork County Council", "Granted", dt.date(2025, 1, 1), -8.0005, 53.0001],
            ["BALANCED", "Cork County Council", "Granted", dt.date(2025, 1, 1), -8.0003, 53.0003],
        ]
    )
    assert _spatial_temporal_matches(residual, rows)["ApplicationNumber"].item() == "BALANCED"


def test_exact_tight_boundary_stays_in_tight_band_and_is_included_when_undated():
    lodged = dt.date(2025, 6, 1)
    dated = _spatial_temporal_matches(
        _residual(abp_case="400008", lon=0.0, lat=0.0, lodged_date=lodged),
        _apps(
            [
                ["TIGHT-EXACT", "Cork County Council", "Refused", dt.date(2025, 4, 1), 0.0006, 0.0006],
                ["WIDE-NEWER", "Cork County Council", "Granted", dt.date(2025, 5, 20), 0.0010, 0.0010],
            ]
        ),
    )
    assert dated["ApplicationNumber"].item() == "TIGHT-EXACT"

    undated = _spatial_temporal_matches(
        _residual(abp_case="400009", lon=0.0, lat=0.0, lodged_date=None),
        _apps([["UNDATED-EXACT", "Cork County Council", "Refused", None, 0.0006, 0.0006]]),
    )
    assert undated["ApplicationNumber"].item() == "UNDATED-EXACT"


def test_either_empty_input_returns_typed_empty_output():
    residual = _residual(abp_case="400010", lon=0.0, lat=0.0, lodged_date=dt.date(2025, 6, 1))
    application = _apps([["A", "Cork County Council", "Granted", dt.date(2025, 5, 1), 0.0, 0.0]])
    empty_residual = residual.head(0)
    empty_apps = application.head(0)
    assert _spatial_temporal_matches(empty_residual, application).is_empty()
    assert _spatial_temporal_matches(residual, empty_apps).is_empty()


@pytest.mark.parametrize(
    ("direction", "dlat", "dlon"),
    [
        ("N", 0.0012, 0.0),
        ("S", -0.0012, 0.0),
        ("E", 0.0, 0.0012),
        ("W", 0.0, -0.0012),
        ("NE", 0.00105, 0.00105),
        ("NW", 0.00105, -0.00105),
        ("SE", -0.00105, 0.00105),
        ("SW", -0.00105, -0.00105),
    ],
)
def test_candidate_in_each_of_the_eight_neighbouring_grid_cells_is_still_found(direction, dlat, dlon):
    # The grid-cell join only reaches a candidate if the 3x3 neighbour-offset table is intact --
    # a mutated offset silently drops one direction instead of raising, so a match this project
    # relies on (a candidate application one grid cell away from its appeal) would vanish with no
    # error. GRID=0.002 puts the cell boundary at 0.001; each offset here crosses it while staying
    # inside SPATIAL_DEG_WIDE (0.0015) so the match should still succeed.
    residual = _residual(abp_case=f"grid-{direction}", lon=0.0, lat=0.0, lodged_date=dt.date(2025, 6, 1))
    apps = _apps([[f"APP-{direction}", "Cork County Council", "Granted", dt.date(2025, 5, 1), dlon, dlat]])
    out = _spatial_temporal_matches(residual, apps)
    assert out["ApplicationNumber"].to_list() == [f"APP-{direction}"]


def test_most_recent_predating_decision_wins_within_the_same_radius_band():
    # Both candidates sit in the tight band (same radius_band), so the DecisionDate sort is what
    # decides -- this is the "(most recent such)" rule the module's docstring documents.
    residual = _residual(abp_case="date-recency", lon=-8.0, lat=53.0, lodged_date=dt.date(2025, 6, 1))
    apps = _apps(
        [
            ["OLDER", "Cork County Council", "Granted", dt.date(2024, 1, 1), -8.0001, 53.0001],
            ["NEWER", "Cork County Council", "Granted", dt.date(2025, 1, 1), -8.0002, 53.0002],
        ]
    )
    assert _spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "NEWER"


def test_full_tie_falls_back_to_source_application_order():
    # Same date, same radius band, same neighbour cell -- the only remaining tiebreaker is
    # _application_order, i.e. the row's position in the source applications table.
    residual = _residual(abp_case="app-order", lon=-8.0, lat=53.0, lodged_date=dt.date(2025, 6, 1))
    apps = _apps(
        [
            ["FIRST", "Cork County Council", "Granted", dt.date(2025, 5, 1), -8.0001, 53.0001],
            ["SECOND", "Cork County Council", "Granted", dt.date(2025, 5, 1), -8.0001, 53.0001],
        ]
    )
    assert _spatial_temporal_matches(residual, apps)["ApplicationNumber"].item() == "FIRST"
