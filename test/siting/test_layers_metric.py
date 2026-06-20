"""Regression test for the longitude-compression distance bug in layers._metric_dist_m.

Before the fix, `nearest*()` reported `deg * 111_320`, which over-stated east–west distance by
~1/cos(lat) (≈1.66× at Irish latitudes) — a road truly 100 m east measured ~167 m and so failed
the 150 m road-sightline threshold. The fix scales the lon component by cos(lat).
"""

from __future__ import annotations

import math

import polars as pl
import shapely
from shapely.geometry import Point

from dail_tracker_core.siting.layers import _M_PER_DEG_LAT, LayerStore, _metric_dist_m

_LAT = 53.3  # Galway-ish; cos(lat) ≈ 0.6, so the lon bug is large here


def test_east_and_north_100m_both_measure_100m():
    cos = math.cos(math.radians(_LAT))
    origin = Point(-9.0, _LAT)
    east = Point(-9.0 + 100 / (_M_PER_DEG_LAT * cos), _LAT)  # 100 m due east
    north = Point(-9.0, _LAT + 100 / _M_PER_DEG_LAT)  # 100 m due north

    assert math.isclose(_metric_dist_m(origin, east, _LAT), 100.0, abs_tol=1.0)
    assert math.isclose(_metric_dist_m(origin, north, _LAT), 100.0, abs_tol=1.0)


def test_east_not_overstated_like_the_old_bug():
    cos = math.cos(math.radians(_LAT))
    origin = Point(-9.0, _LAT)
    east = Point(-9.0 + 100 / (_M_PER_DEG_LAT * cos), _LAT)
    old_buggy = origin.distance(east) * _M_PER_DEG_LAT  # the pre-fix formula
    assert old_buggy > 150  # the bug pushed a 100 m-east feature past the 150 m road threshold
    assert _metric_dist_m(origin, east, _LAT) < 105  # the fix brings it back to ~100 m


def test_point_inside_geometry_is_zero():
    origin = Point(-9.0, _LAT)
    assert _metric_dist_m(origin, origin, _LAT) == 0.0


def test_nearest_selects_metric_not_degree(tmp_path):
    """A road 100 m EAST must beat one 160 m NORTH — degree-space ranks the north one closer, so
    the old tree.nearest would have returned 160 m (> 150 m → road trigger wrongly silent)."""
    cos = math.cos(math.radians(_LAT))
    lon0 = -9.0
    north = (lon0, _LAT + 160 / _M_PER_DEG_LAT)  # 160 m N — smaller DEGREE distance
    east = (lon0 + 100 / (_M_PER_DEG_LAT * cos), _LAT)  # 100 m E — smaller METRIC distance
    geoms = shapely.points([north[0], east[0]], [north[1], east[1]])
    pl.DataFrame({"name": ["N160", "E100"], "wkb": shapely.to_wkb(geoms)}).write_parquet(tmp_path / "roadtest.parquet")
    attrs, dist = LayerStore(tmp_path).nearest("roadtest", lon0, _LAT)
    assert attrs["name"] == "E100"  # metric-nearest, not degree-nearest
    assert math.isclose(dist, 100.0, abs_tol=1.5)
    assert dist <= 150  # → road trigger fires (it would not pre-fix)
