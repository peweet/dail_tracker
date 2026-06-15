"""DEM tests — tile naming (pure) + a live range-read (skipped if S3 unreachable)."""

from __future__ import annotations

import pytest

from dail_tracker_core.siting.dem import Terrain, terrain, tile_url


def test_tile_url_galway_sw_corner():
    # Galway (53.30, -9.05) -> SW integer corner N53 / W010 (floor of -9.05 is -10)
    u = tile_url(-9.05, 53.30)
    assert "Copernicus_DSM_COG_10_N53_00_W010_00_DEM.tif" in u
    assert u.startswith("https://copernicus-dem-30m.s3")


def test_tile_url_east_positive_lon():
    assert "E001" in tile_url(1.5, 52.0)  # SW corner E001


def _live():
    return terrain(-9.049, 53.272)  # Galway city centre


@pytest.mark.skipif(not _live().ok, reason="Copernicus S3 unreachable (offline)")
def test_terrain_galway_city_is_low_and_not_exposed():
    t = _live()
    assert t.ok
    assert 0 <= t.elevation_m < 40       # Galway city sits near sea level
    assert t.exposed is False            # flat coastal city, not a prominent hill
    assert t.slope_deg is not None


def test_terrain_degrades_gracefully_offline_shape():
    # whatever the network state, terrain() returns a Terrain and never raises
    t = terrain(-9.049, 53.272)
    assert isinstance(t, Terrain)
    assert (t.ok and t.elevation_m is not None) or (not t.ok and t.elevation_m is None)
