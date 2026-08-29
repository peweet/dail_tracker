"""geojson_geometries_to_wkb: size-dispatched GeoJSON -> WKB (shapely loop below
GEOJSON_WKB_ROWS_THRESHOLD, DuckDB batch at or above it — see services/geojson_wkb.py)."""

from __future__ import annotations

import pytest
import shapely
from shapely.geometry import MultiPolygon, box, mapping, shape

import services.geojson_wkb as geojson_wkb
import services.spatial_sort as spatial_sort
from services.geojson_wkb import geojson_geometries_to_wkb


def _polygon(x0: float) -> dict:
    return mapping(box(x0, 53.0, x0 + 0.01, 53.01))


@pytest.fixture
def force_duckdb_path(monkeypatch):
    # The correctness tests below use small batches for readability; force them onto the
    # DuckDB branch (the more complex path — SQL, JSON serialization, a round trip) rather than
    # silently exercising only the trivial shapely-loop branch every fixture stays under.
    monkeypatch.setattr(geojson_wkb, "GEOJSON_WKB_ROWS_THRESHOLD", 0)


def test_polygon_round_trips_to_the_same_geometry_as_shapely_shape(force_duckdb_path):
    geom = _polygon(-9.0)
    [wkb] = geojson_geometries_to_wkb([geom])
    assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_multipolygon_round_trips_to_the_same_geometry_as_shapely_shape(force_duckdb_path):
    geom = mapping(MultiPolygon([box(-9.0, 53.0, -8.99, 53.01), box(-8.9, 53.0, -8.89, 53.01)]))
    [wkb] = geojson_geometries_to_wkb([geom])
    assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_none_passes_through_as_none(force_duckdb_path):
    out = geojson_geometries_to_wkb([_polygon(-9.0), None, _polygon(-8.5)])
    assert out[1] is None
    assert out[0] is not None
    assert out[2] is not None


def test_malformed_geometry_in_a_batch_raises(force_duckdb_path):
    with pytest.raises(Exception):  # noqa: B017 - the DuckDB spatial exception type, not asserted here
        geojson_geometries_to_wkb([_polygon(-9.0), {"type": "NotAThing", "coordinates": "x"}])


def test_output_order_matches_input_order(force_duckdb_path):
    geoms = [_polygon(x0) for x0 in (-10.0, -9.5, -9.0, -8.5, -8.0)]
    out = geojson_geometries_to_wkb(geoms)
    for geom, wkb in zip(geoms, out, strict=True):
        assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_result_is_2d_only(force_duckdb_path):
    [wkb] = geojson_geometries_to_wkb([_polygon(-9.0)])
    assert not shapely.has_z(shapely.from_wkb(wkb))


def test_empty_input_returns_empty_list():
    assert geojson_geometries_to_wkb([]) == []


def test_all_none_input_returns_all_none_without_touching_duckdb():
    assert geojson_geometries_to_wkb([None, None]) == [None, None]


def test_shapely_loop_path_round_trips_correctly():
    # The below-threshold branch, exercised directly rather than inherited coverage from the
    # DuckDB-path tests above.
    geom = _polygon(-9.0)
    [wkb] = geojson_geometries_to_wkb([geom])
    assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_malformed_geometry_raises_on_the_shapely_loop_path_too():
    with pytest.raises(Exception):  # noqa: B017
        geojson_geometries_to_wkb([{"type": "NotAThing", "coordinates": "x"}])


def test_dispatches_to_shapely_loop_below_the_threshold(monkeypatch):
    calls = []
    monkeypatch.setattr(geojson_wkb, "_shapely_loop", lambda g: calls.append("shapely") or [None] * len(g))
    monkeypatch.setattr(geojson_wkb, "_duckdb_batch", lambda g, idx: calls.append("duckdb") or [None] * len(g))
    monkeypatch.setattr(geojson_wkb, "GEOJSON_WKB_ROWS_THRESHOLD", 10)
    geojson_geometries_to_wkb([_polygon(-9.0)] * 9)
    assert calls == ["shapely"]


def test_dispatches_to_duckdb_batch_at_or_above_the_threshold(monkeypatch):
    calls = []
    monkeypatch.setattr(geojson_wkb, "_shapely_loop", lambda g: calls.append("shapely") or [None] * len(g))
    monkeypatch.setattr(geojson_wkb, "_duckdb_batch", lambda g, idx: calls.append("duckdb") or [None] * len(g))
    monkeypatch.setattr(geojson_wkb, "GEOJSON_WKB_ROWS_THRESHOLD", 10)
    geojson_geometries_to_wkb([_polygon(-9.0)] * 10)
    assert calls == ["duckdb"]


def test_takes_the_real_duckdb_branch_at_full_scale():
    # No monkeypatching: exercises the actual GEOJSON_WKB_ROWS_THRESHOLD boundary and the real
    # ST_GeomFromGeoJSON call, not a mocked one.
    n = geojson_wkb.GEOJSON_WKB_ROWS_THRESHOLD
    geoms = [_polygon(-10.0 + i * 0.001) for i in range(n)]
    out = geojson_geometries_to_wkb(geoms)
    assert len(out) == n
    for geom, wkb in zip(geoms, out, strict=True):
        assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_shared_connection_is_reused_across_calls():
    con_a = geojson_wkb._connection_with_spatial()
    con_b = geojson_wkb._connection_with_spatial()
    assert con_a is con_b


def test_connection_is_independent_from_spatial_sort_connection():
    con_geojson = geojson_wkb._connection_with_spatial()
    con_sort = spatial_sort._connection_with_spatial()
    assert con_geojson is not con_sort
