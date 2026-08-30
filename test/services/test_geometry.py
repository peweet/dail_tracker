"""services/geometry.py — the one geometry module: GeoJSON parse, polygonal validate/repair,
point construction and the GeoParquet WKB contract, all on DuckDB spatial with no shapely."""

from __future__ import annotations

import numpy as np
import pytest
import shapely
from shapely.geometry import MultiPolygon, Point, Polygon, box, mapping, shape

import services.geometry as geometry_module
import services.spatial_sort as spatial_sort
from services.geometry import geojson_to_wkb, points_to_wkb, polygonal_geometries, validate_wkb

IRELAND_BBOX = (-11.0, 51.0, -5.0, 56.0)


def _polygon(x0: float) -> dict:
    return mapping(box(x0, 53.0, x0 + 0.01, 53.01))


def test_polygon_round_trips_to_the_same_geometry_as_shapely_shape():
    geom = _polygon(-9.0)
    [wkb] = geojson_to_wkb([geom])
    assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_multipolygon_round_trips_to_the_same_geometry_as_shapely_shape():
    geom = mapping(MultiPolygon([box(-9.0, 53.0, -8.99, 53.01), box(-8.9, 53.0, -8.89, 53.01)]))
    [wkb] = geojson_to_wkb([geom])
    assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_none_passes_through_as_none():
    out = geojson_to_wkb([_polygon(-9.0), None, _polygon(-8.5)])
    assert out[1] is None
    assert out[0] is not None
    assert out[2] is not None


def test_malformed_geometry_in_a_batch_raises():
    with pytest.raises(Exception):  # noqa: B017 - the DuckDB spatial exception type, not asserted here
        geojson_to_wkb([_polygon(-9.0), {"type": "NotAThing", "coordinates": "x"}])


def test_output_order_matches_input_order():
    geoms = [_polygon(x0) for x0 in (-10.0, -9.5, -9.0, -8.5, -8.0)]
    out = geojson_to_wkb(geoms)
    for geom, wkb in zip(geoms, out, strict=True):
        assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_result_is_2d_only():
    [wkb] = geojson_to_wkb([_polygon(-9.0)])
    assert not shapely.has_z(shapely.from_wkb(wkb))


def test_empty_input_returns_empty_list():
    assert geojson_to_wkb([]) == []


def test_all_none_input_returns_all_none_without_touching_duckdb():
    assert geojson_to_wkb([None, None]) == [None, None]


def test_single_geometry_batch_round_trips():
    # There is one path now, so a 1-row call must go through the same DuckDB round trip as a
    # 100,000-row one — no small-batch branch to fall back to.
    geom = _polygon(-9.0)
    [wkb] = geojson_to_wkb([geom])
    assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_module_does_not_import_shapely():
    # The point of the orjson swap was that this module no longer needs shapely at all; a
    # reintroduced `shape()` fallback would silently restore the vertex-count regression.
    assert not hasattr(geometry_module, "shape")
    assert not hasattr(geometry_module, "shapely")


def test_holds_a_batch_of_many_geometries():
    # Above any plausible page size, exercising the real ST_GeomFromGeoJSON call at volume.
    geoms = [_polygon(-10.0 + i * 0.001) for i in range(500)]
    out = geojson_to_wkb(geoms)
    assert len(out) == 500
    for geom, wkb in zip(geoms, out, strict=True):
        assert shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)


def test_interior_ring_survives_the_round_trip():
    # A polygon with a hole — the shape most likely to be silently flattened by a geometry
    # rewrite, and not covered by the box() fixtures above.
    outer = [(-9.0, 53.0), (-8.9, 53.0), (-8.9, 53.1), (-9.0, 53.1), (-9.0, 53.0)]
    hole = [(-8.97, 53.03), (-8.93, 53.03), (-8.93, 53.07), (-8.97, 53.07), (-8.97, 53.03)]
    geom = mapping(Polygon(outer, [hole]))
    [wkb] = geojson_to_wkb([geom])
    result = shapely.from_wkb(wkb)
    assert len(result.interiors) == 1
    assert result.equals_exact(shape(geom), tolerance=1e-9)


def test_shared_connection_is_reused_across_calls():
    con_a = geometry_module._connection_with_spatial()
    con_b = geometry_module._connection_with_spatial()
    assert con_a is con_b


def test_connection_is_independent_from_spatial_sort_connection():
    con_geojson = geometry_module._connection_with_spatial()
    con_sort = spatial_sort._connection_with_spatial()
    assert con_geojson is not con_sort


def test_concurrent_calls_from_different_threads_do_not_corrupt_results():
    # Same class of bug as services/spatial_sort.py: a shared connection registering a fixed
    # table name gives concurrent callers zero isolation, and DuckDBPyConnection is not
    # thread-safe (duckdb.org Python client docs, "each thread must have its own connection").
    import concurrent.futures

    def _call(x0: float) -> bool:
        geom = _polygon(x0)
        [wkb] = geojson_to_wkb([geom])
        return shapely.from_wkb(wkb).equals_exact(shape(geom), tolerance=1e-9)

    xs = [-10.0 + i * 0.01 for i in range(40)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_call, xs))
    assert all(results), "a concurrent call returned a corrupted geometry"


# ── polygonal_geometries: the reason ladder (provenance — see services/geometry.py) ──


@pytest.mark.parametrize(
    ("expected_reason", "value"),
    [
        ("ok", mapping(box(-9.0, 53.0, -8.99, 53.01))),
        ("empty", None),
        # A degenerate ring repairs to a Point, so this is not_polygonal in BOTH implementations
        # (checked against ingest._polygonal_geometry, not inferred from the new code).
        ("not_polygonal", {"type": "Polygon", "coordinates": [[[-9, 53], [-9, 53], [-9, 53], [-9, 53]]]}),
        ("unreadable", {"type": "NotAThing", "coordinates": "x"}),
        ("not_polygonal", {"type": "Point", "coordinates": [-6.3, 53.3]}),
        ("bounds_escape", {"type": "Polygon", "coordinates": [[[10, 10], [11, 10], [11, 11], [10, 11], [10, 10]]]}),
        ("repaired", {"type": "Polygon", "coordinates": [[[-9, 53], [-8.9, 53.1], [-8.9, 53], [-9, 53.1], [-9, 53]]]}),
    ],
)
def test_polygonal_reason_codes(expected_reason, value):
    [row] = polygonal_geometries([value], ireland_bbox=IRELAND_BBOX)
    assert row.reason == expected_reason


def test_polygonal_bad_row_is_counted_not_fatal():
    # The opposite of geojson_to_wkb: a malformed row must NOT abort the batch, because the
    # caller (a LIVE pipeline chain) counts it and continues.
    rows = polygonal_geometries(
        [mapping(box(-9.0, 53.0, -8.99, 53.01)), {"type": "NotAThing", "coordinates": "x"}],
        ireland_bbox=IRELAND_BBOX,
    )
    assert [r.reason for r in rows] == ["ok", "unreadable"]
    assert rows[0].wkb is not None and rows[1].wkb is None


def test_polygonal_preserves_input_order_and_length():
    values = [mapping(box(-9.0 + i * 0.1, 53.0, -8.99 + i * 0.1, 53.01)) for i in range(5)]
    values.insert(2, None)
    rows = polygonal_geometries(values, ireland_bbox=IRELAND_BBOX)
    assert len(rows) == len(values)
    assert rows[2].reason == "empty"
    for value, row in zip(values, rows, strict=True):
        if value is not None:
            assert shapely.from_wkb(row.wkb).equals_exact(shape(value), tolerance=1e-9)


def test_polygonal_geometrycollection_keeps_only_polygonal_members():
    collection = {
        "type": "GeometryCollection",
        "geometries": [
            mapping(box(-9.0, 53.0, -8.99, 53.01)),
            {"type": "Point", "coordinates": [-8.995, 53.005]},
        ],
    }
    [row] = polygonal_geometries([collection], ireland_bbox=IRELAND_BBOX)
    assert row.reason == "ok"
    assert shapely.from_wkb(row.wkb).equals(shape(mapping(box(-9.0, 53.0, -8.99, 53.01))))


# ── points_to_wkb ──


def test_points_are_byte_identical_to_shapely():
    lons, lats = np.array([-9.0, -8.5, -6.26]), np.array([53.0, 53.5, 53.35])
    assert points_to_wkb(lons, lats) == [shapely.to_wkb(Point(x, y)) for x, y in zip(lons, lats, strict=True)]


def test_points_empty_input_returns_empty_list():
    assert points_to_wkb(np.array([]), np.array([])) == []


def test_points_length_mismatch_raises():
    with pytest.raises(ValueError, match="length mismatch"):
        points_to_wkb(np.array([1.0, 2.0]), np.array([1.0]))


# ── validate_wkb: the GeoParquet write contract ──


def test_validate_reports_types_bounds_and_missing():
    raw = [shapely.to_wkb(box(-9.0, 53.0, -8.99, 53.01)), shapely.to_wkb(Point(-9.0, 53.0)), None]
    batch = validate_wkb(raw)
    assert batch.type_names.tolist() == ["Polygon", "Point", None]
    assert batch.missing.tolist() == [False, False, True]
    assert batch.finite.tolist() == [True, True, False]
    assert np.allclose(batch.bounds[0], [-9.0, 53.0, -8.99, 53.01])


def test_validate_bounds_match_shapely():
    geoms = [box(-9.0, 53.0, -8.99, 53.01), Point(-6.26, 53.35), MultiPolygon([box(-8.0, 54.0, -7.9, 54.1)])]
    raw = [shapely.to_wkb(g) for g in geoms]
    assert np.allclose(validate_wkb(raw).bounds, shapely.bounds(np.array(geoms, dtype=object)))


def test_validate_flags_invalid_geometry_without_raising():
    bowtie = shapely.from_wkb(
        shapely.to_wkb(shape({"type": "Polygon", "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]]}))
    )
    batch = validate_wkb([shapely.to_wkb(bowtie)])
    # Reports; policy belongs to the caller (geoparquet_io decides what is fatal).
    assert batch.valid.tolist() == [False]


def test_validate_raises_on_bytes_that_are_not_wkb():
    with pytest.raises(ValueError, match="invalid WKB"):
        validate_wkb([b"definitely not wkb"])


def test_validate_empty_input():
    batch = validate_wkb([])
    assert len(batch.bounds) == 0 and batch.type_names.tolist() == []


def test_validate_detects_z_coordinates():
    batch = validate_wkb([shapely.to_wkb(shapely.from_wkt("POINT Z (1 2 3)"))])
    assert batch.has_z.tolist() == [True]


def test_validate_preserves_row_order_at_scale():
    # validate_wkb relies on DuckDB's preserve_insertion_order rather than paying for an ORDER BY
    # on the hot path. That is an ASSUMPTION about engine behaviour, so it is pinned here: if this
    # fails, put the index column and ORDER BY back rather than reordering results.
    geoms = [box(-10.0 + i * 0.001, 53.0, -10.0 + i * 0.001 + 0.0005, 53.0005) for i in range(5000)]
    batch = validate_wkb([shapely.to_wkb(g) for g in geoms])
    assert np.array_equal(batch.bounds, shapely.bounds(np.array(geoms, dtype=object)))


def test_validate_accepts_arrow_polars_and_python_inputs_identically():
    # The Arrow path is the fast one and the one geoparquet_io uses; the others must agree with it
    # exactly, or the fast path is silently a different function.
    import polars as pl
    import pyarrow as pa

    raw = [shapely.to_wkb(box(-9.0, 53.0, -8.99, 53.01)), shapely.to_wkb(Point(-6.26, 53.35))]
    from_python = validate_wkb(raw)
    from_arrow = validate_wkb(pa.array(raw, type=pa.binary()))
    from_polars = validate_wkb(pl.Series("wkb", raw, dtype=pl.Binary))
    assert np.array_equal(from_python.bounds, from_arrow.bounds)
    assert np.array_equal(from_python.bounds, from_polars.bounds)
    assert from_python.type_names.tolist() == from_arrow.type_names.tolist()
    assert from_python.type_names.tolist() == from_polars.type_names.tolist()
