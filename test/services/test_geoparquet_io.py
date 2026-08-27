from __future__ import annotations

import json

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely
from shapely.geometry import Point, Polygon

from services.geoparquet_io import (
    GeoParquetError,
    GeoParquetSafetyError,
    atomic_convert_in_place,
    atomic_restore_backup,
    is_geoparquet,
    sha256_file,
    validate_geoparquet,
)
from services.parquet_io import save_parquet


def _wkb(geometry) -> bytes:
    return shapely.to_wkb(geometry)


def test_spatial_writer_emits_exact_geoparquet_metadata_and_covering(tmp_path):
    dest = tmp_path / "mixed.parquet"
    source = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "wkb": [
                _wkb(Point(-7.0, 53.0)),
                _wkb(Polygon([(0, 0), (2, 0), (2, 1), (0, 0)])),
                _wkb(shapely.from_wkt("POINT Z (1 2 3)")),
            ],
            "bbox_minx": [-7.1, -0.1, 0.9],
        }
    )

    save_parquet(
        source,
        dest,
        geoparquet=True,
        source_crs="EPSG:4326_XY",
        compression_level=9,
    )

    summary = validate_geoparquet(dest, deep=True)
    assert summary.row_count == 3
    assert summary.geometry_types == ("Point", "Point Z", "Polygon")
    assert summary.bbox == (-7.0, 0.0, 2.0, 53.0)
    schema = pq.ParquetFile(dest).schema_arrow
    metadata = json.loads(schema.metadata[b"geo"].decode("utf-8"))
    assert metadata["version"] == "1.1.0"
    assert metadata["primary_column"] == "wkb"
    assert metadata["columns"]["wkb"]["encoding"] == "WKB"
    assert metadata["columns"]["wkb"]["geometry_types"] == [
        "Point",
        "Point Z",
        "Polygon",
    ]
    assert schema.names == [*source.columns, "bbox"]
    assert pl.read_parquet(dest).drop("bbox").equals(source)


def test_nonspatial_writer_is_unchanged(tmp_path):
    dest = tmp_path / "fact.parquet"
    source = pl.DataFrame({"id": [1], "value": ["x"]})
    save_parquet(source, dest)
    assert not is_geoparquet(dest)
    assert pq.ParquetFile(dest).schema_arrow.names == source.columns


def test_spatial_writer_requires_explicit_axis_semantics(tmp_path):
    dest = tmp_path / "missing_crs.parquet"
    with pytest.raises(ValueError, match="source_crs"):
        save_parquet(
            pl.DataFrame({"wkb": [_wkb(Point(0, 0))]}),
            dest,
            geoparquet=True,
        )
    assert not dest.exists()


@pytest.mark.parametrize(
    "geometry, message",
    [
        (shapely.from_wkt("POINT M (1 2 3)"), "does not support M"),
        (Polygon(), "empty geometries"),
        (Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)]), "invalid geometries"),
    ],
)
def test_unsupported_geometry_preserves_previous_file(tmp_path, geometry, message):
    dest = tmp_path / "canonical.parquet"
    good = pl.DataFrame({"sentinel": [1]})
    save_parquet(good, dest)
    before = dest.read_bytes()

    with pytest.raises(GeoParquetError, match=message):
        save_parquet(
            pl.DataFrame({"wkb": [_wkb(geometry)]}),
            dest,
            geoparquet=True,
            source_crs="EPSG:4326_XY",
        )

    assert dest.read_bytes() == before
    assert not list(tmp_path.glob("*.part"))


def test_reserved_bbox_attribute_collision_is_rejected(tmp_path):
    dest = tmp_path / "collision.parquet"
    with pytest.raises(GeoParquetError, match="collides"):
        save_parquet(
            pl.DataFrame(
                {
                    "wkb": [_wkb(Point(0, 0))],
                    "bbox": [{"customer": "attribute"}],
                }
            ),
            dest,
            geoparquet=True,
            source_crs="EPSG:4326_XY",
        )
    assert not dest.exists()


def test_atomic_conversion_retains_and_restores_original(tmp_path):
    data_root = tmp_path / "data"
    rollback_root = tmp_path / "migration" / "rollback"
    source = data_root / "layer.parquet"
    source.parent.mkdir(parents=True)
    frame = pl.DataFrame({"id": [1, 2], "wkb": [_wkb(Point(-7, 53)), _wkb(Point(-6, 52))]})
    frame.write_parquet(source)
    original_sha = sha256_file(source)
    backup = rollback_root / "layer.parquet"

    receipt = atomic_convert_in_place(
        source,
        backup_path=backup,
        backup_root=rollback_root,
        allowed_roots=(data_root,),
        source_crs="EPSG:4326_XY",
        reserve_bytes=0,
        expected_bounds=(-11, 51, -5, 56),
        expected_geometry_types=("Point",),
        expected_rows=2,
        expected_original_sha256=original_sha,
    )

    assert receipt["status"] == "converted"
    assert is_geoparquet(source)
    assert sha256_file(backup) == original_sha
    assert pl.read_parquet(source).drop("bbox").equals(frame)

    converted_bytes = source.read_bytes()
    with pytest.raises(GeoParquetSafetyError, match="current canonical hash changed"):
        atomic_restore_backup(
            source,
            backup,
            expected_sha256=original_sha,
            expected_current_sha256="0" * 64,
            allowed_roots=(data_root,),
            backup_root=rollback_root,
        )
    assert source.read_bytes() == converted_bytes

    atomic_restore_backup(
        source,
        backup,
        expected_sha256=original_sha,
        expected_current_sha256=str(receipt["sha256"]),
        allowed_roots=(data_root,),
        backup_root=rollback_root,
    )
    assert sha256_file(source) == original_sha
    assert not is_geoparquet(source)
    assert backup.exists()


def test_backup_escape_fails_before_creating_outside_directory(tmp_path):
    data_root = tmp_path / "data"
    source = data_root / "layer.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"wkb": [_wkb(Point(0, 0))]}).write_parquet(source)
    rollback_root = tmp_path / "migration" / "rollback"
    outside = tmp_path / "outside" / "layer.parquet"

    with pytest.raises(GeoParquetSafetyError, match="escapes"):
        atomic_convert_in_place(
            source,
            backup_path=outside,
            backup_root=rollback_root,
            allowed_roots=(data_root,),
            source_crs="EPSG:4326_XY",
            reserve_bytes=0,
        )
    assert not outside.parent.exists()
    assert not is_geoparquet(source)


def test_frozen_row_and_geometry_contracts_fail_before_publication(tmp_path):
    data_root = tmp_path / "data"
    source = data_root / "layer.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"wkb": [_wkb(Point(0, 0))]}).write_parquet(source)
    before = source.read_bytes()
    rollback_root = tmp_path / "migration" / "rollback"

    with pytest.raises(GeoParquetError, match="geometry type contract"):
        atomic_convert_in_place(
            source,
            backup_path=rollback_root / "layer.parquet",
            backup_root=rollback_root,
            allowed_roots=(data_root,),
            source_crs="EPSG:4326_XY",
            reserve_bytes=0,
            expected_geometry_types=("Polygon",),
            expected_rows=1,
        )
    assert source.read_bytes() == before


def test_atomic_conversion_rejects_stale_audit_hash_before_backup_or_publication(tmp_path):
    data_root = tmp_path / "data"
    source = data_root / "layer.parquet"
    source.parent.mkdir(parents=True)
    pl.DataFrame({"wkb": [_wkb(Point(0, 0))]}).write_parquet(source)
    before = source.read_bytes()
    rollback_root = tmp_path / "migration" / "rollback"
    backup = rollback_root / "layer.parquet"

    with pytest.raises(GeoParquetSafetyError, match="input changed after audit"):
        atomic_convert_in_place(
            source,
            backup_path=backup,
            backup_root=rollback_root,
            allowed_roots=(data_root,),
            source_crs="EPSG:4326_XY",
            reserve_bytes=0,
            expected_original_sha256="0" * 64,
        )

    assert source.read_bytes() == before
    assert not backup.exists()


def test_atomic_conversion_rechecks_source_immediately_before_publication(monkeypatch, tmp_path):
    from services import geoparquet_io

    data_root = tmp_path / "data"
    source = data_root / "layer.parquet"
    source.parent.mkdir(parents=True)
    frame = pl.DataFrame({"id": [1], "wkb": [_wkb(Point(0, 0))]})
    frame.write_parquet(source)
    original_sha = sha256_file(source)
    backup = tmp_path / "migration" / "rollback" / "layer.parquet"
    real_compare = geoparquet_io.compare_source_payload

    def compare_then_replace_source(*args, **kwargs):
        real_compare(*args, **kwargs)
        frame.write_parquet(source, compression="uncompressed")

    monkeypatch.setattr(geoparquet_io, "compare_source_payload", compare_then_replace_source)
    with pytest.raises(GeoParquetSafetyError, match="input changed before publication"):
        atomic_convert_in_place(
            source,
            backup_path=backup,
            backup_root=backup.parent,
            allowed_roots=(data_root,),
            source_crs="EPSG:4326_XY",
            reserve_bytes=0,
            expected_original_sha256=original_sha,
        )

    assert sha256_file(source) != original_sha
    assert not is_geoparquet(source)
    assert sha256_file(backup) == original_sha


def test_restore_rejects_escaping_target_before_creating_its_lock(tmp_path):
    allowed_root = tmp_path / "allowed"
    allowed_root.mkdir()
    outside = tmp_path / "outside" / "layer.parquet"
    outside.parent.mkdir()
    pl.DataFrame({"wkb": [_wkb(Point(0, 0))]}).write_parquet(outside)
    outside_lock = outside.parent / f".{outside.name}.write.lock"

    with pytest.raises(GeoParquetSafetyError, match="escapes allowed roots"):
        atomic_restore_backup(
            outside,
            tmp_path / "rollback" / "layer.parquet",
            expected_sha256="a" * 64,
            expected_current_sha256=sha256_file(outside),
            allowed_roots=(allowed_root,),
            backup_root=tmp_path / "rollback",
        )

    assert not outside_lock.exists()


def test_validation_rejects_malformed_physical_geometry_and_bbox_types(tmp_path):
    valid = tmp_path / "valid.parquet"
    save_parquet(
        pl.DataFrame({"wkb": [_wkb(Point(-7, 53))]}),
        valid,
        geoparquet=True,
        source_crs="EPSG:4326_XY",
    )
    table = pq.read_table(valid)
    metadata = table.schema.metadata
    bad_wkb = pa.Table.from_arrays(
        [pa.array(["not-wkb"]), table["bbox"]], names=["wkb", "bbox"]
    ).replace_schema_metadata(metadata)
    bad_wkb_path = tmp_path / "bad-wkb.parquet"
    pq.write_table(bad_wkb, bad_wkb_path)
    with pytest.raises(GeoParquetError, match="physical binary"):
        validate_geoparquet(bad_wkb_path, deep=False)

    bad_bbox_type = pa.struct(
        [
            pa.field("xmin", pa.int32()),
            pa.field("ymin", pa.float32()),
            pa.field("xmax", pa.float32()),
            pa.field("ymax", pa.float32()),
        ]
    )
    bad_bbox = pa.array([(-7, 53, -7, 53)], type=bad_bbox_type)
    bad_bbox_table = pa.Table.from_arrays([table["wkb"], bad_bbox], names=["wkb", "bbox"]).replace_schema_metadata(
        metadata
    )
    bad_bbox_path = tmp_path / "bad-bbox-type.parquet"
    pq.write_table(bad_bbox_table, bad_bbox_path)
    with pytest.raises(GeoParquetError, match="floating"):
        validate_geoparquet(bad_bbox_path, deep=False)


def test_deep_validation_rejects_nonfinite_stored_bbox(tmp_path):
    valid = tmp_path / "valid.parquet"
    save_parquet(
        pl.DataFrame({"wkb": [_wkb(Point(-7, 53))]}),
        valid,
        geoparquet=True,
        source_crs="EPSG:4326_XY",
    )
    table = pq.read_table(valid)
    bad_bbox = pa.array([(-7.0, 53.0, float("inf"), 53.0)], type=table["bbox"].type)
    bad = pa.Table.from_arrays([table["wkb"], bad_bbox], names=["wkb", "bbox"]).replace_schema_metadata(
        table.schema.metadata
    )
    path = tmp_path / "bad-finite.parquet"
    pq.write_table(bad, path)
    with pytest.raises(GeoParquetError, match="non-finite"):
        validate_geoparquet(path, deep=True)
