"""GeoParquet 1.1 WKB conversion, validation, and recoverable publication.

The ordinary :mod:`services.parquet_io` writer remains the authority for
non-spatial tables. Spatial producers opt in there with ``geoparquet=True``;
their raw Parquet part is converted here before the atomic promotion. The
converter is deliberately bounded-memory and two-pass: the first pass derives
the exact geometry-type/file-bounds metadata and the second writes row bboxes.

GeoParquet conversion never changes the WKB or any source attribute column. It
adds/replaces only the standard root ``bbox`` struct and the UTF-8 ``geo``
footer metadata. In-place migration keeps a hard-linked original before the
validated replacement is published.

Geometry is written PLAIN, never dictionary-encoded — policy
``plain-wkb-v1``, enforced by :func:`require_plain_geometry` on every validated
file, not only on ones this writer produced. WKB values are unique and large, so
a dictionary never pays for itself and costs every windowed reader a whole
dictionary-page decode before it can skip a row. Attribute columns keep
pyarrow's default, which is worth having for short repeated strings.
"""

from __future__ import annotations

import hashlib
import itertools
import json
import math
import os
import shutil
import tempfile
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .parquet_io import destination_write_lock

GEOPARQUET_VERSION = "1.1.0"
DEFAULT_GEOMETRY_COLUMN = "wkb"
DEFAULT_BBOX_COLUMN = "bbox"
DEFAULT_BATCH_ROWS = 32_768
DEFAULT_RESERVE_BYTES = 64 * 1024**3

# The one name for "geometry is stored PLAIN, never dictionary-encoded". The writer, the
# validator and the tests all resolve the policy through these two constants rather than
# each spelling the pyarrow encoding string themselves.
GEOMETRY_ENCODING_POLICY = "plain-wkb-v1"
_DICTIONARY_ENCODING = "RLE_DICTIONARY"

_TYPE_NAMES = {
    0: "Point",
    1: "LineString",
    2: "LineString",  # Shapely LinearRing; WKB has no distinct ring type.
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}
_TYPE_ORDER = {name: i for i, name in enumerate(dict.fromkeys(_TYPE_NAMES.values()))}


class GeoParquetError(ValueError):
    """A spatial file cannot safely satisfy the GeoParquet contract."""


class GeoParquetSafetyError(RuntimeError):
    """A recoverable migration safety precondition failed."""


@dataclass(frozen=True)
class GeometrySummary:
    row_count: int
    non_null_count: int
    geometry_types: tuple[str, ...]
    bbox: tuple[float, float, float, float] | None


class _SummaryAccumulator:
    def __init__(self) -> None:
        self.row_count = 0
        self.non_null_count = 0
        self.geometry_types: set[str] = set()
        self.bbox: list[float] | None = None

    def add(self, values) -> tuple[Any, Any, Any]:
        import numpy as np
        import shapely

        raw = np.asarray(values.to_pylist(), dtype=object)
        self.row_count += len(raw)
        try:
            geoms = shapely.from_wkb(raw, on_invalid="raise")
        except Exception as exc:  # GEOSException differs across Shapely releases.
            raise GeoParquetError(f"invalid WKB geometry: {exc}") from exc

        missing = np.asarray(shapely.is_missing(geoms), dtype=bool)
        if bool(missing.any()):
            raise GeoParquetError("null geometries are not permitted in canonical spatial datasets")
        present = ~missing
        self.non_null_count += int(present.sum())
        if present.any():
            empty = np.asarray(shapely.is_empty(geoms), dtype=bool)
            if bool(empty[present].any()):
                raise GeoParquetError("empty geometries are not permitted in canonical spatial datasets")
            valid = np.asarray(shapely.is_valid(geoms), dtype=bool)
            if not bool(valid[present].all()):
                raise GeoParquetError("invalid geometries must be repaired or quarantined before conversion")
            has_m = np.asarray(shapely.has_m(geoms), dtype=bool)
            if bool(has_m[present].any()):
                raise GeoParquetError("GeoParquet 1.1 WKB does not support M/ZM coordinates; refusing lossy conversion")
            has_z = np.asarray(shapely.has_z(geoms), dtype=bool)
            type_ids = np.asarray(shapely.get_type_id(geoms), dtype=int)
            for type_id, is_z in zip(type_ids[present], has_z[present], strict=True):
                base = _TYPE_NAMES.get(int(type_id))
                if base is None:
                    raise GeoParquetError(f"unsupported WKB geometry type id {int(type_id)}")
                self.geometry_types.add(f"{base} Z" if bool(is_z) else base)

        bounds = np.asarray(shapely.bounds(geoms), dtype="float64")
        finite = present & np.isfinite(bounds).all(axis=1)
        if not bool(finite.all()):
            raise GeoParquetError("every canonical geometry must have finite XY bounds")
        if finite.any():
            batch_bbox = [
                float(np.min(bounds[finite, 0])),
                float(np.min(bounds[finite, 1])),
                float(np.max(bounds[finite, 2])),
                float(np.max(bounds[finite, 3])),
            ]
            if self.bbox is None:
                self.bbox = batch_bbox
            else:
                self.bbox = [
                    min(self.bbox[0], batch_bbox[0]),
                    min(self.bbox[1], batch_bbox[1]),
                    max(self.bbox[2], batch_bbox[2]),
                    max(self.bbox[3], batch_bbox[3]),
                ]
        return geoms, bounds, finite

    def finish(self) -> GeometrySummary:
        def key(value: str) -> tuple[int, int]:
            base = value.removesuffix(" Z")
            return (_TYPE_ORDER.get(base, 999), int(value.endswith(" Z")))

        bbox = None if self.bbox is None else (self.bbox[0], self.bbox[1], self.bbox[2], self.bbox[3])
        return GeometrySummary(
            row_count=self.row_count,
            non_null_count=self.non_null_count,
            geometry_types=tuple(sorted(self.geometry_types, key=key)),
            bbox=bbox,
        )


def _crs84_projjson() -> dict:
    from pyproj import CRS

    # pyproj supplies a standards-complete PROJJSON object and the longitude,
    # latitude axis order required by WKB/GeoParquet.
    return CRS.from_user_input("OGC:CRS84").to_json_dict()


def _canonical_crs(source_crs: str) -> dict:
    if source_crs not in {"OGC:CRS84", "EPSG:4326_XY"}:
        raise GeoParquetError("source CRS/axis semantics must be explicitly OGC:CRS84 or EPSG:4326_XY")
    return _crs84_projjson()


def _bbox_field():
    import pyarrow as pa

    return pa.field(
        DEFAULT_BBOX_COLUMN,
        pa.struct(
            [
                pa.field("xmin", pa.float32()),
                pa.field("ymin", pa.float32()),
                pa.field("xmax", pa.float32()),
                pa.field("ymax", pa.float32()),
            ]
        ),
        nullable=True,
    )


def _metadata_payload(
    summary: GeometrySummary,
    *,
    geometry_column: str,
    bbox_column: str = DEFAULT_BBOX_COLUMN,
    crs: dict | None = None,
) -> dict:
    column: dict[str, object] = {
        "encoding": "WKB",
        "geometry_types": list(summary.geometry_types),
        "crs": _crs84_projjson() if crs is None else crs,
        "covering": {
            "bbox": {
                "xmin": [bbox_column, "xmin"],
                "ymin": [bbox_column, "ymin"],
                "xmax": [bbox_column, "xmax"],
                "ymax": [bbox_column, "ymax"],
            }
        },
    }
    if summary.bbox is not None:
        column["bbox"] = list(summary.bbox)
    return {
        "version": GEOPARQUET_VERSION,
        "primary_column": geometry_column,
        "columns": {geometry_column: column},
    }


def _output_schema(
    source_schema,
    summary: GeometrySummary,
    *,
    geometry_column: str,
    source_crs: str,
):
    import pyarrow as pa

    geometry_index = source_schema.get_field_index(geometry_column)
    if geometry_index < 0:
        raise GeoParquetError(f"missing geometry column {geometry_column!r}")
    geometry_type = source_schema.field(geometry_index).type
    if not (pa.types.is_binary(geometry_type) or pa.types.is_large_binary(geometry_type)):
        raise GeoParquetError(f"{geometry_column!r} must be Arrow binary WKB, found {geometry_type}")

    bbox = _bbox_field()
    fields = list(source_schema)
    bbox_index = source_schema.get_field_index(DEFAULT_BBOX_COLUMN)
    if bbox_index >= 0:
        raise GeoParquetError("source column 'bbox' collides with the reserved GeoParquet covering column")
    fields.append(bbox)

    metadata = dict(source_schema.metadata or {})
    metadata[b"geo"] = json.dumps(
        _metadata_payload(
            summary,
            geometry_column=geometry_column,
            crs=_canonical_crs(source_crs),
        ),
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return pa.schema(fields, metadata=metadata)


def _outward_float32(values, *, positive: bool):
    import numpy as np

    cast = np.asarray(values, dtype="float64").astype("float32")
    toward = np.float32(math.inf if positive else -math.inf)
    expanded = np.nextafter(cast, toward, dtype=np.float32)
    if not bool(np.isfinite(expanded).all()):
        raise GeoParquetError("geometry bounds overflow the float32 bbox covering")
    return expanded


def _bbox_array(bounds, finite):
    import numpy as np
    import pyarrow as pa

    count = len(bounds)
    children = []
    for index, positive in ((0, False), (1, False), (2, True), (3, True)):
        values = np.full(count, np.nan, dtype="float32")
        if bool(finite.any()):
            values[finite] = _outward_float32(bounds[finite, index], positive=positive)
        children.append(pa.array(values, type=pa.float32()))
    return pa.StructArray.from_arrays(
        children,
        fields=list(_bbox_field().type),
        mask=pa.array(~finite),
    )


def _with_bbox(batch, *, geometry_column: str):
    accumulator = _SummaryAccumulator()
    _, bounds, finite = accumulator.add(batch.column(batch.schema.get_field_index(geometry_column)))
    bbox = _bbox_array(bounds, finite)
    bbox_index = batch.schema.get_field_index(DEFAULT_BBOX_COLUMN)
    if bbox_index >= 0:
        return batch.set_column(bbox_index, _bbox_field(), bbox)
    return batch.append_column(_bbox_field(), bbox)


def summarize_parquet(
    path: str | Path,
    *,
    geometry_column: str = DEFAULT_GEOMETRY_COLUMN,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> GeometrySummary:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(Path(path))
    try:
        if parquet.schema_arrow.get_field_index(geometry_column) < 0:
            raise GeoParquetError(f"{Path(path).name}: missing geometry column {geometry_column!r}")
        summary = _SummaryAccumulator()
        for batch in parquet.iter_batches(columns=[geometry_column], batch_size=batch_rows):
            summary.add(batch.column(0))
        result = summary.finish()
        if result.row_count != parquet.metadata.num_rows:
            raise GeoParquetError(
                f"{Path(path).name}: geometry scan read {result.row_count} of {parquet.metadata.num_rows} rows"
            )
        return result
    finally:
        parquet.close()


def dictionary_columns(names: list[str], geometry_column: str) -> list[str]:
    """The ``use_dictionary`` allow-list for a schema: every column except the geometry one.

    Split out as a pure function so the encoding policy is checkable without writing a file
    — CrossHair proves the postconditions below exhaustively in
    ``test/services/test_geoparquet_geometry_encoding.py``. The third postcondition is the
    one that matters: membership is exactly "is not the geometry column", so no attribute
    column can be silently dropped from dictionary encoding by a future edit.

    pre: len(names) <= 4
    post: geometry_column not in __return__
    post: all((name in __return__) == (name != geometry_column) for name in names)
    post: len(__return__) == len(names) - names.count(geometry_column)
    post: __return__ == [n for n in names if n in __return__]
    """
    return [name for name in names if name != geometry_column]


def geometry_encodings(path: str | Path, geometry_column: str = DEFAULT_GEOMETRY_COLUMN) -> frozenset[str]:
    """Every Parquet encoding used by ``geometry_column`` across all row groups of ``path``.

    Footer metadata only — no row data is read. This is the single definition of what the
    writer produced that the validator and the tests both interrogate; before it, each would
    have had to reach into ``ParquetFile.metadata`` and interpret the footer independently.
    """
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(str(path))
    try:
        metadata = parquet.metadata
        if metadata.num_row_groups == 0:
            return frozenset()
        first = metadata.row_group(0)
        names = [first.column(i).path_in_schema for i in range(first.num_columns)]
        if geometry_column not in names:
            raise GeoParquetError(f"{Path(path).name}: no {geometry_column!r} column to inspect")
        index = names.index(geometry_column)
        found: set[str] = set()
        for group in range(metadata.num_row_groups):
            found.update(str(encoding) for encoding in metadata.row_group(group).column(index).encodings)
        return frozenset(found)
    finally:
        parquet.close()


def require_plain_geometry(path: str | Path, geometry_column: str = DEFAULT_GEOMETRY_COLUMN) -> None:
    """Refuse a file whose geometry column is dictionary-encoded in any row group.

    Policy ``plain-wkb-v1``. WKB values are unique and large, so a dictionary never pays for
    itself, and a reader must decode a row group's dictionary pages whole before it can skip
    a single row — which costs 2-4x on every windowed read. Raises
    :class:`GeoParquetSafetyError`, the same fail-closed class as the other artifact checks
    here, because a file that reaches this point has already been written.
    """
    encodings = geometry_encodings(path, geometry_column=geometry_column)
    if _DICTIONARY_ENCODING in encodings:
        raise GeoParquetSafetyError(
            f"{Path(path).name}: {geometry_column!r} is dictionary-encoded ({sorted(encodings)}); "
            f"policy {GEOMETRY_ENCODING_POLICY} requires PLAIN — rebuild the file"
        )


def convert_parquet_file(
    source: str | Path,
    output: str | Path,
    *,
    source_crs: str,
    geometry_column: str = DEFAULT_GEOMETRY_COLUMN,
    expected_bounds: tuple[float, float, float, float] | None = None,
    expected_geometry_types: Sequence[str] | None = None,
    expected_rows: int | None = None,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    compression: str = "zstd",
    compression_level: int = 3,
) -> GeometrySummary:
    """Convert ``source`` to GeoParquet at a distinct ``output`` path.

    The caller owns atomic publication. A failed conversion removes ``output``.
    Source row order and every non-``bbox`` Arrow value are preserved.
    """
    import pyarrow.parquet as pq

    source = Path(source)
    output = Path(output)
    if source.resolve() == output.resolve():
        raise GeoParquetSafetyError("conversion output must differ from its source")
    output.parent.mkdir(parents=True, exist_ok=True)
    summary = summarize_parquet(source, geometry_column=geometry_column, batch_rows=batch_rows)
    if expected_rows is not None and summary.row_count != expected_rows:
        raise GeoParquetError(f"row contract changed: {summary.row_count} != {expected_rows}")
    if expected_geometry_types is not None and summary.geometry_types != tuple(expected_geometry_types):
        raise GeoParquetError(
            f"geometry type contract changed: {summary.geometry_types!r} != {tuple(expected_geometry_types)!r}"
        )
    if expected_bounds is not None:
        if summary.bbox is None:
            raise GeoParquetError("expected a non-empty file bbox")
        xmin, ymin, xmax, ymax = summary.bbox
        exmin, eymin, exmax, eymax = expected_bounds
        if xmin < exmin or ymin < eymin or xmax > exmax or ymax > eymax:
            raise GeoParquetError(f"geometry bbox {summary.bbox!r} escapes expected bounds {expected_bounds!r}")
    parquet = pq.ParquetFile(source)
    schema = _output_schema(
        parquet.schema_arrow,
        summary,
        geometry_column=geometry_column,
        source_crs=source_crs,
    )
    writer = None
    # Never dictionary-encode the geometry column. pyarrow's ``use_dictionary`` defaults to
    # True for *every* column; WKB values are unique and large, so the dictionary page fills,
    # the writer falls back to PLAIN mid-row-group, and every reader thereafter has to decode
    # that group's dictionary pages before it can skip a single row. Measured on the box with
    # identical rows and row groups: a 2 km window cost 8.0 ms against a PLAIN sibling, 18.0 ms
    # against this writer's default output, 13.9 ms once rewritten without a dictionary; on a
    # one-row-group file the loss of page skipping turned a 7 ms read into 228 ms. Attribute
    # columns keep the default — short repeated strings genuinely do benefit.
    #
    # The allow-list holds top-level field names, while pyarrow keys dictionary decisions on
    # leaf paths, so a struct field's leaves ("bbox.xmin", ...) match nothing here and are
    # written PLAIN. That is the outcome we want for the bbox covering — near-unique float32
    # values a dictionary would only pad, measured 35.7 -> 35.3 MB on 60k polygons — but it
    # would also silently disable dictionaries on a future low-cardinality struct attribute.
    # test_geometry_encoding_map_is_exact pins the whole map so that drift is caught, not
    # discovered.
    allow_dictionary = dictionary_columns(list(schema.names), geometry_column)
    try:
        writer = pq.ParquetWriter(
            output,
            schema,
            compression=compression,
            compression_level=compression_level,
            write_statistics=True,
            # pyarrow documents this parameter as "bool or list, default True"; its type stub
            # narrows it to bool, so the per-column form has to be asserted here rather than
            # checked. test_geometry_encoding_map_is_exact reads the resulting footer back.
            use_dictionary=allow_dictionary,  # type: ignore[arg-type]
        )
        for row_group in range(parquet.metadata.num_row_groups):
            for batch in parquet.iter_batches(row_groups=[row_group], batch_size=batch_rows):
                enriched = _with_bbox(batch, geometry_column=geometry_column)
                writer.write_batch(enriched, row_group_size=max(enriched.num_rows, 1))
        writer.close()
        writer = None
    except BaseException:
        if writer is not None:
            writer.close()
        output.unlink(missing_ok=True)
        raise
    finally:
        parquet.close()
    return summary


def _read_geo_metadata(path: str | Path) -> tuple[Any, dict[str, Any]]:
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(Path(path))
    try:
        raw = (parquet.schema_arrow.metadata or {}).get(b"geo")
        if raw is None:
            raise GeoParquetError(f"{Path(path).name}: missing GeoParquet 'geo' footer metadata")
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise GeoParquetError(f"{Path(path).name}: invalid UTF-8 GeoParquet metadata") from exc
        if not isinstance(payload, dict):
            raise GeoParquetError(f"{Path(path).name}: GeoParquet metadata must be a JSON object")
        return parquet, payload
    except BaseException:
        parquet.close()
        raise


def is_geoparquet(path: str | Path) -> bool:
    try:
        parquet, _ = _read_geo_metadata(path)
    except (GeoParquetError, OSError):
        return False
    parquet.close()
    return True


def validate_geoparquet(
    path: str | Path,
    *,
    geometry_column: str = DEFAULT_GEOMETRY_COLUMN,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    deep: bool = True,
) -> GeometrySummary:
    """Validate footer, exact geometry census, bbox-superset safety, and encoding policy.

    The encoding gate runs here rather than only in :func:`convert_parquet_file` so that a
    producer handing in an already-GeoParquet part is covered too: that branch skips
    conversion entirely, so a dictionary-encoded file would otherwise publish unexamined.

    It runs *last*, after the structural checks. A file whose geometry column is not valid
    WKB has a more fundamental problem than how that column is encoded, and reporting the
    structural fault is what tells a caller what to actually fix.
    """
    path = Path(path)
    parquet, payload = _read_geo_metadata(path)
    try:
        summary = _validate_open_geoparquet(
            path,
            parquet,
            payload,
            geometry_column=geometry_column,
            batch_rows=batch_rows,
            deep=deep,
        )
    finally:
        parquet.close()
    require_plain_geometry(path, geometry_column=geometry_column)
    return summary


def _validate_open_geoparquet(
    path: Path,
    parquet,
    payload: dict,
    *,
    geometry_column: str,
    batch_rows: int,
    deep: bool,
) -> GeometrySummary:
    import numpy as np
    import pyarrow as pa

    if payload.get("version") != GEOPARQUET_VERSION:
        raise GeoParquetError(f"{path.name}: expected GeoParquet {GEOPARQUET_VERSION}")
    if payload.get("primary_column") != geometry_column:
        raise GeoParquetError(f"{path.name}: unexpected primary geometry column")
    column_meta = (payload.get("columns") or {}).get(geometry_column)
    if not isinstance(column_meta, dict) or column_meta.get("encoding") != "WKB":
        raise GeoParquetError(f"{path.name}: missing WKB geometry-column metadata")
    if not isinstance(column_meta.get("crs"), dict):
        raise GeoParquetError(f"{path.name}: explicit PROJJSON CRS is required")
    from pyproj import CRS

    try:
        recorded_crs = CRS.from_json_dict(column_meta["crs"])
    except Exception as exc:
        raise GeoParquetError(f"{path.name}: invalid PROJJSON CRS") from exc
    if not recorded_crs.equals(CRS.from_user_input("OGC:CRS84"), ignore_axis_order=False):
        raise GeoParquetError(f"{path.name}: CRS must be longitude/latitude OGC:CRS84")

    geometry_index = parquet.schema_arrow.get_field_index(geometry_column)
    if geometry_index < 0:
        raise GeoParquetError(f"{path.name}: missing geometry column {geometry_column!r}")
    geometry_type = parquet.schema_arrow.field(geometry_index).type
    if not (pa.types.is_binary(geometry_type) or pa.types.is_large_binary(geometry_type)):
        raise GeoParquetError(f"{path.name}: WKB geometry column must be a physical binary type")

    bbox_index = parquet.schema_arrow.get_field_index(DEFAULT_BBOX_COLUMN)
    if bbox_index < 0:
        raise GeoParquetError(f"{path.name}: missing root bbox covering column")
    bbox_type = parquet.schema_arrow.field(bbox_index).type
    if not pa.types.is_struct(bbox_type) or [field.name for field in bbox_type] != [
        "xmin",
        "ymin",
        "xmax",
        "ymax",
    ]:
        raise GeoParquetError(f"{path.name}: bbox must be a xmin/ymin/xmax/ymax struct")
    if not all(pa.types.is_float32(field.type) or pa.types.is_float64(field.type) for field in bbox_type):
        raise GeoParquetError(f"{path.name}: bbox children must be floating-point values")
    expected_covering = _metadata_payload(GeometrySummary(0, 0, (), None), geometry_column=geometry_column)["columns"][
        geometry_column
    ]["covering"]
    if column_meta.get("covering") != expected_covering:
        raise GeoParquetError(f"{path.name}: invalid GeoParquet bbox covering paths")

    if not deep:
        return GeometrySummary(
            row_count=parquet.metadata.num_rows,
            non_null_count=-1,
            geometry_types=tuple(column_meta.get("geometry_types") or ()),
            bbox=tuple(column_meta["bbox"]) if "bbox" in column_meta else None,
        )

    summary = _SummaryAccumulator()
    for batch in parquet.iter_batches(columns=[geometry_column, DEFAULT_BBOX_COLUMN], batch_size=batch_rows):
        _, bounds, finite = summary.add(batch.column(0))
        bbox = batch.column(1)
        nulls = np.asarray(bbox.is_null(), dtype=bool)
        if not np.array_equal(nulls, ~finite):
            raise GeoParquetError(f"{path.name}: bbox nullability does not match geometry bounds")
        if bool(finite.any()):
            xmin = np.asarray(bbox.field("xmin"), dtype="float64")
            ymin = np.asarray(bbox.field("ymin"), dtype="float64")
            xmax = np.asarray(bbox.field("xmax"), dtype="float64")
            ymax = np.asarray(bbox.field("ymax"), dtype="float64")
            if not (
                np.isfinite(xmin[finite]).all()
                and np.isfinite(ymin[finite]).all()
                and np.isfinite(xmax[finite]).all()
                and np.isfinite(ymax[finite]).all()
            ):
                raise GeoParquetError(f"{path.name}: stored bbox contains non-finite values")
            if not (
                (xmin[finite] <= bounds[finite, 0]).all()
                and (ymin[finite] <= bounds[finite, 1]).all()
                and (xmax[finite] >= bounds[finite, 2]).all()
                and (ymax[finite] >= bounds[finite, 3]).all()
            ):
                raise GeoParquetError(f"{path.name}: bbox does not bracket its WKB geometry")

    result = summary.finish()
    if result.row_count != parquet.metadata.num_rows:
        raise GeoParquetError(f"{path.name}: row-count mismatch during validation")
    if tuple(column_meta.get("geometry_types") or ()) != result.geometry_types:
        raise GeoParquetError(f"{path.name}: geometry_types metadata is not an exact census")
    recorded_bbox = tuple(column_meta["bbox"]) if "bbox" in column_meta else None
    if recorded_bbox != result.bbox:
        raise GeoParquetError(f"{path.name}: file bbox metadata does not match WKB bounds")
    return result


def compare_source_payload(
    source: str | Path,
    target: str | Path,
    *,
    batch_rows: int = DEFAULT_BATCH_ROWS,
) -> None:
    """Prove all source columns except the regenerated ``bbox`` are identical."""
    import pyarrow.parquet as pq

    source_file = pq.ParquetFile(Path(source))
    target_file = pq.ParquetFile(Path(target))
    try:
        source_names = list(source_file.schema_arrow.names)
        expected_target_names = (
            source_names if DEFAULT_BBOX_COLUMN in source_names else [*source_names, DEFAULT_BBOX_COLUMN]
        )
        if list(target_file.schema_arrow.names) != expected_target_names:
            raise GeoParquetSafetyError("payload schema changed outside the regenerated 'bbox' covering column")
        columns = [name for name in source_names if name != DEFAULT_BBOX_COLUMN]
        for name in columns:
            index = target_file.schema_arrow.get_field_index(name)
            if index < 0 or target_file.schema_arrow.field(index).type != source_file.schema_arrow.field(name).type:
                raise GeoParquetSafetyError(f"payload schema changed at column {name!r}")
        source_batches = source_file.iter_batches(columns=columns, batch_size=batch_rows)
        target_batches = target_file.iter_batches(columns=columns, batch_size=batch_rows)
        for index, (before, after) in enumerate(itertools.zip_longest(source_batches, target_batches, fillvalue=None)):
            if before is None or after is None or not before.equals(after):
                raise GeoParquetSafetyError(f"payload parity failed at batch {index}")
    finally:
        source_file.close()
        target_file.close()


def sha256_file(path: str | Path, *, chunk_bytes: int = 8 * 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(chunk_bytes), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _contained(path: Path, roots: Sequence[Path]) -> Path:
    resolved = path.resolve(strict=True)
    for root in roots:
        try:
            resolved.relative_to(root.resolve(strict=True))
            return resolved
        except ValueError:
            continue
    raise GeoParquetSafetyError(f"migration target escapes allowed roots: {resolved}")


def _unique_sibling(path: Path, suffix: str) -> Path:
    descriptor, value = tempfile.mkstemp(prefix=f".{path.name}.", suffix=suffix, dir=path.parent)
    os.close(descriptor)
    result = Path(value)
    result.unlink()
    return result


def _safe_backup_path(path: str | Path, *, backup_root: str | Path, source: Path) -> Path:
    root = Path(backup_root)
    candidate = Path(path)
    if not candidate.is_absolute():
        candidate = root / candidate
    lexical_root = Path(os.path.abspath(root))
    lexical_candidate = Path(os.path.abspath(candidate))
    try:
        lexical_candidate.relative_to(lexical_root)
    except ValueError as exc:
        raise GeoParquetSafetyError(f"rollback path escapes dedicated root: {lexical_candidate}") from exc

    # Validate every existing component before mkdir.  Otherwise a symlinked
    # parent could redirect a supposedly dedicated rollback path outside it.
    probe = lexical_candidate
    while not probe.exists() and probe != probe.parent:
        probe = probe.parent
    while True:
        if probe.is_symlink():
            raise GeoParquetSafetyError("rollback path must not traverse symlinks")
        if probe == probe.parent:
            break
        probe = probe.parent
    root_probe = lexical_root
    while not root_probe.exists() and root_probe != root_probe.parent:
        root_probe = root_probe.parent
    while True:
        if root_probe.is_symlink():
            raise GeoParquetSafetyError("rollback root must not be a symlink")
        if root_probe == root_probe.parent:
            break
        root_probe = root_probe.parent

    root.mkdir(parents=True, exist_ok=True)
    candidate = lexical_candidate
    candidate.parent.mkdir(parents=True, exist_ok=True)
    probe = candidate
    while probe != probe.parent:
        if probe.is_symlink():
            raise GeoParquetSafetyError("rollback path must not traverse symlinks")
        probe = probe.parent
    resolved_root = root.resolve(strict=True)
    resolved = candidate.resolve(strict=False)
    try:
        resolved.relative_to(resolved_root)
    except ValueError as exc:
        raise GeoParquetSafetyError(f"rollback path escapes dedicated root: {resolved}") from exc
    if resolved == source:
        raise GeoParquetSafetyError("rollback path must differ from the canonical source")
    return resolved


def _fsync_directory(path: Path) -> None:
    """Durably publish a POSIX directory entry or fail closed.

    Windows publication uses ``MoveFileExW(..., MOVEFILE_WRITE_THROUGH)`` in
    the helpers below because Python cannot portably fsync a directory handle.
    """

    if os.name == "nt":
        raise GeoParquetSafetyError("Windows directory durability requires a write-through move")
    try:
        descriptor = os.open(path, os.O_RDONLY)
    except OSError as exc:
        raise GeoParquetSafetyError(f"cannot open directory for durability flush: {path}") from exc
    try:
        os.fsync(descriptor)
    except OSError as exc:
        raise GeoParquetSafetyError(f"cannot flush directory entry durability: {path}") from exc
    finally:
        os.close(descriptor)


def _windows_move(source: Path, destination: Path, *, replace: bool) -> None:
    import ctypes
    from ctypes import wintypes

    move_file = ctypes.WinDLL("kernel32", use_last_error=True).MoveFileExW
    move_file.argtypes = (wintypes.LPCWSTR, wintypes.LPCWSTR, wintypes.DWORD)
    move_file.restype = wintypes.BOOL
    flags = 0x00000008 | (0x00000001 if replace else 0)
    if not move_file(str(source), str(destination), flags):
        error = ctypes.get_last_error()
        raise GeoParquetSafetyError(
            f"write-through publication failed: {source} -> {destination}"
        ) from ctypes.WinError(error)


def _durable_replace(source: Path, destination: Path) -> None:
    if os.name == "nt":
        _windows_move(source, destination, replace=True)
    else:
        os.replace(source, destination)
        _fsync_directory(destination.parent)


def _durable_publish_new(source: Path, destination: Path) -> None:
    if os.name == "nt":
        _windows_move(source, destination, replace=False)
    else:
        try:
            os.link(source, destination)
        except FileExistsError as exc:
            raise GeoParquetSafetyError(f"rollback path appeared during snapshot: {destination}") from exc
        _fsync_directory(destination.parent)


def _same_file_identity(left: Path, right: Path) -> bool:
    try:
        return os.path.samestat(left.stat(), right.stat())
    except OSError as exc:
        raise GeoParquetSafetyError(f"cannot prove independent file identity: {left}, {right}") from exc


def _create_independent_snapshot(source: Path, destination: Path, *, expected_sha256: str) -> Path:
    """Create a byte-exact rollback snapshot on an inode independent of source."""

    descriptor, value = tempfile.mkstemp(
        prefix=f".{destination.name}.",
        suffix=".snapshot.part",
        dir=destination.parent,
    )
    part = Path(value)
    try:
        with source.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
            descriptor = -1
            shutil.copyfileobj(reader, writer, length=8 * 1024 * 1024)
            writer.flush()
            os.fsync(writer.fileno())
        if sha256_file(part) != expected_sha256:
            raise GeoParquetSafetyError(f"rollback snapshot changed while copying: {source}")
        _durable_publish_new(part, destination)
        if _same_file_identity(source, destination):
            raise GeoParquetSafetyError(f"rollback snapshot still aliases canonical source: {destination}")
        return destination
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        part.unlink(missing_ok=True)


def _ensure_independent_snapshot(source: Path, destination: Path, *, expected_sha256: str) -> Path:
    """Validate an existing snapshot, rematerializing legacy hard links safely."""

    if destination.exists():
        if destination.is_symlink() or sha256_file(destination) != expected_sha256:
            raise GeoParquetSafetyError(f"rollback path already contains a different file: {destination}")
        if _same_file_identity(source, destination):
            descriptor, value = tempfile.mkstemp(
                prefix=f".{destination.name}.",
                suffix=".snapshot.part",
                dir=destination.parent,
            )
            part = Path(value)
            try:
                with source.open("rb") as reader, os.fdopen(descriptor, "wb") as writer:
                    descriptor = -1
                    shutil.copyfileobj(reader, writer, length=8 * 1024 * 1024)
                    writer.flush()
                    os.fsync(writer.fileno())
                if sha256_file(part) != expected_sha256:
                    raise GeoParquetSafetyError(f"rollback snapshot changed while copying: {source}")
                _durable_replace(part, destination)
            finally:
                if descriptor >= 0:
                    os.close(descriptor)
                part.unlink(missing_ok=True)
    else:
        _create_independent_snapshot(source, destination, expected_sha256=expected_sha256)
    if _same_file_identity(source, destination):
        raise GeoParquetSafetyError(f"rollback snapshot aliases canonical source: {destination}")
    return destination


def atomic_convert_in_place(
    path: str | Path,
    *,
    backup_path: str | Path,
    backup_root: str | Path,
    allowed_roots: Iterable[str | Path],
    source_crs: str,
    reserve_bytes: int = DEFAULT_RESERVE_BYTES,
    geometry_column: str = DEFAULT_GEOMETRY_COLUMN,
    expected_bounds: tuple[float, float, float, float] | None = None,
    expected_geometry_types: Sequence[str] | None = None,
    expected_rows: int | None = None,
    batch_rows: int = DEFAULT_BATCH_ROWS,
    expected_original_sha256: str | None = None,
) -> dict[str, object]:
    """Validate and atomically replace one file while retaining its original.

    ``backup_path`` must not alias a different original.  Rollback snapshots are
    independent copies so an in-place concurrent writer cannot mutate both the
    canonical source and its recovery evidence through a shared inode.
    """
    roots = tuple(Path(root) for root in allowed_roots)
    source_candidate = Path(path)
    # Reject an escaping target before destination_write_lock can create its
    # sibling lock file; then recheck containment under the lock.
    source_lock_path = _contained(source_candidate, roots)
    with destination_write_lock(source_lock_path):
        source = _contained(source_lock_path, roots)
        backup = _safe_backup_path(backup_path, backup_root=backup_root, source=source)
        current_sha256 = sha256_file(source)
        if expected_original_sha256 is not None and current_sha256 != expected_original_sha256:
            raise GeoParquetSafetyError(f"input changed after audit: {source}")
        source_size = source.stat().st_size

        if is_geoparquet(source):
            summary = validate_geoparquet(source, geometry_column=geometry_column, deep=True)
            return {
                "status": "already_geoparquet",
                "path": str(source),
                "rows": summary.row_count,
                "geometry_types": list(summary.geometry_types),
                "sha256": current_sha256,
            }

        backup_requires_copy = not backup.exists()
        if backup.exists():
            if backup.is_symlink() or sha256_file(backup) != current_sha256:
                raise GeoParquetSafetyError(f"rollback path already contains a different file: {backup}")
            backup_requires_copy = _same_file_identity(source, backup)
        snapshot_bytes = source_size if backup_requires_copy else 0
        required = snapshot_bytes + source_size + max(source_size // 4, 256 * 1024**2)
        free = shutil.disk_usage(source.parent).free
        if free - required < reserve_bytes:
            raise GeoParquetSafetyError(
                f"insufficient free space for {source.name}: free={free}, required={required}, reserve={reserve_bytes}"
            )

        original_sha256 = current_sha256
        _ensure_independent_snapshot(source, backup, expected_sha256=original_sha256)

        part = _unique_sibling(source, ".geoparquet.part")
        try:
            summary = convert_parquet_file(
                source,
                part,
                source_crs=source_crs,
                geometry_column=geometry_column,
                expected_bounds=expected_bounds,
                expected_geometry_types=expected_geometry_types,
                expected_rows=expected_rows,
                batch_rows=batch_rows,
            )
            validated = validate_geoparquet(
                part,
                geometry_column=geometry_column,
                batch_rows=batch_rows,
                deep=True,
            )
            if validated != summary:
                raise GeoParquetSafetyError("converted file validation summary changed")
            compare_source_payload(source, part, batch_rows=batch_rows)
            converted_sha256 = sha256_file(part)
            if sha256_file(source) != original_sha256:
                raise GeoParquetSafetyError(f"input changed before publication: {source}")
            _durable_replace(part, source)
        except BaseException:
            part.unlink(missing_ok=True)
            raise

        return {
            "status": "converted",
            "path": str(source),
            "backup": str(backup),
            "rows": summary.row_count,
            "geometry_types": list(summary.geometry_types),
            "bbox": list(summary.bbox) if summary.bbox is not None else None,
            "original_sha256": original_sha256,
            "sha256": converted_sha256,
            "original_bytes": source_size,
            "converted_bytes": source.stat().st_size,
            "source_crs": source_crs,
        }


def atomic_restore_backup(
    path: str | Path,
    backup_path: str | Path,
    *,
    expected_sha256: str,
    expected_current_sha256: str,
    allowed_roots: Iterable[str | Path],
    backup_root: str | Path,
) -> Path:
    """Atomically restore a retained original without consuming the backup link."""
    roots = tuple(Path(root) for root in allowed_roots)
    target_candidate = Path(path)
    # Contain before deriving the sibling lock name.  Otherwise an escaping
    # target could create a lock outside the reviewed data roots before the
    # safety check rejected the restore.
    target_lock_path = _contained(target_candidate, roots)
    with destination_write_lock(target_lock_path):
        target = _contained(target_lock_path, roots)
        if not target.is_file() or sha256_file(target) != expected_current_sha256:
            raise GeoParquetSafetyError(f"current canonical hash changed: {target}")
        backup = _safe_backup_path(backup_path, backup_root=backup_root, source=target).resolve(strict=True)
        if not backup.is_file() or sha256_file(backup) != expected_sha256:
            raise GeoParquetSafetyError(f"rollback hash mismatch for {backup}")
        part = _unique_sibling(target, ".restore.part")
        try:
            _create_independent_snapshot(backup, part, expected_sha256=expected_sha256)
            _durable_replace(part, target)
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        return target
