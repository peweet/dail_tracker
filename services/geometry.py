"""THE geometry module: every GeoJSON/WKB/point operation this repo performs, on DuckDB's native
`spatial` extension, with no shapely and no per-row Python loop.

ONE MODULE, ON PURPOSE. Before this existed the same work was spread across a per-feature
`shapely.geometry.shape()` loop in five extractors, a `map_elements` Point constructor in two
more, and a vectorized validation block inside `services/geoparquet_io.py`. Consolidating them
here is what makes the cost model auditable in one place — and the cost model is the whole point,
because the naive version of this migration was measurably WRONG twice (see below).

    geojson_to_wkb(geometries)                  parse a page of GeoJSON geometry dicts
    polygonal_geometries(values, ireland_bbox)  parse + repair + bounds-check, with reason codes
    points_to_wkb(lons, lats)                   build Point WKB from coordinate arrays
    validate_wkb(raw)                           the GeoParquet write contract

⚠ COST SCALES WITH VERTEX COUNT, NOT ROW COUNT. Both DuckDB and shapely scale with total
coordinates, at different rates, so a benchmark that sweeps N while holding geometry shape
constant measures the wrong axis. An earlier version of this work was justified by a 2.7x win on
a synthetic 5-VERTEX box and was in fact a 0.78x REGRESSION on real Cork parcels (mean 51.3
coords). Real cadastre parcels run mean 23-51 coordinates, p90 up to 123; real planning sites mean
17.8 [measured 2026-08-29 via shapely.get_num_coordinates over data/silver/parquet/]. Any
re-measurement must vary vertex count against realistic geometry — `tools/benchmark_geojson_wkb.py`
does; anything that does not is not evidence.

⚠ SERIALIZATION, NOT DUCKDB, IS THE DOMINANT COST. Decomposed at 24 verts / N=5,000: stdlib
`json.dumps` 140.0ms vs DuckDB's actual SQL 17.2ms, against shapely's 157.7ms — the engine was
~9x faster than shapely while Python serialization ate the entire win. `orjson` removes it
(dublin 6.59x, cork 4.71x on real parcels, where stdlib json managed 1.18x and 0.78x). Every path
here MUST use orjson, and `.decode()` is mandatory, not cosmetic: orjson returns bytes, bytes
registered through numpy reach DuckDB as BLOB, and `ST_GeomFromGeoJSON` raises
InvalidInputException on a BLOB — a `::VARCHAR` cast does NOT rescue it, because DuckDB escapes
the bytes rather than reinterpreting them as UTF-8.

ERROR POLICY DIFFERS PER ENTRY POINT, deliberately, because the callers differ:
  - `geojson_to_wkb` and `validate_wkb` RAISE on bad input — their callers always did.
  - `polygonal_geometries` COUNTS bad input as a reason code and continues, because its caller
    (`planning_applications_ingest`) always did, and turning that into a crash would take down a
    LIVE pipeline chain. `TRY()` in the SQL is what makes the difference.
"""

from __future__ import annotations

import threading
from collections.abc import Sequence
from typing import Any, NamedTuple

import duckdb
import numpy as np
import orjson

_POLYGONAL = {"POLYGON", "MULTIPOLYGON"}

# ST_CollectionExtract's dimension argument: 1=points, 2=lines, 3=polygons.
_POLYGON_DIMENSION = 3

# WKB type ids -> GeoParquet type names, mirroring the GeoParquet 1.1 vocabulary. DuckDB's
# ST_GeometryType returns these names directly, so the id table geoparquet_io used with
# shapely.get_type_id is not needed here.
_GEOPARQUET_TYPES = {
    "POINT": "Point",
    "LINESTRING": "LineString",
    "POLYGON": "Polygon",
    "MULTIPOINT": "MultiPoint",
    "MULTILINESTRING": "MultiLineString",
    "MULTIPOLYGON": "MultiPolygon",
    "GEOMETRYCOLLECTION": "GeometryCollection",
}

_local = threading.local()


class GeometryRow(NamedTuple):
    """One feature's outcome: WKB (or None) plus the reason code the caller counts."""

    wkb: bytes | None
    reason: str
    bounds: tuple[float, float, float, float] | None


class WkbBatch(NamedTuple):
    """Per-row facts about a batch of WKB, for the GeoParquet write contract."""

    bounds: np.ndarray  # (n, 4) float64: minx, miny, maxx, maxy
    finite: np.ndarray  # (n,) bool
    missing: np.ndarray  # (n,) bool
    empty: np.ndarray  # (n,) bool
    valid: np.ndarray  # (n,) bool
    has_m: np.ndarray  # (n,) bool
    has_z: np.ndarray  # (n,) bool
    type_names: np.ndarray  # (n,) object: GeoParquet type name, None where missing


def _connection_with_spatial() -> duckdb.DuckDBPyConnection:
    """One DuckDB connection per thread, `spatial` loaded once per thread, not once per call.

    Thread-local rather than process-global: a DuckDBPyConnection is not thread-safe, and
    `con.cursor()` does not fix it — cursors still serialize on their parent connection
    [Reported — duckdb.org/docs/current/clients/python/overview, read 2026-08-29].

    Deliberately independent from `services.spatial_sort`'s connection of the same name: sharing
    one would save a single `LOAD spatial` (tens of ms, once per thread) at the cost of coupling
    two unrelated modules' lifecycles through one handle.
    """
    con = getattr(_local, "connection", None)
    if con is None:
        con = duckdb.connect()
        # INSTALL first: a fresh runner has no extension directory, and bare LOAD fails there.
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        _local.connection = con
    return con


def _register(con: duckdb.DuckDBPyConnection, name: str, **columns: Any) -> None:
    con.register(name, {key: value for key, value in columns.items()})


def _as_geojson_payload(geometries: Sequence[dict | None], idx: Sequence[int]) -> np.ndarray:
    """orjson-serialize the selected geometries. See the module docstring on why not stdlib json,
    and why `.decode()` is mandatory."""
    return np.array([orjson.dumps(geometries[i]).decode() for i in idx], dtype=object)


# ─────────────────────────────── parse ───────────────────────────────


def geojson_to_wkb(geometries: Sequence[dict | None]) -> list[bytes | None]:
    """WKB bytes for each GeoJSON geometry dict. `None` in -> `None` out, at the same index.

    RAISES on a malformed geometry anywhere in the batch, aborting the whole call — matching the
    per-feature `shapely.geometry.shape()` behaviour this replaced for callers that never
    tolerated bad input. Use `polygonal_geometries` where bad rows must be counted instead.

    `ORDER BY i` is load-bearing: DuckDB does not otherwise guarantee row order, and this must be
    an order-preserving transform.
    """
    idx = [i for i, geometry in enumerate(geometries) if geometry is not None]
    if not idx:
        return [None] * len(geometries)
    con = _connection_with_spatial()
    _register(con, "_geom_parse_in", i=np.array(idx), g=_as_geojson_payload(geometries, idx))
    try:
        out = con.execute(
            "SELECT i, ST_AsWKB(ST_GeomFromGeoJSON(g)) AS wkb FROM _geom_parse_in ORDER BY i"
        ).fetchnumpy()
    finally:
        con.unregister("_geom_parse_in")
    result: list[bytes | None] = [None] * len(geometries)
    for i, wkb in zip(out["i"], out["wkb"], strict=True):
        result[int(i)] = bytes(wkb)
    return result


# ──────────────────── parse + repair + bounds-check ────────────────────

# One pass. TRY() isolates a malformed row to a NULL instead of aborting the batch; every
# downstream expression is NULL-guarded so a NULL flows through rather than raising.
# `was_repaired` must be computed BEFORE the repair, hence the separate CTE.
_POLYGONAL_SQL = f"""
WITH parsed AS (
    SELECT i, TRY(ST_GeomFromGeoJSON(g)) AS geom FROM _geom_poly_in
),
flagged AS (
    SELECT i, geom,
           CASE WHEN geom IS NULL THEN FALSE ELSE NOT ST_IsValid(geom) END AS was_repaired
    FROM parsed
),
repaired AS (
    SELECT i, was_repaired,
           CASE WHEN geom IS NULL THEN NULL
                WHEN was_repaired THEN ST_MakeValid(geom)
                ELSE geom END AS geom
    FROM flagged
),
extracted AS (
    SELECT i, was_repaired,
           CASE WHEN geom IS NULL THEN NULL
                WHEN ST_GeometryType(geom) = 'GEOMETRYCOLLECTION'
                    THEN ST_CollectionExtract(geom, {_POLYGON_DIMENSION})
                ELSE geom END AS geom
    FROM repaired
)
SELECT i, was_repaired,
       CASE WHEN geom IS NULL THEN NULL ELSE ST_GeometryType(geom) END AS geom_type,
       CASE WHEN geom IS NULL THEN NULL ELSE ST_IsEmpty(geom) END AS is_empty,
       CASE WHEN geom IS NULL THEN NULL ELSE ST_XMin(geom) END AS minx,
       CASE WHEN geom IS NULL THEN NULL ELSE ST_YMin(geom) END AS miny,
       CASE WHEN geom IS NULL THEN NULL ELSE ST_XMax(geom) END AS maxx,
       CASE WHEN geom IS NULL THEN NULL ELSE ST_YMax(geom) END AS maxy,
       CASE WHEN geom IS NULL THEN NULL ELSE ST_AsWKB(geom) END AS wkb
FROM extracted
ORDER BY i
"""


def polygonal_geometries(
    values: Sequence[dict | None],
    *,
    ireland_bbox: tuple[float, float, float, float],
) -> list[GeometryRow]:
    """Parse, repair and bounds-check a page of GeoJSON, returning a reason code per row.

    Reason vocabulary, unchanged from the per-feature original it replaces: ``empty`` (falsy
    input, or a geometry that resolves to nothing), ``unreadable`` (parse failure),
    ``not_polygonal``, ``bounds_escape``, ``repaired``, ``ok``.

    ⚠ A BAD GEOMETRY IS COUNTED, NOT FATAL — the opposite of `geojson_to_wkb`. The reason
    histogram is PROVENANCE: it is written to the coverage JSON and asserted in tests, so a
    malformed feature must land in it rather than abort the run.

    SQL does the geometry; this function does the reason ladder. Keeping the branches in Python
    is deliberate — provenance logic must stay readable line by line, not buried in nested CASE.
    """
    rows: list[GeometryRow] = [GeometryRow(None, "empty", None)] * len(values)
    idx = [i for i, value in enumerate(values) if value]
    if not idx:
        return rows
    con = _connection_with_spatial()
    _register(con, "_geom_poly_in", i=np.array(idx), g=_as_geojson_payload(values, idx))
    try:
        out = con.execute(_POLYGONAL_SQL).fetchall()
    finally:
        con.unregister("_geom_poly_in")
    for i, was_repaired, geom_type, is_empty, minx, miny, maxx, maxy, wkb in out:
        rows[int(i)] = _classify(
            was_repaired=bool(was_repaired),
            geom_type=geom_type,
            is_empty=is_empty,
            bounds=(minx, miny, maxx, maxy),
            wkb=wkb,
            ireland_bbox=ireland_bbox,
        )
    return rows


def _classify(
    *,
    was_repaired: bool,
    geom_type: str | None,
    is_empty: bool | None,
    bounds: tuple[float | None, ...],
    wkb: bytes | None,
    ireland_bbox: tuple[float, float, float, float],
) -> GeometryRow:
    """The reason ladder, in the same order as the per-feature original.

    Order is the contract: an empty geometry is ``empty`` even though it is also non-polygonal,
    and a bounds escape is only reachable once the shape is known to be polygonal.
    """
    if geom_type is None:
        return GeometryRow(None, "unreadable", None)
    if is_empty:
        return GeometryRow(None, "empty", None)
    if geom_type.upper() not in _POLYGONAL:
        return GeometryRow(None, "not_polygonal", None)
    # Past the guards above the row has a real, non-empty, polygonal geometry, so the SQL cannot
    # have returned NULL bounds or NULL WKB for it. Asserted rather than cast: a None here would
    # mean the query's NULL-guards and this reason ladder had drifted apart, which should fail
    # loudly rather than be silenced by a type cast.
    raw_minx, raw_miny, raw_maxx, raw_maxy = bounds
    if wkb is None or raw_minx is None or raw_miny is None or raw_maxx is None or raw_maxy is None:
        raise AssertionError(f"polygonal geometry with NULL bounds/wkb (type {geom_type!r})")
    minx, miny, maxx, maxy = float(raw_minx), float(raw_miny), float(raw_maxx), float(raw_maxy)
    west, south, east, north = ireland_bbox
    if not (west <= minx <= maxx <= east and south <= miny <= maxy <= north):
        return GeometryRow(None, "bounds_escape", None)
    return GeometryRow(bytes(wkb), "repaired" if was_repaired else "ok", (minx, miny, maxx, maxy))


# ─────────────────────────────── points ───────────────────────────────


def points_to_wkb(lons: np.ndarray, lats: np.ndarray) -> list[bytes]:
    """Point WKB from coordinate arrays — the vectorized replacement for a per-row constructor.

    Measured 2026-08-29 at N=200,000 against the `pl.struct(...).map_elements(lambda s:
    shapely.to_wkb(shapely.Point(...)))` pattern this replaces: 40.5x, byte-identical output.
    `map_elements` is a row-wise Python callback despite living inside Polars, which is why it
    was the slowest of the three options tested (vectorized shapely was 16.5x).
    """
    lons = np.asarray(lons, dtype="float64")
    lats = np.asarray(lats, dtype="float64")
    if len(lons) != len(lats):
        raise ValueError(f"lons/lats length mismatch: {len(lons)} vs {len(lats)}")
    if not len(lons):
        return []
    con = _connection_with_spatial()
    _register(con, "_geom_points_in", i=np.arange(len(lons)), x=lons, y=lats)
    try:
        out = con.execute("SELECT i, ST_AsWKB(ST_Point(x, y)) AS wkb FROM _geom_points_in ORDER BY i").fetchnumpy()
    finally:
        con.unregister("_geom_points_in")
    return [bytes(wkb) for wkb in out["wkb"]]


def wkb_bounds(raw: Any) -> np.ndarray:
    """(n, 4) float64 minx/miny/maxx/maxy for a batch of WKB — the `shapely.bounds` replacement.

    ⚠ ITS OWN QUERY, NOT `validate_wkb(raw).bounds`. Routing it through the full contract also
    runs ST_IsValid/ST_IsEmpty/ST_HasM/ST_HasZ, and validity checking is the single most expensive
    predicate in that set — it measured 0.20x against shapely (3-5x SLOWER) purely from computing
    facts the caller discards. Asking only for the envelope measured 4.15x FASTER at 20,000 rows
    and 7.03x at 100,000. Same engine, same data; the difference was entirely the work requested.

    ST_Extent walks the vertices ONCE; four separate ST_XMin/ST_YMin/... calls re-walk per call
    and measured ~1.6x slower for identical output.

    Accepts a pyarrow Array/ChunkedArray (zero-copy), a polars Series, or a sequence of bytes.
    NULL rows yield NaN bounds, matching shapely's missing-geometry behaviour.
    """
    import pyarrow as pa

    column = _as_wkb_arrow(raw)
    if not len(column):
        return np.full((0, 4), np.nan, dtype="float64")
    con = _connection_with_spatial()
    con.register("_geom_bounds_in", pa.table({"wkb": column}))
    try:
        out = con.execute(
            "SELECT ST_XMin(e) AS minx, ST_YMin(e) AS miny, ST_XMax(e) AS maxx, ST_YMax(e) AS maxy "
            "FROM (SELECT ST_Extent(ST_GeomFromWKB(wkb)) AS e FROM _geom_bounds_in)"
        ).to_arrow_table()
    except duckdb.Error as exc:
        raise ValueError(f"invalid WKB geometry: {exc}") from exc
    finally:
        con.unregister("_geom_bounds_in")
    return np.column_stack([np.asarray(out.column(i)) for i in range(4)])


def wkb_centroids(raw: Sequence[bytes | None]) -> tuple[np.ndarray, np.ndarray]:
    """(x, y) centroid arrays for a batch of WKB — the `.centroid` replacement.

    Bit-exact against shapely's `.centroid` on all 1,067 rows of the real EPA facilities layer
    (max abs delta 0.0, 2026-08-29). Returns NaN for a NULL row rather than raising.
    """
    count = len(raw)
    xs = np.full(count, np.nan, dtype="float64")
    ys = np.full(count, np.nan, dtype="float64")
    present = [i for i, value in enumerate(raw) if value is not None]
    if not present:
        return xs, ys
    con = _connection_with_spatial()
    _register(
        con,
        "_geom_centroid_in",
        i=np.array(present),
        # `present` was built by filtering out None, so every indexed value is bytes.
        g=np.array([bytes(value) for value in (raw[i] for i in present) if value is not None], dtype=object),
    )
    try:
        out = con.execute(
            "SELECT i, ST_X(ST_Centroid(ST_GeomFromWKB(g))) AS x, ST_Y(ST_Centroid(ST_GeomFromWKB(g))) AS y "
            "FROM _geom_centroid_in ORDER BY i"
        ).fetchnumpy()
    finally:
        con.unregister("_geom_centroid_in")
    xs[out["i"].astype(int)] = out["x"]
    ys[out["i"].astype(int)] = out["y"]
    return xs, ys


# ──────────────────────── GeoParquet write contract ────────────────────────

# ⚠ ARROW IN, ARROW OUT — the handoff is the whole ballgame. An earlier version of this function
# built `np.array([bytes(x) for x in raw], dtype=object)` and read results with `fetchall()`, and
# measured 0.83x against vectorized shapely — a REGRESSION that got shipped and reverted. Both
# ends were the fault: a per-row Python bytes loop going in, a per-row Python tuple loop coming
# out. Handing DuckDB the Arrow array directly and reading back an Arrow table makes the SAME
# query 1.98x FASTER than shapely at 20,000 rows and 2.50x at 100,000 (bounds alone: 4.15x and
# 7.03x), measured 2026-08-29, alternating order, correctness asserted every repeat.
#
# The lesson, because it cost two wrong conclusions in one session: when a DuckDB path loses to
# vectorized shapely, suspect the BOUNDARY before the engine. DuckDB's WKB parser is roughly 6x
# faster than GEOS's; a loss almost always means Python objects are being built on one side.
#
# ST_Extent computes the envelope in ONE vertex walk; four separate ST_XMin/ST_YMin/... calls
# re-walk the geometry each time and measured ~1.6x slower for identical output.
_VALIDATE_SQL = """
SELECT
    ST_IsEmpty(g) AS is_empty,
    ST_IsValid(g) AS is_valid,
    ST_HasM(g) AS has_m,
    ST_HasZ(g) AS has_z,
    ST_GeometryType(g) AS geom_type,
    ST_XMin(x) AS minx, ST_YMin(x) AS miny, ST_XMax(x) AS maxx, ST_YMax(x) AS maxy
FROM (SELECT TRY(ST_GeomFromWKB(wkb)) AS g, TRY(ST_Extent(ST_GeomFromWKB(wkb))) AS x FROM _geom_wkb_in)
"""


def _as_wkb_arrow(raw: Any):
    """Normalise any accepted input to a single Arrow binary array, copying as little as possible.

    A pyarrow Array/ChunkedArray passes straight through — `geoparquet_io` already holds one, so
    the fast path costs nothing. A polars Series converts natively. Only a plain Python sequence
    pays for materialisation, and callers on the hot path should avoid handing one over.
    """
    import pyarrow as pa

    if isinstance(raw, pa.ChunkedArray):
        return raw.combine_chunks()
    if isinstance(raw, pa.Array):
        return raw
    to_arrow = getattr(raw, "to_arrow", None)
    if to_arrow is not None:
        return _as_wkb_arrow(to_arrow())
    return pa.array(list(raw), type=pa.binary())


def validate_wkb(raw: Any) -> WkbBatch:
    """Per-row facts about a batch of WKB, for the GeoParquet contract in `geoparquet_io`.

    Accepts a pyarrow Array/ChunkedArray (the zero-copy path), a polars Series, or any sequence
    of `bytes | None`. Reports; it does not raise on contract violations — the caller owns the
    policy and the error wording. It DOES raise on bytes that are not WKB at all, since there is
    no per-row fact to report about them.

    Distinguishes `missing` (a NULL cell) from unparseable bytes, which the shapely version
    collapsed into one `is_missing`. Type names come back in GeoParquet vocabulary
    (`MultiPolygon`, not `MULTIPOLYGON`); the caller appends any ` Z` suffix.

    ⚠ ROW ORDER IS THE ARROW INPUT ORDER, relied on rather than enforced with an ORDER BY.
    DuckDB preserves insertion order by default (`preserve_insertion_order`), and adding an index
    column costs a join's worth of work on the hot path. `test_validate_preserves_row_order_at_scale`
    pins this; if that test ever fails, add the index column back rather than reordering here.
    """
    import pyarrow as pa

    column = _as_wkb_arrow(raw)
    count = len(column)
    missing = np.zeros(count, dtype=bool)
    bounds = np.full((count, 4), np.nan, dtype="float64")
    empty = np.zeros(count, dtype=bool)
    valid = np.zeros(count, dtype=bool)
    has_m = np.zeros(count, dtype=bool)
    has_z = np.zeros(count, dtype=bool)
    type_names = np.full(count, None, dtype=object)
    if not count:
        return WkbBatch(bounds, np.zeros(count, dtype=bool), missing, empty, valid, has_m, has_z, type_names)

    con = _connection_with_spatial()
    con.register("_geom_wkb_in", pa.table({"wkb": column}))
    try:
        out = con.execute(_VALIDATE_SQL).to_arrow_table()
    except duckdb.Error as exc:
        # TRY() yields NULL for most bad input, but not all: DuckDB raises "Unsupported geometry
        # type in WKB" out of the query. Normalised so callers see ONE exception type whichever
        # layer objected, matching the shapely version's single failure mode.
        raise ValueError(f"invalid WKB geometry: {exc}") from exc
    finally:
        con.unregister("_geom_wkb_in")

    geom_types = np.asarray(out.column("geom_type"))
    missing = np.asarray(column.is_null())
    # A row DuckDB could not parse yields a NULL type while its input cell was not NULL.
    unparseable = np.asarray(out.column("geom_type").is_null()) & ~missing
    if unparseable.any():
        first = int(np.flatnonzero(unparseable)[0])
        raise ValueError(f"invalid WKB geometry at row {first} ({int(unparseable.sum())} total)")

    present = ~missing
    empty[present] = np.asarray(out.column("is_empty"))[present]
    valid[present] = np.asarray(out.column("is_valid"))[present]
    has_m[present] = np.asarray(out.column("has_m"))[present]
    has_z[present] = np.asarray(out.column("has_z"))[present]
    for axis, name in enumerate(("minx", "miny", "maxx", "maxy")):
        bounds[:, axis] = np.asarray(out.column(name))
    # ⚠ VECTORIZED SCATTER, NOT A PER-ROW LOOP. A `for position in np.flatnonzero(...)` here
    # costs O(rows) Python iterations and measured the whole contract at 0.47x — slower than
    # shapely — while the SQL underneath was 2.5x faster. Assign by boolean mask per DISTINCT
    # type name instead: a handful of numpy assignments regardless of row count.
    for name in {value for value in np.unique(geom_types).tolist() if value is not None}:
        type_names[geom_types == name] = _GEOPARQUET_TYPES.get(name.upper())

    finite = present & np.isfinite(bounds).all(axis=1)
    return WkbBatch(bounds, finite, missing, empty, valid, has_m, has_z, type_names)


__all__ = [
    "GeometryRow",
    "WkbBatch",
    "geojson_to_wkb",
    "points_to_wkb",
    "polygonal_geometries",
    "validate_wkb",
    "wkb_bounds",
    "wkb_centroids",
]
