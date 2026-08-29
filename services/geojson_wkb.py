"""geojson_geometries_to_wkb: a page of GeoJSON geometry dicts to WKB, dispatching on batch size
the same way `services.spatial_sort.sort_order` dispatches Morton vs. Hilbert — below
`GEOJSON_WKB_ROWS_THRESHOLD`, a plain `shapely.geometry.shape()` loop; at or above it, ONE DuckDB
round trip via the native `spatial` extension's `ST_GeomFromGeoJSON`/`ST_AsWKB`.

The two paths exist because DuckDB's per-call connection/registration overhead only pays off
above some row count — measured, not assumed. Below it, a plain Python loop wins outright: it's
not just "close enough to not bother," `shapely.geometry.shape()` is a pure-Python dict-walker
with no vectorization, but DuckDB has to serialize every geometry to JSON, register a relation,
and round-trip through SQL, and that fixed cost dominates until there's enough work to amortize
it over. Unlike Hilbert-vs-Morton, this crossover has NO forever-repeated downstream benefit to
tip the scale toward "always DuckDB" (a slow sort costs on every future read of that row group; a
slow parse costs once, at fetch time) — see the crossover-measurement note on
`GEOJSON_WKB_ROWS_THRESHOLD` below for why the dispatcher stays, rather than being dropped the way
the private engine's Morton threshold was.

Verified against duckdb==1.5.5 (this repo's pinned version, 2026-08-29): `ST_GeomFromGeoJSON`
accepts a bare geometry object (`{"type": ..., "coordinates": ...}`, no Feature/FeatureCollection
wrapper needed — the same shape `shapely.geometry.shape()` already consumes), and a malformed
geometry anywhere in a DuckDB batch raises `duckdb.InvalidInputException` out of the query rather
than returning a null/silent result for that row. The shapely-loop path raises on the same bad
row via `shapely.geometry.shape()` itself — both paths crash on malformed input, never skip it.
"""

from __future__ import annotations

import json
from collections.abc import Sequence

import duckdb
import numpy as np
import shapely
from shapely.geometry import shape

# Measured 2026-08-29, this box (median of 9 alternating-order repeats per size, fresh random
# polygons per repeat, via tools/benchmark_geojson_wkb.py's own functions): shapely wins at
# N=100 and below; DuckDB wins consistently from N=150 up, with one noisy flip at N=130. 200 is
# chosen past that noise band. A hardware-dependent crossover, not a constant; re-measure on a
# different box. Unlike a SORT-order threshold, this one is safe to pick on speed alone: it
# governs a one-shot ingest parse and leaves nothing behind in the written file, so there is no
# recurring read cost on the other side to outweigh the saving (see services/spatial_sort.py,
# whose own row-count threshold was removed on 2026-08-29 for exactly that reason).
GEOJSON_WKB_ROWS_THRESHOLD = 200

_connection: duckdb.DuckDBPyConnection | None = None


def _connection_with_spatial() -> duckdb.DuckDBPyConnection:
    """One process-wide DuckDB connection with `spatial` loaded, paid once per process.

    Deliberately independent from `services.spatial_sort`'s connection of the same name: the
    only saving from sharing one connection is skipping a second `LOAD spatial` (tens of ms,
    once per process), not worth coupling this module's lifecycle to the Hilbert/Morton sort
    dispatcher's. Same "duplicated by design" tradeoff that module's own docstring already makes
    across the public/private boundary.
    """
    global _connection
    if _connection is None:
        con = duckdb.connect()
        con.execute("LOAD spatial")
        _connection = con
    return _connection


def _shapely_loop(geometries: Sequence[dict | None]) -> list[bytes | None]:
    return [None if g is None else shapely.to_wkb(shape(g)) for g in geometries]


def _duckdb_batch(geometries: Sequence[dict | None], idx: list[int]) -> list[bytes | None]:
    payload = np.array([json.dumps(geometries[i]) for i in idx], dtype=object)
    con = _connection_with_spatial()
    con.register("_geojson_wkb_in", {"i": np.array(idx), "g": payload})
    try:
        out = con.execute(
            "SELECT i, ST_AsWKB(ST_GeomFromGeoJSON(g)) AS wkb FROM _geojson_wkb_in ORDER BY i"
        ).fetchnumpy()
    finally:
        con.unregister("_geojson_wkb_in")
    result: list[bytes | None] = [None] * len(geometries)
    for i, wkb in zip(out["i"], out["wkb"], strict=True):
        result[int(i)] = bytes(wkb)
    return result


def geojson_geometries_to_wkb(geometries: Sequence[dict | None]) -> list[bytes | None]:
    """WKB bytes for each GeoJSON geometry dict. `None` in -> `None` out, at the same index.

    Dispatches on the count of non-`None` geometries alone (`GEOJSON_WKB_ROWS_THRESHOLD`) —
    callers never pick a path themselves, this is the one entry point, same contract as
    `services.spatial_sort.sort_order`. A malformed geometry anywhere in the batch raises and
    aborts the WHOLE call on both paths — no per-row try/catch, no silent skip.
    """
    idx = [i for i, g in enumerate(geometries) if g is not None]
    if not idx:
        return [None] * len(geometries)
    if len(idx) < GEOJSON_WKB_ROWS_THRESHOLD:
        return _shapely_loop(geometries)
    return _duckdb_batch(geometries, idx)


__all__ = ["geojson_geometries_to_wkb", "GEOJSON_WKB_ROWS_THRESHOLD"]
