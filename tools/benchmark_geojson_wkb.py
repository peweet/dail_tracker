"""Headline benchmark: the old per-feature `shapely.geometry.shape()` loop (the pattern that was
in `extractors.cadastre_parcels_fetch._fetch_county` before this change) vs
`services.geometry.geojson_to_wkb`'s single DuckDB round trip per batch.

Anti-bias measures (see project memory on benchmark traps, and this script's sibling
`tools/benchmark_hilbert_vs_morton.py`):
- Fresh random polygon coordinates generated per trial, never reused across repeats (rules out
  cache flattery).
- Algorithm execution order alternated per repeat, not run as a fixed A-then-B block (rules out
  the fixed-order A/B trap, where whichever runs second inherits warm state).
- DuckDB's connection/spatial-extension load happens once, before ANY timed region.
- Serialization (`orjson.dumps().decode()`) stays INSIDE the DuckDB path's timed region — it is
  that path's DOMINANT cost, not a rounding error, and hiding it outside the timer is what made
  an earlier version of this harness report a win for a change that regressed on real data.
- VERTEX COUNT is swept alongside N (see VERTEX_COUNTS) — sweeping N alone measures the wrong
  axis, because both paths scale with total coordinates at different rates.
- Median of 7 repeats per (N, vertices, path) reported, not a single run.
- No parquet write in the timed region.

    python -m tools.benchmark_geojson_wkb
"""

from __future__ import annotations

# isort: off
import services.runtime_env  # noqa: F401
# isort: on

import time

import numpy as np
import shapely
from shapely.geometry import shape

from services.geometry import geojson_to_wkb

SIZES = (1_000, 20_000, 100_000)

# ⚠ THE AXIS THAT ACTUALLY MATTERS. Both paths scale with total COORDINATES, at different rates,
# so sweeping N alone hides the result: the original harness hard-coded a 5-vertex box and
# reported 2.7x for a change that was 0.78x on real Cork parcels. These four points bracket real
# cadastre geometry — sampled from data/silver/parquet/cadastre/parts/ on 2026-08-29 via
# shapely.get_num_coordinates, dublin mean 23.4 / carlow 34.3 / cork 51.3, p90 up to 123.
VERTEX_COUNTS = (5, 24, 51, 123)


def _random_geoms(rng: np.random.Generator, n: int, vertices: int) -> list[dict]:
    """`n` closed rings of `vertices` distinct coordinates each.

    `vertices` is a REQUIRED argument with no default on purpose: the first version of this
    harness hard-coded a 5-coordinate box, which is what let a 0.78x regression on real parcels
    report as a 2.7x win. There is no safe default here — pick from VERTEX_COUNTS.
    """
    cx = rng.uniform(-10.5, -6.0, n)
    cy = rng.uniform(51.5, 55.5, n)
    radius = rng.uniform(0.0002, 0.002, n)
    angles = np.linspace(0.0, 2.0 * np.pi, vertices, endpoint=False)
    geoms = []
    for x, y, r in zip(cx, cy, radius, strict=True):
        ring = [[float(x + r * np.cos(a)), float(y + r * np.sin(a))] for a in angles]
        ring.append(ring[0])  # GeoJSON rings must close
        geoms.append({"type": "Polygon", "coordinates": [ring]})
    return geoms


def _shapely_loop(geoms: list[dict]) -> None:
    for geom in geoms:
        shapely.to_wkb(shape(geom))


def _bench(n: int, vertices: int, repeats: int = 7) -> tuple[float, float]:
    rng = np.random.default_rng(hash(("bench_geojson_wkb", n, vertices)) & 0xFFFFFFFF)
    shapely_ms, duckdb_ms = [], []
    for i in range(repeats):
        geoms = _random_geoms(rng, n, vertices)
        order = (shapely_ms, duckdb_ms) if i % 2 == 0 else (duckdb_ms, shapely_ms)
        fns = (_shapely_loop, geojson_to_wkb) if i % 2 == 0 else (geojson_to_wkb, _shapely_loop)
        for times, fn in zip(order, fns, strict=True):
            t0 = time.perf_counter()
            fn(geoms)
            times.append(time.perf_counter() - t0)
    return float(np.median(shapely_ms)) * 1000, float(np.median(duckdb_ms)) * 1000


def main() -> None:
    geojson_to_wkb([{"type": "Point", "coordinates": [0.0, 0.0]}])  # pay LOAD spatial once, untimed
    print(f"{'N':>8} | {'verts':>5} | {'shapely_ms':>11} | {'duckdb_ms':>10} | {'ratio':>6} | winner")
    for vertices in VERTEX_COUNTS:
        for n in SIZES:
            shapely_ms, duckdb_ms = _bench(n, vertices)
            winner = "shapely" if shapely_ms < duckdb_ms else "duckdb"
            ratio = shapely_ms / duckdb_ms
            print(f"{n:>8} | {vertices:>5} | {shapely_ms:>11.2f} | {duckdb_ms:>10.2f} | {ratio:>5.2f}x | {winner}")


if __name__ == "__main__":
    main()
