"""Headline benchmark: the old per-feature `shapely.geometry.shape()` loop (the pattern that was
in `extractors.cadastre_parcels_fetch._fetch_county` before this change) vs
`services.geojson_wkb.geojson_geometries_to_wkb`'s single DuckDB round trip per batch.

Anti-bias measures (see project memory on benchmark traps, and this script's sibling
`tools/benchmark_hilbert_vs_morton.py`):
- Fresh random polygon coordinates generated per trial, never reused across repeats (rules out
  cache flattery).
- Algorithm execution order alternated per repeat, not run as a fixed A-then-B block (rules out
  the fixed-order A/B trap, where whichever runs second inherits warm state).
- DuckDB's connection/spatial-extension load happens once, before ANY timed region.
- `json.dumps` serialization stays INSIDE the DuckDB path's timed region — it's a real,
  non-zero cost of that path, not free.
- Median of 7 repeats per (N, path) reported, not a single run.
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

from services.geojson_wkb import geojson_geometries_to_wkb

SIZES = (1_000, 5_000, 20_000, 100_000, 331_000)  # 331k = Dublin, the largest county


def _random_geoms(rng: np.random.Generator, n: int) -> list[dict]:
    x0 = rng.uniform(-10.5, -6.0, n)
    y0 = rng.uniform(51.5, 55.5, n)
    w = rng.uniform(0.0002, 0.002, n)
    return [
        {"type": "Polygon", "coordinates": [[[x, y], [x + dw, y], [x + dw, y + dw], [x, y + dw], [x, y]]]}
        for x, y, dw in zip(x0, y0, w, strict=True)
    ]


def _shapely_loop(geoms: list[dict]) -> None:
    for geom in geoms:
        shapely.to_wkb(shape(geom))


def _bench(n: int, repeats: int = 7) -> tuple[float, float]:
    rng = np.random.default_rng(hash(("bench_geojson_wkb", n)) & 0xFFFFFFFF)
    shapely_ms, duckdb_ms = [], []
    for i in range(repeats):
        geoms = _random_geoms(rng, n)
        order = (shapely_ms, duckdb_ms) if i % 2 == 0 else (duckdb_ms, shapely_ms)
        fns = (_shapely_loop, geojson_geometries_to_wkb) if i % 2 == 0 else (geojson_geometries_to_wkb, _shapely_loop)
        for times, fn in zip(order, fns, strict=True):
            t0 = time.perf_counter()
            fn(geoms)
            times.append(time.perf_counter() - t0)
    return float(np.median(shapely_ms)) * 1000, float(np.median(duckdb_ms)) * 1000


def main() -> None:
    geojson_geometries_to_wkb([{"type": "Point", "coordinates": [0.0, 0.0]}])  # pay LOAD spatial once, untimed
    print(f"{'N':>10} | {'shapely_ms':>11} | {'duckdb_ms':>10} | winner")
    for n in SIZES:
        shapely_ms, duckdb_ms = _bench(n)
        winner = "shapely" if shapely_ms < duckdb_ms else "duckdb"
        print(f"{n:>10} | {shapely_ms:>11.2f} | {duckdb_ms:>10.2f} | {winner}")


if __name__ == "__main__":
    main()
