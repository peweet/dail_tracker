"""Differential for the GeoParquet write contract: `services/geoparquet_io.py`'s shapely
`_SummaryAccumulator` vs `services.geometry.validate_wkb`.

BYTE-FOR-BYTE, NOT APPROXIMATELY. These bounds become the f32 `bbox` struct column written to
every GeoParquet file in the repo, so an f64 difference of one ULP can change a stored byte and
silently alter which row groups a downstream window query prunes. This compares:

  - the derived GeometrySummary (row/non-null counts, geometry_types tuple, file bbox)
  - the raw f64 bounds array, with `np.array_equal` — NOT np.allclose, because "close" is
    exactly the failure this is meant to catch
  - the f32 bbox struct AS SERIALIZED BY ARROW, compared as bytes

Runs over real tracked GeoParquet files, not fixtures — the geometry mix (Point vs Polygon vs
MultiPolygon, Z coordinates, vertex counts) is what makes the comparison meaningful, and no
synthetic corpus reproduces it.

    python -m tools.geoparquet_validate_differential
    python -m tools.geoparquet_validate_differential --limit 3
"""

from __future__ import annotations

# isort: off
import services.runtime_env  # noqa: F401
# isort: on

import argparse
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from services import geoparquet_io as gio
from services.geometry import validate_wkb

ROOT = Path(__file__).resolve().parents[1]
SILVER = ROOT / "data/silver/parquet"


def _shapely_side(column) -> tuple[np.ndarray, np.ndarray] | str:
    """The incumbent, with its contract refusals captured rather than propagated.

    It RAISES `GeoParquetError` on a file that violates the write contract (invalid geometry,
    M/ZM coordinates, nulls). Some tracked files do — the contract is enforced at WRITE time and
    a file predating a rule keeps its rows. Captured so one such file cannot abort the sweep
    before it reaches the rest.
    """
    accumulator = gio._SummaryAccumulator()
    try:
        _, bounds, finite = accumulator.add(column)
    except gio.GeoParquetError as exc:
        return f"REFUSED:{exc}"
    return bounds, finite


def _duckdb_side(column) -> tuple[np.ndarray, np.ndarray]:
    batch = validate_wkb(column.to_pylist())
    return batch.bounds, batch.finite


def _bbox_bytes(bounds: np.ndarray, finite: np.ndarray) -> bytes:
    """Serialize exactly as geoparquet_io writes it, so the comparison is of stored bytes."""
    array = gio._bbox_array(bounds, finite)
    sink = pa.BufferOutputStream()
    table = pa.table({"bbox": array})
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def _compare_file(path: Path, geometry_column: str = "wkb") -> list[str]:
    problems: list[str] = []
    parquet = pq.ParquetFile(path)
    try:
        if parquet.schema_arrow.get_field_index(geometry_column) < 0:
            return []
        rows = 0
        for batch in parquet.iter_batches(columns=[geometry_column], batch_size=50_000):
            column = batch.column(0)
            rows += len(column)
            shapely_result = _shapely_side(column)
            if isinstance(shapely_result, str):
                # The incumbent refuses this file's contents outright, so there are no bounds to
                # compare against. Report it — a file the current writer would reject is worth
                # surfacing — but do not count it as a differential mismatch.
                print(f"  {path.name:<52} rows={rows:>8} INCUMBENT-REFUSES ({shapely_result[8:60]})")
                return []
            sh_bounds, sh_finite = shapely_result
            dk_bounds, dk_finite = _duckdb_side(column)

            if not np.array_equal(sh_finite, dk_finite):
                problems.append(f"finite mask differs ({int((sh_finite != dk_finite).sum())} rows)")
            # NaN != NaN, so compare only where both consider the row finite; a mismatch in
            # WHICH rows are finite is already reported above.
            both = sh_finite & dk_finite
            if not np.array_equal(sh_bounds[both], dk_bounds[both]):
                differing = int((sh_bounds[both] != dk_bounds[both]).any(axis=1).sum())
                worst = float(np.nanmax(np.abs(sh_bounds[both] - dk_bounds[both]))) if differing else 0.0
                problems.append(f"f64 bounds differ on {differing} row(s), max delta {worst:.3e}")
            if _bbox_bytes(sh_bounds, sh_finite) != _bbox_bytes(dk_bounds, dk_finite):
                problems.append("SERIALIZED f32 bbox bytes differ")
        print(f"  {path.name:<52} rows={rows:>8} {'OK' if not problems else 'MISMATCH'}")
    finally:
        parquet.close()
    return problems


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=0, help="check at most N files (0 = all)")
    args = parser.parse_args()

    candidates = sorted(p for p in SILVER.rglob("*.parquet") if p.is_file())
    checked = failed = 0
    print(f"scanning {len(candidates)} parquet file(s) under {SILVER.relative_to(ROOT)}")
    for path in candidates:
        try:
            schema = pq.ParquetFile(path).schema_arrow
        except Exception as exc:  # noqa: BLE001 - an unreadable file is not this tool's business
            print(f"  {path.name:<52} SKIP ({type(exc).__name__})")
            continue
        if schema.get_field_index("wkb") < 0:
            continue
        problems = _compare_file(path)
        checked += 1
        if problems:
            failed += 1
            for problem in problems:
                print(f"      ! {problem}")
        if args.limit and checked >= args.limit:
            break

    print(f"\n  geometry files checked={checked} mismatched={failed}")
    if not checked:
        print("  ⚠ NO geometry files checked — that is a broken run, not a pass")
    raise SystemExit(1 if failed or not checked else 0)


if __name__ == "__main__":
    main()
