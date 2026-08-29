"""Hilbert vs Morton for parquet row-group locality, measured by ROW GROUPS READ.

This tool used to find the row count above which DuckDB's `ST_Hilbert` sorts faster than a numpy
Morton cascade, and `services.spatial_sort` dispatched on that crossover. The crossover was real
and the answer it gave was the wrong one to act on: sort time is paid ONCE, when a file is
written, while row-group pruning is paid on EVERY windowed read of that file afterwards. A
threshold picked on sort speed therefore traded a permanent read cost for a one-off build
saving. `services.spatial_sort` now uses Hilbert at every size, and this tool measures the
quantity that decision actually turns on.

Morton lives here now, not in the production module: it is the comparison baseline, and the only
reason the repo still needs an implementation of it.

What is measured: for each sort order, how many parquet row groups a small query window would
have to touch — computed from each group's own min/max bbox statistics in the footer, which is
exactly what "predicate pushdown: reading N / M row groups" counts. Lower is better; it is a
count of work, not a duration, so it does not move with machine load, thread count or cache
state the way a millisecond figure does.

Anti-bias measures (see project memory on benchmark traps):
- Both files are written from IDENTICAL rows with IDENTICAL row-group sizing; the sort order is
  the only variable.
- Query windows are drawn once and replayed against both files, never re-drawn per file.
- Group counts come from footer statistics, so no timing, no warm/cold cache asymmetry, and no
  dependence on which file was read first.
- Sort duration is reported too, but explicitly as context — it is NOT the deciding quantity,
  and this tool exists partly because it was once mistaken for it.

    python -m tools.benchmark_hilbert_vs_morton
"""

from __future__ import annotations

# isort: off
import services.runtime_env  # noqa: F401
# isort: on

import tempfile
import time
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from services.spatial_sort import hilbert_order

SIZES = (10_000, 50_000, 200_000)
ROWS_PER_GROUP = 2_000
QUERY_WINDOWS = 200
WINDOW_HALF_WIDTH_DEG = 0.02

_MORTON_SPREAD_MASKS = ((8, 0x00FF00FF), (4, 0x0F0F0F0F), (2, 0x33333333), (1, 0x55555555))


def morton_order(cx: np.ndarray, cy: np.ndarray, bits: int = 16) -> np.ndarray:
    """Row order via a numpy Morton (Z-order) cascade — the baseline Hilbert is compared against.

    Bit-spread by the standard magic-number cascade (4 shift-or-and steps) rather than a per-bit
    loop. Retired from `services.spatial_sort` on 2026-08-29 when the dispatch was removed; kept
    here so the comparison this tool makes stays runnable.
    """

    def _spread(v: np.ndarray) -> np.ndarray:
        v = v & np.uint64(0xFFFF)
        for shift, mask in _MORTON_SPREAD_MASKS:
            v = (v | (v << np.uint64(shift))) & np.uint64(mask)
        return v

    span_x = max(float(cx.max() - cx.min()), 1e-12)
    span_y = max(float(cy.max() - cy.min()), 1e-12)
    qx = ((cx - cx.min()) / span_x * ((1 << bits) - 1)).astype(np.uint64)
    qy = ((cy - cy.min()) / span_y * ((1 << bits) - 1)).astype(np.uint64)
    return np.argsort(_spread(qx) | (_spread(qy) << np.uint64(1)), kind="stable")


def _write_ordered(path: Path, cx: np.ndarray, cy: np.ndarray, order: np.ndarray) -> None:
    """One parquet with per-row bbox columns, rows in `order`, fixed row-group size."""
    ox, oy = cx[order], cy[order]
    table = pa.table(
        {
            "bbox_minx": pa.array((ox - 0.001).astype(np.float32)),
            "bbox_miny": pa.array((oy - 0.001).astype(np.float32)),
            "bbox_maxx": pa.array((ox + 0.001).astype(np.float32)),
            "bbox_maxy": pa.array((oy + 0.001).astype(np.float32)),
        }
    )
    pq.write_table(table, path, compression="zstd", write_statistics=True, row_group_size=ROWS_PER_GROUP)


def _groups_touched(path: Path, windows: np.ndarray) -> tuple[int, int]:
    """(total groups a window would read across all windows, groups in the file).

    Reads only the footer: each group's stored bbox min/max against the query box, which is the
    same test predicate pushdown applies when it reports "reading N / M row groups".
    """
    meta = pq.ParquetFile(path).metadata
    total_groups = meta.num_row_groups
    stats = []
    for i in range(total_groups):
        rg = meta.row_group(i)
        by_name = {rg.column(j).path_in_schema: rg.column(j).statistics for j in range(rg.num_columns)}
        stats.append(
            (
                by_name["bbox_minx"].min,
                by_name["bbox_miny"].min,
                by_name["bbox_maxx"].max,
                by_name["bbox_maxy"].max,
            )
        )
    touched = 0
    for qx, qy in windows:
        lo_x, hi_x = qx - WINDOW_HALF_WIDTH_DEG, qx + WINDOW_HALF_WIDTH_DEG
        lo_y, hi_y = qy - WINDOW_HALF_WIDTH_DEG, qy + WINDOW_HALF_WIDTH_DEG
        for g_minx, g_miny, g_maxx, g_maxy in stats:
            if g_maxx >= lo_x and g_minx <= hi_x and g_maxy >= lo_y and g_miny <= hi_y:
                touched += 1
    return touched, total_groups


def main() -> None:
    hilbert_order(np.array([1.0, 2.0]), np.array([1.0, 2.0]))  # pay LOAD spatial once, untimed
    print(
        f"{'N':>9} | {'groups':>6} | {'morton reads':>12} | {'hilbert reads':>13} | "
        f"{'change':>7} | {'morton ms':>9} | {'hilbert ms':>10}"
    )
    for n in SIZES:
        rng = np.random.default_rng(20260829)
        cx = rng.uniform(-10.5, -6.0, n)
        cy = rng.uniform(51.5, 55.5, n)
        windows = np.column_stack(
            (rng.uniform(-10.4, -6.1, QUERY_WINDOWS), rng.uniform(51.6, 55.4, QUERY_WINDOWS))
        )

        t0 = time.perf_counter()
        m_order = morton_order(cx, cy)
        morton_ms = (time.perf_counter() - t0) * 1000
        t0 = time.perf_counter()
        h_order = hilbert_order(cx, cy)
        hilbert_ms = (time.perf_counter() - t0) * 1000

        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            _write_ordered(tmp / "morton.parquet", cx, cy, m_order)
            _write_ordered(tmp / "hilbert.parquet", cx, cy, h_order)
            m_reads, groups = _groups_touched(tmp / "morton.parquet", windows)
            h_reads, _ = _groups_touched(tmp / "hilbert.parquet", windows)

        change = f"{(h_reads - m_reads) / m_reads * 100:+.1f}%" if m_reads else "n/a"
        print(
            f"{n:>9} | {groups:>6} | {m_reads:>12} | {h_reads:>13} | {change:>7} | "
            f"{morton_ms:>9.1f} | {hilbert_ms:>10.1f}"
        )
    print(
        "\nreads = row groups a query window would decode, summed over "
        f"{QUERY_WINDOWS} windows; lower is better. Sort ms is context only, not the decision."
    )


if __name__ == "__main__":
    main()
