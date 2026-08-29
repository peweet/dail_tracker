"""Spatial sort keys for Parquet row-group locality, via DuckDB's native ST_Hilbert.

Sorting rows by a space-filling-curve code of their bbox centre keeps each Parquet row group a
compact patch of ground, so a downstream bbox/window query's min/max statistics pruning can
skip row groups it never touches. DuckDB's spatial extension provides `ST_Hilbert(x, y, bounds)`
natively (verified against duckdb==1.5.5, this repo's pinned version, 2026-08-29) — there is no
bit-interleave loop or quantisation math to hand-roll for it, and Hilbert curves have no long
jumps the way Z-order/Morton curves do, which DuckDB's own team measured giving more consistent
row-group pruning across mixed query patterns
(https://duckdb.org/2025/06/06/advanced-sorting-for-fast-selective-queries).

Hilbert is the sort key at every row count. An earlier revision of this module dispatched to a
numpy Morton cascade below 150,000 rows, on a measured crossover: ST_Hilbert's per-call DuckDB
overhead does make it the slower way to SORT at small N. That optimised the wrong quantity. Sort
time is paid once, when the file is written; row-group pruning is paid on every windowed read of
that file afterwards, and Hilbert prunes at least as well at every size measured. One key also
means a file's physical layout never depends on how many rows happened to land in it, which is
what makes two builds of the same input comparable.

Ties must be broken explicitly, and `hilbert_order` does it. ST_Hilbert quantises to 16 bits per
axis, so codes tie readily on dense data, and DuckDB's ORDER BY is neither stable nor
single-threaded — with no tiebreaker the row order, and therefore the bytes of any file written
from it, follows the builder's core count. That breaks diffing, caching and content-addressing.
"""

from __future__ import annotations

import duckdb
import numpy as np

_connection: duckdb.DuckDBPyConnection | None = None


def _connection_with_spatial() -> duckdb.DuckDBPyConnection:
    """One process-wide DuckDB connection with `spatial` loaded, paid once per process."""
    global _connection
    if _connection is None:
        con = duckdb.connect()
        con.execute("LOAD spatial")
        _connection = con
    return _connection


def hilbert_order(cx: np.ndarray, cy: np.ndarray) -> np.ndarray:
    """Row order (0..N-1) that sorting centres (cx, cy) by Hilbert-curve code would give.

    Quantisation extent is the data's own min/max, computed here and passed to `ST_Hilbert`
    explicitly: valid and comparable only within one call, never across calls with different
    extents. A degenerate (zero-width) axis — e.g. every row sharing the same latitude — is
    padded by 1.0 so `ST_MakeEnvelope` never receives a zero-area box; verified this does not
    error and still orders correctly by the live axis (2026-08-29, empirical check against
    duckdb 1.5.5).

    The trailing `, i` in the ORDER BY is load-bearing, not tidiness: it makes tied Hilbert
    codes resolve by input position instead of by whichever thread's merge got there first.
    See the module docstring for why that matters.
    """
    n = len(cx)
    minx, miny, maxx, maxy = float(cx.min()), float(cy.min()), float(cx.max()), float(cy.max())
    if maxx <= minx:
        maxx = minx + 1.0
    if maxy <= miny:
        maxy = miny + 1.0
    con = _connection_with_spatial()
    con.register("_spatial_sort_pts", {"i": np.arange(n), "cx": cx, "cy": cy})
    try:
        bounds = f"ST_Extent(ST_MakeEnvelope({minx!r}, {miny!r}, {maxx!r}, {maxy!r}))::BOX_2D"
        out = con.execute(
            f"SELECT i FROM _spatial_sort_pts ORDER BY ST_Hilbert(cx, cy, {bounds}), i"
        ).fetchnumpy()
    finally:
        con.unregister("_spatial_sort_pts")
    return out["i"]


def sort_order(cx: np.ndarray, cy: np.ndarray) -> np.ndarray:
    """Row order for spatial locality — the one call a writer should ever need.

    Kept as the named entry point so callers state intent ("sort for locality") rather than
    naming a curve. It no longer dispatches: there is one sort key at every row count.
    """
    return hilbert_order(cx, cy)


__all__ = ["hilbert_order", "sort_order"]
