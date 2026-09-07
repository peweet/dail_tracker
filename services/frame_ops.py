"""Frame primitives that remove per-row Python work without changing results.

`map_unique` is the one lever available when a per-row function CANNOT be expressed as a polars
expression — it splits, branches, or loops in ways the expression API has no equivalent for. The
function still runs in Python, but once per DISTINCT input instead of once per row.

WHEN IT PAYS: the function must be a pure deterministic function of ONE column, and that column
must repeat. Break-even is around 2x repetition; measured wins on this repo's real data
(2026-08-30, byte-identical output, `tools/row_iteration_ab.py`): spend-category canonicalisation
over 401,624 payment rows / 33,484 distinct descriptions (12.0x repetition) = 10.41x; supplier-ref
stripping over 40,526 distinct suppliers (9.9x) = 6.13x; case anonymisation over 785,897 diary rows
/ 12,290 distinct titles (63.95x) = 21.91x.

WHEN IT DOES NOT: a function of more than one column (the distinct set becomes the join product and
the repetition collapses), a non-deterministic function, or a column that is nearly unique.
"""

from __future__ import annotations

from collections.abc import Callable

import polars as pl


def map_unique(
    frame: pl.DataFrame,
    column: str,
    function: Callable,
    *,
    alias: str,
    return_dtype: pl.DataType,
) -> pl.DataFrame:
    """`frame.with_columns(pl.col(column).map_elements(function).alias(alias))`, computed once per
    distinct value of `column`.

    Row order, height and dtype are preserved exactly — verified byte-for-byte (Arrow IPC, after
    chunk normalisation) against the per-row form by `tools/row_iteration_ab.py`.

    ⚠ `maintain_order="left"` IS LOAD-BEARING. A join may otherwise reorder rows, and for a frame
    written straight to parquet that changes what lands on disk. Verified: without it the harness
    catches the reorder as a byte mismatch.

    NULLS, precisely (checked against polars 1.41.2, not assumed): `map_elements` never calls the
    function on a null — it short-circuits to null — so the per-row form always yields null for a
    null input regardless of what the function would return. `nulls_equal=True` therefore does not
    change today's result; a plain left join leaves the null-keyed row unmatched and fills the same
    null. It is set anyway so the null key MATCHES explicitly rather than agreeing by coincidence,
    which keeps this correct if the helper ever returns a second column with a non-null default.
    """
    if frame.is_empty():
        return frame.with_columns(pl.lit(None, dtype=return_dtype).alias(alias))
    distinct = (
        frame.select(column)
        .unique()
        .with_columns(pl.col(column).map_elements(function, return_dtype=return_dtype).alias(alias))
    )
    return frame.join(distinct, on=column, how="left", nulls_equal=True, maintain_order="left")


__all__ = ["map_unique"]
