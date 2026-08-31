"""hilbert_order: row order by Hilbert-curve code, incl. degenerate zero-span axes, and the
tie-stability that makes a written file's bytes independent of the builder's core count."""

from __future__ import annotations

import numpy as np

import services.spatial_sort as spatial_sort
from services.spatial_sort import hilbert_order, sort_order


def test_orders_points_along_a_single_varying_axis():
    cx = np.array([1.0, 2.0, 3.0, 4.0])
    cy = np.array([5.0, 5.0, 5.0, 5.0])  # every row shares the same latitude
    order = hilbert_order(cx, cy)
    assert list(cx[order]) == sorted(cx)


def test_single_row_returns_trivially():
    order = hilbert_order(np.array([1.0]), np.array([5.0]))
    assert list(order) == [0]


def test_result_is_a_permutation_of_every_input_row():
    rng = np.random.default_rng(0)
    cx, cy = rng.uniform(-10, -5, 200), rng.uniform(51, 56, 200)
    order = hilbert_order(cx, cy)
    assert sorted(order.tolist()) == list(range(200))


def test_spatially_close_points_land_close_in_the_output_order():
    # A tight cluster of four points should stay contiguous in Hilbert order, unlike a
    # lexicographic minx sort which would interleave them with unrelated far-away rows.
    cluster = np.array([[0.0, 0.0], [0.01, 0.0], [0.0, 0.01], [0.01, 0.01]])
    far = np.array([[9.0, 9.0], [9.0, 9.01]])
    pts = np.vstack([cluster, far])
    order = hilbert_order(pts[:, 0], pts[:, 1])
    positions = {int(i): rank for rank, i in enumerate(order)}
    cluster_positions = sorted(positions[i] for i in range(4))
    assert cluster_positions[-1] - cluster_positions[0] == 3


def test_tied_codes_come_back_in_input_order_on_any_thread_count():
    """Rows sharing a Hilbert code must keep input order, whatever DuckDB's merge does.

    ST_Hilbert quantises to 16 bits per axis, so a dense input ties constantly. DuckDB's
    ORDER BY is not stable and its merge is parallel, so without the `, i` tiebreaker the row
    order — and therefore the bytes of any parquet written from it — moves with the machine's
    core count. A build that is not reproducible cannot be diffed, cached or content-addressed.
    """
    rng = np.random.default_rng(3)
    loc = rng.uniform([-10.0, 52.0], [-6.0, 55.0], size=(200, 2))
    idx = rng.integers(0, 200, size=20_000)  # 200 distinct sites, so codes are heavily tied
    cx, cy = loc[idx, 0].copy(), loc[idx, 1].copy()

    con = spatial_sort._connection_with_spatial()
    con.execute("SET threads=4")  # force the parallel merge a single-core runner would not take
    try:
        order = hilbert_order(cx, cy)
    finally:
        con.execute("RESET threads")

    # Within every run of equal coordinates, the returned indices must be ascending.
    by_site: dict[tuple[float, float], list[int]] = {}
    for position in order:
        by_site.setdefault((cx[position], cy[position]), []).append(int(position))
    unstable = sum(1 for rows in by_site.values() if rows != sorted(rows))
    assert unstable == 0, f"{unstable} tied-code groups came back out of input order"


def test_repeated_calls_on_the_same_input_give_the_same_order():
    # The determinism the tiebreaker exists for, asserted end to end rather than by inspection.
    rng = np.random.default_rng(11)
    loc = rng.uniform([-10.0, 52.0], [-6.0, 55.0], size=(150, 2))
    idx = rng.integers(0, 150, size=10_000)
    cx, cy = loc[idx, 0].copy(), loc[idx, 1].copy()
    first = hilbert_order(cx, cy)
    second = hilbert_order(cx, cy)
    assert np.array_equal(first, second)


def test_sort_order_delegates_to_hilbert_order():
    # sort_order is the named entry point writers call; it no longer dispatches on row count,
    # so a file's layout cannot depend on how many rows happened to land in it.
    rng = np.random.default_rng(5)
    cx, cy = rng.uniform(-10, -5, 500), rng.uniform(51, 56, 500)
    assert np.array_equal(sort_order(cx, cy), hilbert_order(cx, cy))


def test_real_hilbert_call_works_at_a_size_that_would_have_dispatched_before():
    # The retired threshold sent anything under 150,000 rows to a numpy Morton path. Exercise
    # the real ST_Hilbert call well below that, where the dispatcher used to divert it.
    rng = np.random.default_rng(3)
    n = 5_000
    cx, cy = rng.uniform(-10, -5, n), rng.uniform(51, 56, n)
    order = sort_order(cx, cy)
    assert sorted(order.tolist()) == list(range(n))


def test_shared_connection_is_reused_across_calls():
    # _connection_with_spatial is a per-thread singleton so LOAD spatial is paid once per
    # thread, not per call (services/spatial_sort.py's own stated rationale) — verify it
    # actually is one within a thread.
    con_a = spatial_sort._connection_with_spatial()
    con_b = spatial_sort._connection_with_spatial()
    assert con_a is con_b


def test_concurrent_calls_from_different_threads_do_not_corrupt_results():
    # DuckDBPyConnection is not thread-safe, and con.cursor() does not fix it either — cursors
    # still serialize on the parent connection (duckdb.org Python client docs, "each thread
    # must have its own connection", verified 2026-08-29). A shared connection registering a
    # fixed table name gives concurrent callers zero isolation.
    import concurrent.futures

    n = 300

    def _call(seed: int) -> bool:
        rng = np.random.default_rng(seed)
        cx, cy = rng.uniform(-10, -5, n), rng.uniform(51, 56, n)
        order = hilbert_order(cx, cy)
        return sorted(order.tolist()) == list(range(n))

    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        results = list(ex.map(_call, range(40)))
    assert all(results), "a concurrent call returned a corrupted (non-permutation) order"
