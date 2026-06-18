"""Unit tests for tools/siting_loadtest.py.

Pure unit tests (no marker, default CI lane). The aggregation/formatting/level-parsing logic is
dependency-free and tested directly; the httpx network loop (run_level) is driven through a
mock transport with a deterministic stop predicate, so no live server is needed. The httpx tests
importorskip cleanly where the optional dep is absent.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from tools.siting_loadtest import (  # noqa: E402
    LevelResult,
    deadline_stop,
    format_level,
    make_pool,
    parse_levels,
    percentile,
    run_level,
    summarise,
)

# ── pure logic ───────────────────────────────────────────────────────────────


def test_percentile_empty_is_zero():
    assert percentile([], 95) == 0.0


@pytest.mark.parametrize(
    "p,expected",
    [(0, 1.0), (50, 6.0), (95, 10.0), (99, 10.0), (100, 10.0)],
)
def test_percentile_nearest_rank(p, expected):
    # nearest-rank on 1..10: index = min(9, int(p/100*10))
    assert percentile([float(x) for x in range(1, 11)], p) == expected


def test_make_pool_size_and_distinct():
    pool = make_pool(12)
    assert len(pool) == 12
    assert len(set(pool)) == 12  # all distinct points
    assert all(isinstance(lon, float) and isinstance(lat, float) for lon, lat in pool)


@pytest.mark.parametrize(
    "spec,expected",
    [
        ("20,50,100", [20, 50, 100]),
        (" 20 , 50 ", [20, 50]),
        ("20,,", [20]),
        ("100", [100]),
    ],
)
def test_parse_levels(spec, expected):
    assert parse_levels(spec) == expected


def test_parse_levels_rejects_non_int():
    with pytest.raises(ValueError):
        parse_levels("20,abc")


def test_summarise_throughput_and_percentiles():
    r = summarise(concurrency=4, latencies=[float(x) for x in range(1, 11)], errors=2, wall_s=5.0)
    assert isinstance(r, LevelResult)
    assert r.done == 10
    assert r.throughput_rps == pytest.approx(2.0)  # 10 done / 5s
    assert r.p50_ms == 6.0
    assert r.p95_ms == 10.0
    assert r.max_ms == 10.0
    assert r.errors == 2


def test_summarise_zero_wall_is_zero_throughput():
    r = summarise(concurrency=1, latencies=[1.0], errors=0, wall_s=0.0)
    assert r.throughput_rps == 0.0  # guarded division, never ZeroDivisionError


def test_summarise_empty_sample():
    r = summarise(concurrency=1, latencies=[], errors=3, wall_s=2.0)
    assert r.done == 0
    assert r.throughput_rps == 0.0
    assert r.max_ms == 0.0
    assert r.errors == 3


def test_format_level_contains_key_fields():
    line = format_level(summarise(50, [10.0, 20.0], 1, 1.0))
    assert "50 users" in line
    assert "req/s" in line
    assert "errors 1" in line


def test_deadline_stop_zero_is_immediately_true():
    assert deadline_stop(0.0)() is True


def test_deadline_stop_future_is_false():
    assert deadline_stop(100.0)() is False


# ── network loop (mock transport, deterministic stop) ─────────────────────────


def _stop_after(n: int):
    """A stop predicate that allows exactly ``n`` iterations (False for the first n calls)."""
    calls = {"c": 0}

    def stop() -> bool:
        calls["c"] += 1
        return calls["c"] > n

    return stop


def _drive(handler, n_requests: int):
    """Run a single-user level against a mock transport, stopping after n requests (deterministic)."""
    httpx = pytest.importorskip("httpx")

    async def go() -> LevelResult:
        transport = httpx.MockTransport(handler)
        async with httpx.AsyncClient(transport=transport) as client:
            return await run_level(
                "http://test",
                concurrency=1,
                pool=make_pool(4),
                client=client,
                stop=_stop_after(n_requests),
            )

    return asyncio.run(go())


def test_run_level_records_latencies_no_errors():
    httpx = pytest.importorskip("httpx")
    result = _drive(lambda req: httpx.Response(200, json={"ok": True}), n_requests=5)
    assert result.done == 5
    assert result.errors == 0
    assert result.p50_ms >= 0.0


def test_run_level_counts_non_200_as_error():
    httpx = pytest.importorskip("httpx")
    # a 500 still returns an HTTP response, so latency IS recorded but the request counts as an error
    result = _drive(lambda req: httpx.Response(500, text="boom"), n_requests=4)
    assert result.done == 4
    assert result.errors == 4


def test_run_level_counts_transport_exception_as_error():
    httpx = pytest.importorskip("httpx")

    def boom(req):
        raise httpx.ConnectError("refused")

    result = _drive(boom, n_requests=3)
    assert result.done == 0  # no response -> no latency recorded
    assert result.errors == 3
