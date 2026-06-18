"""Closed-loop load test for the siting API — measure its compute concurrency.

Simulates N concurrent virtual users hammering ``/evaluate`` for a fixed duration and reports
throughput + latency percentiles + errors per concurrency level. The point pool is pre-warmed
first so the DEM cache is hot: this measures the SERVER'S per-request compute concurrency (the
real scaling bottleneck for the geospatial engine), not S3 round-trips.

Promoted from pipeline_sandbox/siting_api_loadtest.py. The pure aggregation logic is split out
(percentile/summarise/format_level/parse_levels) so it is unit-tested without a live server; the
httpx network calls are imported lazily so importing this module needs no optional deps.

Run (server up on :8077, e.g. python pipeline_sandbox/siting_api_prototype.py):

    python tools/siting_loadtest.py --base http://127.0.0.1:8077 --levels 20,50,100 --duration 25
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:  # network types only; the runtime import is lazy (see _make_client)
    import httpx

DEFAULT_BASE = "http://127.0.0.1:8077"
DEFAULT_LEVELS = (20, 50, 100)
DEFAULT_DURATION = 25.0  # seconds each level runs, closed-loop


def make_pool(n: int = 12) -> list[tuple[float, float]]:
    """A spread of distinct Galway sites (urban + rural mix), cycled by the virtual users."""
    return [(-9.06 + (i % 4) * 0.05, 53.24 + (i // 4) * 0.04) for i in range(n)]


def parse_levels(spec: str) -> list[int]:
    """Parse a CLI ``--levels`` spec like ``"20,50,100"`` into ``[20, 50, 100]``."""
    return [int(tok) for tok in spec.split(",") if tok.strip()]


def percentile(xs: list[float], p: float) -> float:
    """The ``p``-th percentile (nearest-rank) of ``xs``; ``0.0`` for an empty sample."""
    if not xs:
        return 0.0
    ordered = sorted(xs)
    return ordered[min(len(ordered) - 1, int(p / 100 * len(ordered)))]


@dataclass(frozen=True)
class LevelResult:
    """The measured outcome of one concurrency level."""

    concurrency: int
    done: int
    wall_s: float
    throughput_rps: float
    p50_ms: float
    p95_ms: float
    p99_ms: float
    max_ms: float
    errors: int


def summarise(concurrency: int, latencies: list[float], errors: int, wall_s: float) -> LevelResult:
    """Reduce a level's recorded latencies into a LevelResult (pure; no I/O)."""
    done = len(latencies)
    return LevelResult(
        concurrency=concurrency,
        done=done,
        wall_s=wall_s,
        throughput_rps=(done / wall_s if wall_s > 0 else 0.0),
        p50_ms=percentile(latencies, 50),
        p95_ms=percentile(latencies, 95),
        p99_ms=percentile(latencies, 99),
        max_ms=max(latencies) if latencies else 0.0,
        errors=errors,
    )


def format_level(r: LevelResult) -> str:
    """Render one LevelResult as the single-line console report."""
    return (
        f"{r.concurrency:>4} users | {r.done:>5} done in {r.wall_s:5.1f}s | "
        f"{r.throughput_rps:6.2f} req/s | p50 {r.p50_ms:6.0f}ms  p95 {r.p95_ms:7.0f}ms  "
        f"p99 {r.p99_ms:7.0f}ms  max {r.max_ms:7.0f}ms | errors {r.errors}"
    )


def deadline_stop(duration_s: float) -> Callable[[], bool]:
    """A stop predicate that returns True once ``duration_s`` has elapsed (monotonic clock)."""
    end = time.monotonic() + duration_s
    return lambda: time.monotonic() >= end


def _make_client(concurrency: int) -> httpx.AsyncClient:
    import httpx  # lazy: keeps module import dependency-free for the pure-logic tests

    limits = httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency + 10)
    return httpx.AsyncClient(timeout=120, limits=limits)


async def warm(client: httpx.AsyncClient, base: str, pool: list[tuple[float, float]]) -> None:
    """Prime the server's DEM/layer caches so the measured run reflects warm compute, not cold I/O."""
    for lon, lat in pool:
        with contextlib.suppress(Exception):
            await client.get(f"{base}/evaluate", params={"lon": lon, "lat": lat})


async def run_level(
    base: str,
    concurrency: int,
    *,
    duration_s: float = DEFAULT_DURATION,
    pool: list[tuple[float, float]] | None = None,
    client: httpx.AsyncClient | None = None,
    stop: Callable[[], bool] | None = None,
) -> LevelResult:
    """Fire ``concurrency`` virtual users that loop request→record until ``stop()`` (or the deadline).

    ``client``/``stop`` are injectable so the network loop can be driven by a mock transport and a
    deterministic stop in tests; left None they default to a real client and a wall-clock deadline.
    """
    pool = pool or make_pool()
    stop = stop or deadline_stop(duration_s)
    latencies: list[float] = []
    errors = 0
    own_client = client is None
    if client is None:
        client = _make_client(concurrency)

    async def vuser(seed: int) -> None:
        nonlocal errors
        i = seed
        while not stop():
            lon, lat = pool[i % len(pool)]
            i += 1
            t0 = time.monotonic()
            try:
                r = await client.get(f"{base}/evaluate", params={"lon": lon, "lat": lat})
                latencies.append((time.monotonic() - t0) * 1000)
                if r.status_code != 200:
                    errors += 1
            except Exception:
                errors += 1

    start = time.monotonic()
    try:
        await asyncio.gather(*[vuser(s) for s in range(concurrency)])
    finally:
        if own_client:
            await client.aclose()
    return summarise(concurrency, latencies, errors, time.monotonic() - start)


async def main(base: str, levels: list[int], duration_s: float) -> list[LevelResult]:
    import httpx  # lazy

    pool = make_pool()
    async with httpx.AsyncClient(timeout=60) as c:
        try:
            health = (await c.get(f"{base}/healthz")).json()
            print(f"server warm_s={health.get('warm_s')}  (pre-warming {len(pool)}-point DEM cache…)", flush=True)
        except Exception:
            print(f"server not reachable on {base}", flush=True)
            return []
        await warm(c, base, pool)

    print(f"closed-loop: N users loop for {duration_s:g}s each\n", flush=True)
    results: list[LevelResult] = []
    for n in levels:
        result = await run_level(base, n, duration_s=duration_s, pool=pool)
        print(format_level(result), flush=True)
        results.append(result)
    return results


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Closed-loop load test for the siting API.")
    p.add_argument("--base", default=DEFAULT_BASE, help="server base URL")
    p.add_argument("--levels", default=",".join(map(str, DEFAULT_LEVELS)), help="comma-separated concurrency levels")
    p.add_argument("--duration", type=float, default=DEFAULT_DURATION, help="seconds each level runs (closed-loop)")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    asyncio.run(main(args.base, parse_levels(args.levels), args.duration))
