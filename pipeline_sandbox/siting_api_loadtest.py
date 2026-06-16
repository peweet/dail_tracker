"""Load-test the siting API prototype — simulate N simultaneous users hitting /evaluate.

Answers "if 20–100 users ping at once, what's the response?" Each level fires N requests with N
in flight (one wave of N concurrent users) and reports throughput + latency percentiles + errors.
The point pool is pre-warmed so the DEM S3 cache is hot — we measure the SERVER'S compute
concurrency (the scaling bottleneck), not S3 round-trips. Assumes the API is up on :8077.

Run:  python pipeline_sandbox/siting_api_loadtest.py
"""

from __future__ import annotations

import asyncio
import time

import httpx

BASE = "http://127.0.0.1:8077"
# 12 distinct Galway sites (mix of urban/rural), cycled, pre-warmed for DEM
POOL = [(-9.06 + (i % 4) * 0.05, 53.24 + (i // 4) * 0.04) for i in range(12)]
LEVELS = [20, 50, 100]
DURATION = 25  # seconds per level (closed-loop: N users loop for DURATION, then drain)


def pctl(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))] if xs else 0.0


async def warm(client: httpx.AsyncClient) -> None:
    for lon, lat in POOL:
        try:
            await client.get(f"{BASE}/evaluate", params={"lon": lon, "lat": lat})
        except Exception:
            pass


async def run_level(concurrency: int) -> None:
    """N concurrent virtual users each loop request→record for DURATION seconds, then drain."""
    lat: list[float] = []
    errors = 0
    deadline = time.time() + DURATION
    limits = httpx.Limits(max_connections=concurrency + 10, max_keepalive_connections=concurrency + 10)
    async with httpx.AsyncClient(timeout=120, limits=limits) as client:
        async def vuser(seed: int):
            nonlocal errors
            i = seed
            while time.time() < deadline:
                lon, lat_ = POOL[i % len(POOL)]
                i += 1
                t0 = time.time()
                try:
                    r = await client.get(f"{BASE}/evaluate", params={"lon": lon, "lat": lat_})
                    lat.append((time.time() - t0) * 1000)
                    if r.status_code != 200:
                        errors += 1
                except Exception:
                    errors += 1

        t0 = time.time()
        await asyncio.gather(*[vuser(s) for s in range(concurrency)])
        wall = time.time() - t0
    thru = len(lat) / wall if wall else 0
    print(f"{concurrency:>4} users | {len(lat):>4} done in {wall:5.1f}s | {thru:5.2f} req/s | "
          f"p50 {pctl(lat,50):6.0f}ms  p95 {pctl(lat,95):7.0f}ms  p99 {pctl(lat,99):7.0f}ms  "
          f"max {max(lat) if lat else 0:7.0f}ms | errors {errors}", flush=True)


async def main():
    async with httpx.AsyncClient(timeout=60) as c:
        try:
            h = (await c.get(f"{BASE}/healthz")).json()
            print(f"server warm_s={h.get('warm_s')}  (pre-warming {len(POOL)}-point DEM cache…)", flush=True)
        except Exception:
            print("server not reachable on :8077", flush=True)
            return
        await warm(c)
    print("closed-loop: N users loop for 25s each\n", flush=True)
    for n in LEVELS:
        await run_level(n)


if __name__ == "__main__":
    asyncio.run(main())
