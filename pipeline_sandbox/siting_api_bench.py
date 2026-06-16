"""Bench the siting API prototype (responsiveness). Assumes the server is up on :8077.

Measures: cold-start warm time, warm latency for a CACHED point (pure overhead), latency for
DISTINCT uncached points (joins + DEM network hop), and concurrent throughput.
Run:  python pipeline_sandbox/siting_api_bench.py
"""

from __future__ import annotations

import statistics as st
import time

import httpx

BASE = "http://127.0.0.1:8077"
# distinct points scattered across Galway so each is an uncached engine evaluation
GALWAY = [(-9.05 + (i % 5) * 0.03, 53.25 + (i // 5) * 0.02) for i in range(15)]


def pctl(xs, p):
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(p / 100 * len(xs)))]


def main():
    with httpx.Client(timeout=60) as c:
        # wait for warm
        for _ in range(60):
            try:
                h = c.get(f"{BASE}/healthz").json()
                if h.get("ok"):
                    print(f"cold-start warm: {h['warm_s']}s (engine loaded layers + council spine at boot)")
                    break
            except Exception:
                time.sleep(1)

        # 1) cached point — pure HTTP+serialize overhead (engine result memoised)
        lat = []
        for _ in range(30):
            t0 = time.time()
            c.get(f"{BASE}/evaluate", params={"lon": -9.0264, "lat": 53.2987})
            lat.append((time.time() - t0) * 1000)
        print(f"\nCACHED point  (n=30): p50 {pctl(lat,50):.0f}ms  p95 {pctl(lat,95):.0f}ms  "
              f"min {min(lat):.0f}ms")

        # 2) distinct uncached points — full joins + DEM range-read each
        lat = []
        srv = []
        for lon, la in GALWAY:
            t0 = time.time()
            r = c.get(f"{BASE}/evaluate", params={"lon": lon, "lat": la})
            lat.append((time.time() - t0) * 1000)
            srv.append(r.json().get("_server_ms", -1))
        print(f"DISTINCT pts  (n={len(GALWAY)}): client p50 {pctl(lat,50):.0f}ms  p95 {pctl(lat,95):.0f}ms  "
              f"| server-only p50 {pctl(srv,50):.0f}ms p95 {pctl(srv,95):.0f}ms")
        print("   (client-server gap ~= DEM S3 round-trip + HTTP; server_ms includes engine+DEM)")

        # 3) concurrency — fire the distinct set with parallel connections
        import concurrent.futures as cf
        def one(pt):
            t0 = time.time()
            httpx.get(f"{BASE}/evaluate", params={"lon": pt[0], "lat": pt[1]}, timeout=60)
            return (time.time() - t0) * 1000
        for workers in (1, 4, 8):
            t0 = time.time()
            with cf.ThreadPoolExecutor(max_workers=workers) as ex:
                res = list(ex.map(one, GALWAY))
            wall = time.time() - t0
            print(f"CONCURRENCY x{workers}: {len(GALWAY)} reqs in {wall:.1f}s = "
                  f"{len(GALWAY)/wall:.1f} req/s  (per-req p50 {pctl(res,50):.0f}ms)")


if __name__ == "__main__":
    main()
