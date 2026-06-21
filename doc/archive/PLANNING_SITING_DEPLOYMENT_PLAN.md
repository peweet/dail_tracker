# Siting Check — deployment architecture plan

> ⚠️ **EXPERIMENTAL.** The whole planning-permission / siting-check feature (Product B) is a
> prototype — not production. On Streamlit Cloud the page is import-guarded to a stub; locally it
> runs on full data. Labelled "Siting Check (experimental)" in the app nav. Nothing here is promoted.

**Status:** planning / prototype-measured (2026-06-16). The siting engine
(`dail_tracker_core.siting`) is in active development; this doc captures the
**deployment** decision (how to ship Product B to users) independent of engine churn.

The problem: every other Dáil Tracker page reads **precomputed gold parquet**, so the
deployed Streamlit app needs only read libs (duckdb/pandas/pyarrow). Siting Check is
**live geospatial compute** over an arbitrary user coordinate — it cannot be pre-baked into
a single gold table, so it needs shapely/rasterio + the designation layers at request time.
That broke the Streamlit Cloud deploy (`ModuleNotFoundError: yaml`, then shapely) because
Cloud installs only the lean core. This doc evaluates how to ship it properly.

---

## Two viable models

### Model A — Grid precompute (read-only)
Precompute OFF-BOX, for every grid cell, which designations fire → ship a compact
`cell → fired-issue` table. Runtime = pure-arithmetic lookup (numpy only, already in core;
**no shapely/rasterio on the box**). Keeps the read-only-UI-tier invariant.

**Prototype measured** (`pipeline_sandbox/siting_grid_precompute_experimental.py`, Galway AOI,
grid-snapped vs the live engine's own trigger functions):

| Resolution | Cells | Parity vs engine |
|---|---|---|
| 100 m | 55,322 | 81.3% → **85.0%** after the engine lon-fix (road mismatches 37 → 25) |
| 50 m | — | (per-point build too slow to finish) |

The engine lon-compression fix (below) accounted for 12 of the road mismatches (engine had been
under-firing true-east roads). The remaining road residual is now **grid-side**: the grid's
anisotropic `dwithin` over-fires north–south vs the now-correct engine, plus 100 m cell-snapping on
the 150 m buffer — both cleaned up by the raster-native build at finer resolution.

- Error concentrates in **linear / "near" triggers** (`road_sightlines` was ~⅔ of the miss;
  septic, protected-structure next). Containment-only nodes (zoning, monument) are near-exact.
- 100 m is **too coarse**; needs ~25–50 m.
- The per-point STRtree build is **too slow** (198 s for 55 k cells → national = hours). The
  production build must be **raster-native**: `rasterize()` each layer once, then handle "near"
  by **morphological dilation** (`buffer_m ÷ cell_m` cells) instead of exact `dwithin` on
  488 k-vertex polygons. The per-point script stays as the **parity oracle**.
- Not yet measured: the real compressed national file size (the slow build ate the budget).

**Bug found while validating:** `engine._road`/`layers.nearest()` computes
`metres = deg * 111_320` — it **ignores longitude compression**, so "within 150 m" is stretched
~1.6× east–west at 53°N. Worth fixing in the engine regardless of deployment model.

### Model B — Live + decoupled API ⭐ (this prototype)
Run the engine as-is in its OWN service; Streamlit becomes a thin HTTP client. Exact (no
accuracy compromise), ships the real engine, and uses the seam the repo already has
(`api/` FastAPI app, `api` extra, `doc/fastapi_query_core_uncoupling_plan.md`).

```
Streamlit Cloud (lean: requests only)
   └─ GET https://siting-api/evaluate?lon&lat&dev   ── thin client, renders cards
                         │
        FastAPI service (shapely + rasterio + simplified layers in RAM)
                         └─ Copernicus DEM via S3 range-read (stateless)
```

The Streamlit box carries **zero** geo deps, zero layer data, zero geo-RAM. All heaviness
lives in one small always-on service (512 MB–1 GB on Fly/Render, or Cloud Run with
`min-instances=1` to avoid cold-start re-loading the layers).

---

## How the live model works (Model B internals)

At runtime the compute process holds four things:

| Component | What | Runtime cost |
|---|---|---|
| Vector layers | NPWS/GSI/SMR/zoning/… as shapely STRtrees in RAM | **memory-bound** (see below) |
| DEM | Copernicus COG, HTTP range-read via rasterio `/vsicurl` | stateless, ~1 S3 hop/point |
| Council resolver | nearest of 495 k planning-app points (STRtree) | ~50–100 MB RAM (or swap for an LA-boundary polygon) |
| Rulebook + catalogue | small YAML/text | negligible |

**Request flow:** `point → resolve council → per-node spatial joins → DEM range-read →
resolve verbatim rule → JSON`. Cached 1 h.

**The one hard variable is layer RAM:**
- Raw layers (today, 291 MB on disk, gitignored) → ~0.5–1.5 GB in memory as shapely trees.
- Simplified ~10 m (§23.9 of `PLANNING_PERMISSION_SCOPING.md`, 27× fewer vertices) → **tens of MB**.

---

## Can Streamlit serve it?

1. **Streamlit the framework, live compute** — yes, trivially (works locally today).
2. **Streamlit *Community Cloud* as the monolith (Model A-in-process / B-in-process)** —
   **marginal.** ~1 GB RAM shared with the rest of the app. Viable only with *simplified*
   layers + geo deps in core + careful RAM watching. Raw data ≈ OOM. Fragile.
3. **Streamlit Cloud serving the *UI* with compute decoupled (Model B)** — **yes, cleanly.**
   Serving a thin HTTP-client page is trivial. This is the production-standard topology:
   **Streamlit serves the UI; it is not the geospatial compute host on the free tier.**

---

## Responsiveness — Model B prototype, measured

Prototype: `pipeline_sandbox/siting_api_prototype.py` (standalone FastAPI, warms the engine
at boot) + `pipeline_sandbox/siting_api_bench.py`. Local loopback, full national-ish Galway
layers in RAM.

**Measured (2026-06-16, local loopback, full Galway/national layers in RAM):**

| Metric | Value | Note |
|---|---|---|
| Cold-start warm | **6.5 s** | one-time boot: council spine (495 k pts) + layer STRtrees |
| Per-eval, warm | **~1.7 s** (p50), 3.1 s (p95) | DEM cached; this is the spatial-join compute cost |
| Throughput | **~0.5 req/s** | flat across concurrency → single-core / GIL-bound |
| Concurrency ×4 | per-req p50 **7.9 s** | requests serialize, latency balloons |
| Concurrency ×8 | per-req p50 **22 s** | unusable under load |

**This is the headline finding.** Model B is correct but, with the engine **as-is**, slow:
- **~1.7 s per evaluation** even warm — dominated by exact spatial ops on **full-precision
  geometry**, above all `dwithin` distance to the 488 k-vertex Lough Corrib SAC polygon
  (european_site 2 km + bats 1.5 km). `evaluate()` is **not memoised** at the engine level, so
  every request re-runs the joins.
- **Throughput caps at ~0.5 req/s per core** — the work is CPU-bound Python; concurrent
  requests serialize (×8 → 22 s each). A public tool would stall under modest load.

**None of this is a blocker — but it makes the productionization steps mandatory, not optional:**
1. **Simplify layers to ~10 m (§23.9).** The same step that shrinks the data also slashes per-eval
   time (far fewer vertices in the Corrib distance test) — likely from ~1.7 s to a few hundred ms.
   The simplify step is the **linchpin for BOTH models**.
2. **Memoise `evaluate()`** (lru_cache on rounded lon/lat/dev) + keep the 1 h result cache → repeat
   / nearby points become instant.
3. **Multi-worker service** (N uvicorn processes) for concurrency → ~N× throughput. Single worker
   is single-core.

**Implication for the A-vs-B choice:** even after simplify, Model B serves each request with live
CPU work and needs multiple workers to handle concurrency. Model A (grid precompute) answers in
**microseconds** (a numpy array index) with trivial concurrency and zero per-request CPU — so at
public scale it is materially more responsive and cheaper to serve. Model B wins on **exactness and
time-to-ship**; Model A wins on **scale and cost**.


### Concurrency load test (`siting_api_loadtest.py`, closed-loop, single worker, full-precision)

| Users | Completed | Throughput | p50 latency | Errors |
|---|---|---|---|---|
| 20 | 12 / 120 s | **0.10 req/s** | **107 s** | 8 timeouts |
| 50 | 0 | 0 | — | **50 — all timed out** |

**The current build collapses under concurrent load.** Causes: (1) single CPU core / GIL → requests
serialize; (2) per-eval **regressed to 5–11 s** (urban) — mostly the in-flux `nearest_junction` O(n²)
road-intersection sub-check, plus the exact `nearest()`; (3) **3.9 GB RAM per worker** (full-precision
layers + 495 k council tree) blocks the obvious fix of adding workers (can't run 2 on a 16 GB box
beside Streamlit + MCP).

**After optimizing (shared pre-warmed store + simplified layers + junction fix), single worker:**

| Users | Completed | Throughput | p50 latency | Errors | vs full-precision |
|---|---|---|---|---|---|
| 20 | 20 | **0.43 req/s** | **42 s** | 0 | was 0.10 req/s, 107 s, 8 errs |
| 50 | 0 | 0 | — | 50 timeouts | (also all timed out) |

Per-eval **7 s → ~1.1 s**, worker RAM **3.9 GB → 1.96 GB**, 20-user p50 **107 s → 42 s, 0 errors**.
Big wins — but **42 s at 20 users is still unusable and 50 still collapses**, because throughput is
capped at **~0.43 req/s on one core**. The single biggest hidden cost was that the prototype called
`evaluate()` with **no shared store**, so every request rebuilt all layer STRtrees; a module-level
pre-warmed store (passed as `evaluate(..., store=_STORE)`) fixed that. **Conclusion: per-request
optimizations cut latency/RAM but cannot beat the core limit — concurrency needs multi-worker (now
affordable at ~2 GB/worker) + result caching, or Model A (grid, zero per-request CPU).** Scaling levers, by impact: ~~fix `nearest_junction` O(n²)~~ **DONE** — STRtree self-join
(`tree.query(geoms, predicate="intersects")`) replaced the all-pairs loop; measured **32× on a dense
urban point** (n=814 segments: 3.06 s → 0.096 s, identical result, suite green) → per-eval back toward
~1.7 s; **simplify layers** (done) → few-hundred-ms eval AND RAM to hundreds of MB; **memoise
`evaluate()`** → repeat points instant; **multi-worker** → N× once RAM drops; **Model A grid** →
microsecond lookup, ~zero CPU/RAM/request, concurrency a non-issue. This is the strongest case for
**Model A at public scale**.

Interpretation guide:
- **Cold-start warm** = one-time boot cost (load council spine + layer STRtrees). An
  always-on service pays it once; a scale-to-zero function pays it every cold request → keep
  layers simplified/small or pin `min-instances=1`.
- **Cached point** = pure HTTP+serialize overhead (engine result memoised).
- **Distinct points** = full joins + DEM S3 round-trip; the client-vs-server gap ≈ the DEM hop.

---

## Simplify step — measured (2026-06-16)

`pipeline_sandbox/siting_layers_simplify_experimental.py`: simplify each layer (shapely,
`preserve_topology=True`) at 5/10/25 m and validate by counting how many of the 495 k national
application points each layer `covered_by`, full vs simplified. Drift = simplified − full count.

**Total: 243.9 MB → 42.8 MB @10 m (5.7×).** Git-shippable ("tens of MB" per §23.9 — confirmed).
But a **uniform 10 m is wrong** — drift is layer-dependent:

| Layer class | Examples | 10 m drift | Verdict |
|---|---|---|---|
| Large smooth polygons | npws_sac (+13), spa (−18), nha (0), pnha (+15), vulnerability (−22), landscape (0) | tiny | 10 m fine (sac 45 MB→3.9 MB) |
| **Fine / dense polygons** | **smr_zone (−962), zoning_gzt (−1056)**, galway_city_aca (−27) | **large negative** | 10 m **over-simplifies → silently misses ~2 k real containments**; use ≤5 m |

**Findings:**
- The §23.9 "10 m sweet spot" was measured on SAC (big polygons) — it does **not** generalize.
  Dense small-polygon layers (archaeology SMR zones, statutory zoning) lose containment fast.
- **Negative drift = missing designations** (telling a site it's NOT in a zone when it is) — the
  unsafe direction for a triage tool. At 5 m it shrinks to smr −407 / zoning −179, but those two
  resist simplification on **both** axes: at 5 m they're still ~12 MB each (~24 MB of the ~41 MB total).
- **Implication:** use **per-layer tolerances** (≤5 m for smr/zoning/aca, 10–25 m for the smooth NPWS
  layers). And note: smr_zone + zoning_gzt — the size *and* accuracy bottleneck of vector
  simplification — are precisely the layers a **raster/grid** representation (Model A) handles best.
  So Model A is not just a scale play; it's the natural fit for the two stubborn layers.

Validation discipline (assert simplified join count ≈ full) is mandatory before promotion — it
caught the smr/zoning loss that a naive global-10 m would have shipped silently.

### Per-layer tuned set (`siting_layers_simplify_finalize.py`, auto-picks largest tol within drift threshold)

**243.9 MB → 54.1 MB (4.5×), git-shippable** (largest file smr_zone 19 MB < GitHub's 50 MB warn).
Chosen tolerances + validated drift (of 495 k points):

| Layer | tol | full hits | drift | drift % | full→simp |
|---|---|---|---|---|---|
| npws_sac | 15 m | 1,644 | **+19** | +1.16% | 45 MB → **3.1 MB** |
| npws_pnha | 15 m | 2,506 | +23 | +0.92% | 19 MB → 2.2 MB |
| npws_spa | 15 m | 1,476 | +5 | +0.34% | 14 MB → 1.9 MB |
| gsi_vulnerability | 10 m | 30,408 | −22 | −0.07% | 27 MB → 14.6 MB |
| zoning_gzt | 5 m | 233,846 | −179 | −0.08% | 65 MB → 12 MB |
| galway_county_landscape | 25 m | 21,759 | −5 | −0.02% | 0.9 MB → 0.2 MB |
| **galway_city_aca** | 7 m | 148 | −19 | **−12.8%** | 0.19 MB → 0.05 MB |
| **smr_zone** | 2 m | 21,334 | **−163** | −0.76% | 74 MB → **19 MB** (over threshold) |

The smooth NPWS layers simplify beautifully (SAC 45 MB → 3.1 MB, and **+** drift = the *safe*
over-flag direction). Two cases the validation flagged:
- **galway_city_aca** — tiny (148 hits); −19 is within the absolute floor but **−12.8%** relative.
  Fix: **never simplify sub-MB layers** (no size benefit). Keep full (0.19 MB).
- **smr_zone** — the one layer that **fails the threshold even at 2 m** (−163, −0.76%) *and* stays
  big (19 MB). It resists vector simplification on **both** axes → **rasterize it (Model A)**.

**Conclusion — the natural end state is a HYBRID:** simplified vectors for the smooth layers
(NPWS/vulnerability/zoning ≈ 35 MB, exact-enough), tiny layers kept full, and **smr_zone (and
optionally zoning) as a raster/grid**. This is Model A and Model B converging on the same answer:
big smooth designations → simplified vectors + live engine; dense small-polygon layers → grid.

## Recommendation

Both poles are now scoped/measured, and the bench reshapes the order:

0. **Simplify the layers to ~10 m FIRST (§23.9).** This is the prerequisite for *everything* —
   it shrinks the data (git-shippable), and the bench shows it also cuts per-eval time (the
   Corrib-polygon distance dominates). Do this before choosing A or B; validate the simplified
   join count against full precision (the ground-truth discipline already in §23.9).
1. **For launch / single-to-modest traffic → Model B (live + decoupled API).** Exact, reuses
   the real engine, leverages the existing FastAPI seam, keeps Streamlit Cloud lean. Mandatory
   add-ons from the bench: memoise `evaluate()`, keep the 1 h cache, run multi-worker. Viable
   once layers are simplified (per-eval drops from ~1.7 s toward a few hundred ms).
2. **For public scale / lowest cost → Model A (grid precompute).** Microsecond array-lookup,
   trivial concurrency, no always-on compute service. Cost: a raster-native build (rasterize +
   dilation) + cell-snap drift. The per-point prototype is its parity oracle. Revisit if/when
   Model B's per-core throughput or service cost bites.
3. ~~Fix the `nearest()` longitude-compression bug~~ **DONE (2026-06-16)** — two layers of fix in
   `layers.py`: (a) **distance** — `_metric_dist_m` (equirectangular cos(lat) correction) backs
   `nearest()` + `nearest_junction()`, so a 100 m-east feature reads 100 m not 167 m; (b)
   **selection** — `nearest()` now picks the true metric-nearest within a radius (not `tree.nearest`'s
   degree-space rank, which could pick a 160 m-north road over a 100 m-east one and wrongly report
   "no road within 150 m"). `_road` benefits automatically (unchanged). Regression tests
   `test/siting/test_layers_metric.py`; siting suite green (103 + 2 skip). `near()` left as the
   documented coarse over-inclusive gate.

**Net:** simplify is unconditional; B is the faster path to an *exact* public beta; A is the
endgame for cheap high-scale serving. They share the engine and the simplified layers, so work on
either is not wasted.

Already done to unblock the deploy: `utility/app.py` guards the siting import (Cloud no longer
crashes — the page degrades to a stub there); siting's geo deps moved to a `siting` extra so the
Cloud core stays lean; the 291 MB layers remain gitignored.
