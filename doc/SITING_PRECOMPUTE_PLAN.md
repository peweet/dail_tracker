# Siting / Planning Compute Scaling — Precompute Plan

> **Status: PLANNING. Multi-session — execute phase-by-phase, re-read this doc each session.**
> Companion to [SCALABILITY_PLAN.md](SCALABILITY_PLAN.md) (the read-only app's Layers 0/1/2).
> This doc covers the *second, compute-bound track*: the geospatial siting engine.

## The problem in one paragraph

The read-only app scales cheaply because its data is static and identical per user
(see [SCALABILITY_PLAN.md](SCALABILITY_PLAN.md)). The **siting engine does not fit that
model**: each `/evaluate` runs live geometry — roughly ~30 `STRtree` queries (a *code
hand-count, NOT profiled*) with exact `covered_by` predicates, plus exactly **2** `terrain()`
calls (DEM is disk+memory cached via `dem_cache.parquet`; a live COG range-read happens only on
a cold-miss point — it is NOT a per-request S3 hop). **A warm `evaluate()` was MEASURED at
~577 ms p50 / 735 ms mean / 1.47 s p95** on this box (shared pre-warmed store, 80 calls,
2026-06-18) — i.e. **< 2 requests/sec/core**. **The cost is NOT the geometry:** 2000
`covered_by` queries on the 433-polygon SAC layer ran in 597 ms (~0.3 ms each, matching §13.7
of [PLANNING_PERMISSION_SCOPING.md](PLANNING_PERMISSION_SCOPING.md)), so the ~30 geometry ops are
< ~10 ms of the 577 ms — **~98 % is non-geometry**. `load_catalogue` / rulebook readers / the
council spine are all `lru_cache`d (so it is NOT naive re-parsing); prime suspects are
`rulebook.resolve()` (called ~13×/eval, top-level uncached) and `resolve_council`'s query against
the 495,632-point application spine. **Phase 0 must `cProfile` it.** Threading helps only partly
(serial 53.4 s vs 4-thread 24.9 s = **2.14×**, MEASURED — *partially* GIL-bound, not the "1×"
processes-only assumption), so worker processes still scale fuller. Lumping it with the cheap
read traffic would let a siting spike starve everything (a handful of users saturate a core).
The fix is to **move the geometry offline (precompute) and make the online path a cheap
lookup**, which folds siting back into the read-bound tier that Layers 1/2 already scale.

## Goal / non-goals

- **Goal:** convert siting from compute-bound to read-bound *without changing any planning
  semantics or weakening the honesty rules*, so it scales like the rest of the app.
- **Non-goal:** rewriting the engine, changing the rulebook, or chasing the literature's
  1000× (that's batch-scale; our online win is "remove exact-polygon predicates from the hot
  path"). Keep the live engine as fallback/precise-mode forever.

## Prior art (validated — this is a solved problem)

The approach is standard: **grid/raster approximation of vector polygons** + a **hybrid
prefilter** (cheap cell lookup, exact GEOS check only on boundary cells).

**Canonical architecture = the two-step "filter-and-refine" paradigm** (Orenstein 1986;
Brinkhoff/Kriegel/Schneider, *Multi-Step Processing of Spatial Joins*, SIGMOD 1993): a cheap
*filter* on an approximation (MBR/grid/index) yields candidates with **no false dismissals**
(conservative — may admit false hits, never drops a true hit), then an exact-geometry *refine*
removes false hits; an intermediate filter can mark candidates sure-hit / sure-miss / indecisive
so only indecisive ones hit exact geometry. Mapping:
- The engine ALREADY does this per-query: `LayerStore` STRtree = MBR filter; exact `covered_by` = refine.
- The grid precompute = **materialise the filter** (scale-independence via incrementally-maintained
  materialised views — Armbrust et al., SIGMOD 2013); grid cell classifies a point sure-in /
  sure-out / boundary-indecisive. **NB the modern approximation papers (GeoBlocks, distance-bounded)
  DROP the refine step** and answer from the grid alone — fine for analytics/viz, NOT for our
  exclusion mask. So we KEEP the exact refine on boundary cells = the **classical** Brinkhoff
  multi-step, deliberately *more* conservative than the grid-only papers.
- **Invariant #1 (exclusion never under-reports) == the filter's no-false-dismissals law.** The
  guarantee comes from building the exclusion grid **CONSERVATIVELY** (any cell *touching* the
  polygon = inside-candidate → only false positives, never false negatives; distance-bounded). The
  exact-on-boundary refine is for **precision** (removing the over-report), NOT for the safety itself.
- **Caveat (CONFIRMED vs paper):** materialisation is sound here ONLY because this is point-vs-static-layer containment.
  Fully materialising a true spatial *conjunctive* query (join + spatial) costs ~result-size space
  (quadratic/cubic; index beats it) — arXiv [2509.10050](https://arxiv.org/abs/2509.10050). Don't
  generalise the grid to spatial joins.

- **GeoBlocks** (arXiv [1908.07753](https://arxiv.org/abs/1908.07753), EDBT 2021): binary
  **cell-covering** (every cell touching the outline is "in" — NOT distances/fractions), error
  bounded by the **cell diagonal √(ε₁²+ε₂²)**, **NO refine step**, + a workload-adaptive trie cache
  of hot regions (≈ our CDN tier). The ~1000× is over on-the-fly **aggregation**. CAVEAT: it is an
  **aggregation-over-query-polygons** system (fixed points; polygon = query) — the *inverse* of our
  point-vs-fixed-polygon case, so it is a partial analogy, not a template. Transferable: the
  cell-diagonal error bound, the resolution/storage data (**level 17 ≈ 100 m balances; level 21
  ≈ 6 m → exponential overhead; 5–50 % storage**), and the hot-region cache. The *representation*
  does NOT transfer.
- **Distance-bounded spatial approximations** (arXiv [2010.12548](https://arxiv.org/abs/2010.12548)):
  raster approximation with **Hausdorff error dH(g,g′) ≤ ε**; a CONSERVATIVE raster yields **only
  false positives, never false negatives**. They DROP exact refine ("answers solely on the
  approximate geometries"), explicitly for viz/interactive where **exact is not required** — so our
  exclusion mask keeps refine. (8.5× at a 10 m bound, 0.15 % median error; 1 m too fine for GPU.)
  This (not GeoBlocks) backs the distance-per-cell idea — but distance-per-cell is OUR synthesis,
  to keep thresholds tunable and avoid the ultra-fine binary grid GeoBlocks shows is exponentially costly.
- **H3 at billion-scale** (Databricks): ~50× faster / ~90× cheaper; pure-grid under-counts
  ~0.1% at boundaries → **hybrid (grid + exact-on-boundary) recovers precision** at ~2× cost.
- **Batch grid-builder engine — UNDECIDED, and the obvious choice is contradicted locally.**
  §13.7 of [PLANNING_PERMISSION_SCOPING.md](PLANNING_PERMISSION_SCOPING.md) benchmarked THIS
  workload on THIS box: **shapely 2.x STRtree (4.6 s, trivial memory) beat DuckDB-spatial
  (24 s tuned `threads=1`; OOM'd 5.5–12.5 GB untuned; no `ST_Subdivide` in 1.5.3)**. The
  external "DuckDB saturates all cores" claim does NOT hold here. Default the builder to the
  proven shapely/`LayerStore` path run in batch; use DuckDB-spatial only if its SQL/parquet-native
  fit justifies the vertex-management/memory tuning.

## Hard invariants (must hold in EVERY phase — these are correctness, not preference)

1. **The hard-exclusion mask must NEVER under-report.** A point inside an SAC/SPA/NHA/National
   Park must always surface. → build the exclusion grid **CONSERVATIVELY** (any cell *touching* the
   polygon = inside-candidate) so it structurally yields only false positives, never false negatives
   (distance-bounded, arXiv 2010.12548) — **that** is what guarantees no under-report; then an
   **exact GEOS check on boundary cells** removes the over-report (precision). (0.1% boundary error
   is fine for taxis, not for statutory exclusions.) See
   [engine.py](../dail_tracker_core/siting/engine.py) `hard_exclusions`.
2. **`layer_missing` semantics preserved.** "Not computed" must stay distinct from "computed,
   no issue" — the grid must encode missing/out-of-extent, never silently return "ok".
3. **No inference, no verdict** (memory: `feedback_no_inference_in_app`,
   `feedback_planning_features_check_council_docs`). Thresholds/triggers stay sourced from the
   rulebook; precompute only changes *how fast* we read the same facts.
4. **Sandbox → vet → promote** (memory: `feedback_pipeline_changes_data_anchored_promotion`).
   Grid build stays in `pipeline_sandbox/` / `c:/tmp` until parity-vetted, then promotes to
   `extractors/` + `data/` with its own checkpoint. Parquet writes use the zstd+stats atomic
   writer (`services/parquet_io.save_parquet`).

## Assets that already exist (don't rebuild)

- Engine: [dail_tracker_core/siting/](../dail_tracker_core/siting/) — `engine.py`, `layers.py`
  (`LayerStore`), `dem.py`, `council.py`, `catalogue.py`, `rulebook.py`.
- Decoupled service prototype: [pipeline_sandbox/siting_api_prototype.py](../pipeline_sandbox/siting_api_prototype.py)
  (FastAPI on :8077, shared pre-warmed store).
- Measurement: [pipeline_sandbox/siting_api_bench.py](../pipeline_sandbox/siting_api_bench.py)
  (emits `_server_ms`) + **[tools/siting_loadtest.py](../tools/siting_loadtest.py)** (closed-loop
  concurrency, promoted + unit-tested).
- Precompute spikes: `pipeline_sandbox/siting_grid_precompute_experimental.py`,
  `siting_layers_simplify_finalize.py` (per-layer-tuned simplify; outputs to
  `c:/tmp/siting_simplify_final` — **NOT promoted to the repo**; the "244→54 MB" figure is from
  a prior run recorded in memory, re-verify by running the script, it is not hardcoded).
- Live pages: [utility/pages_code/siting_check.py](../utility/pages_code/siting_check.py),
  `siting_remote.py`.

---

## Phased plan (each phase = its own session/PR, with a go/no-go gate)

### Phase 0 — Measure & baseline  ·  *gate: profile-fix vs grid*
- **PARTLY DONE (2026-06-18):** warm `evaluate()` ≈ **577 ms p50 / 735 ms mean** measured
  directly (no server) via `c:/tmp/siting_microbench.py` (one-off, not promoted). Geometry proven
  cheap (~10 ms); threads scale 2.14×.
- **STILL TODO:** `cProfile` one `evaluate()` to localise the ~565 ms non-geometry cost
  (suspects: `rulebook.resolve()` ×~13, `resolve_council` 495k-spine query, object building).
  Optionally `siting_api_bench.py` / `tools/siting_loadtest.py` for the end-to-end HTTP number.
- **Gate (already informed):** 577 ms/eval = < 2 req/s/core → the live engine can't serve
  concurrency. BUT the cost isn't geometry, so run **Phase 1's profile-fix FIRST and re-measure**;
  the grid is only justified if the *fixed* engine still can't meet the target.

### Phase 1 — Cheap levers first  ·  *gate: is that enough?*
- **DO THIS FIRST: `cProfile` one `evaluate()`.** Geometry is proven cheap (~10 ms of 577 ms);
  the other ~565 ms is non-geometry (suspects: `rulebook.resolve()` ×~13, `resolve_council`
  495k-spine query, per-call object building). Memoising/restructuring that is likely a **large
  win with NO grid** — and may be enough on its own. Re-measure after.
- Prepared-geometry caching is a **NO-OP** (MEASURED 2026-06-18: shapely 2.1.2 STRtree already
  prepares; 597 ms vs 592 ms) — skip it. Ship the simplified set via `SITING_LAYERS_DIR` (it must
  be **promoted/git-tracked from `c:/tmp/siting_simplify_final` first** — currently untracked).
  Serve with `uvicorn --workers N` (threads give ~2.1×; processes scale fuller — see note above).
- Re-run Phase 0 measurement. **Gate:** if the new req/s × affordable replicas covers the
  target, **defer the grid** (record the number here). Else proceed to Phase 2.

### Phase 2 — Precompute DESIGN spike (sandbox)  ·  *the big "needs refinement" phase*
- Decide the representation (see Open Questions): **continuous distance-to-nearest per cell**
  (preferred — keeps thresholds tunable, distance-bounded error) vs boolean+exact-boundary.
- Pick grid: resolution (tied to 60 m/100 m/150 m thresholds), extent, cell id scheme
  (own grid vs H3/S2). Define the cell schema (one row/cell: per-layer distance/containment,
  DEM slope/elev, nearest road class, council, missing/extent flags).
- Build a TINY grid for one already-ingested area (Galway) and write a **parity harness**:
  grid-backed result vs live `evaluate()` over a random point sample → must agree (and
  exclusions must never be missed). **Gate:** parity within tolerance on the sample.

### Phase 3 — Build the grid (batch, sandbox)
- Batch job (engine per the prior-art note — **default shapely/`LayerStore`, NOT assumed
  DuckDB**): grid points × all layers + DEM → cell parquet, for Galway first.
- Storage check (estimate; land-only is less): a 50 m grid is ~28M cells over the Republic
  (≈70,273 km²) or ~34M over the whole island (≈84,421 km²); × ~15 attrs ≈ tens of GB — fine.
  Confirm the regional (Galway) slice is small. zstd+stats via `services/parquet_io.save_parquet`.

### Phase 4 — Online lookup path (sandbox, behind a flag)
- Point → cell lookup (DuckDB) + live cheap gates (dev_type/units/floor-area) + rule-text
  join + **hybrid exact-GEOS check on boundary cells for the exclusion mask**.
- Wire as a cache layer *inside* the engine path with **live fallback** for out-of-grid
  cells and threshold-straddle points (recompute exactly when distance is within ~1 cell of a
  cutoff).

### Phase 5 — Parity gate & serve
- Full parity test suite (grid vs live; exclusion never-under-report; `layer_missing`
  preserved) in CI. Promote grid build → `extractors/`, grid data → `data/` (own checkpoint).
- Route the siting service to the grid-backed path; it's now read-bound → scales via
  Layers 1/2 (process replicas) + CDN-cache (responses cacheable by cell+dev_type+layer-version).

### Phase 6 — National rollout & refresh integration
- Extend grid to all ingested regions. Hook grid rebuild into the layer-refresh cadence
  (rebuild on layer change; bump a `layer_version` that keys the CDN cache + purge).

---

## Open questions / refinement log  *(EDIT THIS across sessions — this is where the iteration lives)*

- [ ] **Representation:** continuous distance-per-cell vs boolean+exact-boundary. (Leaning
      distance-per-cell for tunable thresholds; confirm storage cost in Phase 3.)
- [ ] **Resolution:** what cell size keeps boundary error < the tightest threshold (monument
      60 m)? Uniform grid vs finer near boundaries vs H3 res 11/12?
- [ ] **"Near" radii up to 2 km** (European site): store distance-to-nearest per layer (cheap)
      vs precomputed boolean rings. Distance-to-nearest preferred.
- [ ] **Boundary-cell definition** for the exact-GEOS hybrid: how wide a margin counts as
      "ambiguous" and triggers the exact check?
- [ ] **DEM:** a point-keyed DEM cache ALREADY EXISTS (`dem_cache.parquet`, ~0.1 m grid key,
      `lru_cache` on top); a per-cell grid is the batch extension of it. Confirm raster→grid
      resample is faithful to `dem.terrain()`.
- [ ] **Council spine** (the "495k → STRtree" in the prototype): precompute council-per-cell?
- [ ] **Cell id scheme:** roll our own integer grid vs adopt H3/S2 (interop + tooling, but
      hexagon non-congruency complicates "distance" semantics).
- [ ] **Parity tolerance:** exact match required for exclusions; what tolerance for "fired"
      flags driven by distance thresholds near a cutoff?

## How to resume (cold-start checklist for a future session)

1. Read this doc + [SCALABILITY_PLAN.md](SCALABILITY_PLAN.md). 2. Check the boxes above for
   what's decided. 3. `git log --oneline` for siting/grid commits since last session.
4. The current phase's "Gate" line tells you the next concrete output to produce.
5. If using subagents for the grid build, **forbid git in their prompts and check `git reflog`
   after** (memory: `feedback_subagents_ran_git_push`).

## Decision log  *(append dated one-liners as gates are passed)*

- 2026-06-18 — Plan created. Prior art validated (GeoBlocks/H3/distance-bounded). `tools/siting_loadtest.py`
  promoted + tested. Phase 0 not yet run — **next action: run the bench + loadtest to get warm `server_ms`.**
- 2026-06-18 — Papers checked AGAINST the plan (full text via ar5iv). GeoBlocks = binary
  cell-covering + aggregation-over-query-polygons (NOT point queries) and DROPS refine → partial
  analogy only; distance-bounded uses Hausdorff ε, also DROPS refine (viz/"exact not required").
  So our grid+exact-refine hybrid is CLASSICAL Brinkhoff, deliberately more conservative than the
  modern approx papers. KEY correction: a CONSERVATIVE exclusion grid alone guarantees no-under-report
  (only false positives) — refine is for precision, not safety. Space-time 2509.10050 blowup CONFIRMED
  join-only (not our containment). Resolution: GeoBlocks level17≈100m / level21≈6m exponential →
  60m threshold favours distance-per-cell over fine binary.
- 2026-06-18 — Unverified claims TESTED (`c:/tmp/siting_microbench.py`, infer-nothing): warm
  `evaluate` ≈ **577 ms p50** (NOT ~5–30 ms — ~20–100× off); geometry only ~10 ms of it (~98 %
  non-geometry → `cProfile` next); threads **2.14×** (partially GIL-bound, not processes-only);
  prepared-geom lever = **NO-OP** (shapely 2.1.2 already prepares); cells **~28.1M** (RoI
  70,273 km²); simplified set untracked in `c:/tmp` (55M), live set 492M (the "244 MB" input is stale).
- 2026-06-21 — **Phase 0 `cProfile` DONE (the gating TODO) — it OVERTURNS the diagnosis above.**
  The ~565 ms "non-geometry" was NOT `rulebook.resolve`/council-spine (both negligible — resolve
  absent from top `tottime`; geometry ~7 ms, STRtree `query` 16 calls = 7 ms). **The real cost is
  LAYER I/O in `LayerStore.load` = polars `read_parquet` (collect) + `shapely.from_wkb` deserialize.**
  No-store `evaluate()` (the DEFAULT path, `store or LayerStore()`) = **~5–6 s/call** — reloads +
  re-parses all 19 layers EVERY call (5 "warm" calls never sped up: 6.5→4.4 s). Shared-store "warm"
  = **~0.7–1 s median with 4 s spikes** because the cache is **LEAKY**: `load` is
  `@lru_cache(maxsize=32)` on a BOUND method (key incl. `self`); force-preloading all 19 geom layers
  STILL leaves misses=8/run + DEM cold-reads (the 4 s spikes = `terrain()` `/vsicurl` COG read on a
  point not in `dem_cache.parquet`). `dem_cache` has no `wkb` col (correctly NOT a geom layer).
  **IMPLICATION — the grid is attacking the wrong cost AND is a sledgehammer:** a process-wide,
  NON-evicting parsed-`Layer` (geoms+STRtree) cache built ONCE + a LOCAL DEM tile (not per-cold-point
  network read) removes the SAME I/O cost with **no grid, no ~28M cells, no tens-of-GB, no staleness,
  full exactness**. **NEW Phase 1 (supersedes the grid pending re-measure):** (1) hold parsed `Layer`
  process-wide & preload at store init (kill the maxsize=32 bound-method eviction); (2) DEM: ship a
  local Ireland tile so `terrain()` never does a cold `/vsicurl` hop; (3) re-measure — expect tens of
  ms warm. Build the grid ONLY if the fixed-warm engine still can't meet the concurrency target.
- 2026-06-18 — Plan audited claim-by-claim vs code + papers (sources, infer-nothing). CORRECTED:
  DuckDB-as-builder / "saturates all cores" → contradicted by §13.7 (shapely STRtree 4.6 s beat
  DuckDB 24 s/OOM); DEM "1–3 S3 reads" → exactly 2 cached `terrain()` calls (`dem_cache.parquet`);
  "34M cells" → ~28M (RoI). FLAGGED UNVERIFIED: per-request ms/cycles (Phase 0), GIL claim,
  prepared-geom lever (likely no-op in shapely 2.x), simplified set not promoted (in `c:/tmp`).
  VERIFIED: all file refs, thresholds, exclusion layers, `save_parquet` defaults, 495k spine, paper figures.
