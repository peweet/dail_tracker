# Scalability Plan — serving more concurrent users cheaply

## Problem

The public app is capped at roughly **500 concurrent users**. The cap is not
Streamlit-the-framework; it is **Streamlit Community Cloud + Streamlit's session
model**:

- Community Cloud runs **one container, one process** (~1 vCPU / ~2.7 GB RAM).
  There is no horizontal-scaling lever on that platform.
- Every viewer holds a **websocket + server-side session**, and *every
  interaction re-runs the whole Python script* for that session. Concurrency is
  therefore bounded by RAM/CPU on that single box, with platform throttling on
  top.

## The fact that makes this cheap

The data is **read-only and identical for every user** — it is baked into the
image and refreshed by rebuild (see the `Dockerfile` header comment), never
mutated at runtime. There is **zero shared mutable state**. That makes the app
*embarrassingly* horizontally scalable: "scaling" reduces to "run more identical
copies" and/or "cache the answers."

The hard part is already built: a **stateless FastAPI read-only API**
([`api/main.py`](api/main.py)) covering essentially every page's data, with a
working `Dockerfile` that bakes the committed gold/silver parquet in. This plan
extends the existing uncoupling work in
[`doc/fastapi_query_core_uncoupling_plan.md`](fastapi_query_core_uncoupling_plan.md).

---

## Three layers, cheapest first

### Layer 0 — Squeeze the single box (free; do regardless)

Cut per-user cost so one instance holds more:

- Confirm the DuckDB connection is `@st.cache_resource` (shared across sessions,
  not per-user) and that every query is wrapped in `@st.cache_data`, so popular
  pages compute once and serve from cache rather than per-viewer.
- Avoid heavy widgets that re-run the whole script on every interaction.

Won't 10× the ceiling, but it is free headroom and makes every Layer-1 replica
go further.

### Layer 1 — Run N replicas of the Streamlit container (cheap; ~no code change)

Move off Community Cloud onto a host that supports `replicas = N` — **Fly.io** or
**Render**.

- Add a `Dockerfile.streamlit` (mirror the existing API `Dockerfile`; bake data
  in the same way) plus a `fly.toml` / `render.yaml`.
- Because data is read-only and baked in, **every replica is byte-identical and
  needs no coordination** — no shared DB, no cache server. `replicas = 4` ≈ 4 ×
  today's ceiling.
- **Requirement: sticky sessions.** Streamlit is websocket-based, so a user must
  stay pinned to one replica. Fly's `sticky` / `fly-replay`, Render session
  affinity, or a cookie pin in the existing Cloudflare Worker all work.
- **Cost:** small `shared-cpu-1x` 512 MB machines, single-digit €/month each;
  Fly autostop / scale-to-zero drives idle cost toward €0. ~2,000 concurrent ≈
  ~4 small machines.

This is the fastest "just more users now."

> **Note — the siting/planning engine is a SEPARATE, compute-bound track.** It does not fit
> the read-only model below (live geometry per request). Its scaling plan is
> [SITING_PRECOMPUTE_PLAN.md](SITING_PRECOMPUTE_PLAN.md): precompute geometry to a grid →
> cheap lookup → folds back into Layers 1/2.

### Layer 2 — CDN-cache the API + thin static frontend (biggest leverage)

The real fix, and ~80% built. The API is **stateless HTTP over static data**, so
identical requests return identical bytes between refreshes — cacheable, which a
websocket Streamlit app can never be.

- Add `Cache-Control: public, s-maxage=…` + `ETag` to API responses (not set
  today), keyed to the data version, with a **cache-purge step in the
  data-refresh GitHub Action**.
- Put **Cloudflare in front of the API** (zone/Worker already owned). Once a
  response is at the edge, the 501st — or 50,000th — user is served **without
  touching the origin**, on Cloudflare's free/cheap cache tier. Popular-read
  concurrency becomes effectively unbounded.
- Serve the **high-traffic public pages as a thin static frontend** (even
  pre-rendered HTML) hitting the cached API. Keep Streamlit as the interactive
  "lab" for power-user drill-downs and the long tail.

---

## Recommended sequence

1. **Layer 0** now — free, prerequisite.
2. **Layer 1** — immediate concurrency bump; get off the single-box platform.
3. **Layer 2** — structural answer; cheapest-per-user, scales to "any number"
   for cents.

**Rough cost at ~2,000 concurrent:** a few Fly machines (~€10–30/mo) + Cloudflare
cache (free tier likely sufficient). No managed DB, no per-user backend — the
payoff of read-only static data.

## Open implementation items (when picked up)

- `Dockerfile.streamlit` + `fly.toml`/`render.yaml` with replicas + session
  affinity (Layer 1).
- Cache-Control/ETag middleware in `api/` + CDN cache rules + Action purge step
  (Layer 2).
- Decide which pages move to the static frontend vs stay in Streamlit (Layer 2).
