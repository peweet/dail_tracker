"""EXPERIMENTAL — standalone FastAPI prototype of the DECOUPLED siting service (topology B).

Tests the "live app, compute decoupled from Streamlit" plan: the geospatial engine runs in
its OWN service; a thin Streamlit client would just HTTP-GET this. This prototype measures
RESPONSIVENESS — cold-start warm time, warm per-request latency (with/without the DEM network
hop), and behaviour under concurrent load.

It imports the engine read-only (dail_tracker_core.siting) — it does NOT modify the in-flux
engine files. Run:  python pipeline_sandbox/siting_api_prototype.py   (serves :8077)
The companion bench is pipeline_sandbox/siting_api_bench.py. Nothing is promoted.
"""

from __future__ import annotations

import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "utility"))

from fastapi import FastAPI, Query  # noqa: E402

from dail_tracker_core.siting.engine import evaluate  # noqa: E402
from dail_tracker_core.siting.layers import LayerStore  # noqa: E402

_WARM_PT = (-9.0264, 53.2987)  # Menlo, Galway — touches many layers

# ONE shared store, built once and reused for every request. Critical: engine.evaluate() does
# `store = store or LayerStore()`, so calling it WITHOUT a store builds a fresh store per request
# and rebuilds every layer's STRtree from scratch — that reload was most of the per-request cost.
# Optional SITING_LAYERS_DIR points at a simplified layer set (e.g. c:/tmp/siting_simplify_final).
import os  # noqa: E402

_LAYERS_DIR = os.environ.get("SITING_LAYERS_DIR")
_STORE = LayerStore(_LAYERS_DIR) if _LAYERS_DIR else LayerStore()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Warm the SHARED store once at startup: loads the council spine (495k pts -> STRtree) and the
    # designation layers' STRtrees into memory, so every request reuses them (no per-request reload).
    t0 = time.time()
    evaluate(*_WARM_PT, store=_STORE)
    app.state.warm_s = time.time() - t0
    app.state.layers_dir = _LAYERS_DIR or "(default full-precision)"
    yield


app = FastAPI(title="Siting API (prototype)", lifespan=lifespan)


def _serialize(res) -> dict:
    return {
        "lon": res.lon,
        "lat": res.lat,
        "dev_type": res.dev_type,
        "council": {
            "name": res.council.council_name or res.council.authority,
            "slug": res.council.slug,
            "on_boundary": res.council.on_boundary,
        },
        "fired": [
            {
                "node_id": i.node_id,
                "title": i.title,
                "mitigation_class": i.mitigation_class,
                "flag": i.flag,
            }
            for i in res.fired
        ],
        "missing_layers": res.missing_layers,
        "disclaimer": res.disclaimer,
    }


@app.get("/healthz")
def healthz() -> dict:
    return {"ok": True, "warm_s": round(getattr(app.state, "warm_s", -1.0), 2)}


@app.get("/evaluate")
def evaluate_endpoint(
    lon: float = Query(...),
    lat: float = Query(...),
    dev: str = Query("one_off_house"),
) -> dict:
    t0 = time.time()
    res = evaluate(lon, lat, dev, store=_STORE)  # reuse the shared, pre-warmed store
    out = _serialize(res)
    out["_server_ms"] = round((time.time() - t0) * 1000, 1)
    return out


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8077, log_level="warning")
