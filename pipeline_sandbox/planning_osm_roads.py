"""Phase 0 (sandbox): OSM local-road network -> roads layer (Galway-first, via Overpass).

The road_sightlines node needs the LOCAL road a site fronts onto (class + maxspeed) — the
level TII's national-roads data omits, where one-off access/sightline refusals bite (§18.4).
Production should pre-ingest the Geofabrik Ireland extract (385 MB, ODbL); for the Galway-first
build we make a SINGLE one-time Overpass bbox pull at INGEST time (not per user request — the
sanctioned use), filtered to highways, stored as the same WKB-line layer format as the others.

Attribution (ODbL): "© OpenStreetMap contributors" must show wherever the road data surfaces.
Limit (honest): OSM gives road centrelines + (sparse) maxspeed, NOT visibility splays — it
locates/classifies the road; sightlines still need a site survey.

    python pipeline_sandbox/planning_osm_roads.py
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import polars as pl
import requests
import shapely
from shapely.geometry import LineString

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_osm_roads")
OUT = Path("pipeline_sandbox/_planning_output/layers")
# Galway city + Menlo + surrounds (S, W, N, E). Galway-first; widen per the rollout.
BBOX = (53.20, -9.25, 53.42, -8.90)
ENDPOINTS = ["https://overpass-api.de/api/interpreter", "https://overpass.kumi.systems/api/interpreter"]
KEEP = ("highway", "maxspeed", "name", "ref")


def fetch_ways() -> list[dict]:
    s, w, n, e = BBOX
    ql = f'[out:json][timeout:120];way["highway"]({s},{w},{n},{e});out geom tags;'
    headers = {"Content-Type": "text/plain", "User-Agent": "dail-tracker-planning-ingest/1.0"}
    last = None
    for url in ENDPOINTS:
        try:
            r = requests.post(url, data=ql.encode("utf-8"), headers=headers, timeout=180)
            r.raise_for_status()
            return r.json().get("elements", [])
        except Exception as ex:  # noqa: BLE001
            LOG.warning("overpass %s failed: %s", url, ex)
            last = ex
    raise SystemExit(f"all overpass endpoints failed: {last}")


def main() -> None:
    setup_standalone_logging("planning_osm_roads")
    OUT.mkdir(parents=True, exist_ok=True)
    ways = fetch_ways()
    LOG.info("overpass returned %d ways", len(ways))

    rows: list[dict] = []
    skipped = 0
    for el in ways:
        geom = el.get("geometry") or []
        if len(geom) < 2:
            skipped += 1
            continue
        line = LineString([(p["lon"], p["lat"]) for p in geom])
        tags = el.get("tags") or {}
        rec = {k: tags.get(k) for k in KEEP}
        rec["wkb"] = shapely.to_wkb(line)
        rows.append(rec)

    df = pl.DataFrame(rows)
    dest = save_parquet(df, OUT / "osm_roads.parquet")
    cov = {"layer": "osm_roads", "source": "OpenStreetMap via Overpass",
           "licence": "ODbL — © OpenStreetMap contributors", "bbox_S_W_N_E": list(BBOX),
           "ways": len(ways), "kept": df.height, "skipped": skipped,
           "keep_fields": list(KEEP), "crs": "EPSG:4326"}
    (OUT / "osm_roads_coverage.json").write_text(json.dumps(cov, indent=2), encoding="utf-8")
    LOG.info("wrote %s (%d roads, %d skipped)", dest, df.height, skipped)
    print(f"OK osm_roads: {dest} | kept {df.height}/{len(ways)} ways | © OpenStreetMap contributors")


if __name__ == "__main__":
    main()
