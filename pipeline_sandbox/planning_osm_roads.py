"""Phase 0 (sandbox): OSM local-road network -> roads layer (via Overpass, region-tiled).

The road_sightlines node needs the LOCAL road a site fronts onto (class + maxspeed) — the
level TII's national-roads data omits, where one-off access/sightline refusals bite (§18.4).
Production should pre-ingest the Geofabrik Ireland extract (385 MB, ODbL); for the build we
make one-time Overpass pulls at INGEST time (the sanctioned use), filtered to highways, stored
as the same WKB-line layer format as the other layers.

A whole COUNTY bbox is too big for a single Overpass query, so a region is TILED into ~0.3°
cells, each pulled separately and MERGED into osm_roads.parquet (dedup by wkb) — mirroring
extractors.planning_layers_ingest.add_region; coverage records the region under `regions_added`
so LayerStore.in_extent opens it up. Outside every ingested region a point is honest layer_missing.

Attribution (ODbL): "© OpenStreetMap contributors" must show wherever the road data surfaces.
Limit (honest): OSM gives road centrelines + (sparse) maxspeed, NOT visibility splays.

    python pipeline_sandbox/planning_osm_roads.py                 # base Galway-city pull
    python pipeline_sandbox/planning_osm_roads.py --region cork    # tiled pull, merge into osm_roads
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import polars as pl
import requests
import shapely
from shapely.geometry import LineString

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_osm_roads")
OUT = Path(__file__).resolve().parents[1] / "data/silver/parquet/planning_layers"
BBOX = (53.20, -9.25, 53.42, -8.90)  # base Galway city + Menlo (S, W, N, E)
ENDPOINTS = ["https://overpass-api.de/api/interpreter", "https://overpass.kumi.systems/api/interpreter"]
KEEP = ("highway", "maxspeed", "name", "ref")
TILE_DEG = 0.3  # a county bbox is too big for one Overpass query -> tile it

# region bboxes in (minlon, minlat, maxlon, maxlat) — mirror planning_layers_ingest.REGIONS
REGIONS: dict[str, tuple[float, float, float, float]] = {
    "galway": (-9.25, 53.20, -8.90, 53.42),
    "dublin": (-6.55, 53.15, -5.95, 53.65),
    "cork":   (-10.3, 51.4, -7.8, 52.4),
}


def fetch_ways(s: float, w: float, n: float, e: float, attempts: int = 3) -> list[dict]:
    """All highway ways in the (S,W,N,E) bbox. Retries both endpoints with backoff (Overpass
    504s under load are common); raises RuntimeError after all attempts so callers can SKIP one
    tile and continue — never SystemExit (which would abort a whole multi-tile region pull)."""
    ql = f'[out:json][timeout:180];way["highway"]({s},{w},{n},{e});out geom tags;'
    headers = {"Content-Type": "text/plain", "User-Agent": "dail-tracker-planning-ingest/1.0"}
    last = None
    for attempt in range(attempts):
        for url in ENDPOINTS:
            try:
                r = requests.post(url, data=ql.encode("utf-8"), headers=headers, timeout=240)
                r.raise_for_status()
                return r.json().get("elements", [])
            except Exception as ex:  # noqa: BLE001
                last = ex
                LOG.warning("overpass %s failed (attempt %d): %s", url, attempt + 1, ex)
        if attempt < attempts - 1:
            time.sleep(10 * (attempt + 1))  # backoff before the next round
    raise RuntimeError(f"overpass failed after {attempts} rounds: {last}")


def _rows(ways: list[dict], have: set) -> tuple[list[dict], int]:
    """Build {keep + wkb} rows from Overpass ways, skipping wkb already in `have` (dedup)."""
    rows, skipped = [], 0
    for el in ways:
        geom = el.get("geometry") or []
        if len(geom) < 2:
            skipped += 1
            continue
        wkb = shapely.to_wkb(LineString([(p["lon"], p["lat"]) for p in geom]))
        if wkb in have:
            continue
        tags = el.get("tags") or {}
        rec = {k: tags.get(k) for k in KEEP}
        rec["wkb"] = wkb
        have.add(wkb)
        rows.append(rec)
    return rows, skipped


def _tiles(minlon, minlat, maxlon, maxlat, step=TILE_DEG):
    """Split a bbox into ~step° (S,W,N,E) tiles small enough for one Overpass query each."""
    out, lat = [], minlat
    while lat < maxlat - 1e-9:
        lon = minlon
        while lon < maxlon - 1e-9:
            out.append((lat, lon, min(lat + step, maxlat), min(lon + step, maxlon)))
            lon += step
        lat += step
    return out


def _write_coverage(region: str | None, ways_total: int, kept: int, skipped: int,
                    failed_tiles: list | None = None) -> None:
    cov_path = OUT / "osm_roads_coverage.json"
    cov = json.loads(cov_path.read_text(encoding="utf-8")) if cov_path.exists() else {
        "layer": "osm_roads", "source": "OpenStreetMap via Overpass",
        "licence": "ODbL — © OpenStreetMap contributors", "keep_fields": list(KEEP), "crs": "EPSG:4326",
    }
    if region is None:
        s, w, n, e = BBOX
        cov["bbox_S_W_N_E"] = list(BBOX)
        cov["bbox_subset"] = [w, s, e, n]  # (minlon,minlat,maxlon,maxlat) for in_extent
        cov["ways"], cov["kept"], cov["skipped"] = ways_total, kept, skipped
    else:
        cov.setdefault("regions_added", [])
        cov["regions_added"] = sorted(set(cov["regions_added"]) | {region})
        cov[f"region_{region}"] = {"bbox": list(REGIONS[region]), "ways": ways_total,
                                   "added": kept, "failed_tiles": failed_tiles or []}
        cov["kept"] = kept
    cov_path.write_text(json.dumps(cov, indent=2), encoding="utf-8")


def base_pull() -> None:
    ways = fetch_ways(*BBOX)
    have: set = set()
    rows, skipped = _rows(ways, have)
    df = pl.DataFrame(rows)
    dest = save_parquet(df, OUT / "osm_roads.parquet")
    _write_coverage(None, len(ways), df.height, skipped)
    print(f"OK osm_roads (base Galway): {dest} | kept {df.height}/{len(ways)} | © OpenStreetMap contributors")


def add_region(region: str) -> None:
    """Tile the region bbox, pull each tile, MERGE new roads into osm_roads.parquet (dedup by wkb)."""
    dest = OUT / "osm_roads.parquet"
    if not dest.exists():
        raise SystemExit("no base osm_roads.parquet — run the base pull first")
    existing = pl.read_parquet(dest)
    have = set(existing["wkb"].to_list())
    tiles = _tiles(*REGIONS[region])
    LOG.info("[osm +%s] %d tiles, %d existing roads", region, len(tiles), existing.height)
    new_rows: list[dict] = []
    seen_ways = 0
    failed: list[list[float]] = []
    for i, (s, w, n, e) in enumerate(tiles, 1):
        try:
            ways = fetch_ways(s, w, n, e)
        except Exception as ex:  # noqa: BLE001 — skip a dead tile, keep the rest (re-run fills it)
            LOG.warning("[osm +%s] tile %d/%d FAILED, skipping: %s", region, i, len(tiles), ex)
            failed.append([s, w, n, e])
            continue
        rows, _ = _rows(ways, have)
        new_rows.extend(rows)
        seen_ways += len(ways)
        LOG.info("[osm +%s] tile %d/%d: %d ways, +%d new (total +%d)",
                 region, i, len(tiles), len(ways), len(rows), len(new_rows))
        time.sleep(1.5)  # polite to the public Overpass endpoint
    merged = pl.concat([existing, pl.DataFrame(new_rows)], how="vertical") if new_rows else existing
    save_parquet(merged, dest)
    _write_coverage(region, seen_ways, merged.height, 0, failed_tiles=failed)
    msg = f"OK osm_roads +{region}: {merged.height} rows (+{len(new_rows)} new)"
    if failed:
        msg += f" | {len(failed)} tile(s) FAILED — re-run to fill (dedup-safe)"
    print(msg + " | © OpenStreetMap contributors")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--region", choices=sorted(REGIONS), help="tile this region's bbox and MERGE")
    args = ap.parse_args()
    setup_standalone_logging("planning_osm_roads")
    OUT.mkdir(parents=True, exist_ok=True)
    if args.region:
        add_region(args.region)
    else:
        base_pull()


if __name__ == "__main__":
    main()
