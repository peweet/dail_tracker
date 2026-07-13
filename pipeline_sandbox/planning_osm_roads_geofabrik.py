"""Geofabrik PBF -> NATIONAL osm_roads planning layer (replaces the fragile Overpass tiling).

The Overpass path (planning_osm_roads.py) 504s on county-scale pulls, so osm_roads stayed
Galway+Dublin and road_sightlines was honest-missing everywhere else. This reads the one-time
Geofabrik island extract (ireland-and-northern-ireland-latest.osm.pbf, ODbL) and streams every
VEHICULAR way into the same {highway, maxspeed, name, ref, wkb} schema, so LayerStore/engine
need no change — the coverage json just goes national (bbox_subset=null).

NI ways are kept deliberately: the road network does not stop at the border, and a Donegal /
Monaghan point's nearest access road can be an NI-mapped way. IRELAND_BBOX covers the island.

    .venv/Scripts/python pipeline_sandbox/planning_osm_roads_geofabrik.py \
        [--pbf c:/tmp/geofabrik/ireland-and-northern-ireland-latest.osm.pbf]

Needs `osmium` (pyosmium) — installed ad hoc via `uv pip install osmium` (a one-time ETL tool,
not a siting runtime dep; re-install after any bare `uv sync`).
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from pathlib import Path

import osmium
import polars as pl
import shapely
from shapely.geometry import LineString

from extractors.planning_layers_ingest import IRELAND_BBOX, OUT
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_osm_roads_geofabrik")

PBF_DEFAULT = Path("c:/tmp/geofabrik/ireland-and-northern-ireland-latest.osm.pbf")
KEEP = ("highway", "maxspeed", "name", "ref")
# vehicular classes only (matches the Dublin append filter): a footway/cycleway is not an
# access road, and nearest() picking one would mis-drive the sightline speed class.
VEHICULAR = {
    "motorway", "motorway_link", "trunk", "trunk_link", "primary", "primary_link",
    "secondary", "secondary_link", "tertiary", "tertiary_link",
    "unclassified", "residential", "living_street", "service", "track",
}
# far more vehicular ways exist on the island (Dublin bbox alone had 47,669); refuse to
# overwrite the good regional layer if extraction somehow yields a fraction of that.
ROW_FLOOR = 200_000
SIMPLIFY_DEG = 0.00005  # ≈5 m at Irish latitudes


class RoadHandler(osmium.SimpleHandler):
    def __init__(self) -> None:
        super().__init__()
        self.rows: list[dict] = []
        self.dropped_invalid = 0
        self.dropped_bounds = 0
        self._t0 = time.time()

    def way(self, w) -> None:
        hw = w.tags.get("highway")
        if hw not in VEHICULAR:
            return
        try:
            coords = [(n.lon, n.lat) for n in w.nodes]
        except osmium.InvalidLocationError:
            self.dropped_invalid += 1
            return
        if len(coords) < 2:
            self.dropped_invalid += 1
            return
        geom = LineString(coords)
        minx, miny, maxx, maxy = geom.bounds
        if not (IRELAND_BBOX[0] <= minx and maxx <= IRELAND_BBOX[2]
                and IRELAND_BBOX[1] <= miny and maxy <= IRELAND_BBOX[3]):
            self.dropped_bounds += 1
            return
        # 5 m Douglas-Peucker: engine thresholds are 100-150 m so a ≤5 m centreline shift is
        # immaterial, and dropping redundant OSM nodes cuts the layer's RAM/tree-build cost
        # (1.28M ways is the store's biggest layer by far). Lines only — no topology concerns.
        geom = geom.simplify(SIMPLIFY_DEG, preserve_topology=False)
        self.rows.append({
            "highway": hw,
            "maxspeed": w.tags.get("maxspeed"),  # keep-as-printed (NI "x mph" stays verbatim)
            "name": w.tags.get("name"),
            "ref": w.tags.get("ref"),
            "wkb": shapely.to_wkb(geom),
        })
        if len(self.rows) % 100_000 == 0:
            LOG.info("kept %dk vehicular ways (%.0fs)", len(self.rows) // 1000, time.time() - self._t0)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pbf", type=Path, default=PBF_DEFAULT)
    args = ap.parse_args()
    setup_standalone_logging("planning_osm_roads_geofabrik")

    h = RoadHandler()
    LOG.info("streaming %s (%.0f MB)...", args.pbf, args.pbf.stat().st_size / 1e6)
    h.apply_file(str(args.pbf), locations=True)
    LOG.info("done: kept %d | invalid %d | out-of-bounds %d",
             len(h.rows), h.dropped_invalid, h.dropped_bounds)

    df = pl.DataFrame(h.rows, schema={"highway": pl.Utf8, "maxspeed": pl.Utf8,
                                      "name": pl.Utf8, "ref": pl.Utf8, "wkb": pl.Binary})
    dest = save_parquet(df, OUT / "osm_roads.parquet", min_rows=ROW_FLOOR, compression_level=9)
    (OUT / "osm_roads_coverage.json").write_text(json.dumps({
        "layer": "osm_roads",
        "source": "OpenStreetMap via Geofabrik ireland-and-northern-ireland-latest.osm.pbf",
        "licence": "ODbL — © OpenStreetMap contributors",
        "kind": "line",
        "pulled": len(h.rows) + h.dropped_invalid + h.dropped_bounds,
        "kept": df.height,
        "quarantined": h.dropped_invalid + h.dropped_bounds,
        "gate_reasons": {"ok": df.height, "invalid_location": h.dropped_invalid,
                         "bounds_escape": h.dropped_bounds},
        "keep_fields": list(KEEP),
        "filter": "vehicular roads only",
        "crs": "EPSG:4326",
        "bbox_subset": None,  # NATIONAL (all-island) — in_extent() reads null as full coverage
    }, indent=2), encoding="utf-8")
    print(f"OK osm_roads NATIONAL: {dest} | kept {df.height} | invalid {h.dropped_invalid} | oob {h.dropped_bounds}")


if __name__ == "__main__":
    main()
