"""Load the gated designation GeoParquet layers and answer point queries.

Layers are stored as parquet of {key fields + wkb} in EPSG:4326 (Phase 0 ingest). We read
them with shapely only (no geopandas/GDAL) and build an STRtree per layer — the validated
§13.7 stack (shapely 2.x STRtree beat DuckDB-spatial and geopandas on this workload).

Containment (`covering`) is CRS-agnostic, so it is EXACT in 4326 — matching the §13 join.
Distance needs metres. `near()` is a deliberately coarse, over-inclusive degree gate (flag-don't-
miss). The `nearest*()` distances feed hard thresholds (e.g. road within 150 m), so they use
`_metric_dist_m` — an equirectangular cos(lat) correction (no pyproj) accurate to <0.1% at sub-km
ranges. Without it, `deg * 111_320` over-states east–west distance by ~1/cos(lat) (≈1.66× at 53°N),
which made true-east features fail the threshold. A future pyproj/ITM pass would make it geodesic.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import numpy as np
import shapely
from shapely import STRtree
from shapely.geometry import Point

from .catalogue import REPO_ROOT

LAYERS_DIR = REPO_ROOT / "data" / "silver" / "parquet" / "planning_layers"
_M_PER_DEG_LAT = 111_320.0


def _metric_dist_m(p: Point, geom, lat: float) -> float:
    """True ground distance (m) from `p` to `geom`, equirectangular — lon scaled by cos(lat).

    `shapely.distance` returns a DEGREE scalar; at Irish latitudes 1° lon ≈ 67 km vs 1° lat ≈
    111 km, so `deg * 111_320` over-states any east–west separation by ~1/cos(lat) (≈1.66× at
    53°N). We take the closest-point segment and scale its lon component by cos(lat). Accurate to
    <0.1% at these (sub-km) ranges — good enough without pulling in pyproj/ITM.
    """
    line = shapely.shortest_line(p, geom)
    (x0, y0), (x1, y1) = line.coords[0], line.coords[1]
    cos = max(math.cos(math.radians(lat)), 0.2)
    return math.hypot((x1 - x0) * _M_PER_DEG_LAT * cos, (y1 - y0) * _M_PER_DEG_LAT)


@dataclass
class Layer:
    name: str
    attrs: list[dict]
    geoms: np.ndarray  # shapely geometries
    tree: STRtree


class LayerStore:
    def __init__(self, layers_dir: Path | str = LAYERS_DIR):
        self.dir = Path(layers_dir)

    def available(self) -> set[str]:
        return {p.stem for p in self.dir.glob("*.parquet") if not p.stem.endswith("_coverage")}

    def layers_matching(self, substr: str) -> list[str]:
        """Available layer names containing `substr` (e.g. 'rps', 'aca', 'landscape').

        Per-LA layers are stored council-scoped (galway_county_rps, galway_city_aca, …);
        geography self-selects, so checking all matching layers is correct Galway-first and
        generalises as more councils are added.
        """
        return sorted(n for n in self.available() if substr in n)

    @lru_cache(maxsize=64)  # noqa: B019
    def _ingest_extents(self, name: str):
        """The bboxes a layer covers — base `bbox_subset` PLUS every region merged via
        add_region (`regions_added` -> `region_<r>.bbox`). Returns None if national (no bbox).
        """
        import json

        cf = self.dir / f"{name}_coverage.json"
        if not cf.exists():
            return None
        c = json.loads(cf.read_text(encoding="utf-8"))
        boxes: list[tuple] = []
        if c.get("bbox_subset"):
            boxes.append(tuple(c["bbox_subset"]))
        for r in c.get("regions_added", []) or []:
            rb = (c.get(f"region_{r}") or {}).get("bbox")
            if rb:
                boxes.append(tuple(rb))
        return boxes or None  # None = national (no bbox restriction)

    def in_extent(self, name: str, lon: float, lat: float, margin: float = 0.02) -> bool:
        """Is the point within this layer's INGESTED extent (any base/region bbox)?

        National layers (no bbox in coverage) cover all Ireland. A bbox/region-limited layer
        covers only its box(es) — a point outside ALL of them has NO data here, so callers MUST
        treat that as 'layer_missing', never 'no issue' (the core honesty rule). As a layer is
        re-pulled nationally or add_region-extended, coverage opens up here for free.
        """
        if name not in self.available():
            return False
        boxes = self._ingest_extents(name)
        if boxes is None:
            return True
        return any(
            (b[0] - margin) <= lon <= (b[2] + margin) and (b[1] - margin) <= lat <= (b[3] + margin) for b in boxes
        )

    @lru_cache(maxsize=32)  # noqa: B019
    def load(self, name: str) -> Layer | None:
        import polars as pl

        path = self.dir / f"{name}.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        geoms = shapely.from_wkb(df["wkb"].to_list())
        attr_cols = [c for c in df.columns if c != "wkb"]
        attrs = df.select(attr_cols).to_dicts() if attr_cols else [{} for _ in geoms]
        return Layer(name=name, attrs=attrs, geoms=np.asarray(geoms, dtype=object), tree=STRtree(geoms))

    def covering(self, name: str, lon: float, lat: float) -> list[dict]:
        """Attributes of every polygon that COVERS the point (exact containment).

        NB shapely STRtree evaluates predicate(input, tree_geom): for a point input we
        want polygon.covers(point) == point.covered_by(polygon), so the predicate is
        "covered_by" (NOT "covers", which would test point.covers(polygon) == always False).
        """
        layer = self.load(name)
        if layer is None:
            return []
        p = Point(lon, lat)
        idx = layer.tree.query(p, predicate="covered_by")
        return [layer.attrs[i] for i in idx]

    def near(self, name: str, lon: float, lat: float, metres: float) -> list[dict]:
        """Attributes of features within ~`metres` (APPROXIMATE — degree conversion)."""
        layer = self.load(name)
        if layer is None:
            return []
        deg = metres / (_M_PER_DEG_LAT * max(math.cos(math.radians(lat)), 0.2))
        p = Point(lon, lat)
        idx = layer.tree.query(p, predicate="dwithin", distance=deg)
        return [layer.attrs[i] for i in idx]

    def nearest(self, name: str, lon: float, lat: float) -> tuple[dict, float] | None:
        """(attributes, true_distance_m) of the metrically-nearest feature, or None.

        `tree.nearest` ranks in DEGREE space, which under-weights east–west separation at Irish
        latitudes (1° lon ≈ 0.6 × 1° lat in metres) and can pick a north–south feature over a
        metrically-closer east–west one. So we take the candidates within a generous metric radius
        and pick the true-nearest by `_metric_dist_m`, widening the radius if empty (final fallback:
        the global degree-nearest, for the rare point >50 km from any feature).
        """
        layer = self.load(name)
        if layer is None or len(layer.geoms) == 0:
            return None
        p = Point(lon, lat)
        cos = max(math.cos(math.radians(lat)), 0.2)
        for radius_m in (500.0, 5_000.0, 50_000.0):
            # cos-corrected deg = the lon-axis radius, so the candidate set is a superset of the
            # true metric circle (never drops an in-range east–west feature).
            idx = layer.tree.query(p, predicate="dwithin", distance=radius_m / (_M_PER_DEG_LAT * cos))
            if len(idx):
                dists = {int(j): _metric_dist_m(p, layer.geoms[j], lat) for j in idx}
                i = min(dists, key=dists.get)
                return layer.attrs[i], dists[i]
        i = int(layer.tree.nearest(p))
        return layer.attrs[i], _metric_dist_m(p, layer.geoms[i], lat)

    def nearest_junction(
        self, name: str, lon: float, lat: float, search_m: float = 400.0, snap_m: float = 8.0
    ) -> tuple[float, int] | None:
        """(approx_distance_m, approx_arm_count) of the nearest road JUNCTION, or None.

        A junction is where two distinct centrelines cross. We find the crossing pairs with a
        spatial-index SELF-JOIN (only pairs whose bounding boxes overlap are exact-tested), so this
        stays ~O(n log n) rather than O(n²): in a dense town `search_m` can hold 100s of OSM
        segments, and the old all-pairs `.intersection()` cost seconds. The crossing set is
        identical to brute force (same `intersects` predicate) — this is a pure speedup.
        `arms` (centrelines within `snap_m` of the node) is a ROUGH T-vs-crossroads hint only —
        OSM ways aren't split at every junction, so don't treat it as authoritative.
        """
        layer = self.load(name)
        if layer is None:
            return None
        p = Point(lon, lat)
        cos = max(math.cos(math.radians(lat)), 0.2)
        deg = search_m / (_M_PER_DEG_LAT * cos)  # generous lon-axis superset for the pre-filter
        idx = layer.tree.query(p, predicate="dwithin", distance=deg)
        if len(idx) < 2:
            return None
        garr = layer.geoms[idx]  # candidate segments (numpy object array)
        sub = STRtree(garr)
        # bulk self-join: every (a, b) whose segments ACTUALLY cross, bbox-pruned by the tree.
        qa, qb = sub.query(garr, predicate="intersects")
        snap_deg = snap_m / (_M_PER_DEG_LAT * cos)
        best: tuple[float, int] | None = None
        for a, b in zip(qa.tolist(), qb.tolist(), strict=False):
            if a >= b:  # each unordered pair once; drop self-pairs (a == b)
                continue
            inter = garr[a].intersection(garr[b])
            if inter.is_empty:
                continue
            pts = [inter] if inter.geom_type == "Point" else list(getattr(inter, "geoms", []))
            for pt in pts:
                if pt.geom_type != "Point":
                    continue
                d = _metric_dist_m(p, pt, lat)
                if best is None or d < best[0]:
                    arms = len(sub.query(pt, predicate="dwithin", distance=snap_deg))  # O(log n)
                    best = (d, arms)
        return best
