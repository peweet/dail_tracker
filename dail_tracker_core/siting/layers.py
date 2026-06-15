"""Load the gated designation GeoParquet layers and answer point queries.

Layers are stored as parquet of {key fields + wkb} in EPSG:4326 (Phase 0 ingest). We read
them with shapely only (no geopandas/GDAL) and build an STRtree per layer — the validated
§13.7 stack (shapely 2.x STRtree beat DuckDB-spatial and geopandas on this workload).

Containment (`covering`) is CRS-agnostic, so it is EXACT in 4326 — matching the §13 join.
Distance (`near`) needs metres; we have no pyproj here yet, so we approximate degrees from
metres at the point's latitude and FLAG it as approximate (a coarse "near" gate, not a survey
measurement). A future metric pass (pyproj/ITM) tightens it.
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

    @lru_cache(maxsize=32)
    def load(self, name: str) -> Layer | None:
        import polars as pl

        path = self.dir / f"{name}.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        geoms = shapely.from_wkb(df["wkb"].to_list())
        attr_cols = [c for c in df.columns if c != "wkb"]
        attrs = df.select(attr_cols).to_dicts() if attr_cols else [{} for _ in geoms]
        return Layer(name=name, attrs=attrs, geoms=np.asarray(geoms, dtype=object),
                     tree=STRtree(geoms))

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
        """(attributes, approx_distance_m) of the single nearest feature, or None."""
        layer = self.load(name)
        if layer is None or len(layer.geoms) == 0:
            return None
        p = Point(lon, lat)
        i = int(layer.tree.nearest(p))
        deg = shapely.distance(p, layer.geoms[i])
        metres = deg * _M_PER_DEG_LAT  # rough; lon-compression ignored for a scalar estimate
        return layer.attrs[i], metres
