"""EXPERIMENTAL — grid-precompute prototype for the siting-check (Product B).

Goal: test "join the polygons onto a grid" as the deployable path. Instead of shipping
291 MB of designation geometry + a live shapely engine to Streamlit Cloud, precompute
OFF-BOX a compact `cell -> fired-issue answer` raster, so the runtime is a pure-arithmetic
lookup (numpy only, already in Cloud core — NO shapely/rasterio).

This script MEASURES the two things that decide whether the idea is viable:
  1. SIZE   — real zstd-compressed parquet size of the grid at several resolutions,
              measured on a national build (latitude-banded to bound RAM).
  2. ACCURACY — grid-snapped fired-set vs the LIVE ENGINE's own trigger functions on N
              random points. This simultaneously validates (a) that the vectorised triggers
              below match dail_tracker_core.siting.engine, and (b) the cell-snapping drift.

It is a PROTOTYPE: it vectorises the spatial-join + near + zoning/vuln nodes (the ones that
dominate size and are resolution-sensitive). The DEM half of `landscape_siting` is deferred
(needs a separate COG resample) and is excluded from the parity comparison, noted in output.

Run:  python pipeline_sandbox/siting_grid_precompute_experimental.py
Nothing is promoted; output goes to _siting_grid_output/ (gitignored sandbox).
"""

from __future__ import annotations

import math
import sys
import time
from pathlib import Path

import numpy as np

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "utility"))

import shapely  # noqa: E402
from dail_tracker_core.siting import engine as ENG  # noqa: E402
from dail_tracker_core.siting.layers import LayerStore  # noqa: E402

OUT = REPO / "_siting_grid_output"
OUT.mkdir(exist_ok=True)
_M_PER_DEG_LAT = 111_320.0

# Nodes the prototype bakes, in a fixed bit order. (DEM-exposure part of landscape and the
# universal deep-link nodes that always fire are handled trivially.)
NODES = [
    "aa_screening", "european_site", "bats", "peat_bog", "monument", "floodplain",
    "septic_groundwater", "road_sightlines", "landscape_siting", "rural_need_zoning",
    "protected_structure",
]
BIT = {n: i for i, n in enumerate(NODES)}


def _deg_for(metres: float, lat: float) -> float:
    """Engine's metres->degrees near() conversion (layers.py), at a given latitude."""
    return metres / (_M_PER_DEG_LAT * max(math.cos(math.radians(lat)), 0.2))


def _first_per_cell(pairs: np.ndarray, n: int) -> np.ndarray:
    """From STRtree.query pairs [cell_idx, poly_idx], an int array cell->first poly (-1 none)."""
    out = np.full(n, -1, dtype=np.int64)
    if pairs.size:
        out[pairs[0]] = pairs[1]  # arbitrary-but-stable pick; tiling layers => unique anyway
    return out


def _any_per_cell(pairs: np.ndarray, n: int) -> np.ndarray:
    m = np.zeros(n, dtype=bool)
    if pairs.size:
        m[pairs[0]] = True
    return m


class GridBuilder:
    """Vectorised re-implementation of the engine triggers over a cell grid."""

    def __init__(self, store: LayerStore):
        self.store = store
        self.avail = store.available()
        # pre-load layers + attribute arrays we need
        self._lay = {}
        self._attr = {}
        for name in ("npws_sac", "npws_spa", "gsi_vulnerability", "gsi_karst", "smr_zone",
                     "zoning_gzt", "niah", "npws_nha", "osm_roads"):
            if name in self.avail:
                self._lay[name] = store.load(name)
        # landscape / rps / aca are council-scoped, name-matched
        for name in store.layers_matching("landscape") + store.layers_matching("rps") + store.layers_matching("aca"):
            self._lay[name] = store.load(name)
        # attribute columns
        z = self._lay.get("zoning_gzt")
        if z is not None:
            self._attr["zone_desc"] = np.array(
                [" ".join(str(a.get(k, "")) for k in ("ZONE_DESC", "ZONE_ORIG", "ZONE_GZT")).lower()
                 for a in z.attrs], dtype=object)
        v = self._lay.get("gsi_vulnerability")
        if v is not None:
            self._attr["vul_cat"] = np.array([(a.get("VUL_CAT") or "").upper() for a in v.attrs], dtype=object)

    def _cov(self, name, pts):
        lay = self._lay.get(name)
        if lay is None:
            return None
        return lay.tree.query(pts, predicate="covered_by")

    def _near(self, name, pts, metres, lat):
        lay = self._lay.get(name)
        if lay is None:
            return None
        return lay.tree.query(pts, predicate="dwithin", distance=_deg_for(metres, lat))

    def build_block(self, lons: np.ndarray, lats: np.ndarray) -> np.ndarray:
        """A block of rows: bulk-query the whole block at once (fast), return fired (nrows,nlon).

        Near-distance degree conversion uses the block's mid-latitude — over a block (~km) cos(lat)
        is effectively constant, so this matches the engine's per-point conversion to <1 m.
        """
        nlon, nrows = len(lons), len(lats)
        lat = float(np.mean(lats))  # mid-block latitude for the deg conversions
        LON, LAT = np.meshgrid(lons, lats)
        pts = shapely.points(LON.ravel(), LAT.ravel())
        n = nrows * nlon
        fired = np.zeros(n, dtype=np.uint16)

        def setbit(node, mask):
            fired[mask] |= np.uint16(1 << BIT[node])

        # universal gates
        setbit("aa_screening", np.ones(n, bool))
        setbit("floodplain", np.ones(n, bool))

        sac = self._cov("npws_sac", pts); spa = self._cov("npws_spa", pts)
        has_eu = "npws_sac" in self.avail or "npws_spa" in self.avail
        if has_eu:
            inside = _any_per_cell(sac, n) | _any_per_cell(spa, n)
            near2k = _any_per_cell(self._near("npws_sac", pts, ENG.NEAR_M["european_site"], lat), n) | \
                _any_per_cell(self._near("npws_spa", pts, ENG.NEAR_M["european_site"], lat), n)
            setbit("european_site", inside | near2k)
            near15 = _any_per_cell(self._near("npws_sac", pts, ENG.NEAR_M["bats"], lat), n) | \
                _any_per_cell(self._near("npws_spa", pts, ENG.NEAR_M["bats"], lat), n)
            setbit("bats", near15)

        # peat_bog: no epa_subsoils layer -> weak NHA-cover signal only
        if "npws_nha" in self.avail:
            setbit("peat_bog", _any_per_cell(self._cov("npws_nha", pts), n))

        if "smr_zone" in self.avail:
            setbit("monument", _any_per_cell(self._cov("smr_zone", pts), n))

        # septic: covered by gsi_vulnerability AND (VUL in X/E/H OR karst within 1km)
        if "gsi_vulnerability" in self.avail:
            vfirst = _first_per_cell(self._cov("gsi_vulnerability", pts), n)
            covered = vfirst >= 0
            vcat = np.where(covered, self._attr["vul_cat"][np.clip(vfirst, 0, None)], "")
            high = np.isin(vcat, ["X", "E", "H"])
            karst = _any_per_cell(self._near("gsi_karst", pts, 1000, lat), n) if "gsi_karst" in self.avail \
                else np.zeros(n, bool)
            setbit("septic_groundwater", covered & (high | karst))

        # road: any road within 150 m (engine uses nearest<=150 -> equivalent)
        if "osm_roads" in self.avail:
            setbit("road_sightlines", _any_per_cell(self._near("osm_roads", pts, 150, lat), n))

        # landscape: LCA-layer cover only (DEM-exposure deferred)
        lca = np.zeros(n, bool)
        for name in [k for k in self._lay if "landscape" in k]:
            lca |= _any_per_cell(self._lay[name].tree.query(pts, predicate="covered_by"), n)
        setbit("landscape_siting", lca)

        # rural_need: unzoned -> fired; else agri keyword in zone desc
        if "zoning_gzt" in self.avail:
            zfirst = _first_per_cell(self._cov("zoning_gzt", pts), n)
            unzoned = zfirst < 0
            desc = np.where(zfirst >= 0, self._attr["zone_desc"][np.clip(zfirst, 0, None)], "")
            agri = np.array([any(w in d for w in ("agricult", "rural", "amenity", "open space", "green"))
                             for d in desc])
            setbit("rural_need_zoning", unzoned | agri)

        # protected structure: niah within 250 m OR rps/aca cover/near 250 m
        ps = np.zeros(n, bool)
        if "niah" in self.avail:
            ps |= _any_per_cell(self._near("niah", pts, ENG.NEAR_M["protected_structure"], lat), n)
        for name in [k for k in self._lay if "rps" in k or "aca" in k]:
            t = self._lay[name].tree
            ps |= _any_per_cell(t.query(pts, predicate="covered_by"), n)
            ps |= _any_per_cell(t.query(pts, predicate="dwithin",
                                        distance=_deg_for(ENG.NEAR_M["protected_structure"], lat)), n)
        setbit("protected_structure", ps)

        return fired.reshape(nrows, nlon)


# ---- ground-truth: call the ENGINE's own triggers at a point (no DEM/network) ----
_COMPARE_NODES = [n for n in NODES if n not in ("landscape_siting",)]  # DEM-half deferred


def engine_fired_at(store, lon, lat) -> int:
    """Bitset from the LIVE engine trigger functions (engine.TRIGGERS) at a point."""
    bits = 0
    for node in _COMPARE_NODES:
        trig = ENG.TRIGGERS.get(node)
        if trig is None:
            continue
        fired, _detail, _status = trig(store, lon, lat, "one_off_house", None)
        if fired:
            bits |= 1 << BIT[node]
    return bits


def grid_lookup(grid, lon, lat) -> int:
    j = int((lon - grid["lon0"]) / grid["step_lon"])
    i = int((lat - grid["lat0"]) / grid["step_lat"])
    if not (0 <= i < grid["nlat"] and 0 <= j < grid["nlon"]):
        return 0
    return int(grid["fired"][i, j])


def build_grid(builder, lon0, lon1, lat0, lat1, cell_m, label=""):
    step_lat = cell_m / _M_PER_DEG_LAT
    midlat = (lat0 + lat1) / 2
    step_lon = cell_m / (_M_PER_DEG_LAT * math.cos(math.radians(midlat)))
    nlon = int((lon1 - lon0) / step_lon)
    nlat = int((lat1 - lat0) / step_lat)
    lons = lon0 + (np.arange(nlon) + 0.5) * step_lon
    all_lats = lat0 + (np.arange(nlat) + 0.5) * step_lat
    fired = np.zeros((nlat, nlon), dtype=np.uint16)
    BLOCK = max(1, min(96, 1_500_000 // max(1, nlon)))  # ~1.5M points/block, RAM-bounded
    t0 = time.time()
    for r0 in range(0, nlat, BLOCK):
        rows = all_lats[r0:r0 + BLOCK]
        fired[r0:r0 + len(rows), :] = builder.build_block(lons, rows)
        if label:
            print(f"   [{label} {cell_m}m] rows {r0}/{nlat}  {time.time()-t0:.0f}s", flush=True)
    return {"fired": fired, "lon0": lon0, "lat0": lat0, "step_lon": step_lon,
            "step_lat": step_lat, "nlon": nlon, "nlat": nlat, "cells": nlon * nlat,
            "build_s": time.time() - t0}


def measure_size(grid) -> dict:
    import polars as pl
    flat = grid["fired"].ravel()
    df = pl.DataFrame({"fired": flat})
    dense = OUT / "grid_dense.parquet"
    df.write_parquet(dense, compression="zstd", compression_level=9)
    dense_kb = dense.stat().st_size / 1024
    # sparse: only cells that fire something beyond the two universal gates
    universal = (1 << BIT["aa_screening"]) | (1 << BIT["floodplain"])
    nontrivial = flat & ~np.uint16(universal)
    idx = np.nonzero(nontrivial)[0]
    sp = pl.DataFrame({"cell": idx.astype(np.uint32), "fired": flat[idx]})
    sparse = OUT / "grid_sparse.parquet"
    sp.write_parquet(sparse, compression="zstd", compression_level=9)
    sparse_kb = sparse.stat().st_size / 1024
    return {"dense_kb": dense_kb, "sparse_kb": sparse_kb, "nontrivial": len(idx),
            "bytes_per_cell_dense": dense.stat().st_size / grid["cells"]}


def main():
    store = LayerStore()
    builder = GridBuilder(store)
    print("layers loaded:", sorted(builder._lay.keys()))

    # ---- ACCURACY: rich Galway AOI, several resolutions ----
    AOI = dict(lon0=-9.20, lon1=-8.90, lat0=53.20, lat1=53.45)  # Galway City + Corrib + Menlo
    rng = np.random.default_rng(42)
    test_pts = [(AOI["lon0"] + rng.random() * (AOI["lon1"] - AOI["lon0"]),
                 AOI["lat0"] + rng.random() * (AOI["lat1"] - AOI["lat0"])) for _ in range(400)]
    truth = [(lon, lat, engine_fired_at(store, lon, lat)) for lon, lat in test_pts]

    print("\n=== ACCURACY (grid-snapped vs live engine triggers, 400 random Galway pts) ===")
    print(f"{'cell':>6} | {'exact-match':>11} | {'any-mismatch node breakdown'}")
    for cell_m in (100, 50, 25):
        g = build_grid(builder, AOI["lon0"], AOI["lon1"], AOI["lat0"], AOI["lat1"], cell_m, "AOI")
        exact = 0
        per_node_miss = {n: 0 for n in _COMPARE_NODES}
        for lon, lat, tb in truth:
            gb = grid_lookup(g, lon, lat)
            # compare only the bits we baked + compare-nodes
            mask = 0
            for n in _COMPARE_NODES:
                mask |= 1 << BIT[n]
            if (gb & mask) == (tb & mask):
                exact += 1
            else:
                for n in _COMPARE_NODES:
                    b = 1 << BIT[n]
                    if (gb & b) != (tb & b):
                        per_node_miss[n] += 1
        miss = {k: v for k, v in per_node_miss.items() if v}
        print(f"{cell_m:>5}m | {exact}/{len(truth)} = {exact/len(truth)*100:5.1f}% | {miss}")

    # ---- SIZE: national build at coarse res (RAM-bounded, latitude-banded already) ----
    NAT = dict(lon0=-10.60, lon1=-5.90, lat0=51.40, lat1=55.45)
    print("\n=== SIZE (national build) ===")
    for cell_m in (200, 100):
        g = build_grid(builder, NAT["lon0"], NAT["lon1"], NAT["lat0"], NAT["lat1"], cell_m, "NAT")
        sz = measure_size(g)
        print(f"  national @{cell_m}m: {g['cells']:,} cells, build {g['build_s']:.0f}s | "
              f"dense {sz['dense_kb']:.0f} KB ({sz['bytes_per_cell_dense']:.3f} B/cell) | "
              f"sparse {sz['sparse_kb']:.0f} KB ({sz['nontrivial']:,} non-trivial cells)")


if __name__ == "__main__":
    main()
