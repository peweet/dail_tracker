"""EXPERIMENTAL — simplify the siting designation layers + VALIDATE the join is preserved.

The §23.9 linchpin step (PLANNING_PERMISSION_SCOPING.md): simplify each GeoParquet layer so it
is git-shippable (tens of MB) AND faster to query live (the ~1.7 s/eval bench is dominated by the
488 k-vertex Corrib SAC polygon). The rule: ~10 m is the sweet spot (~27x smaller, +-1 join drift);
50 m BREAKS containment. So we MEASURE per layer, never assume.

Validation (ground truth = the same containment join the engine uses): for each POLYGON layer,
count how many of the 495 k national planning-application points are `covered_by` it, at full
precision vs each simplified tolerance. Drift = |full_count - simp_count|. Same discipline that
caught the -9e12 out-of-bounds coords.

Sandbox-safe: simplified parquet is written to c:/tmp (NOT the repo) so nothing is accidentally
committed; promotion (git-tracking the simplified layers + pointing the engine at them) is a
separate vetted step. Run:  python pipeline_sandbox/siting_layers_simplify_experimental.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import polars as pl
import shapely

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "utility"))

LAYERS_DIR = REPO / "data" / "silver" / "parquet" / "planning_layers"
SILVER = REPO / "data" / "silver" / "parquet" / "planning_applications_silver.parquet"
OUT = Path("c:/tmp/siting_simplify")
OUT.mkdir(parents=True, exist_ok=True)

TOLS_M = [5, 10, 25]  # bracket the §23.9 ~10 m sweet spot
_M_PER_DEG = 111_320.0
POLY_TYPES = {"Polygon", "MultiPolygon"}


def _load_app_points() -> np.ndarray:
    df = pl.read_parquet(SILVER, columns=["lon", "lat"]).filter(
        pl.col("lon").is_not_null() & pl.col("lat").is_not_null()
    )
    return shapely.points(df["lon"].to_numpy(), df["lat"].to_numpy())


def _covered_count(geoms, pts) -> int:
    tree = shapely.STRtree(geoms)
    pairs = tree.query(pts, predicate="covered_by")
    return int(np.unique(pairs[0]).size) if pairs.size else 0


def _write_size_kb(geoms, attrs_df: pl.DataFrame, path: Path) -> float:
    out = attrs_df.with_columns(pl.Series("wkb", shapely.to_wkb(geoms)))
    out.write_parquet(path, compression="zstd", compression_level=9)
    return path.stat().st_size / 1024


def main():
    pts = _load_app_points()
    print(f"loaded {len(pts):,} national application points for the containment ground truth\n")

    files = sorted(
        p for p in LAYERS_DIR.glob("*.parquet")
        if not p.stem.endswith("_coverage") and "wkb" in pl.read_parquet_schema(p)  # skip dem_cache etc.
    )
    grand_full = grand_simp = 0.0
    print(f"{'layer':<26} {'geom':<8} {'verts':>9} {'full KB':>8} | "
          + " ".join(f"{t}m:KB/drift" for t in TOLS_M))
    for f in files:
        df = pl.read_parquet(f)
        geoms = shapely.make_valid(shapely.from_wkb(df["wkb"].to_list()))
        attrs = df.drop("wkb")
        gtypes = set(shapely.get_type_id(geoms))
        is_poly = any(shapely.get_type_id(geoms) >= 3)  # 3=Polygon,6=MultiPolygon
        vfull = int(shapely.get_num_coordinates(geoms).sum())
        full_kb = _write_size_kb(geoms, attrs, OUT / f"{f.stem}__full.parquet")
        grand_full += full_kb
        full_hits = _covered_count(geoms, pts) if is_poly else -1

        cells = []
        best_kb_for_grand = full_kb
        for tol_m in TOLS_M:
            simp = shapely.make_valid(shapely.simplify(geoms, tol_m / _M_PER_DEG, preserve_topology=True))
            simp_kb = _write_size_kb(simp, attrs, OUT / f"{f.stem}__{tol_m}m.parquet")
            if is_poly:
                hits = _covered_count(simp, pts)
                drift = hits - full_hits
                cells.append(f"{simp_kb:>6.0f}/{drift:+d}")
            else:
                cells.append(f"{simp_kb:>6.0f}/  na")
            if tol_m == 10:
                best_kb_for_grand = simp_kb
        grand_simp += best_kb_for_grand
        geom_lbl = "poly" if is_poly else ("/".join(sorted(str(t) for t in gtypes))[:7])
        print(f"{f.stem:<26} {geom_lbl:<8} {vfull:>9,} {full_kb:>8.0f} | " + "  ".join(cells))

    print(f"\nTOTAL full: {grand_full/1024:.1f} MB  ->  @10m: {grand_simp/1024:.1f} MB  "
          f"({grand_full/max(grand_simp,1):.1f}x smaller)")
    print("drift = simplified covered-count minus full-precision covered-count (target: ~0; <=+-a few ok)")
    print(f"simplified parquet written to {OUT} (sandbox; NOT in repo)")


if __name__ == "__main__":
    main()
