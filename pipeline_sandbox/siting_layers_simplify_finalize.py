"""EXPERIMENTAL — produce the per-layer-TUNED simplified layer set (promotable artifact).

Follows siting_layers_simplify_experimental.py. That run showed a uniform 10 m over-simplifies the
dense layers (smr_zone -962, zoning_gzt -1056 containments). Here we AUTO-PICK, per polygon layer,
the LARGEST tolerance whose containment drift stays within a tight threshold, then write that
layer at that tolerance. Points are copied unchanged (simplify is a no-op); lines (roads) get a
light 5 m simplify (negligible vs the 150 m "near" buffer they feed).

Threshold: |drift| <= max(25, 0.1% of full covered-count). Negative drift (missing a real
designation) is the unsafe direction, so the floor is deliberately small.

Output -> c:/tmp/siting_simplify_final (sandbox; NOT the repo). Promotion (git-track + point the
engine's LAYERS_DIR at these) is a SEPARATE vetted step, held while the engine files are in flux.
Run:  python pipeline_sandbox/siting_layers_simplify_finalize.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import polars as pl
import shapely

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "utility"))

LAYERS_DIR = REPO / "data" / "silver" / "parquet" / "planning_layers"
SILVER = REPO / "data" / "silver" / "parquet" / "planning_applications_silver.parquet"
OUT = Path("c:/tmp/siting_simplify_final")
OUT.mkdir(parents=True, exist_ok=True)

_M_PER_DEG = 111_320.0
CANDIDATE_TOLS_M = [25, 15, 10, 7, 5, 3, 2]  # largest-first; pick the largest that passes


def _load_app_points() -> np.ndarray:
    df = pl.read_parquet(SILVER, columns=["lon", "lat"]).filter(
        pl.col("lon").is_not_null() & pl.col("lat").is_not_null()
    )
    return shapely.points(df["lon"].to_numpy(), df["lat"].to_numpy())


def _covered_count(geoms, pts) -> int:
    tree = shapely.STRtree(geoms)
    pairs = tree.query(pts, predicate="covered_by")
    return int(np.unique(pairs[0]).size) if pairs.size else 0


def _write(geoms, attrs_df: pl.DataFrame, path: Path) -> float:
    attrs_df.with_columns(pl.Series("wkb", shapely.to_wkb(geoms))).write_parquet(
        path, compression="zstd", compression_level=9
    )
    return path.stat().st_size / 1024


def main():
    pts = _load_app_points()
    files = sorted(
        p for p in LAYERS_DIR.glob("*.parquet")
        if not p.stem.endswith("_coverage") and "wkb" in pl.read_parquet_schema(p)
    )
    print(f"{len(pts):,} ground-truth points\n")
    print(f"{'layer':<26} {'tol':>5} {'full_hits':>9} {'drift':>7} {'drift%':>7} "
          f"{'full KB':>8} {'simp KB':>8}")
    tot_full = tot_simp = 0.0
    chosen = {}
    for f in files:
        df = pl.read_parquet(f)
        geoms = shapely.make_valid(shapely.from_wkb(df["wkb"].to_list()))
        attrs = df.drop("wkb")
        is_poly = bool(np.any(shapely.get_type_id(geoms) >= 3))
        is_line = bool(np.any(shapely.get_type_id(geoms) == 1))
        full_kb = _write(geoms, attrs, OUT / f"{f.stem}.parquet_FULLTMP")
        (OUT / f"{f.stem}.parquet_FULLTMP").unlink()
        tot_full += full_kb

        if not is_poly:
            # points: copy unchanged; lines: light 5 m simplify (negligible vs near-buffers)
            tol = 5 if is_line else 0
            out_geoms = shapely.make_valid(shapely.simplify(geoms, tol / _M_PER_DEG, preserve_topology=True)) \
                if is_line else geoms
            simp_kb = _write(out_geoms, attrs, OUT / f"{f.stem}.parquet")
            tot_simp += simp_kb
            chosen[f.stem] = tol
            print(f"{f.stem:<26} {('line5' if is_line else 'pts'):>5} {'na':>9} {'na':>7} {'na':>7} "
                  f"{full_kb:>8.0f} {simp_kb:>8.0f}")
            continue

        full_hits = _covered_count(geoms, pts)
        thresh = max(25, round(0.001 * full_hits))
        picked = None
        for tol in CANDIDATE_TOLS_M:
            simp = shapely.make_valid(shapely.simplify(geoms, tol / _M_PER_DEG, preserve_topology=True))
            drift = _covered_count(simp, pts) - full_hits
            if abs(drift) <= thresh:
                picked = (tol, simp, drift)
                break
        if picked is None:  # even the smallest tol over-drifts -> use it but flag
            tol = CANDIDATE_TOLS_M[-1]
            simp = shapely.make_valid(shapely.simplify(geoms, tol / _M_PER_DEG, preserve_topology=True))
            picked = (tol, simp, _covered_count(simp, pts) - full_hits)
        tol, simp, drift = picked
        simp_kb = _write(simp, attrs, OUT / f"{f.stem}.parquet")
        tot_simp += simp_kb
        chosen[f.stem] = tol
        flag = "" if abs(drift) <= thresh else "  <-- OVER THRESHOLD"
        print(f"{f.stem:<26} {tol:>4}m {full_hits:>9,} {drift:>+7d} "
              f"{drift/max(full_hits,1)*100:>6.2f}% {full_kb:>8.0f} {simp_kb:>8.0f}{flag}")

    print(f"\nTOTAL  full {tot_full/1024:.1f} MB  ->  tuned {tot_simp/1024:.1f} MB  "
          f"({tot_full/max(tot_simp,1):.1f}x)")
    print("chosen tolerances:", {k: f"{v}m" for k, v in chosen.items()})
    print(f"promotable set -> {OUT}")


if __name__ == "__main__":
    main()
