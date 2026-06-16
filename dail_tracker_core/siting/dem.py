"""Terrain (elevation / slope / prominence) from Copernicus GLO-30, by HTTP range-read.

Copernicus DEM GLO-30 is a free, openly-licensed (commercial + re-host OK with attribution)
Cloud-Optimized GeoTIFF on AWS Open Data. We range-read the few cells under the user's point
via rasterio's /vsicurl — no full raster download, no system GDAL (rasterio bundles its own).
Source: s3://copernicus-dem-30m (EPSG:4326). 1°×1° tiles named by SW integer corner.

This feeds the landscape_siting node: is the site elevated/exposed/prominent (a skyline
risk)? We report the measured factors and let the rule speak — we never prescribe a design.
NOTE: GLO-30 is a DSM (surface, incl. canopy/buildings), ±~30 m horizontal — a coarse siting
signal, not a survey. Attribution: "Derived from Copernicus DEM GLO-30 © DLR/Airbus".
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass
from functools import lru_cache

os.environ.setdefault("GDAL_DISABLE_READDIR_ON_OPEN", "EMPTY_DIR")
os.environ.setdefault("CPL_VSIL_CURL_ALLOWED_EXTENSIONS", ".tif")

_BUCKET = "https://copernicus-dem-30m.s3.eu-central-1.amazonaws.com"
_M_PER_DEG_LAT = 111_320.0
# a site sitting this far above its ~2 km surroundings reads as locally prominent/exposed
EXPOSED_REL_HEIGHT_M = 10.0


def tile_url(lon: float, lat: float) -> str:
    ns = "N" if lat >= 0 else "S"
    ew = "E" if lon >= 0 else "W"
    la = abs(int(math.floor(lat)))
    lo = abs(int(math.floor(lon)))
    name = f"Copernicus_DSM_COG_10_{ns}{la:02d}_00_{ew}{lo:03d}_00_DEM"
    return f"{_BUCKET}/{name}/{name}.tif"


@dataclass(frozen=True)
class Terrain:
    elevation_m: float | None
    slope_deg: float | None
    relative_height_m: float | None  # point elevation minus the ~2 km window mean
    exposed: bool
    ok: bool                          # False if the read failed (offline / nodata)
    note: str = ""


# Persistent DEM cache: makes terrain() fully deterministic and OFFLINE-capable. The COG is
# fixed, so a cached value is the canonical value; we only ever cache successful reads (never
# a transient offline/nodata failure), keyed on the point rounded to a ~0.1 m grid + radius.
from .catalogue import REPO_ROOT  # noqa: E402

DEM_CACHE = REPO_ROOT / "data" / "silver" / "parquet" / "planning_layers" / "dem_cache.parquet"
_CACHE_PRECISION = 6


def _cache_key(lon: float, lat: float, radius_m: float) -> tuple[float, float, float]:
    return (round(lon, _CACHE_PRECISION), round(lat, _CACHE_PRECISION), float(radius_m))


@lru_cache(maxsize=1)
def _load_cache() -> dict[tuple[float, float, float], Terrain]:
    if not DEM_CACHE.exists():
        return {}
    import polars as pl

    out: dict[tuple[float, float, float], Terrain] = {}
    for r in pl.read_parquet(DEM_CACHE).iter_rows(named=True):
        out[(r["lon_key"], r["lat_key"], r["radius_m"])] = Terrain(
            r["elevation_m"], r["slope_deg"], r["relative_height_m"],
            r["exposed"], r["ok"], r["note"] or "")
    return out


def _persist(key: tuple[float, float, float], t: Terrain) -> None:
    import polars as pl

    cache = _load_cache()
    cache[key] = t  # mutate the lru-cached dict so it stays warm in-process
    rows = [{"lon_key": k[0], "lat_key": k[1], "radius_m": k[2], "elevation_m": v.elevation_m,
             "slope_deg": v.slope_deg, "relative_height_m": v.relative_height_m,
             "exposed": v.exposed, "ok": v.ok, "note": v.note} for k, v in cache.items()]
    DEM_CACHE.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(DEM_CACHE)


@lru_cache(maxsize=2048)
def terrain(lon: float, lat: float, radius_m: float = 2000.0) -> Terrain:
    """Elevation/slope/prominence at (lon, lat) — disk-cached for offline determinism.

    Disk cache first (the COG is fixed, so a cached value is canonical); on a miss, compute
    live and persist ONLY a successful read (a transient offline failure is not cached, so it
    retries next time online). In-process lru_cache on top for speed.
    """
    key = _cache_key(lon, lat, radius_m)
    cached = _load_cache().get(key)
    if cached is not None:
        return cached
    t = _compute_terrain(lon, lat, radius_m)
    if t.ok:
        _persist(key, t)
    return t


def _compute_terrain(lon: float, lat: float, radius_m: float = 2000.0) -> Terrain:
    """Elevation, slope and local prominence at (lon, lat) — the live range-read."""
    try:
        import numpy as np
        import rasterio
        from rasterio.windows import Window

        with rasterio.open(tile_url(lon, lat)) as ds:
            row, col = ds.index(lon, lat)
            # cell size in metres (degrees -> m, lon compressed by cos(lat))
            dy = abs(ds.transform.e) * _M_PER_DEG_LAT
            dx = abs(ds.transform.a) * _M_PER_DEG_LAT * max(math.cos(math.radians(lat)), 0.2)
            rad = int(max(1, round(radius_m / min(dx, dy))))
            r0, c0 = row - rad, col - rad
            win = Window(c0, r0, 2 * rad + 1, 2 * rad + 1)
            arr = ds.read(1, window=win, boundless=True,
                          fill_value=ds.nodata if ds.nodata is not None else 0).astype("float64")
            if ds.nodata is not None:
                arr = np.where(arr == ds.nodata, np.nan, arr)

        cr = arr.shape[0] // 2
        elev = arr[cr, cr]
        if not np.isfinite(elev):
            return Terrain(None, None, None, False, ok=False, note="nodata at point (sea?)")

        # slope from the central 3x3 gradient
        g = arr[cr - 1: cr + 2, cr - 1: cr + 2]
        if np.isfinite(g).all():
            gy, gx = np.gradient(g, dy, dx)
            slope = math.degrees(math.atan(math.hypot(gx[1, 1], gy[1, 1])))
        else:
            slope = None

        win_mean = float(np.nanmean(arr))
        rel = float(elev) - win_mean
        exposed = rel >= EXPOSED_REL_HEIGHT_M
        return Terrain(round(float(elev), 1),
                       round(slope, 1) if slope is not None else None,
                       round(rel, 1), exposed, ok=True)
    except Exception as e:  # noqa: BLE001 — offline / S3 error -> degrade, never crash the engine
        return Terrain(None, None, None, False, ok=False, note=f"{type(e).__name__}: {e}")
