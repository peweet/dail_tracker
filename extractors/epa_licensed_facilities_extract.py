"""EPA licensed facilities — IPPC/IPC/Industrial-Emissions + waste licence holders, geocoded.

Pulls the EPA's public GeoServer WFS (no auth) for the two LEMA facility layers and writes a
siting-layer-store parquet keyed on ``Name``/``Address``/``RegCD``/``wkb``. This is the layer
``extractors/hsa_comah_extract.py:match_epa`` name-joins the HSA COMAH register against to
recover coordinates — before this extractor existed, that read hit a hard ``FileNotFoundError``.

Not the same thing as ``pipeline_sandbox/epa_licence_register_scrape.py``: that script is the
environmental-credential leg of the supplier-capability register (different schema — pandas,
``licensee_name``/``licence_number``, raw un-reprojected ITM x/y — different path
(``data/sandbox/parquet/``), different consumer). This extractor is the simpler, live,
siting-layer version: Polars, reprojected to WGS84, one parquet under ``planning_layers/``.

Layers pulled (both ``EPA:<layer>`` typeNames on the same WFS, ~1k rows each):
  * LEMA_Facilities_P_IPPC_IPC_IEL — combined IPPC/IPC/Industrial-Emissions licences (Name=company)
  * LEMA_Facilities_Waste          — waste licences (Name=facility/operator)

Geometry is served in Irish Transverse Mercator (EPSG:29902) and reprojected here to WGS84
(EPSG:4326) with pyproj (ingest-only dep — the runtime layer store stays shapely-only, same
precedent as ``planning_layers_ingest.py``'s Dublin/Cork CRS repairs). Reprojected points are
anchor-checked against Ireland's rough envelope before any write — the same CRS-mis-projection
bug already hit once on the DCC RPS layer (Irish Grid mis-projected as ITM, landed at lat~48.8).

Writes:
  data/silver/parquet/planning_layers/epa_licensed_facilities.parquet
  data/_meta/epa_licensed_facilities_coverage.json

Run:
  python extractors/epa_licensed_facilities_extract.py            # fetch + reproject + write
  python extractors/epa_licensed_facilities_extract.py --dry-run  # fetch + reproject, print, no write
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from urllib.parse import urlencode

import polars as pl
import shapely

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from services.coverage_io import save_coverage  # noqa: E402
from services.extract_runner import run_extractor  # noqa: E402
from services.http_engine import fetch_bytes, polite_headers  # noqa: E402
from services.parquet_io import save_parquet  # noqa: E402

LOG = logging.getLogger("epa_licensed_facilities")

_WFS = "https://gis.epa.ie/geoserver/EPA/ows"
# layer -> licence_class label; Name is the licensee/facility name in both layers.
_LAYERS = {
    "LEMA_Facilities_P_IPPC_IPC_IEL": "industrial",  # IPPC + IPC + Industrial Emissions (combined)
    "LEMA_Facilities_Waste": "waste",
}
_OUT = ROOT / "data" / "silver" / "parquet" / "planning_layers" / "epa_licensed_facilities.parquet"
_COV = ROOT / "data" / "_meta" / "epa_licensed_facilities_coverage.json"
# Ireland's rough envelope (lon, lat) — a reprojected point outside this is the mis-projection
# bug the DCC RPS ingest already hit once (Irish Grid mis-projected as ITM, lat~48.8); refuse
# to write rather than ship silently-wrong coordinates.
_IE_ENVELOPE = (-10.7, 51.3, -5.3, 55.5)  # lon_min, lat_min, lon_max, lat_max
# First successful pull (2026-07-31): 1067 industrial + 206 waste = 1273 raw, 1271 after the
# empty-name/no-geometry/dedupe guards. Floor set with margin below that observed count — fewer
# than this means the WFS query broke or a layer went (near-)empty.
_MIN_ROWS = 1000


def _fetch_layer(layer: str) -> list[dict]:
    params = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": f"EPA:{layer}",
        "outputFormat": "application/json",
        "count": "10000",
    }
    url = f"{_WFS}?{urlencode(params)}"
    data = fetch_bytes(url, headers=polite_headers(browser=True))
    if not data:
        raise SystemExit(f"fetch failed: {url}")
    payload = json.loads(data)
    feats = payload.get("features", [])
    LOG.info("  %s: %d features", layer, len(feats))
    return feats


def _point_xy(geom: dict | None) -> tuple[float, float] | None:
    """Representative (x, y) in the source CRS from a GeoJSON Point/MultiPoint geometry.

    The EPA WFS serves these layers as MultiPoint (single-member) rather than Point — drill
    into the nested coordinate list until a numeric pair surfaces, same shape as
    pipeline_sandbox/epa_licence_register_scrape.py's own ``_centroid`` helper.
    """
    if not geom:
        return None
    c = geom.get("coordinates")
    while isinstance(c, list) and c and isinstance(c[0], list):
        c = c[0]
    if isinstance(c, list) and len(c) >= 2:
        return float(c[0]), float(c[1])
    return None


def fetch_all_layers() -> tuple[list[dict], dict[str, int]]:
    """Fetch both WFS layers and reproject each feature's point ITM(29902) -> WGS84(4326)."""
    from pyproj import Transformer  # ingest-only dep — the runtime layer store stays shapely-only

    transformer = Transformer.from_crs(29902, 4326, always_xy=True)
    rows: list[dict] = []
    layer_counts: dict[str, int] = {}
    for layer, klass in _LAYERS.items():
        feats = _fetch_layer(layer)
        layer_counts[layer] = len(feats)
        for f in feats:
            props = f.get("properties") or {}
            xy = _point_xy(f.get("geometry"))
            lon: float | None = None
            lat: float | None = None
            if xy is not None:
                lon, lat = transformer.transform(xy[0], xy[1])
            rows.append(
                {
                    "Name": props.get("Name"),
                    "Address": props.get("Address"),
                    "RegCD": props.get("RegCD"),
                    "licence_status": props.get("LicenceStatusType"),
                    "licence_type": props.get("LicenceTypeName"),
                    "category": props.get("Category"),
                    "licence_class": klass,
                    "source_layer": layer,
                    "lon": lon,
                    "lat": lat,
                }
            )
    return rows, layer_counts


def clean_rows(rows: list[dict]) -> tuple[pl.DataFrame, dict[str, int]]:
    """Drop empty-Name and no-geometry rows, dedupe on (RegCD, Name) keep first.

    A facility holding both an industrial and a waste licence would otherwise appear twice
    (same guard as pipeline_sandbox/epa_licence_register_scrape.py's dedupe, translated to
    polars). Rows with no reprojectable point are dropped separately — match_epa() reads
    ``wkb`` unconditionally, so a null-geometry row can't be carried into the layer.
    """
    df = pl.DataFrame(rows) if rows else pl.DataFrame(schema={c: pl.Utf8 for c in ("Name", "Address", "RegCD")})
    total = df.height
    df = df.with_columns(pl.col("Name").fill_null("").str.strip_chars().alias("Name"))
    empty_name = df.filter(pl.col("Name").str.len_chars() == 0).height
    df = df.filter(pl.col("Name").str.len_chars() > 0)
    no_geom = df.filter(pl.col("lon").is_null() | pl.col("lat").is_null()).height
    df = df.filter(pl.col("lon").is_not_null() & pl.col("lat").is_not_null())
    before_dedupe = df.height
    df = df.unique(subset=["RegCD", "Name"], keep="first")
    dedupe_dropped = before_dedupe - df.height
    stats = {
        "total_raw": total,
        "dropped_empty_name": empty_name,
        "dropped_no_geometry": no_geom,
        "dropped_dedupe": dedupe_dropped,
    }
    return df, stats


def anchor_check(df: pl.DataFrame) -> dict:
    """Refuse to proceed if any reprojected point lands outside Ireland's envelope."""
    lon_min, lat_min, lon_max, lat_max = _IE_ENVELOPE
    if df.is_empty():
        return {"envelope": _IE_ENVELOPE, "checked": 0, "outside": 0}
    bad = df.filter(
        (pl.col("lon") < lon_min)
        | (pl.col("lon") > lon_max)
        | (pl.col("lat") < lat_min)
        | (pl.col("lat") > lat_max)
    )
    if bad.height:
        raise SystemExit(
            f"epa_licensed_facilities: {bad.height}/{df.height} reprojected points fell outside "
            f"the Ireland envelope {_IE_ENVELOPE} — CRS transform likely wrong, refusing to write"
        )
    return {"envelope": _IE_ENVELOPE, "checked": df.height, "outside": 0}


def to_wkb_frame(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.struct(["lon", "lat"])
        .map_elements(
            lambda s: shapely.to_wkb(shapely.Point(s["lon"], s["lat"])), return_dtype=pl.Binary
        )
        .alias("wkb")
    ).drop("lon", "lat")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    rows, layer_counts = fetch_all_layers()
    clean_df, drop_stats = clean_rows(rows)
    anchor = anchor_check(clean_df)

    LOG.info(
        "epa_licensed_facilities: layers=%s | raw=%d dropped(empty_name=%d no_geom=%d dedupe=%d) "
        "-> %d rows | anchor-check: %s",
        layer_counts,
        drop_stats["total_raw"],
        drop_stats["dropped_empty_name"],
        drop_stats["dropped_no_geometry"],
        drop_stats["dropped_dedupe"],
        clean_df.height,
        anchor,
    )

    if args.dry_run:
        print(f"layers: {layer_counts}")
        print(f"dropped: {drop_stats}")
        print(f"rows after cleanup: {clean_df.height}")
        print(f"anchor-check: {anchor}")
        return

    final_df = to_wkb_frame(clean_df)
    save_parquet(final_df, _OUT, min_rows=_MIN_ROWS)
    save_coverage(
        {
            "source": _WFS,
            "layers": layer_counts,
            "rows": final_df.height,
            "dropped": drop_stats,
            "crs_transform": "EPSG:29902 (Irish Transverse Mercator) -> EPSG:4326 (WGS84), pyproj always_xy",
            "anchor_check": anchor,
            "grain": "one row per facility x active licence (industrial or waste), deduped on (RegCD, Name)",
            "licence": "gis.epa.ie public WFS (GeoServer, no auth); facts-only extraction",
        },
        _COV,
    )
    print(f"OK {_OUT.name}: {final_df.height} facilities ({layer_counts})")


if __name__ == "__main__":
    run_extractor(main)
