"""WFS -> planning-layer parquet ingester, for EPA GeoServer layers the ArcGIS/esridump path can't reach.

The EPA estate (gis.epa.ie) is GeoServer: it serves rendered tiles via WMS (not vector-pullable) but
also vector features via WFS GetFeature (GeoJSON). esridump only speaks ArcGIS REST, so this is the
WFS sibling. Reuses the SAME vetted two-axis quarantine gate + atomic save_parquet as the ArcGIS
ingester, so quarantine/coverage behaviour is identical.

READY-TO-FIRE: configured for the two EPA layers we want the moment gis.epa.ie maintenance ends:
  - epa_uww_agglomeration : UWWT agglomeration BOUNDARY polygons = "served by public sewer" (fixes the
    septic node's long-standing 'we can't tell if sewered' gap + serves commercial wastewater).
  - epa_wfd_lakes         : WFD Lake Waterbodies polygons = the 'in open water / non-buildable' check.

    python pipeline_sandbox/planning_layers_wfs.py --layer epa_uww_agglomeration

Note: EPA field names couldn't be confirmed while the server is down; `keep` is a generous SUPERSET
(missing fields store None — harmless, like the HC generator) and containment needs only the geometry.
Confirm the WFD-lakes typeName against the live GetCapabilities before relying on it.
"""

from __future__ import annotations

import argparse
import json
import logging
import re

import polars as pl
import requests
import shapely
from shapely.geometry import shape

from extractors.planning_layers_ingest import OUT, gate  # reuse the SAME quarantine gate + output dir
from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_layers_wfs")
H = {"User-Agent": "dail-tracker-planning-ingest/1.0"}

WFS_LAYERS: dict[str, dict] = {
    # typeNames CONFIRMED against live GetCapabilities 2026-07-13 (the pre-maintenance guesses
    # were UWW_AgglomerationBound / WFD_Lakes; the server's actual names differ).
    "epa_uww_agglomeration": {
        "url": "https://gis.epa.ie/geoserver/EPA/wfs",
        "typename": "EPA:UWW_AgglomerationBoundaries",
        "keep": ("AgglomName", "Agg_Name", "Name", "Agglomeration", "LicStatus", "Licence", "AggType", "PE"),
    },
    "epa_wfd_lakes": {
        "url": "https://gis.epa.ie/geoserver/EPA/wfs",
        "typename": "EPA:WFD_LakeWaterBodiesActive",
        "keep": ("Name", "WB_NAME", "Lake_Name", "EU_CD", "WFD_Code"),
    },
}


def fetch_wfs(url: str, typename: str, page: int = 2000) -> list[dict]:
    """WFS 2.0 GetFeature as GeoJSON (EPSG:4326). Raises on HTTP error.

    The EPA GeoServer 400s on `startIndex` without a `sortBy` (verified live 2026-07-13), so we do
    NOT page: resultType=hits gives the total, then ONE GetFeature with count>=total pulls all.
    Both target layers are small (agglomerations 1,076 / lakes 812). A layer too big for one
    response must grow a sortBy-paged path — fail loudly rather than truncate silently.
    """
    base = {"service": "WFS", "version": "2.0.0", "request": "GetFeature", "typeNames": typename}
    r = requests.get(url, params={**base, "resultType": "hits"}, timeout=240, headers=H)
    r.raise_for_status()
    m = re.search(r'numberMatched="(\d+)"', r.text)
    total = int(m.group(1)) if m else 0
    if total > 50_000:
        raise RuntimeError(f"[{typename}] {total} features — too big for single-shot; add a sortBy-paged path")
    r = requests.get(url, params={
        **base, "outputFormat": "application/json", "count": max(total, page), "srsName": "EPSG:4326",
    }, timeout=600, headers=H)
    r.raise_for_status()
    if "json" not in r.headers.get("content-type", "").lower():
        raise RuntimeError(f"WFS did not return JSON (server down/maintenance?): {r.text[:120]}")
    feats = r.json().get("features", [])
    LOG.info("[%s] fetched %d of %d advertised", typename, len(feats), total)
    if total and len(feats) != total:
        raise RuntimeError(f"[{typename}] pulled {len(feats)} != advertised {total} — truncated response")
    return feats


def rows_from_features(feats: list[dict], keep: tuple[str, ...]) -> tuple[list[dict], dict]:
    """Run every feature through the two-axis gate; build {keep + wkb} rows. Returns (rows, reasons)."""
    rows: list[dict] = []
    reasons: dict[str, int] = {}
    for f in feats:
        clean, reason = gate(shape(f["geometry"]) if f.get("geometry") else None)
        reasons[reason] = reasons.get(reason, 0) + 1
        if clean is None:
            continue
        props = f.get("properties") or {}
        rec = {k: props.get(k) for k in keep}
        rec["wkb"] = shapely.to_wkb(clean)
        rows.append(rec)
    return rows, reasons


def ingest_wfs(key: str) -> None:
    cfg = WFS_LAYERS[key]
    feats = fetch_wfs(cfg["url"], cfg["typename"])
    rows, reasons = rows_from_features(feats, cfg["keep"])
    df = pl.DataFrame(rows)
    dest = save_parquet(df, OUT / f"{key}.parquet", compression_level=9)
    (OUT / f"{key}_coverage.json").write_text(json.dumps({
        "layer": key, "source": cfg["url"], "typename": cfg["typename"],
        "pulled": len(feats), "kept": df.height, "gate_reasons": reasons,
        "keep_fields": list(cfg["keep"]), "crs": "EPSG:4326", "bbox_subset": None,
    }, indent=2), encoding="utf-8")
    print(f"OK {key}: {dest} | kept {df.height}/{len(feats)} | {reasons}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", required=True, choices=sorted(WFS_LAYERS))
    args = ap.parse_args()
    setup_standalone_logging("planning_layers_wfs")
    ingest_wfs(args.layer)


if __name__ == "__main__":
    main()
