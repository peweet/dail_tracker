"""Phase 0 (sandbox): ingest the siting-check designation layers -> gated GeoParquet.

For each spatial-trigger layer in planning_rules/SOURCE_REGISTRY.md, pull the ArcGIS
FeatureServer with the dedicated puller (esridump = pyesridump) and run the §13.6/§13.8
two-axis quarantine gate before persisting. Stored as a plain parquet of {key fields + wkb}
(geometry as WKB bytes) so the runtime reads it with shapely only — no geopandas/GDAL.

THE GATE (memory reference_geometry_validation_sources — geometry validity is TWO axes):
  1. topology (self-intersection etc.) -> REPAIRABLE via make_valid
  2. coordinate-domain / out-of-bounds (the -9e12 case) -> NOT repairable; make_valid LAUNDERS
     it into a plausible band. So we bounds-check against the Ireland envelope BOTH before AND
     after make_valid, and quarantine (never silently keep/repair) anything that escapes.
Plus: pull at outSR=4326 (containment matches the apps feed + the user point, like §13);
geometry_precision=7; reconcile pulled-count == server returnCountOnly (catch truncation).

    python pipeline_sandbox/planning_layers_ingest.py --layer npws_sac      # smoke (433 feats)
    python pipeline_sandbox/planning_layers_ingest.py --layer all
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path

import polars as pl
import requests
import shapely
from esridump.dumper import EsriDumper
from shapely.geometry import shape

from services.logging_setup import setup_standalone_logging
from services.parquet_io import save_parquet

LOG = logging.getLogger("planning_layers_ingest")

OUT = Path(__file__).resolve().parents[1] / "data/silver/parquet/planning_layers"
IRELAND_BBOX = (-11.0, 51.0, -5.0, 56.0)  # (min_lon, min_lat, max_lon, max_lat)
VERTEX_GIANT = 200_000  # flag (not drop) extreme polygons; esridump precision caps payload

_NPWS = "https://services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/arcgis/rest/services/NPWSDesignatedAreas/FeatureServer"
_NMS = "https://services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services"
_DHLGH = "https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services"


@dataclass(frozen=True)
class LayerSpec:
    name: str
    url: str
    kind: str  # polygon | point
    keep: tuple[str, ...]  # property fields to retain (others dropped)
    # giant high-vertex polygons need a smaller page + longer timeout (§13.8)
    max_page_size: int | None = None
    timeout: int = 60
    # optional (minlon, minlat, maxlon, maxlat) envelope to pull a regional subset
    # (Galway-first: e.g. the huge national GSI layers); None = national pull
    bbox: tuple[float, float, float, float] | None = None


# Galway county + city + Menlo envelope, for the big national layers we ship Galway-first
GALWAY_BBOX = (-10.2, 53.0, -8.4, 53.8)
# Greater Dublin envelope (the 4 Dublin LAs: Fingal N, City, South Dublin W, DLR S)
DUBLIN_BBOX = (-6.55, 53.15, -5.95, 53.65)
# Cork County (largest county) + Cork City — Mizen Head (W) to the Waterford border (E)
CORK_BBOX = (-10.3, 51.4, -7.8, 52.4)
# named regions for extending a bbox-limited layer's coverage WITHOUT re-pulling the rest.
# The engine reads one same-named parquet, so merging a region just widens coverage; outside
# every ingested region a layer honestly returns nothing (the gate + reconcile run per region).
REGIONS: dict[str, tuple[float, float, float, float]] = {
    "galway": GALWAY_BBOX,
    "dublin": DUBLIN_BBOX,
    "cork": CORK_BBOX,
}
_HC = "https://services-eu1.arcgis.com/v5dOXTEOb7ZHdNyQ/arcgis/rest/services"  # Heritage Council
_GSI = "https://gsi.geodata.gov.ie/server/rest/services/Groundwater"
_GSI_Q = "https://gsi.geodata.gov.ie/server/rest/services/Quaternary"
_NPWS_ORG = "https://services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/arcgis/rest/services"  # NPWS org
_OSI = "https://services-eu1.arcgis.com/FH5XCsx8rYXqnjF5/arcgis/rest/services"  # Tailte Eireann (OSi) open data


SPECS: dict[str, LayerSpec] = {
    # NPWS designated areas: layer 0=SPA, 1=pNHA, 2=NHA, 3=SAC (confirmed §13/§23.10)
    "npws_sac": LayerSpec("npws_sac", f"{_NPWS}/3", "polygon", ("SITECODE", "SITE_NAME", "COUNTY"), 25, 180),
    "npws_spa": LayerSpec("npws_spa", f"{_NPWS}/0", "polygon", ("SITECODE", "SITE_NAME", "COUNTY"), 25, 180),
    "npws_pnha": LayerSpec("npws_pnha", f"{_NPWS}/1", "polygon", ("SITECODE", "SITE_NAME", "COUNTY"), 25, 180),
    "npws_nha": LayerSpec("npws_nha", f"{_NPWS}/2", "polygon", ("SITECODE", "SITE_NAME", "COUNTY"), 25, 180),
    # NMS archaeology: the OPERATIVE constraint is the Zone of Notification polygon (§18.1)
    "smr_zone": LayerSpec("smr_zone", f"{_NMS}/SMRZoneOpenData/FeatureServer/0", "polygon", ("ZONE_ID",)),
    "smr_points": LayerSpec(
        "smr_points",
        f"{_NMS}/SMROpenData/FeatureServer/0",
        "point",
        ("ENTITY_ID", "MONUMENT_CLASS", "TOWNLAND", "ZONE_ID_1"),
        # NATIONAL (was Galway/Cork/Dublin) — monument NAMING for every county; ~151k points.
    ),
    # MyPlan zoning composite (material-contravention context)
    "zoning_gzt": LayerSpec(
        "zoning_gzt",
        f"{_DHLGH}/GZT_Current_Plan/FeatureServer/0",
        "polygon",
        ("ZONE_GZT", "ZONE_ORIG", "ZONE_DESC", "PLAN_FROM", "PLAN_TO", "PLAN_NAME"),
    ),
    # NIAH architectural heritage (points)
    "niah": LayerSpec("niah", f"{_NMS}/NIAHBuildingsOpenData/FeatureServer/0", "point", ("REG_NO", "NAME")),
    # GSI septic site-suitability — NATIONAL (covers all councils for the septic node; ~221k polys,
    # one-time slow pull). VUL_CAT X/E/H/M/L. (Was Galway-bbox; national-ised for generalisation.)
    "gsi_vulnerability": LayerSpec(
        "gsi_vulnerability",
        f"{_GSI}/IE_GSI_Groundwater_Vulnerability_40K_IE26_ITM/FeatureServer/0",
        "polygon",
        ("VUL_CAT", "VUL_DESC"),
        max_page_size=1000,
        timeout=180,
    ),
    "gsi_karst": LayerSpec(
        "gsi_karst",
        f"{_GSI}/IE_GSI_Karst_Datasets_40K_IE32_ITM/FeatureServer/0",
        "point",
        ("KARST_TYPE", "KARST_NAME"),
        # NATIONAL (was Galway/Cork/Dublin) — karst -> septic severity for every county; ~18k points.
    ),
    # per-LA Galway heritage / landscape (Heritage Council org; CC-BY 4.0)
    "galway_county_rps": LayerSpec(
        "galway_county_rps", f"{_HC}/Galway_County_RPS/FeatureServer/0", "point", ("NAME", "TOWNLAND", "FEATURES")
    ),
    "galway_city_aca": LayerSpec(
        "galway_city_aca", f"{_HC}/Galway_City_ACA/FeatureServer/0", "polygon", ("DESCRIPTIO", "ADDRESS", "SPECIALINT")
    ),
    "galway_county_landscape": LayerSpec(
        "galway_county_landscape", f"{_HC}/Galway_County_Landscape_Categories/FeatureServer/0", "polygon", ("NAME",)
    ),
    # per-LA Cork heritage / landscape (Heritage Council org; CC-BY 4.0). Per-county schema
    # variance: County RPS = STRUCTURE/TOWNLAND, City RPS = Building_Name, landscape = TYPE.
    "cork_county_rps": LayerSpec(
        "cork_county_rps", f"{_HC}/Cork_County_RPS/FeatureServer/0", "point", ("STRUCTURE", "TOWNLAND", "DED")
    ),
    "cork_city_rps": LayerSpec(
        "cork_city_rps", f"{_HC}/Cork_City_RPS/FeatureServer/0", "point", ("Building_Name", "Address_1", "Address_2")
    ),
    "cork_county_landscape": LayerSpec(
        "cork_county_landscape", f"{_HC}/Cork_County_Landscape_Categories/FeatureServer/0", "polygon", ("TYPE",)
    ),
    # NPWS National Parks — the strongest amenity/nature designation (6 nationally; incl. Connemara
    # + Burren near Galway). National pull (tiny). DESIG/SITE_NAME.
    "national_parks": LayerSpec(
        "national_parks", f"{_NPWS_ORG}/NationalParkBoundaries/FeatureServer/0", "polygon", ("DESIG", "SITE_NAME")
    ),
    # Gaeltacht (Irish-speaking) areas — Tailte Eireann (OSi), 7 county polygons. NATIONAL. A point
    # inside triggers the statutory linguistic planning rules (PDA s.10(2)): a Linguistic Impact
    # Statement + (in stronger sub-areas) an Irish-language occupancy condition. GT_ENGLISH/GT_GAEILGE.
    "gaeltacht": LayerSpec(
        "gaeltacht",
        f"{_OSI}/Gaeltacht_Boundaries_Generalised_20m___2015/FeatureServer/0",
        "polygon",
        ("GT_ENGLISH", "GT_GAEILGE", "COUNTY", "PROVINCE"),
    ),
    # GSI Quaternary Sediments = subsoil TYPE incl. peat/blanket-bog (for the peat_bog node).
    # Galway-bbox; QSED_TYPE / LEGENDDESC carry the peat label.
    "gsi_quaternary": LayerSpec(
        "gsi_quaternary",
        f"{_GSI_Q}/IE_GSI_Quaternary_Sediments_50K_IE26_ITM/FeatureServer/0",
        "polygon",
        ("QSED_TYPE", "LEGENDDESC"),
        max_page_size=1000,
        timeout=180,
        # NATIONAL (was Galway/Cork/Dublin) — peat/blanket-bog subsoil for every county; ~206k polys.
    ),
}

# ── Heritage Council per-LA RPS / landscape / ACA, generated from a compact table ──────────────
# Same org (_HC) as the curated Galway/Cork entries above; this widens protected_structure +
# landscape_siting (the AONB / high-amenity / scenic equivalent — Connemara, Killary, Dingle, the
# Curragh's county etc.) coverage to every county the Heritage Council publishes. `keep` is a
# per-category SUPERSET: fields a given layer lacks just store None (ingest() filters props by
# `keep`; EsriDumper pulls all fields), so no per-layer field curation is needed. The key MUST
# contain 'rps' | 'landscape' | 'aca' — the engine's layers_matching() is a substring test.
_RPS_KEEP = ("NAME", "STRUCTURE", "Building_Name", "TOWNLAND", "DED", "Address_1", "Address_2", "ADDRESS", "DESCRIPTIO")
_LSC_KEEP = ("NAME", "TYPE", "CATEGORY", "CLASS", "LCA", "LCT", "SENSITIVI", "DESCRIPTIO")
_ACA_KEEP = ("NAME", "DESCRIPTIO", "ADDRESS", "SPECIALINT")
_HC_PER_LA: dict[str, tuple[str, str, tuple[str, ...]]] = {
    # RPS — protected structures (points)
    "cavan_rps": ("Cavan_RPS", "point", _RPS_KEEP),
    "clare_rps": ("Clare_RPS", "point", _RPS_KEEP),
    "donegal_rps": ("Donegal_RPS", "point", _RPS_KEEP),
    "fingal_rps": ("Fingal_RPS", "point", _RPS_KEEP),
    "kerry_rps": ("Kerry_RPS", "point", _RPS_KEEP),
    "laois_rps": ("Laois_RPS", "point", _RPS_KEEP),
    "leitrim_rps": ("Leitrim_RPS", "point", _RPS_KEEP),
    "limerick_city_rps": ("Limerick_City_RPS", "point", _RPS_KEEP),
    "limerick_county_rps": ("Limerick_County_RPS", "point", _RPS_KEEP),
    "louth_rps": ("Louth_RPS", "point", _RPS_KEEP),
    "mayo_rps": ("Mayo_RPS", "point", _RPS_KEEP),
    "monaghan_rps": ("Monaghan_RPS", "point", _RPS_KEEP),
    "offaly_rps": ("Offaly_RPS", "point", _RPS_KEEP),
    "roscommon_rps": ("Roscommon_RPS", "point", _RPS_KEEP),
    "south_dublin_rps": ("South_Dublin_RPS", "point", _RPS_KEEP),
    "south_tipperary_rps": ("South_Tipperary_RPS", "point", _RPS_KEEP),
    "north_tipperary_rps_nenagh": ("North_Tipperary_NIAHRPS_Nenagh_20132019", "point", _RPS_KEEP),
    "north_tipperary_rps_templemore": ("North_Tipperary_RPS_Templemore_20122018", "point", _RPS_KEEP),
    "north_tipperary_rps_thurles": ("North_Tipperary_RPS_Thurles_20092015", "point", _RPS_KEEP),
    "waterford_county_rps": ("Waterford_County_RPS", "point", _RPS_KEEP),
    "wicklow_rps": ("Wicklow_RPS", "point", _RPS_KEEP),
    # landscape — high-value / sensitivity / categories (polygons; the AONB/scenic equivalent)
    "cavan_landscape": ("Cavan_High_Value_Landscape_Areas", "polygon", _LSC_KEEP),
    "clare_landscape": ("Clare_Landscape_Categories", "polygon", _LSC_KEEP),
    "cork_city_landscape": ("Cork_City_High_Value_Landscapes", "polygon", _LSC_KEEP),
    "fingal_landscape": ("Fingal_Landscape_Categories", "polygon", _LSC_KEEP),
    "kildare_landscape": ("Kildare_Landscape_Sensitivity_Areas", "polygon", _LSC_KEEP),
    "laois_landscape": ("Laois_Landscape_Categories", "polygon", _LSC_KEEP),
    "limerick_county_landscape": ("Limerick_County_Landscape_Categories", "polygon", _LSC_KEEP),
    "louth_landscape": ("Louth_Landscape_Categories", "polygon", _LSC_KEEP),
    "monaghan_landscape": ("Monaghan_Landscape_Character_Areas", "polygon", _LSC_KEEP),
    "roscommon_landscape": ("Roscommon_Landscape_Character_Areas", "polygon", _LSC_KEEP),
    "wexford_landscape": ("Wexford_Adopted_Landscapes_of_Greater_Sensitivity_2013", "polygon", _LSC_KEEP),
    "wicklow_landscape": ("Wicklow_Landscape_Categories", "polygon", _LSC_KEEP),
    # ACA — architectural conservation areas (polygons)
    "dublin_city_aca": ("Dublin_City_ACA", "polygon", _ACA_KEEP),
    "kildare_aca": ("Kildare_ACA", "polygon", _ACA_KEEP),
    "kilkenny_aca": ("Kilkenny_ACA", "polygon", _ACA_KEEP),
}
for _k, (_svc, _geom, _keep) in _HC_PER_LA.items():
    SPECS.setdefault(_k, LayerSpec(_k, f"{_HC}/{_svc}/FeatureServer/0", _geom, _keep))


def _bbox_args(bbox: tuple[float, float, float, float] | None) -> dict:
    if not bbox:
        return {}
    return {
        "geometry": ",".join(str(c) for c in bbox),
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
    }


def server_count(url: str, bbox=None) -> int | None:
    try:
        params = {"where": "1=1", "returnCountOnly": "true", "f": "json", **_bbox_args(bbox)}
        r = requests.get(url + "/query", params=params, timeout=60)
        return r.json().get("count")
    except Exception as e:  # noqa: BLE001
        LOG.warning("count probe failed for %s: %s", url, e)
        return None


def _in_bbox(geom) -> bool:
    minx, miny, maxx, maxy = geom.bounds
    return IRELAND_BBOX[0] <= minx and maxx <= IRELAND_BBOX[2] and IRELAND_BBOX[1] <= miny and maxy <= IRELAND_BBOX[3]


def gate(geom) -> tuple[object | None, str]:
    """Two-axis quarantine gate. Returns (clean_geom | None, reason)."""
    if geom is None or geom.is_empty:
        return None, "empty"
    # axis 2 FIRST: out-of-bounds coords are NOT repairable (catches the -9e12 case)
    if not _in_bbox(geom):
        return None, "bounds_escape"
    # axis 1: topology — repair, then RE-CHECK bounds (make_valid can launder bad coords)
    if not geom.is_valid:
        geom = shapely.make_valid(geom)
        if geom.is_empty:
            return None, "empty_after_make_valid"
        if not _in_bbox(geom):
            return None, "bounds_escape_after_make_valid"
    return geom, ("ok_giant" if shapely.get_num_coordinates(geom) > VERTEX_GIANT else "ok")


def ingest(spec: LayerSpec) -> Path:
    OUT.mkdir(parents=True, exist_ok=True)
    expected = server_count(spec.url, spec.bbox)
    LOG.info("[%s] %s | server count=%s%s", spec.name, spec.url, expected, " (bbox subset)" if spec.bbox else "")

    dumper = EsriDumper(
        spec.url,
        outSR=4326,
        geometry_precision=7,
        timeout=spec.timeout,
        max_page_size=spec.max_page_size,
        extra_query_args=_bbox_args(spec.bbox) or None,
    )

    rows: list[dict] = []
    reasons: dict[str, int] = {}
    pulled = 0
    for feat in dumper:
        pulled += 1
        clean, reason = gate(shape(feat["geometry"]) if feat.get("geometry") else None)
        reasons[reason] = reasons.get(reason, 0) + 1
        if clean is None:
            continue
        props = feat.get("properties") or {}
        rec = {k: props.get(k) for k in spec.keep}
        rec["wkb"] = shapely.to_wkb(clean)
        rows.append(rec)
        if pulled % 20000 == 0:
            LOG.info("[%s] pulled %d (kept %d)", spec.name, pulled, len(rows))

    kept = len(rows)
    quarantined = pulled - kept
    LOG.info("[%s] pulled=%d kept=%d quarantined=%d reasons=%s", spec.name, pulled, kept, quarantined, reasons)

    # reconcile: a truncated pull must fail loudly. A NATIONAL pull must match the server
    # exactly (any drift = truncation/corruption). A BBOX subset pull tolerates small edge
    # drift — the count query and esridump's paged geometry-envelope queries handle the
    # boundary slightly differently — but a large drift still indicates a real problem.
    if expected is not None:
        drift = abs(pulled - int(expected))
        tol = max(50, int(expected) * 0.02) if spec.bbox else max(2, int(expected) * 0.001)
        if spec.bbox and drift > max(2, int(expected) * 0.001):
            LOG.warning("[%s] bbox edge drift pulled=%d server=%d (within tol=%d)", spec.name, pulled, expected, tol)
        assert drift <= tol, f"[{spec.name}] count drift pulled={pulled} server={expected} (truncated pull?)"

    df = pl.DataFrame(rows)
    # Geometry layers are large WKB blobs written rarely and read often: bump zstd to
    # level 9 (~10-22% smaller on these layers; decompression speed is level-independent,
    # so the read path is unaffected — only the one-time ingest write is slower).
    dest = save_parquet(df, OUT / f"{spec.name}.parquet", compression_level=9)
    coverage = {
        "layer": spec.name,
        "url": spec.url,
        "kind": spec.kind,
        "server_count": expected,
        "pulled": pulled,
        "kept": kept,
        "quarantined": quarantined,
        "gate_reasons": reasons,
        "keep_fields": list(spec.keep),
        "crs": "EPSG:4326",
        "bbox_subset": list(spec.bbox) if spec.bbox else None,
    }
    cov_path = OUT / f"{spec.name}_coverage.json"
    cov_path.write_text(json.dumps(coverage, indent=2), encoding="utf-8")
    LOG.info("[%s] wrote %s (%d rows) + %s", spec.name, dest, df.height, cov_path.name)
    print(f"OK {spec.name}: {dest} | kept {kept}/{pulled} | quarantined {quarantined} {reasons}")
    return dest


def add_region(spec: LayerSpec, region: str) -> Path:
    """Pull ONE region's bbox and MERGE it into the layer's existing parquet (dedup by wkb).

    Widens a bbox-limited layer (e.g. Galway-only GSI/SMR) to a new region (e.g. Dublin)
    without the heavy full re-pull. The region pull runs the same two-axis gate + a per-region
    reconcile; coverage JSON records the region under `regions_added`. The engine reads the
    one same-named parquet, so the new region's polygons just widen what containment finds.
    """
    bbox = REGIONS[region]
    dest = OUT / f"{spec.name}.parquet"
    if not dest.exists():
        raise SystemExit(f"[{spec.name}] no existing parquet to extend; run a full ingest first")
    existing = pl.read_parquet(dest)
    have = set(existing["wkb"].to_list())

    expected = server_count(spec.url, bbox)
    LOG.info(
        "[%s] +region=%s bbox=%s | server count=%s (have %d rows)", spec.name, region, bbox, expected, existing.height
    )
    dumper = EsriDumper(
        spec.url,
        outSR=4326,
        geometry_precision=7,
        timeout=spec.timeout,
        max_page_size=spec.max_page_size,
        extra_query_args=_bbox_args(bbox) or None,
    )
    rows: list[dict] = []
    reasons: dict[str, int] = {}
    pulled = added = 0
    for feat in dumper:
        pulled += 1
        clean, reason = gate(shape(feat["geometry"]) if feat.get("geometry") else None)
        reasons[reason] = reasons.get(reason, 0) + 1
        if clean is None:
            continue
        wkb = shapely.to_wkb(clean)
        if wkb in have:  # region overlap with already-ingested coverage -> skip the duplicate
            continue
        props = feat.get("properties") or {}
        rec = {k: props.get(k) for k in spec.keep}
        rec["wkb"] = wkb
        rows.append(rec)
        have.add(wkb)
        added += 1

    if expected is not None:  # per-region reconcile (bbox edge handling differs)
        # edge drift = polygons straddling the region envelope, counted by the intersects count
        # query but paged slightly differently by esridump. It scales with the boundary, not the
        # subset size, so a small regional pull needs a higher ABSOLUTE floor than a % alone
        # (e.g. ~66 boundary polys on a ~2,800-poly Dublin subset = 2.4%, real not truncation).
        drift = abs(pulled - int(expected))
        tol = max(100, int(expected) * 0.03)
        assert drift <= tol, (
            f"[{spec.name}] region={region} count drift pulled={pulled} server={expected} "
            f"(>tol={tol}; likely a truncated pull, not edge drift)"
        )
    LOG.info("[%s] region=%s pulled=%d added=%d (new) reasons=%s", spec.name, region, pulled, added, reasons)

    merged = pl.concat([existing, pl.DataFrame(rows)], how="vertical") if rows else existing
    save_parquet(merged, dest, compression_level=9)  # match the full-ingest write (see ingest())
    cov_path = OUT / f"{spec.name}_coverage.json"
    cov = json.loads(cov_path.read_text(encoding="utf-8")) if cov_path.exists() else {"layer": spec.name}
    cov["kept"] = merged.height
    cov.setdefault("regions_added", [])
    cov["regions_added"] = sorted(set(cov["regions_added"]) | {region})
    cov[f"region_{region}"] = {
        "bbox": list(bbox),
        "server_count": expected,
        "pulled": pulled,
        "added": added,
        "gate_reasons": reasons,
    }
    cov_path.write_text(json.dumps(cov, indent=2), encoding="utf-8")
    LOG.info("[%s] merged -> %s (%d rows, +%d from %s)", spec.name, dest, merged.height, added, region)
    print(f"OK {spec.name} +{region}: {merged.height} rows (+{added} new) {reasons}")
    return dest


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--layer", required=True, help="layer name or 'all'")
    ap.add_argument(
        "--add-region",
        choices=sorted(REGIONS),
        help="pull this region's bbox and MERGE into the existing parquet (no full re-pull)",
    )
    args = ap.parse_args()
    setup_standalone_logging("planning_layers_ingest")
    names = list(SPECS) if args.layer == "all" else [args.layer]
    for n in names:
        if n not in SPECS:
            raise SystemExit(f"unknown layer {n!r}; choices: {', '.join(SPECS)} or 'all'")
        if args.add_region:
            add_region(SPECS[n], args.add_region)
        else:
            ingest(SPECS[n])


if __name__ == "__main__":
    main()
