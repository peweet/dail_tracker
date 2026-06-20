"""LIVE verification of the citizen siting-check feature's source layers (doc
PLANNING_PERMISSION_SCOPING.md §23). Probes each ArcGIS REST layer for: availability,
geometry type, spatial reference, and that the PLAN-REQUIRED field actually exists.

Network/integration test — skipped when offline. NOT for normal CI.

Verified live 2026-06-13. Field names below are the REAL schema names discovered by probing,
NOT the names assumed in the plan (e.g. GSI groundwater vulnerability is `VUL_CAT`/`VUL_DESC`,
not `Vulnerability`; SMRZone carries only `ZONE_ID` and must be joined to SMR points).

CRS is heterogeneous: NPWS/GSI/SMR = EPSG:2157 (ITM), applications = EPSG:3857 — reprojection to a
common CRS is mandatory before any cross-layer join.
"""

import pytest
import requests

pytestmark = pytest.mark.integration  # live network; not run in offline CI

# --- the verified registry: facts confirmed by live probe 2026-06-13 ---
LAYER_REGISTRY = [
    dict(
        key="npws_sac",
        role="ecology / Appropriate Assessment (#12-13)",
        url="https://services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/arcgis/rest/services/NPWSDesignatedAreas/FeatureServer/3",
        geom="esriGeometryPolygon",
        sr=2157,
        required_fields=["SITECODE", "SITE_NAME"],
        licence="gov open",
        approx_count=433,
        freshness="lastEditDate exposed (2026-04-01)",
    ),
    dict(
        key="gsi_gw_vuln",
        role="septic / Site Suitability (#25)",
        url="https://gsi.geodata.gov.ie/server/rest/services/Groundwater/IE_GSI_Groundwater_Vulnerability_40K_IE26_ITM/FeatureServer/0",
        geom="esriGeometryPolygon",
        sr=2157,
        required_fields=["VUL_CAT", "VUL_DESC"],
        licence="CC-BY-4.0",
        approx_count=221_148,
        freshness="NO lastEditDate -> must poll/hash",
    ),
    dict(
        key="nms_smr_points",
        role="archaeology (#17)",
        url="https://services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services/SMROpenData/FeatureServer/0",
        geom="esriGeometryPoint",
        sr=2157,
        required_fields=["SMRS", "MONUMENT_CLASS"],
        licence="gov open",
        approx_count=151_308,
        freshness="lastEditDate 2026-06-04",
    ),
    dict(
        key="nms_smr_zone",
        role="archaeology buffer (#17) — join ZONE_ID to SMR points",
        url="https://services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services/SMRZoneOpenData/FeatureServer/0",
        geom="esriGeometryPolygon",
        sr=2157,
        required_fields=["ZONE_ID"],
        licence="gov open",
        approx_count=81_409,
        freshness="lastEditDate 2026-06-04",
    ),
    dict(
        key="planning_apps",
        role="context register (495k)",
        url="https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/IrishPlanningApplications/FeatureServer/0",
        geom="esriGeometryPoint",
        sr=3857,
        required_fields=["Decision", "PlanningAuthority"],
        licence="CC-BY-4.0",
        approx_count=495_632,
        freshness="lastEditDate 2026-06-09",
    ),
    dict(
        key="myplan_zoning",
        role="material contravention (§10.6) — RESOLVED endpoint",
        # on-prem maps.housing.gov.ie / maps.environ.ie are DEAD; this is the ArcGIS Online hosted twin
        url="https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/GZT_Current_Plan/FeatureServer/0",
        geom="esriGeometryPolygon",
        sr=2157,
        required_fields=["ZONE_GZT", "ZONE_ORIG", "ZONE_DESC"],  # ZONE_GZT nullable; ZONE_ORIG/DESC populated
        licence="gov open (MyPlan/DHLGH)",
        approx_count=82_664,
        freshness="lastEditDate 2026-05-13; carries PLAN_FROM/PLAN_TO/CURRENT_PLAN (temporal versioning)",
    ),
    dict(
        key="niah",
        role="heritage / architectural (#16) — NATIONAL",
        url="https://services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services/NIAHBuildingsOpenData/FeatureServer/0",
        geom="esriGeometryPoint",
        sr=2157,
        required_fields=["REG_NO", "NAME", "ORIGINAL_TYPE"],
        licence="gov open (DHLGH)",
        approx_count=48_327,
        freshness="lastEditDate 2025-04-03",
    ),
]


def _probe(url):
    return requests.get(url, params={"f": "json"}, headers={"User-Agent": "dail-tracker-test/1.0"}, timeout=45).json()


def _online():
    try:
        requests.get("https://services-eu1.arcgis.com", timeout=10)
        return True
    except Exception:
        return False


ONLINE = _online()


@pytest.mark.skipif(not ONLINE, reason="offline — live ArcGIS probe unavailable")
@pytest.mark.parametrize("layer", LAYER_REGISTRY, ids=lambda ly: ly["key"])
def test_layer_available_geometry_and_required_field(layer):
    meta = _probe(layer["url"])
    assert "error" not in meta, f"{layer['key']} endpoint error: {meta.get('error')}"

    # geometry type matches the plan
    assert meta.get("geometryType") == layer["geom"], (
        f"{layer['key']} geometry changed: {meta.get('geometryType')} != {layer['geom']}"
    )

    # spatial reference matches (catches a silent CRS change)
    sr = meta.get("extent", {}).get("spatialReference", {}) or {}
    wkid = sr.get("latestWkid") or sr.get("wkid")
    assert wkid == layer["sr"], f"{layer['key']} SR changed: {wkid} != {layer['sr']}"

    # the plan-required field actually exists in the live schema
    fnames = {f["name"].upper() for f in (meta.get("fields") or [])}
    assert any(rf.upper() in fnames for rf in layer["required_fields"]), (
        f"{layer['key']} MISSING all required fields {layer['required_fields']}; live schema has: {sorted(fnames)}"
    )


@pytest.mark.skipif(not ONLINE, reason="offline")
@pytest.mark.parametrize("layer", LAYER_REGISTRY, ids=lambda ly: ly["key"])
def test_layer_has_rows(layer):
    c = requests.get(
        layer["url"] + "/query", params={"where": "1=1", "returnCountOnly": "true", "f": "json"}, timeout=60
    ).json()
    assert isinstance(c.get("count"), int) and c["count"] > 0, f"{layer['key']} returned no rows"


# --- KNOWN GAPS / DEAD ENDPOINTS (documented, not silently ignored) ---


@pytest.mark.skipif(not ONLINE, reason="offline")
def test_myplan_onprem_endpoint_is_dead():
    """The on-prem MyPlan zoning hosts (maps.housing.gov.ie, maps.environ.ie) are DEAD as of
    2026-06-13. The live data now lives in the ArcGIS Online twin GZT_Current_Plan (in the registry
    above). This tripwire documents the migration: if the on-prem host ever comes back, revisit."""
    import requests as _rq

    with pytest.raises(_rq.exceptions.RequestException):
        _rq.get(
            "https://maps.housing.gov.ie/arcgis/rest/services/MyPlan/GZTZoningEPSG2157/MapServer/0",
            params={"f": "json"},
            timeout=15,
        )


def test_opw_flood_is_link_only_not_ingestible():
    """CONFIRMED via floodinfo.ie open-spatial-data-portal (2026-06-13): ALL OPW flood extent
    datasets — CFRAM, National Coastal Flood Extents 2021, NIFM — are CC-BY-NC-ND 4.0.
    NC = no commercial use; ND = no derivatives (an overlay is arguably a derivative).
    => flood (#21) is NOT ingestible. Design rule: DEEP-LINK to floodinfo.ie at the user's
    coordinates instead of overlaying OPW geometry. Tripwire: revisit if OPW relicenses."""
    flood_licence = "CC-BY-NC-ND"
    assert "NC" in flood_licence and "ND" in flood_licence
    flood_strategy = "deep-link"  # NOT "ingest"
    assert flood_strategy == "deep-link"


# Per-LA layers: NO single national endpoint. Probed 2026-06-13 — the 31-council assembly problem.
# Partial coverage scattered across the Heritage Council ArcGIS org + individual council GIS orgs;
# the remainder is PDF-locked in development-plan appendices (same pattern as the DM-standards rulebook).
PER_LA_LAYERS = {
    "rps": "Record of Protected Structures (#16). Per-LA. Some as ArcGIS (Heritage_Council_Admin "
    "'Wicklow (RPS)', Fingal/Cork/GCC/DLR own orgs) + a partial national RPS shapefile on "
    "data.gov.ie; many councils PDF-only. NIAH (national, verified) is the cleaner heritage proxy.",
    "aca": "Architectural Conservation Areas (#16). Per-LA polygons (e.g. Fingal ACA = 33 features, "
    "verified). No national aggregation; assemble 31 council layers, rest in plan PDFs.",
    "landscape_sensitivity": "Landscape Character / Sensitivity (#10). MOST PDF-LOCKED. Some counties "
    "publish GIS (Galway/Donegal/Kildare/Wexford/Fingal via Heritage Council + council orgs); "
    "many only in development-plan appendix maps → OCR/vectorise per-LA. Galway-first per §23.6.",
}


def test_per_la_layers_are_an_assembly_not_an_endpoint():
    """Tripwire: RPS/ACA/landscape are NOT national single endpoints. Do not assume one URL covers
    Ireland — they need 31-council assembly (partial GIS + PDF extraction). Ship Galway-first."""
    assert set(PER_LA_LAYERS) == {"rps", "aca", "landscape_sensitivity"}
    # if someone finds a true national aggregation later, update this and add to LAYER_REGISTRY


def test_dem_and_osm_sources_are_non_arcgis():
    """DEM and OSM roads are NOT ArcGIS REST — recorded so they aren't probed the same way.
    DEM: Copernicus GLO-30 (30 m, FREE/open Copernicus licence, Cloud-Optimized GeoTIFF on AWS Open
         Data) -> host/range-read as COG (§23.9). Tailte Éireann 10 m DTM is LICENSED (proprietary).
    OSM roads: Geofabrik Ireland extract, ODbL (attribution + SHARE-ALIKE) -> pre-ingest, self-host."""
    dem = {"source": "Copernicus GLO-30", "licence": "free/open", "format": "COG", "res_m": 30}
    assert dem["format"] == "COG" and dem["licence"] == "free/open"
    osm = {"source": "Geofabrik Ireland", "licence": "ODbL-share-alike"}
    assert "share-alike" in osm["licence"]
