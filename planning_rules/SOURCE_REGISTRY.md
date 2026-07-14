# Planning Source Registry (SEED)

**Purpose.** One row per **obligation axiom** (from each `required_assessments.md`) → the **external
dataset** that evaluates its spatial/site predicate. This is the data shopping-list for the
**consolidated planning feature** (the §16 obligation-set reconstructor): join these layers to the
national application points → emit, for any site, the assessments it was obliged to carry, plus — for
appealed cases — the actual reason text.

**Derivation.** The axioms are the rulebook; the sources are *how you evaluate them*. Environmental
axioms (#12-14, #21) → NPWS SAC/SPA + OPW flood. Archaeology (#17) → NMS SMR zones. Access/sightlines
(#6-8) → TII + OSM roads. See `doc/archive/PLANNING_PERMISSION_SCOPING.md` §18 for the narrative.

**Status legend:** `live` = ArcGIS/REST endpoint probed this session · `located` = found, not probed ·
`endpoint-TBD` = needs discovery. Galway checklist `#` = row in
`county_councils/galway_county_council/required_assessments.md`.

_Last updated: 2026-06-14 (endpoints verified; flood Zone-A/B + zoning composite corrected)._

> **Operational/freshness layer:** `_corpus_registry/planning_corpus_seed.csv` holds the resolved URLs,
> `update_cadence`, `supersession_risk`, `poll_method` and `last_known_update` for every source here.
> Poll mechanics: ArcGIS `?f=json → editingInfo.lastEditDate` (**NPWS = service `Last-Modified`/`ETag`**,
> editor-tracking off); CKAN `metadata_modified`; PxStat `ReadMetadata.updated`; per-council pages for plans/contributions.

## Spatial-trigger sources (join to application point)

| axiom_# | obligation | layer | endpoint | geom | status | join_predicate | key_fields |
|---|---|---|---|---|---|---|---|
| 12,13,14 | Appropriate Assessment / NIS / EcIA / Habitats | NPWS Designated Areas (SAC/SPA/NHA/pNHA) | `services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/arcgis/rest/services/NPWSDesignatedAreas/FeatureServer` (layer 3 = SAC; 0=SPA,1=pNHA,2=NHA) | polygon | live | `point ∈ polygon` | SITECODE, SITE_NAME, COUNTY |
| 17 | Archaeological assessment | NMS SMR **Zone of Notification** | `services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services/SMRZoneOpenData/FeatureServer/0` | polygon | live | `point ∈ zone` | ZONE_ID |
| 17 | (monument identity / context) | NMS SMR points | `services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services/SMROpenData/FeatureServer/0` | point | live | nearest / within-zone | ENTITY_ID, MONUMENT_CLASS, TOWNLAND, ZONE_ID_1 |
| 21 | Flood Risk Assessment + Justification Test | OPW **NIFM** river flood extents (national) | `data.gov.ie/dataset/nifm-river-flood-extents-current-scenario` (SHP; verified S3 `catalogue.floodinfo.opw/nifm/nifm_ext_f_m.zip`); depth GeoTIFF `…/nifm-river-flood-depth-current-scenario` | polygon | located | `point ∈ flood extent` | scenario · ⚠️ CC-BY-NC-ND |
| 21 | (statutory Flood Zone A/B — the actual trigger) | OPW CFRAM via floodinfo.ie + CFRAM AFA/UoM boundaries | **No public ArcGIS FeatureServer** (old item withdrawn) → live raster = `floodinfo.ie/geoserver/wms` (GetCapabilities); full Zone A/B vector via OPW FRM **data-request form** (flood_data@opw.ie, ~20 working days); `data.gov.ie/dataset/cfram-areas-for-further-assessment-afa-boundaries` (SHP). ⚠️ **CC-BY-NC-ND (non-commercial)** | polygon | WMS-live / vector-on-request | `point ∈ Zone A/B` | flood_zone |
| 21 | (pluvial / groundwater flood) | GSI surface-water + historic groundwater flood maps | `data.gov.ie/dataset/20152016-surface-water-flood-map-120000-ireland-roi-itm`; `…/historic-groundwater-flood-map-120000-ireland-roi-itm` (ESRI REST/SHP) | polygon | located | `point ∈ flood map` | — |
| 16 | Architectural Heritage Assessment | **NIAH national dataset** (architectural heritage; basis of RPS) | `data.gov.ie/dataset/national-inventory-of-architectural-heritage-niah-national-dataset` (CSV + ESRI REST) | point | located (national) | nearest / `point ∈ near` | reg_no |
| 16 | (statutory protected structures / ACA) | RPS + ACA boundaries — **per-council** (no national) | per-council on data.gov.ie (e.g. Galway City `…/record-of-protected-structures2`); `localgov.ie` | point/polygon | located (per-LA) | `point ∈ / near RPS·ACA` | RPS_REF |
| 10 | Visual Impact Assessment | Landscape sensitivity (LCA Class 2/3) — **per-council** (no national) | per-council on data.gov.ie (Monaghan `…/landscape-character-types`, Cork, Galway/Heritage Council) | polygon | located (per-LA) | `point ∈ class` | lca_class |
| 6,7,8 | Road Safety Audit / RSIA / TTA; national/regional roads | TII National Road Network + RMO Regional Road Network | `data.gov.ie/dataset/national-road-network-2013` (KML); `…/regional-road` (ArcGIS) | line | located (national) | nearest road class | road_class |
| 6,7,8 | sightlines/access on **local** roads (the gap TII leaves) | **OSM via Overpass API** ✅ verified | `overpass-api.de/api/interpreter` · `overpass-turbo.eu` | line | live (probed 2026-06-14) | nearest `highway=` | highway, maxspeed, name |
| 25 | Site Suitability (septic) — groundwater risk | **GSI Groundwater Vulnerability 1:40,000** (+ Karst, Protection Scheme) | `data.gov.ie/dataset/groundwater-vulnerability-140000-ireland-roi-itm` (ESRI REST/SHP/WMS); `…/groundwater-karst-data-ireland-roini-itm`; `…/groundwater-protection-scheme-reports-50k-ireland-roi-itm` | polygon | located (national) | `point ∈ vulnerability/karst class` | vuln_class |
| 25 | (subsoil permeability / soils) | **GSI Subsoil Permeability 1:40,000** + EPA National Soils/Subsoils | `data.gov.ie/dataset/groundwater-subsoil-permeability-140000-ireland-roi-itm`; EPA `…/national-soils-map`, `…/national-subsoils-map` | polygon | located (national) | `point ∈ soil unit` | soil_type |
| 25 | (sewered vs unsewered — the #25 antecedent) | EPA Urban Waste Water Treatment **Agglomeration Boundaries** | `data.gov.ie/dataset/urban-waste-water-treatment-agglomeration-boundaries` (WMS) | polygon | located (national) | `point ∉ agglomeration` → on-site WW required | agg_name |
| — (material contravention §10.6) | zoning conflict | Development-Plan Zoning composite (national, MyPlan GZT) + per-council zonings | `services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/GZT_Current_Plan/FeatureServer/0` (live; +`GZT_Expired_Plan`) — **legacy `…zoning-ireland1` + `maps.housing.gov.ie/MyPlan` now 404** | polygon | live | `land-use ≠ zoned use` | zone_obj |

## Attribute-trigger sources (no spatial join — derive from the feed)

| axiom_# | obligation | derive from |
|---|---|---|
| 4, 18 | Landscaping + native-planting; energy-rating cert | always-on (every application) — no logic |
| 1,2,3,5,9,19,20,22,23,24,26 | scale-gated reports (design statement, phasing, daylight, mobility, climate, SuDS, waste, noise, restoration) | feed attributes: ApplicationType, NumResidentialUnits, FloorArea, AreaofSite |
| 15 | EIA / EIAR | Schedule 5 thresholds vs development type/scale (EPA) |

## Reason text (the *why* — only public when appealed)

| source | locator | notes |
|---|---|---|
| eplanning.ie council portal | `LinkAppDetails` e.g. `eplanning.ie/{LA}/AppFileRefDetails/{ref}/0` | **NO reason text** — outcome + "Number of Conditions: N" only (verified raw HTML + JS render, City & County). Reasons are in the unlinked CE Order PDF. |
| ACP Board order PDF | `pleanala.ie/anbordpleanala/media/abp/cases/orders/{ddd}/d{case}.pdf` (`{ddd}`=first 3 digits) | scanned image → OCR; carries the operative reasons/conditions |
| ACP inspector report PDF | `…/cases/reports/{ddd}/r{case}.pdf` | usually text-extractable; also restates the planning authority's refusal reasons |
| ACP case page | `pleanala.ie/en-ie/case/{case}` | metadata + PDF links; join `AppealRefNumber` 6-digit core → ACP `ABPCASEID` |

## Supplementary / cross-feature sources (not a direct axiom, but enrich the feature)

| Source | Endpoint / locator | Use |
|---|---|---|
| Tailte Éireann **National Land Cover 2018** (HVD) | `data.gov.ie/dataset/high-value-dataset-national-land-cover-2018` (ESRI REST) | greenfield vs built context per site |
| Tailte Éireann **Buildings / Building Groups** (HVD) | `…/high-value-dataset-buildings` | built density / "consolidate existing development" reasoning |
| Tailte Éireann **CSO Small Areas 2022** boundaries | `…/cso-small-areas-national-statistical-boundaries-2022-ungeneralised1` | join to census/deprivation; rural-vs-urban context |
| NMS thematic monument sets | `data.gov.ie/dataset/national-monuments-service-monuments-to-visit` (+ stone-circles, sweathouses, Community Monuments Fund) | supplement the SMR (authoritative remains SMR/SMRZone) |
| **Derelict Sites / Vacant Sites registers** (per-council) | e.g. `…/derelict-sites-register-sdcc1`, `…/vacant-sites-register` | land-activation / vacancy cross-feature with housing supply |
| DAFM **Anonymous LPIS** (agricultural parcels) | `data.gov.ie/dataset/anonymous-lpis-and-n-p-for-2020` (SHP) | one-off-rural / agricultural-land context (CPO thread) |
| Tree Preservation Orders (per-council) | e.g. `…/tree-preservation-orders` (Cork City) | minor constraint axiom |
| CSO PxStat planning stats / DHLGH Residential Commencement Notices | `…/residential-commencement-notices`; CSO BHQ/BHA | reconciliation + build-out tracking (see scoping §15.4) |

## Overpass (OSM) — verified useful for the local-road axiom (2026-06-14)
Probed live around the Menlo/Castlegar study point (53.3003, −9.0597):
- **78 road segments within 800 m**, classified (`tertiary/residential/unclassified/track/service/footway`,
  +1 `proposed` — likely the GCRR/N59 corridor), with `maxspeed` tags (50/30/20) and street names. This is
  exactly the **local-road network the TII national-roads data omits** — the layer where one-off
  sightline/access refusals (#6-8) actually bite ("local road, 50 kmph, substandard alignment").
- 12 `historic` features within 1500 m (archaeological_site, ruins, mass_rock, lime_kiln, castle) — useful
  *context* only; **NMS SMR/SMRZone stays authoritative** (OSM is crowd-sourced, variable completeness).
- **Limits:** `maxspeed` is sparse (~18/78 tagged); OSM gives road **centrelines, not visibility splays** —
  it locates/classifies the road, it does not compute sightlines (those still need a site survey).
- **Call:** raw QL as POST body, headers `Content-Type: text/plain` + a descriptive `User-Agent`
  (default UA → HTTP 406). Mirror fallback `overpass.kumi.systems/api/interpreter`.

## Ingest notes
- Pull ArcGIS layers with **pyesridump** (OID-chunking + `geometryPrecision=7`), then the bounds/validity
  quarantine gate (`make_valid` + Ireland-envelope assert) — see `PLANNING_PERMISSION_SCOPING.md` §13.6/§13.8
  and memory `reference_geometry_validation_sources`.
- **Granularity rule:** for monuments, the constraint is the **zone polygon**, not point distance. Same
  pattern as SAC (§13): proximity ≠ obligation; containment = obligation.
- **National vs per-council:** environment (SAC/SPA, OPW/GSI flood, GSI vulnerability), archaeology (SMR),
  NIAH, land cover, national+regional roads = **single national pull**. RPS, ACA, landscape sensitivity,
  derelict/vacant, TPOs, per-council zonings = **31-LA assembly** (same pattern as the rulebook).
- **Format caveat:** prefer the DHLGH/GSI/Tailte/OPW/EPA copies (ESRI REST or SHP) over Heritage Council
  mirrors, which are often `ARCSDE CONNECTION` (not directly fetchable).
