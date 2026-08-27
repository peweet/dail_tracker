# Tailte ArcGIS public GET routes versus MapGenie shells

**Verified 2026-08-26 against Tailte's official ArcGIS REST catalogue and service metadata.**

## The durable rule

An ArcGIS UUID is an identity pointer, not evidence that its data may be queried, copied or
redistributed. Admit a source only when the exact publisher item, exact service/layer and exact
licence all agree:

1. Read GET /sharing/rest/content/items/{item_id}?f=pjson and require the returned ID,
   access: public, publisher-owned service URL and an item-specific reuse grant.
2. Use GET .../{item_id}/data?f=pjson only to map a Web Map's declared source graph. It is
   configuration/provenance, never permission to acquire the referenced service.
3. Read the service and layer metadata, require Query capability, bind CRS/geometry/schema/
   object identity/count, then use count-only or ID-only GETs before a receipted acquisition.
4. Re-read the contracts before atomic promotion. A changed item URL, access, licence or schema
   fails closed.

The direct public Tailte source host is normally
services-eu1.arcgis.com/FH5XCsx8rYXqnjF5/... . A public ArcGIS Online shell under
utility.arcgis.com/usrsvcs/servers/{uuid} is a separate, often gated proxy.

## Confirmed MapGenie boundary

- The official open-data group is 529cd10af4d84273b5e3c3b2d2324618; it returned 178 public
  Feature Service items and must be paginated with start=1 and start=101.
- The 22 public Web Maps whose title contains MapGenie declared 18 distinct utility-service
  UUIDs. One root ?f=json GET per declared service returned structured ArcGIS error.code=403 for
  all 18, and each item carried restrictive terms. This includes historic maps, imagery,
  Discovery, Premium, Core and Standard configurations.
- Further utility Feature Service shells for Core (2aa621..., 4bdf21...), National Land Cover
  (66cb...), office locations and National Townlands also returned 403. A root GET can have HTTP
  success while its JSON body contains that ArcGIS denial; inspect the body, not only HTTP status.
- One separate Standard MapServer UUID (7f5e90aee2dc45c0a9a59552bbf7d0a4) exposed
  Map,TilesOnly,Tilemap, not Query, and has no reuse grant. A visible tile service is neither a
  vector enrichment route nor permission to retain or publish tiles.
- The open National Land Cover item 3f3cec799db545a684f1361ab433ab5e is a different direct,
  CC-BY Feature Service from the restricted 66cb... MapGenie proxy. Shared titles do not prove
  equivalence.

Do not retry layer URLs, guess tokens, scrape tiles, or fuzz protected UUIDs. Classify these as
gated/procurement_only unless Tailte grants a separately authorised licence.

## What is genuinely recoverable by public GET

The following unselected sources passed the exact-item, access: public, CC-BY 4.0, direct service
and Query,Extract checks on 2026-08-26. This is an admission candidate list, not a decision to
ingest or serve them.

| Candidate | Item / layer | Count | Potential bounded use | Limit |
| --- | --- | ---: | --- | --- |
| 2026 Dail constituencies | eb549055f0c6425181a3fe9f21b2d02c, layer 0 | 362 pieces | Separate public Dail Tracker constituency lookup/map label | Electoral geography, not a planning boundary or an ED substitute |
| Motorway access exit points | 0626e6b32a3d4eca9d0a07ef2f2c2804, layer 0 | 42 points | Named navigation-context comparator | Does not prove legal access, entrance availability or route suitability |
| Islands | 00f0f87e6efe476bbd55c3c4353393a8, layer 0 | 62 polygons | Low-priority labelled geographic context | Generalised cartography, not site analytics |
| Tailte Sites | f477136c339d4c29beba594f50f64434, layer 5 | 360 polygons | At most a cartographic comparator | Antiquity/Ruin names/forms do not replace NMS SMR or notification zones |

The remaining High Value Dataset siblings are technically queryable but do not currently add a
named product fact: Building Groups (838), Locales (51,195), Rail Network Segments (8,430), Rail
Points (8,261), Water Points (663,676), Water Single Stream (372,225), Way GDF2 (336,126) and Way
Points (311,821) expose geometry plus GUID only. Do not acquire them merely because GET works.

## Reusable acquisition gate

Before any new capture, write down the reviewer question and prove all of: exact item identity,
public access, item-specific licence, direct publisher service match, meaningful retained fields,
capability/schema/CRS/count/identity contract, bounded acquisition route, refresh signal,
attribution and non-inference wording. Prefer a local receipted snapshot over a live report-path
query; never send a private site boundary from the browser to a public ArcGIS service.

Related evidence: planning/product/doc/TAILTE_GIS_PRODUCT_AND_COST_AUDIT_2026_08_25.md,
planning/product/doc/TAILTE_GIS_AUDIT_ADDENDUM_2026_08_25.md, and the diffable
planning/product/doc/TAILTE_ARCGIS_GROUP_INVENTORY_2026_08_25.csv.
