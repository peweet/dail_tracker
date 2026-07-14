# Planning corpus — seed registry

A **tracking catalogue** of every dataset the planning-accountability app would eventually need.
This is a *seed registry only* — it records what to ingest, where it lives, how it joins, and its
status. **No ingestion happens here.** (Same pattern as `data/_meta/procurement_publishers/`.)

File: [`planning_corpus_seed.csv`](planning_corpus_seed.csv) — one row per dataset.

## Columns
`id · dataset · category · role · provider · locator · format · spatial · join_key · licence · privacy · status · priority · notes`

## Status legend
- **ingested** — in the pipeline (silver/gold)
- **extracted** — captured to repo but not pipelined (e.g. the DM-standards markdown)
- **partial** — some of it done
- **used_live** — queried on demand, not persisted (e.g. NPWS designation joins)
- **scoped** — endpoint/schema known, not built
- **not_started** — identified, not yet scoped
- **blocked** — no bulk access (per-file scrape / scanned / paywalled)
- **reference** — read for rules/process, not a row-level dataset
- **audited** — one-off analysis done

## How this feeds the data model (the "how to graph it" question)
The registry is deliberately shaped to become the corpus graph later. The two columns that matter:

- **`category` ≈ node type.** The recurring categories are the entity classes of the eventual model:
  `applications` (the hub), `appeals`, `dm_standards` (rules), `zoning`, `contributions`,
  `designation` (SAC/SPA/flood/ACA/RPS/landscape), `aggregate_stats`, `influence`, `reference`.
- **`join_key` ≈ edge.** Two join mechanics recur: **administrative** (`PlanningAuthority` → that
  council's rules/zoning/contributions) and **spatial** (`point-in-polygon` of the application point
  into each designation layer). Those two joins are the whole graph: an application is `governed_by`
  its authority's rules and `intersects` the designation layers, which `trigger` assessments.

That is exactly the **obligation-set** validated in scoping-doc §20 (Menlo case): Site →(in)→ Authority
→(applies)→ DM-Standards; Site →(intersects)→ Designation →(triggers)→ Assessment →(requires)→
report/condition. So the graph is not freeform — it's a small fixed schema hung off two join keys.
Detailed model design is deferred (a separate step); this registry is the input to it.

## Priority
1 = foundation (applications + rules + the SAC/SPA designation join — already largely in hand).
2 = high-value next (zoning, flood, ACA/RPS, contributions, CSO/BCMS reconciliation).
3 = enrichment / blocked / cross-reference.

Source context: `doc/archive/PLANNING_PERMISSION_SCOPING.md` (§8 ingest, §10–13 sources, §15 resource register,
§19 contributions, §20 worked case study). Per-council rules: `planning_rules/`. Criteria: `planning_rules/_criteria_map/`.
Companion (axiom→source view): `planning_rules/SOURCE_REGISTRY.md`.

## Enriched 2026-06-14 — verified links + polling/supersession
Every row now carries a **verified `resolved_url`** plus `update_cadence`, `supersession_risk`, `poll_method`,
`last_known_update`. Poll mechanics by source type:
- **ArcGIS REST** (most): `GET <layer>?f=json` → `editingInfo.lastEditDate`. **Exception — NPWS** has editor
  tracking OFF, so use the service `Last-Modified` header / `ETag` + a `returnCountOnly` delta.
- **CKAN** (BCMS, per-council RPS/ACA): `package_show` → `metadata_modified` (+ resource-UUID stability).
- **CSO PxStat**: `ReadMetadata/{CODE}` → `updated` timestamp.
- **Per-council PDFs/plans** (PC05, PC08): no feed — poll the council page / `consult.<council>.ie` portal.

### Endpoint corrections found (feed back to SOURCE_REGISTRY.md)
- **PC07 zoning composite**: the registry's `…/development-plan-land-use-zoning-ireland1` and
  `maps.housing.gov.ie/.../MyPlan` are **DEAD (404/NXDOMAIN)**. Live endpoint is
  `services.arcgis.com/NzlPQPKn5QF9v2US/.../GZT_Current_Plan/FeatureServer/0` (+ `GZT_Expired_Plan`).
- **PC14 OPW flood**: there is **no public ArcGIS FeatureServer** (the old item was withdrawn). Use the
  S3 NIFM shapefile zip + `floodinfo.ie/geoserver/wms`. Licence is **CC-BY-NC-ND (non-commercial)** — check fit.
- **PC13 NPWS derogations**: data.gov.ie **dated** dataset slugs get superseded (404) — poll by *publisher*, not slug.

### Highest supersession risk (most monitoring effort)
`PC05 per-council plans` ≈ `PC08 contributions` (31 sources each, staggered) > `PC07 zoning`, `PC15 ACA`,
`PC17 landscape` (per-council, plan-period-named services churn) > `PC09–11 NPWS` (S.I. amendments) >
`PC21 national framework` (few sources, but overrides every LA plan).
