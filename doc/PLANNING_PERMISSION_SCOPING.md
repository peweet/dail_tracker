# Planning Permission — Source Scoping & Probe Findings

**Date:** 2026-06-13
**Status:** SCOPING / PROBE ONLY — no ETL built. Live endpoints sampled, schemas captured.
**Author context:** Feasibility pass for ingesting Irish planning-permission data into Dáil Tracker.

> **PROBE UPDATE 2026-06-13 (sampled real rows):**
> - **PII DE-RISKED:** applicant identity fields are **0 / 495,632 populated** — DHLGH already nulls
>   `ApplicantForename` / `ApplicantSurname` / `ApplicantAddress` in the published feed. The columns
>   exist in the schema but are empty. Residual privacy = **site/development addresses** only
>   (incl. Eircodes), not applicant names.
> - **DATA-QUALITY bugs seen in 5-row sample:** a `DecisionDate` of **2075**-12-03 (year typo for
>   2025); `FloorArea` placeholder noise (values of `0` and `1`); two inconsistent one-off flags
>   (`OneOffHouse` = Y/blank vs `OneOffKPI` = Yes/No).
> - `LinkAppDetails` gives a **direct per-application council eplanning.ie URL**
>   (e.g. `http://www.eplanning.ie/CarlowCC/AppFileRefDetails/2560152/0`).

---

## TL;DR

Irish planning data is **one of the more ingestible sources scoped to date**. Two national
ArcGIS REST FeatureServers cover the whole country with open/clean licensing and **no scraping
required**. The real work is **(a) decision-field normalisation across 31 councils** and
**(b) a privacy decision on applicant PII** — not access.

| Source | Verdict | Records | Licence | Privacy |
|---|---|---|---|---|
| **National Planning Applications** (DHLGH) | ✅ INGESTIBLE NOW | 495,632 | Open (data.gov.ie) | ⚠️ applicant name+address |
| **An Coimisiún Pleanála appeals** (ACP) | ✅ INGESTIBLE NOW | 26,079 | CC-BY 4.0 | ⚠️ dev address (no applicant name) |
| GeoHive National Planning Hub | ✅ aggregator (same backend) | — | mixed (incl. CC-0) | — |
| ACP pre-2016 archive (1995–2015) | 🟡 SCRAPE | — | — | — |
| Unified 31-council portal (localgov.ie) | ❌ UNCONFIRMED | — | — | — |
| CSO planning stats / BCMS / Tailte | ⚪ LEADS (not probed) | — | — | low (aggregate) |

---

## 1. National Planning Applications — DHLGH `IrishPlanningApplications`  ✅ INGESTIBLE NOW

**Endpoint:** `https://services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/IrishPlanningApplications/FeatureServer`
**data.gov.ie:** https://data.gov.ie/dataset/national-planning-applications
**Owner portal:** http://data-housinggovie.opendata.arcgis.com/

- **Capabilities:** `Query,Extract` · `maxRecordCount: 2000` · export to CSV/GeoJSON/Shapefile/GeoPackage/Excel.
- **Layers:** `0 = Planning Application Points` (point) · `1 = Planning Application Sites` (polygon).
- **Total records (Layer 0, probed 2026-06-13):** **495,632**, all **31 local authorities**.
- **Freshness:** `max(ETL_DATE) = 2026-06-09` (4 days before probe). `max(ReceivedDate) = 2026-06-21`
  → **future-dated rows present** (data quirk — councils pre-stamp; guard on this).

### Schema (Layer 0, 37 fields) — captured live

```
OBJECTID (OID)            PlanningAuthority         ApplicationNumber
DevelopmentDescription    DevelopmentAddress        DevelopmentPostcode
ITMEasting / ITMNorthing  ApplicationStatus         ApplicationType
ApplicantForename *PII*    ApplicantSurname *PII*    ApplicantAddress *PII*
Decision                  LandUseCode               AreaofSite
NumResidentialUnits       OneOffHouse *priv*        FloorArea
ReceivedDate              WithdrawnDate             DecisionDate
DecisionDueDate           GrantDate                 ExpiryDate
AppealRefNumber           AppealStatus              AppealDecision
AppealDecisionDate        AppealSubmittedDate       FIRequestDate / FIRecDate
LinkAppDetails            OneOffKPI *priv*          ETL_DATE
SiteId                    ORIG_FID
```

### ⚠️ TRAP — `Decision` is free-text, NOT a clean enum

The research pass claimed `Decision` = {Granted/Refused/Invalid/Withdrawn}. **FALSE.** It is
per-council free-text with heavy variant spellings. Live top values:

```
CONDITIONAL                          235,215
Grant Permission                      70,762
N/A                                   56,060
(blank)                               36,495
REFUSED                               35,051
REFUSE PERMISSION                     11,928
Granted (Conditional)                 11,304
APPLICATION DECLARED INVA(LID)         5,791
GRANT PERMISSION FOR RETENTION         5,473
UNCONDITIONAL                          4,888
```

→ **A `decision_normalised` mapping (council-aware) is the core ETL deliverable.** Same applies
to `ApplicationType` (PERMISSION 404k / RETENTION 55k / OUTLINE 11k / EXTENSION OF DURATION 8k,
plus mixed-case duplicates "Permission for Retention" vs "RETENTION").

### Privacy gate — LOWER than expected (probe-verified)

The applicant-identity columns `ApplicantForename` / `ApplicantSurname` / `ApplicantAddress`
are **0 / 495,632 populated** — DHLGH already strips them in the published feed. So the major
GDPR concern (naming private applicants) **does not arise** from this source as published.

Residual consideration = **`DevelopmentAddress`** (site addresses, incl. one-off private homes)
and `OneOffHouse`/`OneOffKPI` flags. These are statutory-register facts about a site, not a named
person. Still recommend a light review before surfacing one-off-house addresses; the
personal-insolvency quarantine pattern (`feedback_personal_insolvency_privacy`) is available if a
stricter line is wanted, but full quarantine is likely unnecessary here.

---

## 2. An Coimisiún Pleanála (appeals) — `Cases_2016_Onwards`  ✅ INGESTIBLE NOW

**Endpoint:** `https://services-eu1.arcgis.com/o56BSnENmD5mYs3j/arcgis/rest/services/Cases_2016_Onwards/FeatureServer`
**Layer id:** **3** (named `Cases`; NOT layer 0 — gotcha) · polygon geometry · `maxRecordCount: 2000`
**data.gov.ie:** https://data.gov.ie/dataset/cases-2016-onwards-received-or-decided-by-an-bord-pleanala-on-or-after-1st-january-2016
**Licence:** **Creative Commons Attribution 4.0** (commercial reuse OK, attribution only).
**PSI page:** https://www.pleanala.ie/en-ie/re-use-of-public-sector-information-(psi)

- **Total records (probed 2026-06-13):** **26,079** · temporal: received/decided on/after 2016-01-01.
- **Freshness:** sampled `UPDATED_ON = 2026-06-05` → confirms declared **weekly** cadence (live, despite
  data.gov.ie metadata timestamp being frozen at 2023-03-15 — ignore that stale flag).

### Schema (Layer 3, 13 fields)

```
OBJECTID  ABPCASEID  DEVDESC  DEVADDRESS  LODGEDON  DECISION  DECIDED_ON
LINKABPWEB  PLANINGATY  CATEGORY  UPDATED_ON  Shape__Area  Shape__Length
```

- `LINKABPWEB` → per-case web page, e.g. `https://www.pleanala.ie/en-ie/case/501070`.
- `PLANINGATY` = originating planning authority (council).
- **CATEGORY breakdown:** Appeals 23,238 · Referrals 1,097 · Housing 549 · Substitute Consent 284 ·
  Appeals-LRD 250 · SID 217 · Appropriate Assessment 200 · Protected structure 83.
- **`DECISION` ALSO free-text** (same normalisation trap): "Grant permission with conditions" /
  "Grant Permissions with Conditions" / "Grant Perm. w Conditions" are one outcome; "Refuse
  Permission" 5,915; many invalid-reason variants.
- **Privacy:** `DEVADDRESS` includes private home addresses (sampled: a Sutton attic→home-office),
  but **no applicant-name field** → lighter PII profile than the applications feed.

### Join key — the two feeds genuinely link

Applications feed carries `AppealRefNumber` populated on **21,074** rows → joinable to ACP
`ABPCASEID` (+ `PLANINGATY`/council). Enables an application → decision → appeal → outcome chain.

### Pre-2016 archive — 🟡 SCRAPE

`https://archive.pleanala.ie` (1995–2015) has no documented bulk feed. Only needed for deep history.

---

## 3. GeoHive National Planning Geospatial Data Hub — aggregator  ✅

https://planning.geohive.ie/ (DHLGH + Tailte Éireann). Surfaces the same Irish Planning
Applications layers plus tiered datasets:
- **Tier 1 Planning Permissions** — https://data.gov.ie/dataset/tier1-planning-permissions1 (**CC-0**).
- **Tier 2A Planning Applications** — geohive dataset `5b23b7553b8f4e1893039524667f1479_18`.

⚠️ **Open question:** how Tier 1 / Tier 2A relate to & dedupe against the main 495k feed — must
avoid double-counting before any gold layer. Same ArcGIS-REST ingestion path.

---

## 4. NOT confirmed / refuted

- **Unified 31-council portal** (`planning.localgov.ie`, LGMA, shared Idox/ePlan): the
  "single national lodge/search across all councils" hypothesis was **refuted/unconfirmed** in
  research. **But likely moot** — the DHLGH national feed already aggregates all 31, so we don't
  need per-council scraping for the core data.
- **MyPlan.ie** "national 10-year coverage" claim refuted; MyPlan is an HTML viewer over the same
  ArcGIS backend — ingest the REST layer, not the viewer.

---

## 5. Leads named-but-not-probed (next pass)

| Lead | URL | Why |
|---|---|---|
| **CSO planning permissions** | https://www.cso.ie/en/statistics/buildingandconstruction/planningpermissions/ + PxStat `csodata` | Clean aggregate stats, **no PII** — fast safe headline-trends win |
| **BCMS commencement notices** | https://data.nbco.gov.ie/ · https://data.gov.ie/dataset/bcms-commencement-notices-2020 | Building-start signal, complements permissions |
| **Tailte Éireann boundaries/zoning** | https://tailte.ie/services/mapgenie/ | Spatial join to constituencies (ties to constituency_population_boundaries) |

---

## 6. Ingestion notes / gotchas (for when ETL is built)

- All "APIs" are **Esri/ArcGIS REST FeatureServer** (proprietary GeoServices), **not OGC WFS**.
  Paginate via `resultOffset`/`resultRecordCount` (cap 2000) or use `Extract`/`createReplica` for
  a one-shot bulk pull.
- Dates are **epoch milliseconds UTC** in JSON.
- **Coordinates (probe-corrected):** the `ITMEasting`/`ITMNorthing` *attribute columns are EMPTY*
  (0/495,632) — DEAD columns. Precise location lives ONLY in the **point geometry**, which is
  **100% populated** (every row geocoded to an address/townland-level point, verified inside
  Ireland's bbox). Pull with `returnGeometry=true&outSR=4326` → store `lon`/`lat`. ACP geometry EPSG:2157.
- **Eircode:** native `DevelopmentPostcode` is sparse (2,523/495,632). Deriving the rest needs the
  **licensed Eircode ECAD** database (paid; Loqate/Ideal Postcodes/Autoaddress) — NOT free and NOT
  needed (coordinates already cover mapping/alerts/constituency rollup). Keep native Eircodes, leave rest null.
- **Naming:** An Bord Pleanála → **An Coimisiún Pleanála** (18 June 2025, Planning & Development
  Act 2024). Legacy "An Bord Pleanala" persists in dataset titles/URLs — same body.
- **Privacy-first build:** quarantine `ApplicantForename/Surname/Address` before gold; keep
  development-level analytics fields. One-off-house addresses are the sensitive tail.

## 7. Suggested next steps (not yet actioned)

1. Decide the privacy line (drop applicant identity at ingest? keep council+dev only?).
2. Pull a bounded sample (e.g. one council, one year) to silver and build the
   `decision_normalised` + `application_type_normalised` crosswalks.
3. Resolve Tier1/Tier2A vs main-feed dedup before any counts.
4. Probe CSO PxStat for the no-PII aggregate companion series.

---

## 8. Phase 0 Build Plan — National Applications → silver parquet  (LOCKED 2026-06-13)

**Scope:** ONE deliverable. `pipeline_sandbox/planning_applications_ingest.py` →
`planning_applications_silver.parquet`. Sandbox only. NO appeals join, NO gold, NO page, NO alerts.

**Why this one:** cleanest source (no applicant PII at source), no scraping, full national coverage,
and it's the prerequisite every later feature (map, alerts, constituency rollup) depends on.

### Steps

1. **Geometry pull (ArcGIS REST, Layer 0) — use `pyesridump`, NOT a hand-rolled loop**
   *(DECIDED 2026-06-13, verified §13.8: a hand-rolled `resultOffset` loop is what truncated the
   giant polygon → corrupted the SAC data. The dedicated puller gives OID-chunking, dedup,
   `geometryPrecision=7`, and retries for free, and pulled the pathological layer 433/433 clean.)*
   - `EsriDumper(layer_url, outSR=4326, timeout=180, max_page_size=…)` — **tune `timeout`/`max_page_size`
     down for any layer with giant polygons** (the 30 s default timed out on the 488k-vertex SAC).
     *(Applications Layer 0 is points — far lighter than the SAC polygons; default settings likely fine,
     but keep the tuning lever.)*
   - **`returnGeometry=true&outSR=4326`** → persist `lon`/`lat` per row. *(The `ITMEasting`/`ITMNorthing`
     columns are empty — coordinates come ONLY via geometry.)*
   - Drop dead columns `ITMEasting`/`ITMNorthing` and the empty `Applicant*` PII columns.
   - Smoke-test one council first (`PlanningAuthority='Carlow County Council'`) before the full sweep.
   - **Run every geometry through the §13.6/§13.8 quarantine gate** (null → bounds-escape →
     vertex-overflow → OGC-validity), reconcile `Σ(pulled) == returnCountOnly`. The puller prevents
     truncation-corruption; the gate catches anything that still slips through. `esridump` is a
     light pure-Python dep (`requests`+`click`, no GDAL) — add via `uv` if promoted from sandbox.

2. **Decision normalisation crosswalk** *(the real work)*
   - Hand-curated `data/_meta/planning_decision_map.csv`: raw free-text →
     `{Granted, Granted-Conditional, Refused, Invalid, Withdrawn, Other}`. Keep gitignore negation rule.
   - Same for `application_type`. Cover top ~95% by row count; unmatched → `Other`, raw preserved in
     `decision_raw` (no-inference — never guess an outcome).

3. **DQ guards** (Polars)
   - Future/garbage dates: `DecisionDate` year > current+1 (the 2075 bug) → null + `dq_flag`.
   - `FloorArea ∈ {0,1}`, `AreaofSite=0` → missing, not real.
   - Reconcile two one-off flags → single bool `is_one_off_house` (`OneOffHouse='Y'` OR `OneOffKPI='Yes'`).
   - **Geo guard:** assert `lon`/`lat` inside Ireland bbox (−11,51 .. −5,56) — catch geocode failures.
   - Row-count assertion (~495k ± tolerance) so a truncated pull fails loudly.

4. **Write** via `services/parquet_io.save_parquet` (atomic tmp→replace, zstd L3, statistics=True).
   Standalone logging → `logs/standalone/`.

### Settled decisions
- **PII:** none to handle — applicant fields empty at source. Keep `DevelopmentAddress` as a site fact.
- **Eircode:** keep native (2,523), do NOT derive (licensed/paid, redundant vs coordinates).
- **Location:** lon/lat from geometry = 100% coverage, address/townland precision. Site polygons
  (Layer 1) deferred unless exact parcel extent is needed.
- **Engine:** Polars. **Sandbox only** until validated.

### Definition of done
- `planning_applications_silver.parquet`, ~495k rows.
- `decision_normalised` ≥95% non-`Other`; zero future dates; lon/lat 100% in-bbox.
- Row-count assertion passing; 10-row sample eyeballed against council eplanning URLs.

### ✅ BUILT & VALIDATED 2026-06-14
`pipeline_sandbox/planning_applications_ingest.py` → `pipeline_sandbox/_planning_output/planning_applications_silver.parquet`.
- **495,632 rows = exact live count** (row-count assertion passed). 38 cols. Smoke-tested on Carlow first (`--authority`).
- **`decision_normalised`** (keyword normaliser, raw preserved, no-inference; 99.5% mapped, `Other`=2,266/0.46%):
  Granted-Conditional 249,012 · Undecided/None (N/A/blank) 92,555 · Granted 86,902 · Refused 50,119 ·
  Invalid 11,875 · Withdrawn 2,903 · Other 2,266. **National refusal rate ≈ 10.1% of decided.**
- **`application_type_normalised`** (99.3% mapped): Permission 407,516 · Retention 64,227 · Outline 10,806 ·
  Extension of Duration 9,669 · Other 3,413.
- **one-off houses 77,516 (15.6%)** · **geo_in_bounds 495,632/495,632 (100%)**.
- Date hygiene: out-of-range dates nulled + `dq_flags` audit (33 garbage dates: ExpiryDate 15, GrantDate 11,
  DecisionDueDate 4, DecisionDate 3, + FI/appeal). PII (`Applicant*`) and dead ITM columns dropped.
- TRAP fixed in build: `UNCONDITIONAL` must map to `Granted` (not `Granted-Conditional`) — it literally
  contains "CONDITIONAL", so the UNCONDITION branch must precede the CONDITION branch.
- **Still sandbox** (`pipeline_sandbox/_planning_output/`); promotion to a registered gold view is the next step.

---

## 9. Open questions — RESOLVED (research pass 2, 2026-06-13)

### Angle 1 — Tier 1 / Tier 2A dedup → **RESOLVED. DO NOT MERGE into the register.**
Tier1/Tier2A are **Dublin Housing Supply Coordination Task Force** monitoring tiers (origin: Action 2
of *Construction 2020*, May 2014), NOT subsets of the 495k application register. Verified 3-0:
- **Tier 1** = sites with a final grant of permission implementable immediately (incl. commenced-not-
  completed; excl. completed; excl. phasing). **Tier 2A** = application lodged / within 4-week appeal
  window / under ACP appeal.
- **Geography: 4 Dublin LAs only** (DCC, DLR, SDCC, Fingal) — not national.
- **Methodology filter:** only schemes **≥10 units**; **excludes** social housing, Part 8, and
  student/shared-living. → does NOT reconcile 1:1 with the register.
- **Grain:** site/unit level (unit-delivery counts + build status), row-per-SITE not per-application.
- **Endpoint:** quarterly ArcGIS services, e.g. `…/Q1_2024/FeatureServer/28` (Tier1 layer, ~482 records);
  carries `Tier`, `Planning_Reference`, `ABP_Reference`, `Units_Permitted`, `Units_Under_Construction`,
  `Units_Completed`, `Activity_On_Site`. data.gov.ie open licence.
- **VERDICT:** parallel, Dublin-only, large-scheme **monitoring** series. Not a dedup target, but
  **complementary** — it adds build-status / unit-delivery the register lacks. Optional later
  enrichment for Dublin housing pipeline; NOT part of Phase 0.

### Angle 4 — Appeals ↔ applications join → **RESOLVED (direct-probe verified).**
- `ABPCASEID` = bare 6-digit; `AppealRefNumber` has 3 formats (`ABP-301245-18`, `301741-18`, `300219`).
- **Join recipe:** regex first 6-digit run from `AppealRefNumber` → match `ABPCASEID`, validate on
  planning authority. Probe-confirmed: `301245` → ACP case Carlow / "Refuse Permission".
- **Corroboration:** the Tier1 monitoring layer independently stores BOTH `Planning_Reference` and
  `ABP_Reference` — confirms (council-ref + ABP-ref) is the canonical join pair.

### Angle 2 — CSO PxStat planning stats → **PARTIAL (found, not vote-verified — budget cut-off).**
Aggregate, no-PII companion series. Unverified-but-primary-sourced facts:
- CSO "Planning Permissions", **quarterly**, **BHQ-series** PxStat tables (BHQ03/04/13/14/15/16/17),
  product code "PP" at `data.cso.ie`.
- **PxStat JSON-stat API**; formats JSON-stat / PX / XLSX / CSV; `csodata` R package hits `data.cso.ie`.
- Dimensions: planning region/county, development type (multi-dev houses, **one-off houses**, apartments),
  SHD vs non-SHD, units + floor area; ~Q1 2022 → current.
- Licence **CC-BY 4.0**.
- **Use:** independent reconciliation check against counts derived from the 495k register. RE-VERIFY when budget returns.

### Angle 3 — Citizen planning-alerts product → **PARTIAL (found, not vote-verified — budget cut-off).**
- **PlanningAlerts.org.au:** `applications.json?lat&lng&radius(m)` = the core "near you" geometry;
  does NOT crawl — aggregates community scrapers on morph.io (`@planningalerts-scrapers`); each authority
  record has name/short-name/**contact email** for sending comments; ATDIS standard (`openaustralia/atdis`).
- **PlanIt (UK):** `/api/applics/{fmt}` (csv/geojson/georss/json/tsv); circular (lat/lng/krad or pcode),
  bbox, polygon. Fields: `name,uid,altid,area_name,area_id,start_date,address,description,location,link,last_scraped`.
- **mySociety PlanningAlerts** died from per-council **scraper-maintenance** burden — the exact pain
  Ireland's single national feed removes.
- **Feature-set gap analysis:** a "developments near you" feature needs geo-point + radius, address,
  description, decision/status, dates, source link — the Irish feed **already supplies ALL of these**
  (lon/lat 100%, address, description, decision, dates, `LinkAppDetails`). Only missing piece is a
  comment/objection workflow, which is council-side, not data.
- **NB:** a `planningalerts.ie` already exists (industry/alerts site) — check before any build.

---

## 10. Planning-process & regulatory map (wide-net scoping, 2026-06-13)

> Inline web-research pass (NOT 3-vote verified — the workflow hit the account spend cap, so this
> was done with direct searches). Purpose: understand the *regulatory reasons* behind decisions and
> map where each datum lives. **Legend per item:** [S]=structured field in a feed · [PDF]=lives as a
> PDF attachment in the council file · [TXT]=free-text in DevelopmentDescription/conditions ·
> [EXT]=separate external register.

### 10.1 The eplanning file — where the deep data actually lives
- Most LAs lodge/search via the **Local Government Online Planning portal** (`planning.localgov.ie`),
  covering **all councils except Cork City**; older files vary (e.g. Limerick online from 2008).
  Cork County runs its own `planning.corkcoco.ie/ePlan`.
- A council file holds: application form, **site notice** + newspaper notice, drawings, the technical
  reports (below), **planner's report**, conditions, and the **Chief Executive's Order** (the decision).
  All public except confidential info.
- **Ingestibility wall:** the *summary* (ref, address, decision, dates) is in the national 495k feed [S];
  the *documents themselves* are **[PDF] per-council**, no bulk API, session/portal-bound — this is the
  hard scrape the national feed deliberately spares us. **Verdict: deep document content = BLOCKED/scrape-only.**

### 10.2 Environmental assessment layer
- **Appropriate Assessment (AA)** — Habitats Directive Art 6(3), transposed by the **Birds & Natural
  Habitats Regs 2011**. *Screening* runs on **every** application; if a likely significant effect on a
  **Natura 2000 site (SAC/SPA)** can't be excluded, a **Natura Impact Statement (NIS)** is required and
  permission can't be granted unless site integrity is assured. ([NPWS](https://www.npws.ie/protected-sites/guidance-appropriate-assessment-planning-authorities), [OPR PN01](https://publications.opr.ie/)) → outcome [PDF]+[TXT]; trigger is the **SAC/SPA boundary** [EXT, ingestible — NPWS spatial].
- **EIA / EIAR** — thresholds in **Schedule 5, Parts 1 & 2** of the Planning & Development Regs 2001.
  Part 1 over-threshold = mandatory EIA; Part 2 + **sub-threshold** = screening determination. Gov is
  currently **reviewing screening thresholds** (2026). ([EPA](https://www.epa.ie/our-services/monitoring--assessment/assessment/environmental-impact-assessment/)) → [PDF]; whether EIA applied is sometimes [TXT].
- **Ecological / bat survey** — **lesser horseshoe bat** is **Annex II** (all bats Annex IV) Habitats
  Directive. Disturbing a roost needs an **NPWS derogation licence** (Regs 51/54 of the 2011 Regs).
  **NPWS publishes the derogation licence applications + bat survey PDFs** on npws.ie → **[EXT, scrapable]**.
- **Hydrology / flood** — handled via OPW flood data + council assessment [PDF].

### 10.3 Site-technical layer (one-off rural house)
- **Sight lines / visibility splays** — expressed as **'x' and 'y' distances** + forward visibility
  (stopping sight distance); assessed against **TII** Rural Road Link Design & junction-geometry
  standards + DMURS. A **Traffic & Transport Assessment (TTA)** is required above category thresholds.
  Common refusal reason: inadequate sightlines / hedgerow removal to achieve them. ([TII DN-GEO-03031](https://cdn.tii.ie/publications/DN-GEO-03031-10.pdf)) → [PDF]/[TXT].
- **Site suitability assessment** (septic/on-site wastewater) = desk study + **trial hole** (1.2–2.1m,
  checks subsoil + water table + rock) + **percolation test** (EPA Code of Practice) → system rec.
  Mandatory wherever a septic tank/on-site treatment is proposed. ([Clare CoCo](https://clarecoco.ie/services/planning/applications/before-you-apply/site-suitability-assessment/), [EPA CoP]) → [PDF].

### 10.4 Process & actors
- **Further Information (RFI)** vs **Significant FI** — SFI re-triggers public notice/re-advertising;
  miss the deadline → application **deemed withdrawn**. → dates partly [S] (FIRequestDate/FIRecDate in feed).
- **Retention permission** — after-the-fact permission for unauthorised works; refusal → demolition. → [S] ApplicationType.
- **Conditions** — grants are typically **CONDITIONAL** (the 235k-row modal value we found) → [TXT]/[PDF].
- **Planning consultant** — packages the application, reads local policy, marshals specialist reports
  (architect, ecologist, traffic engineer), drafts the planning rationale and FI responses. → not data, an actor.
- **Objections / observations** — anyone may submit; **€20 fee**, within **5 weeks** of lodgement;
  paying makes you a "participant" with appeal standing. → **[EXT] submitter identities are in the
  council file, NOT the national feed** (privacy-relevant).

### 10.5 Heritage & conservation constraints
- **Protected Structure (RPS)** — any work *materially affecting character* needs permission, incl.
  interior, **curtilage**, and **boundary treatments** (this is the "can't touch the old wall" rule).
  Applications must include an **Architectural Heritage Impact Assessment (AHIA)**. → [PDF]; RPS list [EXT].
- **Architectural Conservation Area (ACA)** — external works (roofs, windows, **boundary walls**, new
  features) need permission. → [EXT] ACA boundaries (council GIS).
- **No compensation** for refusal/conditions on architectural/heritage/archaeological grounds. ([DLR](https://www.dlrcoco.ie/conservation/protected-structures), [DCC plan](https://www.dublincity.ie/dublin-city-development-plan-2022-2028/written-statement/chapter-11-built-heritage-and-archaeology/115-policies-and-objectives))

### 10.6 The legal machinery of a decision
- Statutory test = **"proper planning and sustainable development."**
- **Material contravention** of the Development Plan can only be granted if **≥¾ (two-thirds+) of all
  councillors vote** for it after public notice (Sec 34(6)). → the formal lever for elected-member influence.
- **Section 28 Ministerial Guidelines / Specific Planning Policy Requirements** override the Development
  Plan where they conflict — now being **replaced by National Planning Statements** (Planning & Dev Act 2024).
- **Statutory consultees / prescribed bodies** (TII, Uisce Éireann, OPW, **An Taisce**, NPWS, EPA, MARA,
  Arts Council…) must be notified for relevant cases; their submissions shape conditions/refusals. → [PDF].

### 10.7 Political influence & integrity (the opaque part)
- **Where influence is *lawful & documentable*:** the **Sec 34 material-contravention vote** (named
  councillor votes) and the **Sec 140** power (councillors directing the executive). Section 28/National
  Planning Statements = ministerial steer.
- **Integrity history:** the **Mahon/Flood Tribunal** (1997–2012) found **endemic corruption** in
  Dublin-area rezoning/planning payments to politicians. ([Mahon](https://en.wikipedia.org/wiki/Mahon_Tribunal), [An Taisce](https://www.antaisce.org/news/independent-planning-regulator-required-guard-against-endemic-corruption-mahon-found-heart))
- **2022 An Bord Pleanála scandal:** the **Remy Farrell SC report** into deputy chair **Paul Hyde**'s
  undisclosed conflicts (incl. a refusal near a site he part-owned) → referred to the DPP; triggered the
  reorganisation into **An Coimisiún Pleanála** (Planning Commissioners + separated governance) and JR reforms. ([JURIST](https://www.jurist.org/news/2022/08/irish-authorities-refer-report-on-corruption-in-planning-body-to-prosecutors-for-review/))
- **Project tie-in:** this is exactly where Dáil Tracker's **lobbying.ie + TD declared-interests** data
  could surface *documentable* influence (councillor votes, lobbying returns on planning matters,
  landowner interests) — vs the merely alleged. Strong cross-feature opportunity; no-inference caveat applies.

### 10.8 Policy: one-off houses vs compact growth
- **2005 Sustainable Rural Housing Guidelines** + **National Planning Framework (2018)**: "urban-generated"
  rural housing (no local ties) steered to settlements; **compact growth** disfavours dispersed one-offs.
  This is why ~half the feed's records are **one-off houses** flagged & often conditioned/refused.
- **Change incoming:** Government confirms a **new National Planning Statement on Rural Housing in 2026**
  to *support* one-off housing for those with local connections. ([RTÉ](https://www.rte.ie/brainstorm/2026/0421/1569148-ireland-one-off-rural-housing-planning-laws/), [NPF](https://www.npf.ie/nss/publications/reports-guidelines/)) → watch for 2026 regs change.

### 10.9 NEW ingestible datasets surfaced (bonus)
- **Development Plan (Land-Use) Zoning, Ireland** — a **standardised national composite** of all LA
  zoning from individual Development Plans, on **data.gov.ie**. → enables "what's the zoning here / is this
  a material contravention" context. **[EXT, ingestible geospatial] — strong future enrichment.**
- **NPWS derogation licence register** (bat/species) — published applications + survey PDFs on npws.ie. **[EXT, scrapable].**
- **SAC/SPA + ACA + RPS boundaries** — council/NPWS GIS layers, joinable to application points for
  "this application sits in a designated area" flags. **[EXT, ingestible].**

### 10.10 Bottom line on depth
The national 495k feed gives the **skeleton** (who/where/what/decision/dates) [S]. Everything that
explains *why* a decision went the way it did — AA/EIA, ecology, sightlines, percolation, heritage,
consultee + third-party submissions — lives as **[PDF] inside per-council files** (BLOCKED for bulk)
or as **[EXT] designation layers** (mostly ingestible). The *influence* story is partly **[S/EXT]**
(councillor material-contravention votes, lobbying returns, interests) and partly un-documentable.
**Realistic depth ceiling without scraping council PDFs: the skeleton + designation-layer context +
the influence cross-reference — which is already a substantial, novel civic product.**

---

## 11. Chain of custody — the full planning lifecycle (deep dive, 2026-06-13)

> Inline web-research (not 3-vote verified). The **statutory clocks** here are the spine — every clock
> is a date field we can derive or already have, which makes the *process* itself measurable.

### 11.1 Stage-by-stage with the statutory clocks  ⏱️
| # | Stage | Clock / rule | In our data? |
|---|-------|--------------|--------------|
| 0 | **Pre-planning consultation** (Sec 247) | meeting capped **4 weeks**; ~6–8 wks typical | ❌ not public |
| 1 | **Public notice** — newspaper + **site notice** erected | app must be lodged **within 2 weeks** of newspaper notice; site notice must stay up **5 weeks** | partial (ReceivedDate) |
| 2 | **Validation** | invalid → **returned, fee refunded** | ApplicationStatus [S] |
| 3 | **Public participation** — observations/objections | **€20 fee**, within **5 weeks** of lodgement; paying = "participant" w/ appeal standing | ❌ submitters not in feed |
| 4 | **Assessment + (optional) RFI** | RFI **stops the clock**; no reply in **6 months** → **deemed withdrawn** | FIRequestDate/FIRecDate [S] |
| 5 | **Decision** = **Chief Executive's Order** (signed) | target **8 weeks** from valid lodgement; no decision + no RFI → **default permission** | DecisionDate/Decision [S] |
| 6 | **Appeal window** to ACP | **4 weeks** from CE order | derivable |
| 7 | **Grant issues** (if no appeal) | after the 4-week window | GrantDate [S] |
| 8 | **Commencement / compliance** | conditions, Part V (social housing on ≥10 units) | ❌ (BCMS commencement notices = separate feed) |

**Key insight:** stages 1–7 are all **date-stamped** and most are already in the 495k feed — so we can
measure **decision latency, RFI rate, withdrawal rate, default-permission incidence, and appeal rate
per council** without any PDF scraping. That's a strong analytics spine on its own.

### 11.2 The An Coimisiún Pleanála appeal chain of custody
1. **Lodge appeal** — **4 weeks** from the CE order date (strict).
2. **Standing** — third-party appeal requires you **made an observation** (paid the €20) at stage 3;
   otherwise only an **adjoining landowner** can seek **"leave to appeal"** (Art 37(6)).
3. **Validity check** → acknowledgement letter.
4. **Comment period** — participants **4 weeks** from the Board's letter.
5. **Inspector** — site visit, photos, **report + recommendation**.
6. **Board/Commission decision** — statutory objective **18 weeks** (inclusive of any request periods).
7. **de novo** — ACP **re-decides the whole application**: can grant what the council refused, **refuse
   what the council granted**, or rewrite conditions. (This is why a third-party appeal puts the *entire*
   permission at risk.)
- **ACCOUNTABILITY HOTSPOT:** where the **Board rejects the inspector's recommendation it must state
  reasons.** Board-vs-inspector divergence is the single most scrutinised signal in Irish planning
  (it was central to the 2022 Paul Hyde controversy). The inspector report + board direction are
  **PDFs on pleanala.ie per case** (reachable via the `LINKABPWEB` field we already have) → **[scrapable,
  high-value]** — a "how often does the Board overrule its own inspector, by member" metric would be novel.
- **SID (Strategic Infrastructure):** applied **directly to the Board**, oral hearings common, and the
  Board decision **cannot be appealed** — single-stage. Only route to challenge = judicial review.

### 11.3 Common pitfalls (the process failure modes)
- **Invalidation** (returned + fee refunded): lodged >2 weeks after newspaper notice; permission **type
  not shown on site notice**; **protected structure not declared** in both notices; missing fee/plans;
  site notice removed/not up the full 5 weeks. ([Clare CoCo common errors](https://www.clarecoco.ie/planning-and-building/make-planning-application/what-include-your-planning-application/common-errors-lead-invalid-applications))
- **Deemed withdrawn** — no FI response within **6 months**.
- **de novo risk** — appealing (or being appealed) re-opens the whole decision; a granted permission can
  be lost on a third-party appeal.
- **"Unauthorised but immune"** — the **7-year rule** (Sec 157(4)) bars *enforcement* after ~7 yrs+119
  days, but does **NOT** legalise the development; condition breaches re land use are **never** time-barred.
  ([Law Society](https://www.lawsociety.ie/gazette/in-depth/unauthorised-but-immune/))

### 11.4 City Council vs County Council
- **Same legal status**; both are planning authorities. The pre-2014 system had **88** (29 county, 5 city,
  49 **town** councils — town councils **abolished** by the Local Government Reform Act 2014); now **31 LAs**.
- **Cities:** Dublin City, Cork City, Galway City standalone; **Limerick** and **Waterford** are merged
  **City-and-County**. Counties cover broad **rural** areas; cities are dense **urban**.
- **The Development Plan is the core instrument**, and **adopting it is a *reserved function* of the
  elected councillors** — i.e. the political layer sets the rulebook each authority's officials then apply.
- **Why outcomes differ by authority (and why our `Decision` field is so messy):** each LA has its **own
  Development Plan + Local Area Plans/SDZs + house style**, so the same proposal can pass in one county and
  fail next door, and each council labels decisions differently. City plans emphasise density/LAPs/SDZs;
  county plans carry the **one-off rural housing** policy that drives most refusals.
- **Municipal districts** exist in all LAs **except** the Dublin authorities, Cork City and Galway City.

### 11.5 Mitigation strategies (how applicants de-risk)
- **Section 247 pre-planning consultation** (capped 4 wks) to agree the *principle* + design parameters.
- Submit **design alternatives + policy rationale**; pre-empt likely objections in the design.
- **Track the statutory clocks**; answer RFI well inside 6 months.
- For ≥10 units, plan **Part V** social-housing provision up front.
- Engage specialist consultants (ecologist for AA/bat, traffic engineer for sightlines, conservation
  architect for RPS) **before** lodgement so the reports are in the first submission, not forced by RFI.

### 11.6 Planning rules — the gating concepts
- **Exempted development** — minor works (small extensions etc.) need **no permission**; thresholds
  (size/height) in **Schedule 2** of the 2001 Regs; exceed them and exemption falls away.
- **Section 5 declaration** — formal binding ruling on whether something **is/isn't exempt** (**€80,
  4-week** decision). → these are themselves a **separate decision dataset** some councils publish.
- **Default permission** — the 8-week clock's teeth (decision-by-inaction).
- **Enforcement** — warning letter → **enforcement notice** → prosecution; bounded by the 7-year rule above.

### 11.7 What this unlocks for the project
The **process is measurable from dates alone** (§11.1) — decision latency, RFI/withdrawal/default rates,
appeal rates, and **Board-overturns-inspector** (§11.2, via `LINKABPWEB` PDFs) are all derivable without
the per-council document wall. Combined with the **influence cross-reference** (§10.7) and **designation
layers** (§10.9), the realistic build is a *planning-accountability* product, not just an application map.

---

## 12. RFI mechanics + the discoverable decision rules + Galway probe (2026-06-13)

### 12.1 The Further Information (RFI) process — requirements & internal rules
- **Statutory basis:** Section 33 of the Planning & Development Act 2000 + **Article 33** of the
  Planning & Development Regs 2001; significant-FI public-notice format is set by **Article 35**.
- **Two tiers:**
  - **Regular FI** = clarifications / filling gaps; does **not** fundamentally change the proposal.
  - **Significant Further Information (SFI)** = info that could **significantly change the perceived
    impact** (environment / infrastructure / community) → **triggers fresh public notice** (new site +
    newspaper notice, re-opening the objection window).
- **Clock effects:** an RFI **stops the 8-week decision clock** (this is what prevents a default
  permission issuing); no response within **6 months** → application **deemed withdrawn**.
- **What can be requested:** "any further information" incl. estate/interest in land and environmental
  effects — broad discretion, but an authority **cannot** use FI to fish indefinitely; one FI request is
  standard, a second needs the response to the first to have raised new issues.
- **Why it matters analytically:** the RFI is the single clearest signal of *what the planner was not
  satisfied with* — i.e. the de-facto deciding issues. FIRequestDate/FIRecDate are **[S] in our feed**;
  the *content* of the request is **[PDF]** in the council file (scrape-only).

### 12.2 The deciding rules ARE discoverable — they live in the Development Plan
The "internal rules" that decide applications are **not secret** — they are the **Development Plan's
"Development Management Standards" chapter** (typically Ch.14/15), adopted by councillors as a reserved
function. Planners apply these quantitative standards and **cite them in RFIs and refusal reasons**.
Granting *against* them = a **material contravention** (needs the ¾ councillor vote, §10.6).

### 12.3 PROBE — Galway County Development Plan 2022-2028, Chapter 15  ✅ SCRAPABLE
Fetched `consult.galway.ie/.../chapter-15-development-management-standards`. **Verdict: clean HTML,
machine-scrapable** — numbered sections (15.x), named "DM Standard N", embedded tables, `<h2/h3/h4>`
hierarchy, plus a downloadable PDF. Extracted concrete, quantitative deciding rules, e.g.:

| Rule | Galway standard |
|------|-----------------|
| Residential density | 35–50 (med/high), 15–35 (low/med), 5–12 (low) **dwellings/ha**; default 35 DPH |
| Car parking | **1.5** spaces (1–3 bed) / **2** spaces (4+ bed) per dwelling; retail/office/school scales |
| Separation | **22 m** back-to-back & opposing first-floor windows; **2 m** to side boundary |
| Sight distance ('Y') | 215 m @100 km/h … 35 m @30 km/h (Table 15.3); 'x' = 2.4 m from carriageway |
| Building setbacks | 90 m motorway / 35 m national / 25 m regional / 15 m local road |
| **Rural one-off site** | **min 2,000 m²** (on-site wastewater); +10 m² per 1 m² of house >200 m² |
| **Linear development** | **5+ houses per 250 m** of road frontage = "linear", generally refused |
| Site coverage / plot ratio | industrial ≤75% / 1:2; commercial 75/60/50% by height |
| EV / bicycle / bin standards | 20% EV-equipped; bike 0.8×1.8 m, 1/bedspace; 3×240 L bins/10 apts |

### 12.4 The big finding — a shared, cross-council platform
The development plans sit on a **common "Online Consultation Portal" platform** — `consult.galway.ie`,
`consult.fingal.ie`, `consult.dublincity.ie`, etc. all share the same per-chapter HTML structure. So the
**Development Management Standards are scrapable in a *uniform* way across many of the 31 LAs.**
- **NEW DATASET CANDIDATE:** a machine-readable **cross-council DM-standards comparison** (car-parking
  ratios, density, rural site minimums, setbacks per authority). **Nobody publishes this** — it would let
  you say "Galway demands 2,000 m² for a rural house; Mayo demands X" and tie decisions to the actual rule
  applied. **[SCRAPABLE, novel, high-value].**
- **Caveat:** not every LA is on the shared portal (Cork City runs its own), and "adopted" vs "draft" +
  **variations** must be version-tracked (Galway already has Adopted Variation No.1).

### 12.5 How it all fits
RFI/refusal **reasons** [PDF] → cite **DM Standards** [SCRAPABLE, §12.3] → set in the **Development Plan**
[adopted by councillors, §10.6]. So even though the *reason text* is PDF-locked per application, the
**rulebook it cites is fully ingestible** — meaning we can contextualise any decision against the
quantitative standard that governed it, and flag material contraventions, without scraping the file PDFs.

---

## 13. GALWAY CASE STUDY — live data join, application points × SAC designation (2026-06-13)

First end-to-end join of two open feeds, proving the obligation-set concept reflects in real outcomes.

> ⚠️ **CORRECTED 2026-06-13 (root cause confirmed).** The original INSIDE-SAC figures (24.8%, 319
> points) are **WRONG — caused by a single corrupt polygon.** The 472k-vertex SAC (Lough Corrib) was
> truncated/mis-serialised when the first session saved its GeoJSON, producing a garbage longitude
> coordinate (**−8,992,822,267,307**). `make_valid()` silently laundered that into a thin latitude
> band (53.12–53.28°) that spuriously "contained" ~140 extra Galway points — inflating inside
> 179→317 and diluting the rate 35.9%→24.6%. The verified result is **35.9% inside (52/145) vs 15.2%
> elsewhere**, confirmed by **three independent methods** (bulk wide-bbox pull; per-SAC individual
> full-precision fetch; cached set with the corrupt polygon removed → 173) — all ≈179 inside, ~35.9%.
> The effect is **real and stronger** than first reported. Full validation log + the defensive
> bbox-sanity assert in §13.6.

### Data pulled (all live, no scraping) — ✅ all re-confirmed
- **Galway County applications:** 18,406 (Galway City: 3,355) from the IrishPlanningApplications feed,
  **100% point geometry (18,406/18,406)**. **8,085 are one-off houses (44%).** Decision mix:
  CONDITIONAL 13,271 / REFUSED 2,478 / N/A 2,320 / UNCONDITIONAL 337 -> **county refusal rate 15.4%**
  (2,478/16,086 decided) — *all exact on re-run.*
- **SAC polygons:** from the **NPWS Designated Areas FeatureServer**
  (`services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/.../NPWSDesignatedAreas/FeatureServer`, **layer 3 =
  "Special Area of Conservation"** confirmed; 0=SPA, 1=pNHA, 2=NHA; fields SITECODE, SITE_NAME, COUNTY).
  **185** SACs intersect a generous Galway bbox (the count is **bbox-sensitive** — the original "119"
  used a tighter extent). **⚠️ 25 of the 185 polygons are geometrically invalid** (self-intersections;
  one SAC has 472,977 vertices) — `make_valid()` before the join is mandatory or points inside those
  25 are silently misclassified.
- **Join:** shapely exact point-in-polygon (STRtree + `make_valid` + `covers()`), 18,406 unique points,
  zero missing geometry.

### Result — refusal rate is far higher INSIDE a SAC (decided apps) — ✅ verified
| Band | Refused / Decided | Refusal rate |
|------|-------------------|--------------|
| **INSIDE a SAC** (exact, repaired geom) | **52 / 145** | **35.9%** ✅ |
| everything else | 2,426 / 15,941 | **15.2%** ✅ |
| ALL Galway County | 2,478 / 16,086 | 15.4% ✅ |
| ~~within ~500 m~~ | ~~799 / 4,512~~ | ~~17.7%~~ ⚠️ **UNVERIFIED** |

⚠️ The middle **~500 m near-band is NOT reproduced** — it needs metric reprojection (pyproj/ITM, not
installed locally) to compute a true 500 m buffer; the first-pass degree-approximation is unreliable
and is the likely source of the original contaminated "inside" figure. Treat the near-band as TODO,
not fact. **~180 applications fall strictly inside a SAC** (not 319).

### What this proves
A real **dose-response**: applications **inside a SAC are refused at ~2.4× the baseline** (35.9% vs
15.2% elsewhere). The **AA obligation** (triggered by SAC proximity, checklist #12/#13) is visible in
the actual decision record — exactly the obligation-set hypothesis of §12. Built entirely from **two
open ArcGIS feeds + a local spatial join**; no per-council PDF needed.
- **Caveat (no-inference):** correlation, not causation — SAC areas are also wetter/scenic/constrained,
  so multiple refusal reasons co-occur. The product states the *association*, never imputes the reason.
- **Caveat (no hardcoded rates):** the exact percentage is method-fragile (polygon vintage, bbox,
  containment predicate all move it). The app must compute it **live from a version-stamped polygon
  set with exact containment** and publish the *direction*, never a frozen "35.9%".

### Pipeline validated
`IrishPlanningApplications (points)` -> `NPWS designation layers (polygons)` -> `make_valid` ->
`shapely exact containment` -> `obligation + outcome cross-tab`. The same pattern extends to **flood
zones (OPW), ACA/RPS, landscape sensitivity**. Reusable artifacts saved under `c:/tmp/`
(galway_points.geojson, galway_sac.geojson; validation scripts `c:/tmp/validate_*.py`).

### 13.6 Validation log — assumptions checked against live feeds (2026-06-13)
Every load-bearing assumption behind Phase 0 (§8) and the obligation-set model was re-probed live.
**14 of 15 validated exactly; 1 (the SAC headline rate) was corrected as above.**

| Assumption | Live result | Verdict |
|---|---|---|
| Total register ~495,632 | `returnCountOnly` = **495,632** | ✅ exact |
| 31 LAs; group counts sum to total | 31 authorities, Σ = 495,632 | ✅ |
| Galway Co. 18,406 / City 3,355 | exact both (groupBy stats) | ✅ |
| Point geometry 100% populated | 18,406/18,406 | ✅ |
| ITMEasting/Northing are dead columns | **0 / 495,632** populated (national) | ✅ dead |
| Eircode sparse | `DevelopmentPostcode` = **2,523 / 495,632** | ✅ exact |
| Dates = epoch-ms UTC | `1492992000000` → 2017-04-24 | ✅ |
| NPWS layer 3 = SAC (0/1/2 = SPA/pNHA/NHA) | exact | ✅ |
| maxRecordCount 2000; pagination; `Query,Extract` | confirmed (standardMax 16,000) | ✅ |
| Decision mix 13271/2478/2320/337 | exact | ✅ |
| **SAC inside refusal 24.8% / 319 pts** | **35.9% / ~180 pts** | ❌ **corrected** |

**Ingestion gotchas surfaced empirically (not from vendor docs):**
1. **`returnDistinctValues` is silently ignored** by this hosted FeatureServer (even with
   `orderByFields`) — it returns raw rows. Use `groupByFieldsForStatistics` + `outStatistics` (count)
   for domains/per-key counts; that query also gives the reconciliation Σ(groups) == total.
2. **Polygon geometry must be repaired** — 25/185 SAC polygons invalid; `make_valid()` is a required DQ
   gate before any spatial join, else `contains()/covers()` silently drops points.
3. **`make_valid()` LAUNDERS corrupt geometry — add a bbox-sanity assert.** This is what produced the
   bad 24.8% (see §13 banner): a truncated giant polygon (longitude −9e12) survived `make_valid()` as a
   plausible-looking band that swallowed ~140 spurious points. **Defense:** after repair, assert every
   polygon's `bounds` fall inside the data's expected envelope (Ireland: lon −11..−5, lat 51..56) — a
   polygon escaping it is corrupt → drop or refetch. Also **fetch pathological high-vertex geometries
   individually** (per-OBJECTID, full precision) so one giant feature can't truncate a bulk response
   (the 472k-vertex SAC produced a ~23 MB GeoJSON that truncated mid-stream → JSONDecodeError).
4. **Reconcile before trusting a pull** — assert `Σ(groupBy counts) == returnCountOnly` so a truncated
   paginated sweep fails loudly. `exceededTransferLimit` was `None` here (no truncation), but check it.
5. **Location is geometry-only** — `returnGeometry=true&outSR=4326`; ITM attribute columns are 0/495,632.
6. **Metric ops need a CRS** — distance/buffer (the near-band) needs ITM reprojection; degree
   approximations are unreliable. DuckDB-spatial's `ST_Transform` bundles PROJ (no pyproj install) but
   hit the **axis-order trap** (returned garbage without `always_xy=true`); pyproj is the safer route.

### 13.7 Spatial-tooling benchmark (this workload, this Windows box, 2026-06-13)
18,406 points × ~118–185 SAC polygons, point-in-polygon → count. Verdict: **geopandas is NOT the
best fit here.**
| Tool | Result | Speed | Memory | Dep weight |
|---|---|---|---|---|
| **shapely 2.x STRtree** | ✅ 179 | join **4.6 s** (+~11 s `make_valid` on giant) | trivial | shapely only (installed) |
| **DuckDB spatial** | ✅ 173 | **24 s** tuned (`threads=1`) | **OOM'd 5.5–12.5 GB** untuned | duckdb+ext (installed); no `ST_Subdivide` in 1.5.3 |
| **geopandas** | — | not run | — | **GDAL stack — not installed; a blocker (ENRICHMENTS.md)** |
- **Ingest/validation → shapely + requests** (what we used): lean, robust, fast; `sjoin` in geopandas
  is shapely+pandas under the hood, so geopandas adds GDAL weight for no engine gain.
- **Analytical layer → DuckDB spatial** *only* for its architectural fit (SQL, parquet-native, matches
  the views layer) — but it is **memory-fragile on high-vertex polygons** here; needs vertex management.
- **geopandas**: ecosystem default, but its only edge is interactive ergonomics; don't take the GDAL
  dependency into the pipeline on this box.

### 13.8 ArcGIS-dedicated puller libraries — USE ONE, don't hand-roll the loop
Surveyed the OSS projects built specifically for dumping ArcGIS REST services (2026-06-13). The
hand-rolled `requests` loop is what let the giant polygon truncate → corrupt the data; the dedicated
tools are engineered around exactly that failure. **Recommendation: pull the planning layer with
`pyesridump` (or the GDAL ESRIJSON driver), then run our bounds/validity quarantine gate on top.**

| Project | What it is | Relevance |
|---|---|---|
| **[pyesridump](https://github.com/openaddresses/pyesridump)** (openaddresses) | de-facto Python ArcGIS→GeoJSON dumper (powers OpenAddresses) | the gold-standard recipe — use this |
| esri-dump | Node.js sibling, same strategy | — |
| GDAL **ESRIJSON / FeatureService driver** (`gdal.org/.../esrijson.html`) | OGR built-in `ogr2ogr`/geopandas use | auto-pages (`FEATURE_SERVER_PAGING=YES`), **needs `orderByFields=OBJECTID`**; but **reads all into memory** (limit on huge layers) |
| [arcgis2geojson](https://github.com/chris48s/arcgis2geojson) (chris48s) | EsriJSON→GeoJSON converter | 11M-vertex ring issue [#3] → pre-simplify upstream |
| Esri/arcgis-python-api (`arcgis`) | Esri's official Python API | `FeatureLayer.query` chokes / returns `exceededTransferLimit` on large pulls [#2450] |
| koopjs/koop | Esri-sponsored GeoServices translation server | serving layer, not an ingest validator |

**pyesridump's battle-tested recipe** (read from `dumper.py`) — a 4-tier fallback because servers
cap/lie/refuse to paginate: (1) `resultOffset` pagination if `supportsPagination`; (2) **OBJECTID
min/max range chunking** (`WHERE oid > a AND oid <= b`); (3) full OID enumeration → fixed slices;
(4) **recursive geographic envelope splitting** when a tile returns too many. Plus the parts that map
straight onto our bug:
- **`geometryPrecision=7` on every query** → caps payload size; **would have prevented the 23 MB
  giant-polygon truncation** (hence the −9e12 corruption, which came from a truncated save).
- **dedup via a `saved` set of OBJECTIDs** → the exact guard against the double-count inflation (317→179).
- **never trusts `exceededTransferLimit`** — sidesteps the "server lies about counts" problem by
  chunking on OBJECTID instead of relying on one bulk response. Retries + backoff + rate-limit pauses.

**The honest limit:** NONE of these (pyesridump, GDAL, arcgis2geojson, Esri API) validate that
coordinates are *sane* — they assume server geometry is good and just transport it. The −9e12
"is this even on Earth" check is still ours to add (the open GeoPandas bounds gap, issue #1915). So:
**dedicated puller prevents the truncation that creates corruption; the bounds-assert catches corruption
if it slips through anyway — use both.** (Sources recorded in §15.7 + memory
`reference_geometry_validation_sources`.)

**✅ VERIFIED on the NPWS SAC layer (2026-06-13)** — `c:/tmp/test_pyesridump_sac.py`, `esridump 1.13.0`:
pyesridump pulled **433/433** features (reconciles with server count, **no truncation** where our
hand-rolled `f=geojson` pull threw JSONDecodeError mid-stream). The pathological giant (**488,665
vertices**) came through **intact with sane bounds** `(−7.59,52.18,−6.77,53.19)`; **0 polygons escape
the Ireland envelope** (vs the −9e12 corruption from the hand-saved file); coordinate precision capped
at **7 decimals** as advertised; the Galway join lands on the correct **179 inside / 35.9%**.
**Caveat — needs tuning:** the 30 s default read-timeout FAILED on the giant; required `timeout=180`
+ `max_page_size=25` (smaller pages isolate the giant into its own OID chunk). Pull took ~99 s.

---

## 14. NEXT STEPS (roadmap)

**Immediate (data, no spend-cap risk — direct probes):**
1. **Add OPW flood zones** to the Galway join → test the §12 Justification-Test trigger (does
   "inappropriate flood-zone use" correlate with refusal like SAC does?). Endpoint discovery = TODO (§15).
2. **Add ACA + RPS (protected structures)** designation layers → completes the heritage trigger (#16/#61).
3. **Layer the Development-Plan zoning composite** → enables a *material-contravention* flag (application
   land-use vs zoned use).
4. **Generalise the DM-standards scrape** to a 2nd/3rd council (Fingal, Mayo) off the shared `consult.*`
   portal → prove the cross-council standards dataset; start a normalised schema.

**Then (modelling):**
5. Build the **obligation-set reconstructor**: for each application point, compute which assessments it was
   obliged to carry (from designation joins) + which numeric standards governed it (from scraped plan).
6. Wire the **appeals join** (§ Angle 4 recipe: 6-digit core of `AppealRefNumber` → ACP `ABPCASEID`),
   and the **Board-vs-inspector overturn** scrape (via `LINKABPWEB` PDFs) for the accountability metric.
7. **CSO PxStat** aggregate series as an independent reconciliation check (re-verify §9 facts).

**Then (Phase 0 ingest, when greenlit):** the locked plan in §8 (national applications → silver parquet).

**Decisions still needed:** privacy line on site addresses (low, per §1); whether to ingest Dublin
Tier1/Tier2A as a complementary housing-pipeline layer (§9); national vs Galway-first build scope.

---

## 15. RESOURCE REGISTER (all sources, for later review)

> ⚙️ = **directly linked to internal planning DECISION logic** (the rules/triggers a planner applies).
> ✅ = probed live this session · 🔎 = surfaced but not yet probed · 📄 = saved local artifact.

### 15.1 ⚙️ Internal decision-logic sources (PRIORITY — the rulebook & its triggers)
| Resource | URL / locator | Notes |
|---|---|---|
| ⚙️✅ **Galway Co. DP Ch.15 — Development Management Standards** | `consult.galway.ie/en/consultation/draft-galway-county-development-plan-2022-2028/chapter/chapter-15-development-management-standards` | 71 DM Standards + 7 tables; HTML-scrapable |
| ⚙️📄 Galway Ch.15 full extract | `doc/galway_ch15_dm_standards_FULL.md` | verbatim numeric standards |
| ⚙️📄 Galway required-assessments checklist | `doc/galway_required_assessments_checklist.md` | 26 triggered reports + trigger conditions |
| ⚙️ **Shared council consultation portal** (DM standards across LAs) | `consult.galway.ie`, `consult.fingal.ie`, `consult.dublincity.ie`, `consult.kilkenny.ie`, `consult.wexfordcoco.ie`, `consult.waterfordcouncil.ie` | same platform → uniform scrape (Cork City excepted) |
| ⚙️ Development-Plan (Land-Use) Zoning composite | `data.gov.ie/dataset/development-plan-land-use-zoning-ireland1` | national zoning → material-contravention flag |
| ⚙️ OPR practice notes / guide | PN01 (AA screening) `publications.opr.ie`; PN03 (Conditions); `opr.ie/.../The-OPRs-Guide-to-the-Planning-Process.pdf` | how authorities decide |
| ⚙️ Statutory core | Act 2000 §33 (FI), §34 (decision test), §140, §247 (pre-planning), §157(4) (7-yr); Regs 2001 **Art 33** (FI), **Art 35** (SFI notice), **Sch 5** (EIA), **Sch 2** (exempted dev); **Act 2024** | `irishstatutebook.ie`, `revisedacts.lawreform.ie` |
| ⚙️ TII road/sightline standards | `cdn.tii.ie/publications/DN-GEO-03031` (rural road link), `DN-GEO-03043` (junctions); DMURS | sightline 'x/y' rules |
| ⚙️ Flood Risk Mgmt Guidelines 2009 + Circular PL2/2014 | gov.ie | Justification Test basis |
| ⚙️ EPA Code of Practice — on-site wastewater | `epa.ie` | percolation/trial-hole site suitability |
| ⚙️ Habitats Directive + Birds & Natural Habitats Regs 2011 | `npws.ie`, EUR-Lex | AA/NIS legal trigger |
| ⚙️ Sustainable Rural Housing Guidelines 2005 / NPF 2018 / **National Planning Statement on Rural Housing (2026, forthcoming)** | `npf.ie` | one-off-house policy |

### 15.2 ⚙️ Designation layers (decision TRIGGERS — spatial joins to application points)
| Layer | Endpoint | Status |
|---|---|---|
| ⚙️✅ **NPWS Designated Areas** (SPA 0 / pNHA 1 / NHA 2 / SAC 3) | `services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/arcgis/rest/services/NPWSDesignatedAreas/FeatureServer` | live; SITECODE/SITE_NAME/COUNTY/HA |
| ⚙️ NPWS (alt mirror / habitats) | `services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/.../NPWSDesignatedAreas/FeatureServer`; `webservices.npws.ie/arcgis/rest/services/NPWS/SscoSACHabitats/MapServer` | 🔎 |
| ⚙️ NPWS boundary downloads (shapefile, ITM) | `npws.ie/maps-and-data/designated-site-data/download-boundary-data` | 🔎 |
| ⚙️ NPWS species derogation licences (bat surveys) | `npws.ie` derogation pages | 🔎 scrapable PDFs |
| ⚙️ **OPW flood — national** (NIFM extents + CFRAM AFA/UoM) | `data.gov.ie/dataset/nifm-river-flood-extents-current-scenario` (SHP); `…/cfram-areas-for-further-assessment-afa-boundaries` (SHP); statutory **Zone A/B** still on `floodinfo.ie` (ArcGIS endpoint = TODO) | ✅ located 2026-06-14 (SHP); Zone A/B endpoint 🔎 |
| ⚙️ GSI flood — pluvial/groundwater | `data.gov.ie/dataset/20152016-surface-water-flood-map-120000-ireland-roi-itm`; `…/historic-groundwater-flood-map-120000-ireland-roi-itm` | ✅ located (ESRI REST/SHP) |
| ⚙️✅ **GSI site-suitability pack** (#25 septic) | Groundwater Vulnerability 1:40,000 `…/groundwater-vulnerability-140000-ireland-roi-itm`; Subsoil Permeability `…/groundwater-subsoil-permeability-140000-ireland-roi-itm`; Karst `…/groundwater-karst-data-ireland-roini-itm` | ✅ located 2026-06-14 (ESRI REST/SHP/WMS, national) |
| ⚙️ EPA — sewered extent + soils | `data.gov.ie/dataset/urban-waste-water-treatment-agglomeration-boundaries` (the unsewered=on-site-WW antecedent for #25); `…/national-soils-map`, `…/national-subsoils-map` | ✅ located |
| ⚙️ **NIAH national** (#16 heritage; basis of RPS) | `data.gov.ie/dataset/national-inventory-of-architectural-heritage-niah-national-dataset` (CSV + ESRI REST) | ✅ located 2026-06-14 (national) |
| ⚙️ ACA / Record of Protected Structures (per-council, no national) | per-council on data.gov.ie (e.g. Galway City `…/record-of-protected-structures2`); `localgov.ie/services/heritage-and-architectural-conservation` | 🔎 per-LA |
| ⚙️ Landscape sensitivity (#10 VIA, per-council, no national) | per-council on data.gov.ie (Monaghan `…/landscape-character-types`, Cork, Galway/Heritage Council) | 🔎 per-LA |
| ⚙️ Road network — national/regional | TII `data.gov.ie/dataset/national-road-network-2013` (KML); RMO `…/regional-road` (ArcGIS) | ✅ located (local roads → OSM, §15.2 below) |
| ⚙️ EPA SAC metadata | `gis.epa.ie/geonetwork/...d86f3a31...` | 🔎 |
| ⚙️✅ **NMS Sites & Monuments Record — SMR points** (archaeology trigger #17 / DM Std 62) | `services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services/SMROpenData/FeatureServer/0`; CSV/SHP bulk via `archaeology.ie` open data; `data.gov.ie/dataset/national-monuments-service-archaeological-survey-of-ireland` | **probed 2026-06-14**; point geom; fields `ENTITY_ID/MONUMENT_CLASS/TOWNLAND/ZONE_ID_1/WEBSITE_LINK`; `distance`+`units` buffer query works |
| ⚙️✅ **NMS SMR *Zone of Notification* (polygons)** — the **operative** archaeology constraint | `services-eu1.arcgis.com/HyjXgkV6KGMSF3jt/arcgis/rest/services/SMRZoneOpenData/FeatureServer/0` | **probed 2026-06-14**; `point ∈ zone` is the AA-equivalent trigger. **Proximity to a point alone is NOT a constraint** — the zone polygon is (verified: a site 173 m from a ringfort but outside its zone drew zero archaeology objection). |
| ⚙️✅ **An Coimisiún Pleanála decision PDFs** (the *only* public reason text — see §18) | Board order `pleanala.ie/anbordpleanala/media/abp/cases/orders/{ddd}/d{case}.pdf`; inspector report `…/reports/{ddd}/r{case}.pdf` (`{ddd}` = first 3 digits of case no.) | **probed 2026-06-14**; orders are scanned (OCR); inspector reports are usually text-extractable |
| ⚙️ **OSM via Overpass API** — road network (sightline/access triggers #6-8) + built/heritage context | `overpass-api.de/api/interpreter` · `overpass-turbo.eu` | 🔎 free, no key; roads/monuments/buildings; complements TII for *local* roads (where one-off refusals actually bite) |

### 15.3 ✅ Core application & appeal feeds
| Feed | Endpoint |
|---|---|
| ✅ National Planning Applications (points L0 / sites L1) | `services.arcgis.com/NzlPQPKn5QF9v2US/arcgis/rest/services/IrishPlanningApplications/FeatureServer` · `data.gov.ie/dataset/national-planning-applications` |
| ✅ ACP appeals Cases_2016_Onwards (layer **3**) | `services-eu1.arcgis.com/o56BSnENmD5mYs3j/arcgis/rest/services/Cases_2016_Onwards/FeatureServer` · CC-BY |
| ✅ ⚙️ ACP per-case page (inspector report + board direction PDFs) | `pleanala.ie/en-ie/case/{ABPCASEID}` (= `LINKABPWEB`) — **Board-vs-inspector accountability** |
| ACP case search / pre-2016 archive | `pleanala.ie/en-ie/case-search` · `archive.pleanala.ie` |
| Per-application council file | `LinkAppDetails` field (e.g. `eplanning.ie/{LA}/AppFileRefDetails/{ref}/0`) — ⚙️ PDF decision docs |
| GeoHive National Planning Hub | `planning.geohive.ie` |
| Tier1/Tier2A (Dublin housing-supply monitoring) | `data.gov.ie/dataset/tier1-planning-permissions1` · `…/Q1_2024/FeatureServer/28` |

### 15.4 Statistics & reconciliation
| Source | Locator |
|---|---|
| CSO Planning Permissions (PxStat, BHQ tables, CC-BY) | `cso.ie/en/statistics/buildingandconstruction/planningpermissions` · `data.cso.ie` product "PP" · `csodata` R pkg |
| BCMS commencement notices | `data.nbco.gov.ie` · `data.gov.ie/dataset/bcms-commencement-notices-2020` |

### 15.5 Influence / accountability (cross-ref to existing Dáil Tracker data)
lobbying.ie returns on planning · TD/councillor declared interests · **Mahon/Flood Tribunal** · **2022 ABP/Paul Hyde report** (`jurist.org`) · material-contravention votes (Act §34(6)) · §140 directions. ⚙️ where influence is documentable.

### 15.6 Comparator products (for product design)
PlanningAlerts.org.au (+ `/api/developer`, github `openaustralia/planningalerts`, ATDIS) · PlanIt `planit.org.uk` (+ `/api/applics/{fmt}`) · Symbium · BuildZoom · Shovels · **planningalerts.ie** (pre-existing — check before any build).

### 15.7 Geometry-validation references (for the §13.6 DQ gate)
Backing the two-axis gate: topology (REPAIRABLE via `make_valid`) vs out-of-bounds coordinates
(detect-only — the −9e12 trillion case). See memory `reference_geometry_validation_sources`.
- ArcGIS Pro **Check Geometry** `pro.arcgis.com/.../check-geometry.htm` — `SE_COORD_OUT_OF_BOUNDS` is a distinct error class.
- ArcGIS Pro **Repair Geometry** `pro.arcgis.com/.../repair-geometry.htm` — confirms out-of-bounds coords are **"will not be repaired"** (detect+quarantine only).
- PostGIS geometry quality (vertex-threshold + single-SRID + CHECK constraints) `dev.to/philip_mcclarence_2ef9475/...-4i0f`
- PostGIS validity workshop `postgis.net/workshops/postgis-intro/validity.html` · pgEdge geometry validation `docs.pgedge.com/postgis/development/data-management/geometry-validation/`

### 15.8 ArcGIS-dedicated puller libraries (use one instead of a hand-rolled loop — see §13.8)
- **pyesridump** `github.com/openaddresses/pyesridump` (Python; OID-chunking + envelope-split + `geometryPrecision=7` + OID dedup) · **esri-dump** `github.com/openaddresses/esri-dump` (Node sibling)
- **GDAL ESRIJSON/FeatureService driver** `gdal.org/en/stable/drivers/vector/esrijson.html` (needs `orderByFields=OBJECTID`; reads all to memory)
- **arcgis2geojson** `github.com/chris48s/arcgis2geojson` (converter; 11M-ring issue #3) · **Esri/arcgis-python-api** (`arcgis` pkg; large-query `exceededTransferLimit` #2450) · **koopjs/koop** `github.com/koopjs/koop` (serving, not validation)

---

## 16. CONCEPTUAL MODEL — the rulebook is the axioms (2026-06-13)

This is the framing that governs the whole build. **The Development Management Standards ARE the
axioms** from which everything else is derived. Read them here:
- Live: `consult.galway.ie/.../chapter-15-development-management-standards`
- Verbatim numeric axioms: `doc/galway_ch15_dm_standards_FULL.md` (71 DM Standards + 7 tables)
- Conditional (trigger) axioms: `doc/galway_required_assessments_checklist.md` (26 triggered assessments)

### Three kinds of axiom
1. **Numeric** — a value to compare (e.g. rural site ≥ 2,000 m²; 22 m window separation; 1.5 parking
   spaces/dwelling; sight 'Y' = 215 m @100 km/h). → needs a **measurement**.
2. **Spatial-conditional** — "IF site ∈ designated area THEN obligation". → needs a **layer join**.
3. **Always-on** — applies to every application (native-planting schedule, AA screening, energy cert).
   → no trigger logic.

### Why spatial joins are derived from the rulebook (not arbitrary)
Many axioms have a **spatial predicate** as their antecedent. A polygon join is simply *how you
evaluate that predicate* for a given application. Each join maps 1:1 to a named axiom:

| Axiom (from the rulebook) | Spatial predicate | Join that evaluates it |
|---|---|---|
| DM Std 51 / checklist #12-13: near a Natura 2000 site → Appropriate Assessment | `point ∈ SAC/SPA` | point × NPWS SAC/SPA layer |
| DM Std 69 / #21: use inappropriate to Flood Zone → Justification Test | `point ∈ Flood Zone A/B` | point × OPW flood layer |
| DM Std 59-61 / #16: works to Protected Structure → heritage assessment | `point ∈ RPS / ACA` | point × RPS/ACA layer |
| DM Std 47 / #10: sensitive landscape → Visual Impact Assessment | `point ∈ Landscape Class 2/3` | point × landscape-sensitivity |
| material contravention (§10.6) | `land-use ≠ zoned use` | point × zoning composite |

### Two distinct uses of the same join — do NOT conflate
- **(a) Evaluate an axiom** — per-site: "is THIS site in a SAC → does it trigger an AA?" This is the
  core operation (and what the decision-tree front-end calls at runtime).
- **(b) Measure a correlation** — bulk/historical: "do SAC sites get refused more?" (§13: **35.9% vs
  15.2%**, verified). A legitimate *side-validation* that the axioms bite in the real record, but a different
  purpose. The §13 SAC join already proved this; it need not be repeated per layer.

### The decision tree is a FRONT-END, not the substance
The "guide a user through the opaque process" decision tree (app idea) is just a friendly walk through
these same axioms: location → which designation layers it hits → development type → triggered
assessments (conditional axioms) + governing numeric standards → likely pitfalls. It demystifies; it
does not add logic. **All logic lives in the rulebook.**

### Implication for the build
Formalise the axiom set once (mark each DM Standard as numeric / spatial / always-on), attach each
spatial axiom to its designation layer, and both the analytics (correlation) and the app (decision
tree, per-site obligation lookup) fall out of the same source of truth. **Derive everything from the
rulebook.**

---

## 17. PRIOR ART — what other countries built ("Rules as Code") (2026-06-13)

Our "rulebook as axioms" approach is an established international movement: **Rules as Code (RaC)** —
translating planning/building rules into machine-consumable logic. Validates the approach AND confirms
the Irish gap (none of the below exist for Ireland). Maturity spectrum, closest-to-our-idea first:

### Tier 1 — Citizen self-triage "do I need permission?" (= our decision-tree idea, already built)
- 🇬🇧 **PlanX / Open Digital Planning** — `planx.uk` · `opendigitalplanning.org` — by **Open Systems Lab**
  (non-profit), backed by MHCLG Digital Planning. Councils build **flowcharts** ("flows") that let a
  homeowner self-triage: *"Find out if you need planning permission."* **Open-source, ~18+ councils**
  (Lambeth, Southwark, Camden, Buckinghamshire). Explicitly **NOT AI** — accountable flowcharts authored
  by the planning authority. **This is almost exactly our decision-tree concept — and it's reusable.**
- 🇳🇿 **Wellington City Council — Resource Consent Checker** — guided Q&A telling residents the planning
  requirements for *their* property. A clean RaC citizen example.

### Tier 2 — Parcel "what can I build?" zoning lookup
- 🇺🇸 **Symbium** (`symbium.com`) — "computational law"; real-time zoning/building/energy compliance as you
  scope a project; partnered with **UpCodes** (6M+ code sections as code).
- 🇺🇸 **Gridics** (`gridics.com`) — MuniMap / ZoneCheck: click any parcel → allowed uses, setbacks, density,
  overlays, 3D buildable envelope. Adopted by US cities (e.g. Fort Lauderdale).

### Tier 3 — Automated compliance checking (heavy / BIM-based) — ⛔ OUT OF SCOPE for this app
> **Decision (2026-06-13): too much for our purpose.** These are national, government-operated,
> BIM/IFC-model regulatory platforms — they require 3D model submission, multi-agency integration and
> statutory authority. Not relevant to a civic-data demystification tool. Recorded for completeness only.
- 🇸🇬 **CORENET X** (`info.corenet.gov.sg`) — national platform; automated checks on 3D **BIM (IFC+SG)**
  models across architectural/structural/M&E; multi-agency review in 20 working days. World-first.
- 🇪🇪 **Estonia EHR BIM building-permit** — **47 automatic code checks** against the Building Code; live.
  (EU **ACCORD** project extends this across EE/FI/DE/UK/ES.)

### What this tells us
1. **The pain point is real and globally tackled** — but along a spectrum from *citizen triage* (cheap,
   flowchart) to *full BIM auto-checking* (heavy, model-based).
2. **Ireland has none of these** — confirms the gap (consistent with §10.7 / §15.6 findings).
3. **Our level = Tier 1** (citizen triage / demystify), which is the **cheapest and highest-impact** entry,
   and **PlanX is open-source** — potential to adapt rather than build from scratch.
4. **RaC = our framing.** "Rulebook as axioms" (§16) is literally the Rules-as-Code thesis; the difference
   is we *derive ours by scraping the existing plan* (the DM Standards) rather than hand-authoring flows —
   a faster path to coverage, at the cost of needing the scrape-to-logic step.

---

## 18. AXIOM-DRIVEN SOURCE REGISTRY (SEED) — substantiating *why* a decision went the way it did (2026-06-14)

Operationalises §16: each **conditional axiom** in the required-assessments checklist
(`planning_rules/<la>/required_assessments.md`, the Galway exemplar = 26 triggered reports) names a
**designation/data layer** as its antecedent. That layer is an **external dataset we must ingest** to
evaluate the axiom per-site. So the obligation checklist *is* the source shopping-list: "environmental →
NPWS SAC/SPA + OPW flood", "archaeology → NMS SMR zones", "access → TII/OSM roads", and so on. The
machine-readable seed lives at **`planning_rules/SOURCE_REGISTRY.md`** (one row per axiom→source); this
section is its narrative.

### 18.1 Empirical reason-source findings (this investigation, 2026-06-14)
Tested against ~30 live refusals (Galway City + County, near the N6 GCRR corridor) across one-off houses,
a 44-unit scheme, a 148-unit LRD, and a retention:
- **eplanning.ie publishes NO reason text.** Confirmed via raw HTML, WebFetch, **and** a full JS render
  (Playwright): the portal exposes only the outcome and a **"Number of Conditions: N"** count — the reason
  wording lives solely in the unlinked Chief Executive's Order PDF. This **hardens the §10.1 "[PDF]
  BLOCKED" verdict with direct evidence**, for both the County (`GalwayCC`) and City (`GalwayCity`) instances.
- **The only public reason text is the ABP/ACP decision PDF** (appealed cases only). Direct media-path
  pattern (no portal session): order `…/cases/orders/{ddd}/d{case}.pdf`, inspector report
  `…/cases/reports/{ddd}/r{case}.pdf`. Orders are scanned images (OCR); inspector reports carry text.
- **Archaeology axiom (#17) granularity matters.** The constraint is `point ∈ SMR *Zone of Notification*`
  (polygon), **not** distance to a monument point. Verified: a site 173 m from a ringfort but **outside**
  its zone attracted zero archaeology objection and was refused on traffic grounds; where a monument *was*
  relevant (148-unit LRD), archaeology resolved to a **monitoring condition** ("low archaeological
  potential"), never a refusal. The §13 SAC dose-response pattern is the model to replicate here.
- **N6 route-safeguarding rarely refuses one-offs** — across the sampled refusals the dominant grounds were
  settlement/zoning (urban-generated rural housing, Tier-6, zoning "G"), local-road traffic/sightlines, and
  wastewater/SAC. The one scheme abutting the GCRR reservation was **granted** on appeal. (Detail in the
  conversation log; relevant to the CPO "no-scheme" question.)

### 18.2 The seed registry — one external source per obligation axiom
Each row = a conditional axiom from the checklist → the dataset that evaluates its spatial/site predicate
→ ingest status → join key. ✅ = endpoint probed live · 🔎 = located, not probed · ❌ = endpoint TBD.

| Axiom (checklist #) | Obligation it triggers | Source needed | Endpoint / locator | Status | Join |
|---|---|---|---|---|---|
| #12 / #13 / #14 | Appropriate Assessment / NIS / EcIA / Habitats screening | **NPWS Designated Areas** (SAC/SPA/NHA/pNHA) | `services-eu1.arcgis.com/Jhij7i46ouO8Cc0N/.../NPWSDesignatedAreas/FeatureServer` (L3=SAC) | ✅ §13 | `point ∈ polygon` |
| #21 | Flood Risk Assessment + Justification Test | **OPW NIFM extents + CFRAM AFA** (national, SHP) + GSI pluvial/groundwater | `data.gov.ie/dataset/nifm-river-flood-extents-current-scenario`; CFRAM `…/cfram-areas-for-further-assessment-afa-boundaries`; statutory **Zone A/B** still `floodinfo.ie` (endpoint TODO) | ✅ located (Zone A/B 🔎) | `point ∈ flood extent` |
| #17 | Archaeological assessment | **NMS SMR Zone of Notification** (+ SMR points) | `…/SMRZoneOpenData/FeatureServer/0` · `…/SMROpenData/FeatureServer/0` | ✅ §15.2 | `point ∈ zone` |
| #16 | Architectural Heritage Assessment | **NIAH national** (CSV+REST); RPS/ACA per-council | `data.gov.ie/dataset/national-inventory-of-architectural-heritage-niah-national-dataset`; RPS e.g. `…/record-of-protected-structures2` | ✅ NIAH national; RPS/ACA 🔎 per-LA | `point ∈ / near RPS·ACA` |
| #10 | Visual Impact Assessment | **Landscape sensitivity** (LCA Class 2/3) — per-council, no national | per-council on data.gov.ie (Monaghan `…/landscape-character-types`, Cork, Galway) | 🔎 per-LA | `point ∈ class` |
| #6 / #7 / #8 | Road Safety Audit / RSIA / TTA; sightlines | TII+RMO national/regional roads + **OSM/Overpass (local roads)** ✅ verified | `data.gov.ie/dataset/national-road-network-2013`; `…/regional-road`; `overpass-api.de/api/interpreter` | ✅ (OSM probed; local-road `maxspeed`) | nearest road class / junction |
| #25 | Site Suitability (septic / on-site wastewater) | **GSI Vulnerability + Subsoil Permeability + Karst** (national, ESRI REST); EPA soils + agglomeration boundaries (sewered extent) | `data.gov.ie/dataset/groundwater-vulnerability-140000-ireland-roi-itm` (+ subsoil/karst); `…/urban-waste-water-treatment-agglomeration-boundaries` | ✅ located (national) | `point ∈ vuln/karst class; ∉ agglomeration` |
| (material contravention, §10.6) | zoning conflict | **Development-Plan Zoning composite (national)** | `data.gov.ie/dataset/development-plan-land-use-zoning-ireland1` | 🔎 | `land-use ≠ zoned use` |
| #15 | EIA / EIAR | EPA EIA portal / Schedule 5 thresholds (attribute, not spatial) | `epa.ie` | 🔎 | by type/scale |
| (always-on #4/#18, scale #1-3/#5/#9/#19/#20/#22-24/#26) | universal or scale-gated reports | **no external layer** — derive from feed attributes (type, units, floor area) | the 495k feed [S] | ✅ | n/a |
| (reason text, all) | *why* refused/conditioned | **ACP decision PDFs** (appealed only) + council file PDF (blocked) | `…/cases/orders/d{case}.pdf`, `…/reports/r{case}.pdf`; `LinkAppDetails` (no reasons) | ✅ §18.1 | `AppealRefNumber 6-digit → ABPCASEID` |

### 18.3 Priority for a consolidated planning feature (revised after data.gov.ie sweep)
1. **Already in hand:** NPWS SAC/SPA (✅), NMS SMR zones (✅), zoning composite, NIAH national, GSI
   site-suitability pack, OPW NIFM flood extents — **all located as national ESRI-REST/SHP** layers.
2. **Quick national pulls:** GSI vulnerability/subsoil/karst (#25), NIAH (#16), OPW/GSI flood (#21),
   Tailte land-cover/buildings/Small-Areas (context) → one ingest each, no scrape.
3. **Remaining endpoint discovery:** statutory **Flood Zone A/B** on floodinfo.ie (the precise
   Justification-Test trigger; NIFM extents are the national stand-in until then).
4. **Per-council assembly (31-LA pattern):** RPS, ACA, landscape sensitivity, derelict/vacant registers.
5. **Local-road access (#6-8):** OSM/Overpass — verified to carry the local roads + `maxspeed` that TII omits.

Built this way, the consolidated feature is the §16 obligation-set reconstructor: for any application
point, join the designation layers → emit the obligation set (which assessments it owed) → cross with the
numeric standards → and, for appealed cases, attach the ACP-PDF reason text. **No per-council PDF scrape
required for the skeleton + obligation context.**

### 18.4 data.gov.ie / Open Data Ireland sweep (2026-06-14) — gaps filled
Systematic CKAN `package_search` against every axiom gap. **Open Data Ireland fills nearly all of them,
mostly with national ESRI-REST/SHP layers.** Full endpoints in `planning_rules/SOURCE_REGISTRY.md`.
- **Flood (#21):** OPW NIFM extents/depth (national), CFRAM AFA/UoM, Community-Scale Coastal extents; GSI
  surface-water + historic-groundwater flood maps. (Statutory Zone A/B still floodinfo.ie.)
- **Site suitability (#25):** GSI Groundwater Vulnerability + Subsoil Permeability + Karst + Protection
  Scheme (national 1:40,000, ESRI REST); EPA National Soils/Subsoils; **EPA Urban Waste Water Agglomeration
  Boundaries** = the sewered-vs-unsewered antecedent. (Karst + high vulnerability ≈ the Menlo "effluent
  can't be safely disposed" refusal.)
- **Heritage (#16):** NIAH National Dataset (DHLGH, national CSV+REST); RPS/ACA per-council only.
- **Roads (#6-8):** TII National + RMO Regional (national); **local roads → OSM** (below).
- **Supplementary / cross-feature:** Tailte HVD National Land Cover 2018 / Buildings / CSO Small Areas 2022;
  NMS thematic monument sets; **Derelict + Vacant Sites registers** (vacancy/activation angle); **DAFM
  Anonymous LPIS** (agricultural parcels — the one-off-rural / CPO land context); CSO PxStat + Residential
  Commencement Notices (reconciliation/build-out). Per the curated-meta convention, version-stamp on pull.

**Overpass / OSM (`overpass-turbo.eu`) — PROBED & USEFUL (2026-06-14).** Live query around the
Menlo/Castlegar study point returned **78 road segments within 800 m**, classified
(`tertiary/residential/track/unclassified/service` + 1 `proposed` = likely the GCRR/N59 corridor) with
`maxspeed` tags (50/30/20) and street names — exactly the **local-road layer TII's national-roads data
omits**, and the level at which one-off sightline/access refusals (#6-8) actually bite. Also 12 `historic`
features within 1.5 km (archaeological_site/ruins/mass_rock/castle) — useful *context* only; **NMS SMR
stays authoritative**. Limits: `maxspeed` sparse (~18/78); OSM gives road centrelines, **not visibility
splays** (sightlines still need a survey). Call note: POST raw QL with `Content-Type: text/plain` + a
descriptive `User-Agent` (default UA → HTTP 406); mirror `overpass.kumi.systems/api/interpreter`.
5. **Caveat seen across RaC literature:** converting NL rules → machine logic is the hard part (CODE-ACCORD
   corpus, Urban Institute zoning automation) — which is exactly why our pre-structured, table-heavy
   **DM Standards scrape is an advantage**: much of the Irish rulebook is already semi-structured.

**Resources:** `planx.uk` · `opendigitalplanning.org` (open-source flows) · Wellington Resource Consent
Checker · `symbium.com` · `gridics.com` · `info.corenet.gov.sg` · Estonia e-ehitus / ACCORD (`accordproject.eu`).

### 17.1 PlanX deep-dive (the Tier-1 reference to study)
The closest precedent to our idea. Funded by the UK **Planning Software Improvement Fund**, built by
**Open Systems Lab** under the **Open Digital Planning (ODP)** programme.

**Repo:** `github.com/theopensystemslab/planx-new` (+ `planx-core`, `planx-api`, `planx-docker`).
- **Licence: MPL-2.0** (Mozilla Public Licence) — file-level copyleft, **permissive enough to fork/adapt**.
- **Stack: TypeScript (93.9%)** monorepo — **React** editor+preview (Material UI, **GOV.UK** patterns);
  **Node/Express** REST API (`apps/api.planx.uk`); **Hasura GraphQL** over **PostgreSQL**; **ShareDB**
  (JSON Operational Transformation) for realtime collaborative flow editing; **Pulumi/AWS** IaC.
- **Activity:** ~6,066 commits on main, actively maintained — but only **18 stars / 4 forks** (it's a
  gov-internal product, not a big OSS community; expect to lean on OSL directly, not a forum).

**How it works (the rules-as-code model):**
- Service designers build a **flow** = a flowchart in a drag-and-drop visual editor — **no code**.
- **Node types:** questions, checklists, text inputs, file uploads, notices, calculations, etc.
- As an applicant answers, state accumulates in a **"passport"** (the collected answer/data object) which
  drives branching/routing through the flow and is reused across services.
- Output is **structured machine-readable data, NOT a PDF** — directly automatable downstream.
- **Explicitly NOT AI** — accountable flowcharts authored & owned by the planning authority.

**Proven impact (UK):** ~**60% fewer invalid applications**, ~**45% lower processing time**; **16–18 LPAs**
(Lambeth, Southwark, Camden, Buckinghamshire, Tewkesbury). Independent **JUMPSEC** security assessment positive.

**Relevance / reuse verdict for Ireland:**
- ✅ The **flow + passport model is exactly our decision-tree**, and MPL-2.0 lets us adapt it.
- ✅ Validates the *citizen-triage* product shape and its measurable benefit.
- ⚠️ **UK-coupled:** GOV.UK design system, UK LPA integrations, UK legislation in the flow content — so
  reuse = **adopt the engine/model, replace the content** (our scraped Irish DM Standards become the flows).
- ⚠️ It is **operator-authored** (councils hand-build flows); our edge is **deriving flows from the scraped
  rulebook** (§16) rather than hand-authoring — faster coverage, but needs the scrape→flow compiler.
- **Open question to resolve next:** can the Galway DM Standards (numeric + triggered-assessment axioms)
  be expressed directly in PlanX's flow/passport schema, i.e. reuse their engine fed by our rulebook?

---

## 18. THE LEGISLATIVE SPINE — Planning Act 2024 commencement via SIs (2026-06-13)

Probed the Tracker's existing **Statutory Instruments** data (MCP `search_statutory_instruments`). It
**already contains the planning commencement orders** — connecting the legislation layer to our rulebook.

### ⚠️ Two different "commencements" — do not conflate
1. **SI Commencement *Orders*** (IN the SI data) — bring an **Act into legal force**. They switch on the
   *rulebook itself*. ← what this section is about.
2. **BCMS Commencement *Notices*** (NOT in SI; separate ops feed `data.nbco.gov.ie`, §15.4) — a builder's
   pre-construction notice to Building Control on one project. A step in an individual project's lifecycle.

### What's in the SI data — Planning & Development Act 2024, commenced in stages
| Date | SI | Order |
|------|-----|-------|
| 2025-06-10 | 2025/239 | Planning & Development Act 2024 (Commencement) Order |
| 2025-06-20 | 2025/256 | …(No.2) |
| 2025-08-01 | 2025/379 | …(No.3) |
| 2025-10-03 | 2025/452 | …(No.4) |
| 2025-10-10 | 2025/470 | Historic & Archaeological Heritage Act 2023 (Commencement) |
| 2025-11-28 | 2025/555 | Historic & Archaeological Heritage Act 2023 (Commencement) |

All tagged policy domain `housing_planning_local_gov`. (DQ note: bill-match mislabels these to the
"An Taisce Bill 2024" and truncates parent to "Development Act 2024" — cosmetic, the SIs are correct.)

### How they fit the bigger picture — the TEMPORAL / VERSION layer of the rulebook
The **Planning & Development Act 2024** is the biggest overhaul of Irish planning law since the 2000 Act
(which underpins our whole rulebook). It is being switched on **piece by piece** — these orders are the
authoritative record of **which rules are in force on which date**:
- An Bord Pleanála → **An Coimisiún Pleanála**; Section 28 Guidelines → **National Planning Statements**;
  new **10-year** Development Plan cycle; revised timelines and consent classes.

**Implication for the axioms (§16):** an application decided in early 2025 sits under the *2000 Act*
regime; one in 2026 may be under parts of the *2024 Act*. To derive the correct **obligation-set** for any
application you must know **which rulebook was live on its decision date** — and the SI commencement
timeline IS that index. The axioms are therefore **version-stamped**, and the SI feed supplies the stamps.

### Strategic differentiator
The Tracker already ingests the **legislation/SI** layer AND is now scoping the **planning-operations**
layer. So it can connect *the law being switched on* (SIs) to *the decisions made under it* (applications)
— a cross-domain link planning-only tools (PlanX, Symbium) structurally cannot make. Ties to existing
MCP tools `search_statutory_instruments` / `get_bill` (the Planning & Development Bill 2023 → its SIs).

---

## 19. DEVELOPMENT CONTRIBUTIONS — the "tax paid to the council" on a grant (scan, 2026-06-14)

> User-prompted scan: the per-floor-area charge a council levies when it grants permission (≈"€9 per
> square foot for a dwelling"), and "the government had a specific exemption for it until 2024." **Both
> confirmed.** This is the **financial sibling of the §16 rulebook** — and it was entirely absent from the
> `planning_rules` collection (only one incidental Galway "Special Contribution" line existed). Verified
> against the **NOAC LA Performance Indicator Report 2024** (`doc/source_pdfs/NOAC_LA_PerfInd_2024.txt`,
> already in-repo) + council scheme pages + DHLGH circular.

### 19.1 What it is — Section 48 Development Contributions
- **Legal basis:** **Section 48** of the Planning & Development Act 2000 = the *general* Development
  Contribution Scheme. Also **Section 49** (*supplementary* contributions for a specific infrastructure
  service, e.g. Luas/Metro/rail) and **Section 48(2)(c)** *special* contributions (one-off, project-specific
  — this is the lone existing mention, in `galway_county_council/dm_standards.md:351`, re quarry road works).
- **Who sets it:** each local authority **adopts its own scheme as a reserved function of the elected
  councillors** — exactly like the Development Plan / DM standards (§10.6, §16). So it is **per-authority,
  non-uniform, and version-dated** (typical scheme runs ~3–6 years; e.g. DCC 2023-2026, Meath 2024-2029,
  SDCC 2021-2025→2026-2028). Same "not universal, not static" property as the rulebook.
- **How it's charged:** a money condition on the **grant of permission**, calculated **per square metre of
  gross floor area**, at rates the scheme sets, **differentiated by class** (residential / commercial /
  industrial / retail / etc.). Funds public infrastructure (roads, water, amenities, community facilities).

### 19.2 The "€9 per square foot" figure — grounded
- Dublin City Council **residential** rate ≈ **€86.40/m²** (2020-2023 scheme), indexed up (+2.93% from
  1 Apr 2025) → ~€89/m². SDCC applied **+6.18%** to residential from 01/01/2024 (SCSI Tender Price Index).
- The user's **"€9/sq ft" = €96.9/m²** (×10.764) — i.e. the **upper / Dublin end** of council residential
  rates; rural counties are materially lower. A sound rule-of-thumb, **not a national constant**: rates
  vary by authority and are **re-indexed annually**, so any figure the app shows must be **per-council,
  per-scheme-year, sourced** (no hardcoded national rate — same no-inference discipline as the SAC %).
- **Exemptions** are scheme-specific but commonly include social/affordable housing, the first ~40 m² of a
  domestic extension, certain agricultural/community/not-for-profit/sports buildings. Capture per council.

### 19.3 The waiver — the "specific exemption until 2024" (CONFIRMED)
A **Housing for All** temporary, time-limited measure: **waiver of section 48 development contributions**
+ **Uisce Éireann** water/wastewater **connection-charge refund**, to activate housing supply and cut
build cost.
- **Original window (Govt Decision 25 Apr 2023):** waiver for permitted **residential development that
  commenced on site 25 Apr 2023 → 24 Apr 2024**, completed by 31 Dec 2025. *(NOAC quotes this verbatim.)*
- **Extension (Govt 23 Apr 2024 — DHLGH Circular PL 02/2024):** §48 waiver for residential **commenced not
  later than 31 Dec 2024**; UÉ connection refund for connections **commenced ≤ 30 Sep 2024** (refund
  requests to UÉ by 31 Dec 2024); **completion deadline pushed 31 Dec 2025 → 31 Dec 2026**.
- **Documented effect (NOAC 2024):** a **huge surge of commencement notices**, many lodged right at the
  year-end deadline so developers could qualify — which **depressed council building-inspection rates in
  2024** (NOAC flags this as the cause of the dip under the inspection indicators). This is a **measurable
  fingerprint of the waiver in the data** (BCMS commencement-notice spike, §15.4) — a real analytics hook.

### 19.4 Where the data lives & ingestibility
| Datum | Location | Verdict |
|---|---|---|
| **Scheme rates + exemptions** (per LA, per class, per scheme-year) | each council's "Development Contribution Scheme" page/PDF | 🟡 **scrape — the DM-standards pattern (§12.4) extends directly**; a cross-council rate table is **novel, nobody publishes it** |
| **Income actually collected / owed** (€) | each LA **Annual Financial Statements (AFS)** + **NOAC** indicators | 🟡 ties to existing `project_new_sources_scoping` LA-budget→AFS lead — the "how much €" companion |
| **Waiver legal switches** | DHLGH **Circular PL 02/2024** + the §18 SI/legislation layer | ✅ documentable; cross-links to existing SI data |
| **Waiver behavioural effect** | **BCMS commencement notices** spike late-2024 (`data.nbco.gov.ie`, §15.4) | ✅ ingestible aggregate, no PII |

### 19.5 Recommendation (not yet executed)
Add a **`development_contributions` dimension to the `planning_rules` collection** — per council, capture
{scheme name + years, residential €/m², commercial/industrial €/m², key exemptions, indexation basis,
source URL}. It is the **cost axis** that pairs with the DM-standards **rule axis**: the rulebook says
*what you may build*; the contribution scheme says *what that permission costs you*. Same 31-LA scrape
shape, same version-tracking caveat. **Scope note:** the rates are mostly in per-council **PDFs** (the
HTML `consult.*` portal carries the plan, not always the contribution scheme), so this is a 31-PDF
extraction pass — flagged as the actionable next step, not done in this scan.

> **UPDATE 2026-06-14 — cross-council rate scan DONE (28/31).** The per-council residential rate scan
> was executed → **`doc/PLANNING_DEVELOPMENT_CONTRIBUTIONS.md`**: a national comparison + a worked
> "240 m² house" cost per council. **Headline: a >13× spread** (~€1,900 Monaghan-rural base →
> ~€25,550 Fingal for the same house); Galway County one-off rural = **€2,200** (the "€9/sq ft"≈€23k
> rule of thumb is a **Dublin/city-tier** rate, e.g. Galway City €21,600 — it does NOT hold for rural
> counties). 5 structure types identified (flat/unit · flat/m² · banded/m² · hybrid · banded/dwelling).
> **3 gaps:** Laois + Longford (sites hard-block bots) and Wicklow S.48 residential (unpublished).
> Rates are scheme **base** figures — most index annually (WPI Building & Construction), so use live.
> The doc also covers the **relevant-documents chain** (scheme → s.28 guidance → grant condition →
> s.49 supplementary → commencement-notice payment trigger → indexation order), **retention** (no
> waiver, often a multiplier — Wexford 3×, Cavan 1.5×; substitute-consent/EIA caveat), and **change of
> house plans** (no statutory non-material route; charged on **net additional floor area** only — pure
> design changes with no extra m² generally nil, e.g. Leitrim exempts them).

**Sources:** NOAC LA Performance Indicator Report 2024 (in-repo) · DHLGH Circular PL 02/2024
(`gov.ie/en/circular/08374-...`) · Mason Hayes & Curran "Development Levy Waiver…" (`mhc.ie`) · CIF
extension note (`cif.ie/2024/05/03/...`) · Dublin City Council §48 scheme 2023-2026 (`dublincity.ie`) ·
SDCC / Meath / DLR / Fingal scheme pages · Uisce Éireann refund scheme (`water.ie/connections`).

---

## 20. LAND-ACQUISITION / CPO COMPENSATION — the inverse money layer (BUILT, sandbox, 2026-06-14)

The **third money layer**, and the mirror of §19. Development contributions = what a *developer pays
the council* for a grant. This = what the *State pays a landowner* to acquire land — compulsory
purchase orders, dwelling / land-bank purchases, road-scheme land. It is the cost the public bears to
assemble sites for housing/roads/infrastructure, by area and year.

**Source:** the consolidated public-body payment fact (`data/gold/parquet/procurement_payments_fact.parquet`),
NOT a new feed — the councils' own published "Payments over €20,000" lists already carry these.

**PRIVACY — the whole design point.** Those source lists publish payee **name + amount + year**, and
many CPO/land payees are **private individuals** (they sit quarantined, `public_display=False`, in the
gold fact and never surface in the app — see `project_procurement_drilldowns_2026_06_13`). The new
extractor keeps that quarantine intact and lifts out only the **non-identifying** facts the planning
feature needs — the **figure × year × location (council)** — with the **payee identity dropped**. Output
carries **no name column** (runtime invariant refuses to write otherwise), so it is **strictly more
private than the council's own published list** (name removed, figures aggregated). This is the safe way
to surface the public-interest cost data the §13 SAC work and §19 contributions layer pair with.

**Build:** `pipeline_sandbox/planning_cpo_compensation.py` → `pipeline_sandbox/_planning_output/cpo_land_acquisition_by_area_year.parquet`
(+ `data/_meta/cpo_land_acquisition_coverage.json`). Tests: `pipeline_sandbox/test_planning_cpo_compensation.py`.

**Validated 2026-06-14:** 211 land-acquisition rows → **68 anonymized cells, €63.8m, 11 bodies, 2016–2026.**
- Grain: `acquiring_body (council/dept) × year × acquisition_type × payee_type` → `n_payments`,
  `n_distinct_payees` (count only — no names), `total_compensation_eur`, `low_count` flag.
- `acquisition_type` (source-grounded from the published description, no-inference): dwelling €29.7m ·
  land_general €17.2m · land_bank €9.3m · cpo €4.6m · road_land €3.1m.
- `payee_type` (a CLASS, not a name): **individual €35.75m** (the anonymized private/CPO set) · company
  €27.9m · public_body €0.2m.
- **Donegal dominates** (~€47m — it publishes structured "Purchase of Dwelling Asset" / "Land Bank Asset
  Purchase" / "CPO Interest" lines); Meath, HSE, Offaly, Kilkenny, Wexford follow.

**Caveats / honest limits:**
- **Location = council-level only.** The descriptions rarely carry a townland/road (mostly just
  "Purchase of Dwelling Asset" / "Compulsory Purchase Order"), so this is a council×year cost layer, NOT
  point-level. The feature's fine (lon/lat) location comes from the §1 ArcGIS applications feed; the two
  **join at council level** ("this council spent €X assembling land in year Y").
- **`low_count` cells** (single payee/payment) re-state a single already-published figure with the name
  removed — flagged so the UI can band/caveat if a stricter line is wanted; NOT suppressed (it is strictly
  more private than the source).
- Coverage is only the councils whose >€20k lists are in the payment fact; not a national CPO register.

**NO €/hectare from this layer (price-only — do NOT infer).** Probed 2026-06-14: **0 / 211** land
descriptions carry any area unit (ha/acre/m²), and ~half the spend (€29.7m) is *dwellings* (houses, where
€/ha is meaningless). We have the price but not the area, so a €/ha would require guessing the hectares —
the exact inference §13/§19 forbid. The CORRECT €/ha is **external & already published**: **CSO Agricultural
Land Prices** (regional median €/acre & €/ha, arable vs grassland, annual — 2024 national median ≈
€9,988/acre, Dublin ≈ €24,125; methodology bands €10k–€62k/ha) and the **SCSI/Teagasc Agricultural Land
Market Review** (€/acre by region/quality). Ingest/cite those as a separate, clearly-labelled *context*
layer — **with the caveat that CPO road/housing land is often DEVELOPMENT land (valued far above
agricultural) and CPO compensation includes disturbance/severance/injurious-affection premiums**, so the ag
benchmark contextualises but never equals CPO value. A genuine per-parcel €/ha would need an AREA source
(Tailte Éireann / Land Registry folio area, or the CPO award docs) joined to the payment — future
enrichment, not derivable today. Sources:
[CSO Agricultural Land Prices](https://www.cso.ie/en/releasesandpublications/ep/p-alp/agriculturallandprices2024/keyfindings) ·
[SCSI/Teagasc Land Market Review 2025](https://scsi.ie/press-release-scsi-teagasc-agricultural-land-market-review-and-outlook-report-2025/).

---

## 21. RETENTION & AMENDMENT — the two "after the fact" branches (process scan, 2026-06-14)

The contributions doc (`doc/PLANNING_DEVELOPMENT_CONTRIBUTIONS.md`) covers the *levy* treatment of
retention and plan-changes; this section covers the **processes** themselves and **where they live in our
data**. Both are deviations from the clean lodge→decide→grant lifecycle of §11 — and one of them
(**retention**) is **already a structured field we hold**, so it is measurable today.

### 21.1 Retention — regularising what's already built
- **Statutory basis:** application under **s.34** PD Act 2000 (same process as a normal application — site
  + newspaper notice, planner's report, EIA/heritage reports if triggered). It is **retrospective**: the
  development already exists.
- **Why people apply:** (1) **selling** a property (solicitor/title requires it), (2) a **mortgage/loan**
  drawdown, (3) a council **enforcement warning letter**. (Warning letter → ~4 weeks to respond;
  lodging a retention application usually **suspends enforcement** until the decision.)
- **Cost penalties (two distinct 3×/multiplier hits — don't conflate):**
  - **Application FEE** = **3× the normal planning fee** (statutory, Planning & Development Regs fee schedule).
  - **Development CONTRIBUTION** = no waiver, often a multiplier (Wexford 3×, Cavan 1.5×) — see contributions doc.
- **Outcome:** if granted, the development is authorised; if **refused**, enforcement can follow
  (alteration/**demolition** order). Retention does **not** absolve prosecution already commenced, and does
  **not** by itself defeat the **7-year enforcement rule** question (§11.3).
- **The EIA/AA bar:** unauthorised development that *required* EIA or Appropriate Assessment **cannot** use
  ordinary retention → the route is **substitute consent (s.177)**, and post-**PD(M&V)(Amendment) Act 2022 /
  s.34(12)** both *past and present* EIA/AA positions must be satisfied or the application is **deemed
  withdrawn**. (Version-stamp: the **2024 Act** is reforming this regularisation regime as it commences.)
- **Grant rate (selfbuild.ie hard data, beats the "85%" industry claim):** **~55% at LA level
  (6,075 granted / 11,064 applications)** and **~32% on appeal to An Coimisiún Pleanála (176 / 545)**.
  Most retentions are granted, but **not** the ~85% retention-specialist sites assert (bayt.ie /
  retentionpermission.ie — treat as marketing). Mix ≈ **60-70% minor works** (extensions, garden rooms,
  boundary walls) vs 30-40% major (houses, large extensions, ag sheds).
- **Volume surging:** **~5,500 retention applications/yr (2024-25)** vs **~2,200/yr (2019-23)** — a ~150%
  jump (selfbuild.ie), **independently matching our feed** (§21.3): ~5,500/yr ≈ the
  `GRANT PERMISSION FOR RETENTION` (5,473) + RETENTION-type volume we already hold.

### 21.2 Change of house plans — Ireland has NO non-material-amendment route
Unlike the UK's **s.73** "minor material amendment", **Irish law has no general statutory non-material
amendment process** (verified — *the alteration & extension of planning permission*, M. Furminger; the
"Section 73" seen in some Irish advisory content is **UK practice bleeding in**). The actual levers:
- **s.146A** — the deciding authority may amend a permission **only** to correct clerical errors, to
  facilitate works "reasonably regarded as contemplated by" the permission, or to "otherwise facilitate the
  operation" of it — and **`s.146A(2)` expressly prohibits any amendment that is a *material alteration* of
  the terms of the development.** So: non-material only.
- **Fresh application** — for anything material there is **no direct statutory mechanism**; the courts
  recognise an **implied power** requiring a **new planning application** (*South-West Regional Shopping
  Centre Promotion Association v An Bord Pleanála* [2016] — without it "the planning system would be
  burdensome and unworkable").
- **In practice** minor construction-stage changes are handled by **agreement of revised drawings with the
  planner under condition compliance** (no new application) — the day-to-day reality, sitting beneath the
  s.146A / fresh-application formal frame.
- **Contribution effect** follows the **net-additional-floor-area** rule (contributions doc §"Change of
  house plans"): extra m² → pay on the excess; no extra m² → generally nil (Leitrim explicitly exempts a
  change-of-house-plan with no floor-area change).
- **(Separate, don't confuse):** **s.42** = *extension of duration* of an un-commenced/part-built
  permission, and the **PD(Amendment) Act 2025** added a further extension power + a JR clock-pause — these
  extend *time*, they do **not** alter the *design*.

### 21.3 Where this lives in our data — retention is measurable NOW
- **Retention is a structured value in the national feed (§1):** `ApplicationType` carries
  **RETENTION ≈ 55k** rows (+ mixed-case "Permission for Retention"), and `Decision` carries
  **"GRANT PERMISSION FOR RETENTION" 5,473**. So **retention frequency and grant-rate *per council* are
  derivable from the feed alone** — no PDFs — and our `application_type_normalised` crosswalk (Phase 0,
  §8) already folds the spelling variants into a single `Retention` class. A "which councils have the most
  retrospective/unauthorised-build activity" metric is a **near-free analytics win**.
- **Amendments are NOT cleanly in the feed:** a fresh application for a revised design is just *another
  application* (a new `ApplicationNumber`) with no structured link to the one it modifies; s.146A
  corrections live **[PDF]** in the council file. So plan-change *chains* are **not** reconstructable
  without the document layer — a known ceiling, consistent with §10.1.
- **The honest pairing:** retention = a strong, ingestible signal (structured, national); amendment =
  PDF-locked (skeleton only). Both fold into the §16 axiom model as *lifecycle branches* off the main
  lodge→decide→grant spine.

**Sources:** **selfbuild.ie** ("Thousands build first and ask for forgiveness later" — the 55%/32%
grant rates + 5,500/yr volume; "Planning application process" — the 5-week/8-week/6-month/4-week
clocks, all corroborated) · bayt.ie / retentionpermission.ie / jearchitecture.ie (retention process +
the overstated 85% claim, industry) · Planning & Development Act 2000 ss.34, 146A, 177, 42 (`irishstatutebook.ie` /
`revisedacts.lawreform.ie`) · PD(Maritime & Valuation)(Amendment) Act 2022 (`mhc.ie`) · M. Furminger,
"The alteration and extension of planning" (Irish planning-law substack) · *South-West Regional Shopping
Centre Promotion Assoc. v ABP* [2016] · PD(Amendment) Act 2025 commencement (`gov.ie`).

---

## 22. WORKED CASE STUDY — blind obligation-set reconstruction vs the real file (2026-06-14)

**Purpose.** End-to-end validation of the ingestion model (§12 numeric standards + the triggered-assessment
checklist + designation-layer joins): reconstruct, **without looking at the application**, what a one-off
house at a given point would have to satisfy, then reconcile against the **actual granted file**.

**Subject.** Galway **City** Council reg. **22/207**, single dwelling at **Menlo** (≈53.3062, −9.0520),
granted **23 May 2023**, commenced **29 Dec 2024**. Development contribution **€21,420 waived** (Housing
for All §48 waiver — commenced inside the 31 Dec 2024 window; see §19). Source: public eplanning register.

**Method.** (a) Coords → live **NPWS Designated Areas FeatureServer** query → Lough Corrib **SAC (000297)**
+ **SPA (004042)** within 3 km. (b) Ingested Galway DM standards (`planning_rules/`) + the assessment-trigger
checklist. (c) **eplanning PDF extraction** — see recipe below. Documents are **scanned (DjVu-origin), image-only**;
`pdftotext` yields only the "Inspection Purposes Only" watermark, and **local OCR is barred** (box-crash rule,
`feedback_paddleocr_crashes_local_box`), so pages were rasterised with **PyMuPDF (fitz)** and read **visually**.

### Reconciliation scorecard

| Predicted blind (from ingested standards) | Actual file | Verdict |
|---|---|---|
| Location = Menlo / Corrib, elevated | Menlo; FFL 52.70, rock 48.7 (≈50 m AOD) | ✅ (open-elevation said 41 m — ~10 m low) |
| Enhanced wastewater, **not a bare septic tank** (karst + SAC) | **Molloy Chieftain packaged secondary treatment system** | ✅ exact |
| Shallow rock → **raised / imported-soil polishing filter** per EPA CoP | Raised filterbed; "imported soil tested as per EPA CoP"; rock ~1 m down | ✅ exact |
| **NIS** required (SAC qualifying species/water pathway) | NIS prepared; referenced on drawings | ✅ |
| **Lesser Horseshoe Bat** → dark-corridor lighting mitigation | "lighting … maintained in line with the dark corridor / NIS attenuation measures" | ✅ exact |
| **Single-storey / dormer** (elevation + scenic Corrib) | Single-storey, long low profile | ✅ |
| Vernacular: **simple long plan, traditional materials** | Long linear plan; natural stone + plaster; slate/tile roof | ✅ |
| Floor area >200 m² → site-size scaling engaged | Drawing label ~**226 m²** (recalled 240) | ✅ (both trigger scaling) |
| Detached, ancillary domestic garage (DM Std 6) | Detached domestic garage | ✅ |
| **RFI** likely | "Reply to Further Information", 10 Mar 2023 | ✅ |
| Regional-road access restriction (DM Std 28) a risk | Resolved via an **existing Right-of-Way** (owner's letter), not a new road entrance | ✅ (resolution differed) |

### Key learnings
1. **The national framework drove every substantive outcome** — EPA CoP (treatment/imported-soil filter),
   Habitats Directive/NIS + NPWS bat protection (dark-corridor lighting), Sustainable Rural Housing
   Guidelines (single-storey vernacular). These apply **regardless of council**, which is why a blind
   reconstruction matched the real file.
2. **CORRECTION — wrong council's *numbers*:** the blind run quoted Galway **County** Ch.15 thresholds
   (2,000 m² site rule, 90/35/25/15 setbacks, sightline table). The site is **Galway City** (extended
   boundary includes Menlo), and a May-2023 grant was assessed under the then-current **Galway City**
   plan. The *conclusions* held; the *exact numeric thresholds* must come from the right authority +
   the plan in force at decision date. Reinforces that the rulebook is per-council AND time-versioned.
3. The **obligation-set reconstruction is real**: designation join + ingested standards predicted the
   actual report/assessment set (NIS, bat lighting, EPA-CoP treatment, RFI) without reading the PDFs.

### eplanning extraction recipe (reusable; for the audit toolkit)
`galwaycity.eplanning.ie/idocsweb` (LGMA iDocs): `listFiles.aspx?catalog=planning&id=<n>` lists docs as
`docid`s. Per doc: GET `ViewFiles.aspx?docid=<id>&format=pdf` **with a cookie jar** (server stages a
session copy; may need a 2nd hit) → parse the `ViewPdf.aspx?...&file=<GUID>.pdf` iframe → GET
`/idocsweb/files/<GUID>.pdf`. Files are scanned/image-only → render with `fitz` (`get_pixmap(dpi=130)`),
read visually; **do not** local-OCR. The big "Correspondence" doc (86 MB, **363 pages** scanned) holds
the planner's report/decision/conditions/NIS/bat survey — pulled and read selectively (front pages =
decision + conditions; a contact-sheet montage at ~42 dpi located the sections).

### Decision conditions — VERIFIED from the Chief Executive's Order (reg 22/207, Order 76012, 23 May 2023)
The worded grant confirms the blind reconstruction almost line-for-line:
- **Site area = 5,600 m²** (NOT the 0.52 ac recalled) → the DM site-size rule passes by >2×; 45 m² filter bed.
- Full description: *"a single storey dwelling house, sewerage treatment plant, percolation area, Domestic
  shed, access roadway"* at Menlo.
- **Cond. 6 (wastewater):** packaged plant to **EPA CoP (PE ≤ 10) + I.S. EN 12566**; commissioning report
  by a qualified person pre-occupation; **maintenance contract**; **land retained (not sold separately) to
  meet the EPA CoP separation distances** — the DM-Std-9 site-size-for-effluent logic written in as a
  condition. (= the hydrology analysis.)
- **Cond. 9–11 (design):** retain boundary **hedgerows/trees/stone walls**; **additional native
  tree/hedgerow planting**; **front wall in local unplastered stone**; **roof blue/black**. (= the
  landscaping gap-fill + vernacular/muted-materials prediction.)
- **Cond. 12 (ecology keystone):** *"All mitigation measures … in Chapter 6 of the updated Natura Impact
  Statement (NIS) and Chapter 8 of the submitted Bat Survey Report shall be implemented in full under the
  supervision of a suitably qualified ecologist."* (= Lesser Horseshoe Bat + dark-corridor lighting + AA/NIS.)
- **Cond. 13 (occupancy):** **Section 47 enurement** — first occupied by the applicant/family, **7-year**
  occupancy; reason: *"to comply with agricultural land use-zoning objectives in the Galway City Development
  Plan 2023-2029."* → confirms the **City plan 2023-2029 + agricultural zoning** (the County→City correction)
  and the local-need gate.
- **Cond. 14:** €21,420 contribution (s.48) — later **waived** (commenced 29 Dec 2024, Housing for All).

Net: the only material miss in the blind run was quoting **County** rather than **City** numeric thresholds;
every substantive obligation (wastewater design, ecology/bat/NIS, landscaping/materials, single-storey, RFI,
enurement, contribution) was predicted from the ingested standards + designation join before reading the file.

---

## 23. CITIZEN SITING-CHECK FEATURE — design + scope (2026-06-14)

**Concept.** A user enters an **address / Eircode / XY** for a prospective site; the app returns a
**decision tree of the planning issues that site triggers** (ecology/bats, archaeology, flood, septic
viability, road/sightlines, heritage, landscape/siting), each annotated with **the governing DM Standard
quoted verbatim** and **which report they'd have to submit**. It is the §16 obligation-set reconstructor
with a citizen front-end — the Tier-1 "do I need permission / what applies here" pattern of PlanX/Symbium
(§17). **Feasibility: high** — ~80% of the data backbone is already located (registry); the engine is
spatial joins + the rulebook; the new build is a join service + UI.

### 23.1 Pipeline
```
address / Eircode / XY  →  geocode to a point (ITM + WGS84)
   → spatial join against every SOURCE_REGISTRY layer  (point-in-polygon / nearest)
   → emit triggered obligations  + quote the governing DM Standard verbatim
   → DEM lookup (elevation, slope, skyline/prominence) for the siting factors
   → render as a checklist / decision tree, with the no-advice caveat
```
Each tree node = one **axiom we already hold**, evaluated by a join we already have the source for. The
front-end adds **no logic** (§16): all logic lives in the rulebook.

### 23.2 What's computable now vs the one gap
| User-visible issue | Derivation | Source | Status |
|---|---|---|---|
| Bats / ecology | `point ∈/near SAC/SPA/NHA` → EcIA/AA likely (#12-13) | NPWS Designated Areas | ✅ live |
| Archaeology | `point ∈ SMR Zone of Notification` (#17) | NMS SMRZone | ✅ live |
| Flood | `point ∈ flood extent` (#21) | OPW NIFM / GSI | ✅ located (Zone A/B 🔎) |
| Septic viability | `point ∉ sewer agglomeration` **and** groundwater vulnerability/karst class (#25) | EPA UWWT + GSI | ✅ located |
| Road / sightlines | nearest `highway=` + `maxspeed` on the **local** road (#6-8) | **OSM (pre-ingested)** | ✅ verified |
| Heritage | near RPS/ACA/NIAH (#16) | NIAH national + per-LA RPS | ✅ / 🔎 per-LA |
| Landscape / siting | `point ∈ landscape Class 2/3` (#10) → VIA + DM Std 8 siting/materials | per-council LCA | 🔎 per-LA |
| **Elevation / exposed-hill siting** | **DEM** lookup → elevation + slope + skyline/prominence | ⚠️ **DEM not yet in registry** | **gap** |

**The one new source the elevation example needs = a national Digital Terrain Model.** Candidates:
**Copernicus DEM** (10–30 m, free, EU) or **Tailte Éireann national DTM** (confirm licence/endpoint).
Add as a registry row before this feature can answer "41 m on an exposed hill."

### 23.3 Overpass / OSM — prototype on the API, ship on a local extract
- Verified useful (§18.4): returns the local roads + `maxspeed` that TII's national-roads data omits — the
  level at which one-off sightline/access refusals (#6-8) bite.
- **Do NOT call public `overpass-api.de` per user request** in production — its ToS/rate limits are for
  interactive/research use. **Pre-ingest the Ireland OSM extract** (Geofabrik, weekly refresh) into the
  pipeline DB, or self-host an Overpass instance. Overpass-turbo stays the tool to *design* the queries.
- OSM `historic`/heritage tags are **context only** — NMS SMR/SMRZone remain authoritative.
- Limits to surface honestly: `maxspeed` is sparsely tagged; OSM gives road **centrelines, not visibility
  splays** — it locates/classifies the road, it does not compute sightlines (a site survey still does).

### 23.4 The advice / liability boundary — the load-bearing design rule
Two very different outputs; the product does the first, never the second:
- ✅ **Surface + quote the rule and its trigger.** *"Your site is on a skyline in Landscape Class 3, so a
  Visual Impact Assessment is likely (#10). DM Standard 8: 'new buildings should respect the landscape
  context and not impinge scenic views or skylines… materials reflective of traditional vernacular.'"*
- ⛔ **Prescribe the design** ("you need a grey dormer bungalow"). That is the **professional judgment** an
  architect/planning consultant is paid and insured to give; stating it as a requirement is both inaccurate
  (the rule says *integrate*, not *be grey*) and a liability if a user builds to it and is refused.

This is exactly where PlanX (§17) stops — accountable flowcharts that surface the authority's rules, **not**
AI design verdicts — and it aligns with the project's no-inference principle (memory
`feedback_no_inference_in_app`). UI framing: **"issues your site triggers + what each report is + what the
Development Plan says about siting here,"** with a visible *"not professional planning advice"* caveat.

### 23.5 Risk framing — "likely", never "will"
Decisions are discretionary ("proper planning and sustainable development"). The §13 SAC finding showed
designation **raises refusal odds** (35.9% vs 15.2%) — a signal, not a verdict. The tool reports
**triggered obligations + elevated risk**, never "you will be refused/granted."

### 23.6 Hard edges to plan around
- **Eircode / address geocoding is licensed** (ECAD/ECAF is not open data). MVP: a **map-click / coordinate
  picker** (+ accept XY) sidesteps the licence; add Eircode later via a licensed geocoder or An Post/Tailte.
- **Per-council layers** (RPS, ACA, landscape sensitivity) need the 31-LA assembly (same pattern as the
  rulebook) before national coverage; ship Galway-first, matching the existing scope.
- **Plan vintage / temporal layer** (§18 legislative spine): standards change per Development-Plan cycle and
  per National Planning Statement — the quoted rule must be version-stamped to the plan in force.

### 23.7 Build order
1. **Join service** over the national layers already located (SAC/SPA, SMR, flood, GSI septic, NIAH,
   MyPlan/GZT zoning) → triggered-obligation list + verbatim standards. (Reuses the §13 shapely+`make_valid`
   spatial stack and the §13.8 pyesridump ingest gate.)
2. **Add the DEM** (Copernicus/Tailte) — the only new source the siting example needs.
3. **Pre-ingest OSM** (Geofabrik Ireland) for local roads/sightlines.
4. **Decision-tree front-end** (PlanX-style), stopping at *rules + triggers + caveat*, not prescriptions.
5. Galway-first; generalise per-council layers on the rulebook cadence.

### 23.8 Decision-tree content spec → `doc/PLANNING_SITING_DECISION_TREE.md`
The **content** of the tree (issue nodes, mitigation branches, ACP-precedent links, commercial scope) is
specified separately. It is a **hand-authored issue catalogue** (NOT PlanX-style drag-drop flow-authoring
or auto-generated code — overkill). Key shape:
- **Three layers:** universal gates → location triggers (spatial joins) → type & siting (inputs + DEM).
- **Each issue node** = trigger → source → plain flag → *engage [specialist]* → **mitigation class
  (Procedural / Mitigable-by-design / Often-fatal)** → **linked real ACP decision** (the "ultimate
  arbiter" — the Board's own words settle severity, sidestepping the inference line).
- Worked examples in the spec: bats → ecologist + lighting specialist; SAC/karst (Burren/Corrib) →
  effluent often the dealbreaker; floodplain → Justification Test; monument → archaeologist + monitoring;
  rural-need/zoning → the dominant non-mitigable refusal.
- **Commercial scope:** free triage funnel → paid pre-purchase site-report PDF → B2B API; opt-in specialist
  referral marketplace (kept separate from the neutral assessment); positioned as **planning-risk
  due-diligence, not planning advice**.

### 23.9 Storage & data format — the layers are SMALL (measured, 2026-06-13)
The siting-check holds the **boundary layers**, not per-location answers (§ the precompute reframe), so
the data footprint is modest. Worst-case layer measured (`c:/tmp/test_layer_size.py`): the Galway SAC
set including the 488k-vertex Lough Corrib polygon.

| Stored as | Vertices | Size | join answer |
|---|---|---|---|
| GeoJSON (source) | 684,634 | **34.3 MB** | 173 |
| GeoParquet+zstd, no simplify | 684,634 | **11.7 MB** | 173 ✅ |
| GeoParquet+zstd, ~5 m simplify | 93,951 | **1.9 MB** | 172 ✅ |
| **GeoParquet+zstd, ~10 m simplify** | 64,273 | **1.3 MB** | 171 ✅ |
| GeoParquet+zstd, ~50 m simplify | 23,022 | 0.48 MB | **218 ❌ broken** |

**Decisions:**
- **Format = GeoParquet + zstd** (matches `services/parquet_io` convention; DuckDB-spatial reads it
  natively; most compact vector format). Reformatting alone is 3× smaller, lossless.
- **Simplify to ~10 m, but VALIDATE the tolerance against the full-precision join answer** — 5–10 m is
  the sweet spot (~27× smaller, ±1 drift); **50 m BREAKS containment** (inside jumped 173→218). Same
  "verify against ground truth" discipline as the −9e12 fix. Ship a tiny test asserting simplified ==
  full-precision count.
- **Vectors fit git easily:** worst layer = **1.3 MB**; most layers (flood/SMR/zoning) far simpler; the
  full ~15–20-layer national stack ≈ tens of MB total, each file single-digit MB → **nowhere near the
  100 MB/file GitHub limit**. Git-track like `data/_meta`, or R2 per the data-policy split.
- **Raster (DEM) does NOT go in git** — a national DTM is hundreds of MB–GB. Host as a **Cloud-Optimized
  GeoTIFF (COG) on Cloudflare R2** (existing backup infra, [[project_data_backup_r2]]) and HTTP
  range-read the cells under the user's point; or precompute only the derived elevation/slope values.
  **Split: vectors in git (small, versioned), raster in R2 (big, range-read live).**

### 23.10 SOURCE VERIFICATION — all siting-check layers probed LIVE (2026-06-13)
Every layer the feature needs was probed live (not assumed). Tripwire test:
`pipeline_sandbox/test_planning_siting_layers.py` (18 passing). Field names below are the REAL schema
names discovered, not the plan's assumptions.

**National GIS — verified, build-ready (7 layers):**
| Layer | Endpoint (org) | geom / CRS | count | required field | freshness |
|---|---|---|---|---|---|
| NPWS SAC | `Jhij7i46…/NPWSDesignatedAreas/FS/3` | poly / **2157** | 433 | `SITE_NAME` | 2026-04-01 |
| GSI Groundwater Vuln | `gsi.geodata.gov.ie/.../IE_GSI_Groundwater_Vulnerability_40K…/FS/0` | poly / 2157 | **221,148** | **`VUL_CAT`** (X/E/H/M/L) | no lastEdit → poll/hash |
| NMS SMR points | `HyjXgkV6…/SMROpenData/FS/0` | point / 2157 | 151,308 | `MONUMENT_CLASS` | 2026-06-04 |
| NMS SMRZone | `HyjXgkV6…/SMRZoneOpenData/FS/0` | poly / 2157 | 81,409 | `ZONE_ID` (→join SMR) | 2026-06-04 |
| **MyPlan zoning** | `NzlPQPKn…/GZT_Current_Plan/FS/0` | poly / 2157 | 82,664 | `ZONE_GZT/ORIG/DESC` + `PLAN_FROM/TO` | 2026-05-13 |
| NIAH (heritage) | `HyjXgkV6…/NIAHBuildingsOpenData/FS/0` | point / 2157 | 48,327 | `REG_NO/NAME` | 2025-04-03 |
| Planning apps | `NzlPQPKn…/IrishPlanningApplications/FS/0` | point / **3857** | 495,632 | `Decision` | 2026-06-09 |

⚠️ **CRS is mixed** (most = 2157 ITM, apps = 3857) → reprojection mandatory before any join.
⚠️ **MyPlan on-prem hosts are DEAD** (`maps.housing.gov.ie`, `maps.environ.ie`) — use the ArcGIS
Online twin `GZT_Current_Plan` above. ⚠️ GSI field is `VUL_CAT` (NOT `Vulnerability`).

**Raster:** DEM = **Copernicus GLO-30** (30 m, FREE/open, Cloud-Optimized GeoTIFF on AWS Open Data →
host/range-read as COG per §23.9). Tailte Éireann 10 m DTM is LICENSED (proprietary) — only if precision needed.

**Link-only (NOT ingestible):** OPW flood (CFRAM / NIFM / National Coastal Flood Extents) is
**CC-BY-NC-ND 4.0** — NonCommercial + NoDerivatives. Confirmed via floodinfo.ie open-spatial-data-portal.
**Design rule: DEEP-LINK to floodinfo.ie at the user's coordinates; do NOT overlay/ingest OPW geometry.**

**Per-LA layers — the Heritage Council org is the aggregator (one org, not 31 councils):**
`services-eu1.arcgis.com/v5dOXTEOb7ZHdNyQ` (heritagemaps.ie) hosts **build-ready** FeatureServers:
**26 RPS** (`{County}_RPS/FS`, points, 2157, e.g. Cavan=592), **~14 county landscape-character**
(`{County}_Landscape_Categories/Character_Types`, polygons; ⚠️ filter out national thematic noise —
`Biologically Sensitive Area`, `Margaritifera`, `BirdWatch` are NOT landscape-character), **4 ACA**
(Dublin City, Galway City, Kildare, Kilkenny). Full 55-URL list: `c:/tmp/per_la_sources_heritage_council.csv`.
- **Corrected coverage (earlier "landscape is overwhelmingly PDF" was WRONG — undersampled):**
  RPS ~**24/31** GIS + NIAH national fallback; **landscape ~14/31** as GIS character/sensitivity (NOT mostly
  PDF); **ACA the patchiest ~8–12/31** (rest in council-own orgs or development-plan PDFs).
- **The remaining per-LA work is SCHEMA HARMONISATION, not discovery** — 26 RPS schemas differ per county
  (Cavan = `Name_Struc/Address_St/Townland/Special_In/Building_T`; others vary) → a per-county column
  crosswalk (same pattern as the §12 DM-standards rulebook). Statutory: every LA HAS an RPS, so a missing
  GIS row = it's in the plan PDF, not absent.

---

## 22. NATIONAL DECISION-PROFILE PASS — §13 generalised to the whole 495k corpus (BUILT, 2026-06-14)

The Galway SAC case study (§13) extended to the **entire country**. For every one of the 495,632
applications, attach (a) the structured **decision-function** fields and (b) the spatial **obligation
triggers** (which NPWS designations the site sits in), then measure the national **dose-response**.

**Build:** `pipeline_sandbox/planning_decision_profiles.py` →
`pipeline_sandbox/_planning_output/planning_decision_profiles.parquet` (495,632 per-decision profiles)
+ `data/_meta/planning_decision_profiles_coverage.json`. Sources: PC01 applications × PC09 SAC / PC10
SPA / PC11 NHA+pNHA. Method: shapely STRtree, NPWS polygons fetched **generalised (~55 m,
`maxAllowableOffset`)** so the 472k-vertex Lough Corrib SAC can't truncate the pull (§13.6), `make_valid`
+ Ireland-bbox guard. **This is a national CORRELATION pass** — generalised geometry undercounts
boundary containment slightly; the app must use exact containment + live polygons (the §13 no-frozen-rate
caveat still holds; never publish a frozen %).

**Profile fields:** decided / granted / refused, `decision_latency_days`, `had_rfi`, `appealed`,
`is_one_off_house`, `application_type_normalised`, units, lon/lat, and `in_sac / in_spa / in_nha /
in_pnha / in_natura2000`.

**National dose-response (decided apps; baseline refusal 13.0%, n=386,033):**
| Trigger | Refusal | Lift |
|---|---|---|
| in_NHA | 30.5% | ×2.35 (n=59, small) |
| **in_SAC** | **19.6%** | **×1.51** |
| in_Natura2000 (SAC∪SPA) | 19.0% | ×1.46 |
| in_SPA | 18.1% | ×1.39 |
| in_pNHA | 17.8% | ×1.37 |
| one-off house | 14.0% | ×1.07 |

The §13 SAC dose-response **holds nationally but is more moderate** (×1.51 vs Galway's ×2.4 — Galway is
unusually SAC-/rural-constrained). The decisive read is the **compound effect**: a **one-off house
inside a Natura site is refused 22.7%** vs 13.9% outside (×1.6 within that cohort), and at the
**principle stage (Outline) inside Natura → 55.4%** vs 44.4% outside. So designations bite hardest on
exactly the cohorts the §10/§16 rulebook predicts (rural housing + nature constraints), and hardest when
the *principle* is tested — empirical support for the mitigation-profile taxonomy (§21): the spatial,
fixed-fact triggers (SAC/SPA boundary) are the **hard** ones.

**Lifecycle profile (national):** median decision latency **55 days**, **23.8%** carry an RFI, **4.9%**
appealed. (⚠️ `AppealDecision` is an *empty string* not null on most rows — guard on trimmed length, or
the appeal rate inflates to 96%; per-council appeal semantics also vary, so the trustworthy appeal metric
needs the PC02 ACP-feed join, not this self-reported field.)

**Not yet joined (next obligation layers, all in the registry):** OPW flood (PC14 — CC-BY-**NC**-ND, no
clean FeatureServer), RPS/ACA heritage (PC15/16), SMR archaeology zones (PC28), zoning composite (PC07,
for material-contravention). Each adds another trigger column to the same profile.

**Registry:** added PC33 (CSO Agricultural Land Prices), PC34 (SCSI/Teagasc Land Market Review), PC35
(Property Arbitration — CPO compensation, blocked/no register) to
`planning_rules/_corpus_registry/planning_corpus_seed.csv`.

---

## 23. ARCHAEOLOGY TRIGGER + AUTHORITATIVE OVERTURN METRIC (BUILT, 2026-06-14)

Two extensions to §22, both OCR-free.

### 23.1 SMR archaeology zone added — and it VALIDATES the §21 hard-vs-mitigatable taxonomy
Added `in_smr_zone` (NMS SMR Zone of Notification, registry **PC28**, 81,408 zones) to
`planning_decision_profiles.py`. National result (decided, baseline 13.0%):

| Trigger | Refusal | Lift | §21 class |
|---|---|---|---|
| in_SAC | 19.6% | **×1.51** | HARD (fixed site fact) |
| in_SMR_zone (archaeology) | 14.0% | **×1.08** | MITIGATABLE (testing / preservation-by-record) |

**12,547 decided applications sit in an archaeology zone** yet refusal barely moves (×1.08) — whereas
sitting in a SAC lifts it ~1.5×. That is direct empirical support for the mitigation-profile axis: a
**mitigatable** obligation (do the archaeological report) is near-neutral on the outcome; a **hard**
spatial constraint (SAC integrity) materially raises refusal. The taxonomy isn't just theory — the
corpus shows it.

### 23.2 Authoritative council-overturn metric — fixes the §22 caveat
`planning_appeal_outcomes.py` → `_planning_output/planning_appeal_outcomes.parquet`. Joins the **ACP's
own decision** (PC02, 26,079 cases, CC-BY) to applications via the §Angle-4 recipe (6-digit core of
`AppealRefNumber` → `ABPCASEID`) — replacing the unreliable self-reported `AppealDecision`.

- 15,182 appeals joined (of 20,923 with a ref); **13,053 clear grant/refuse both sides**.
- **ABP overturned the council 26.4%** (matches the known ~⅓), split **grant→refuse 1,342** vs
  **refuse→grant 2,100** → councils are *net more restrictive* than ABP on appealed cases (applicant
  appeals succeed more often than third-party ones).
- Per-council (min 25 appeals): **Donegal 44.2%**, Mayo 33.8%, Wexford **33.2%**, Cavan 33.1%, Galway
  32.2%. These are now CREDIBLE — the self-reported field had manufactured fake 100%s for Westmeath /
  Wexford (empty-string default + vendor quirks). Use THIS (the ACP feed), not the applications field.
- Caveat: ACP appeals are *de novo*; "overturn" = outcome flipped. A record, not a quality judgement.

Tests: `test_planning_decision_profiles.py`, `test_planning_appeal_outcomes.py` (incl. a guard that no
council shows ≥95% overturn — the artifact tripwire). Coverage JSONs in `data/_meta/`.
