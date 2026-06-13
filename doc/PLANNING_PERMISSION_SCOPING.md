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

1. **Paginated geometry pull** (ArcGIS REST, Layer 0)
   - Loop `resultOffset` in 2000-row pages (`maxRecordCount` cap), `where=1=1`.
   - **`returnGeometry=true&outSR=4326`** → persist `lon`/`lat` per row. *(CORRECTION: the
     `ITMEasting`/`ITMNorthing` columns are empty — coordinates come ONLY via geometry.)*
   - Drop dead columns `ITMEasting`/`ITMNorthing` and the empty `Applicant*` PII columns.
   - Smoke-test one council first (`PlanningAuthority='Carlow County Council'`) before full ~248-page sweep.

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

> ⚠️ **CORRECTED 2026-06-13 (re-validated against live feeds).** The original INSIDE-SAC figures
> (24.8%, 319 points) **did not reproduce** on a clean re-run and are now known to be wrong — almost
> certainly near-band leakage (degree-buffer, no pyproj) and/or join double-counting in the first-pass
> script. The verified result is **35.9% inside vs 15.2% elsewhere** from a **geometry-repaired,
> deduped, exact point-in-polygon** join. The effect is **real and stronger** than first reported;
> only the precise numbers changed. Full validation log in §13.6.

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
3. **Reconcile before trusting a pull** — assert `Σ(groupBy counts) == returnCountOnly` so a truncated
   paginated sweep fails loudly. `exceededTransferLimit` was `None` here (no truncation), but check it.
4. **Location is geometry-only** — `returnGeometry=true&outSR=4326`; ITM attribute columns are 0/495,632.
5. **Metric ops need a CRS** — any distance/buffer (the near-band) requires pyproj/ITM reprojection;
   degree approximations are unreliable and were the likely source of the original bad "inside" figure.

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
| ⚙️ **OPW flood zones / CFRAM** | `floodinfo.ie` (ArcGIS endpoint = **TODO discover**) | 🔎 next probe |
| ⚙️ ACA / Record of Protected Structures | council GIS + `localgov.ie/services/heritage-and-architectural-conservation` | 🔎 |
| ⚙️ EPA SAC metadata | `gis.epa.ie/geonetwork/...d86f3a31...` | 🔎 |

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
