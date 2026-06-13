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
