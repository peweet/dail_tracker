# Galway County DP 2022-2028 Ch.15 — Triggered-Assessment / Required-Report Checklist

**Companion to** `galway_ch15_dm_standards_FULL.md` (the numeric standards) and `PLANNING_PERMISSION_SCOPING.md` §12.
**Source:** scraped HTML of Galway County DP draft Ch.15, 2026-06-13. Draft text — version-track vs adopted plan + Variation No.1.

This is the **qualitative decision logic**: the reports/assessments an application must contain, each with its
**trigger condition**. Unlike the numeric standards (a value to compare), these are **presence/absence flags** —
"did this development need, and submit, assessment X?" Most are tied to a **designation layer** we can join to the
application point, so the *trigger* is reconstructable even when the report PDF is locked in the council file.

## The checklist (26 required documents)

| # | Required document | Trigger condition | DM Std | Tie-in layer |
|---|-------------------|-------------------|--------|--------------|
| 1 | **Design Statement / Design Brief** | Significant development / housing schemes (by scale & impact) | 1 | — |
| 2 | **Masterplan** | Where deemed appropriate by Planning Authority | 1–2 | — |
| 3 | **Phasing Plan** | All large/medium residential development | 2 | — |
| 4 | **Landscaping Plan + indigenous native-species schedule** | **ALL** applications; + large car-park areas (visual) | 12 | — |
| 5 | **Microclimate / daylight / shadow study** | Tall / landmark buildings | 2 | — |
| 6 | **Road Safety Audit (RSA)** | Significant dev / identified safety risk | 34 | road network |
| 7 | **Road Safety Impact Assessment (RSIA)** | Significant development proposals | 34 | road network |
| 8 | **Traffic & Transport Assessment (TTA) / Traffic Impact** | Significant development proposals | 34 | road network |
| 9 | **Mobility Management Plan** | Medium–large residential/commercial/mixed/industrial | 35 | — |
| 10 | **Visual Impact Assessment (VIA)** | Sensitive landscape locations | 47 | landscape sensitivity (Table 6) |
| 11 | **Transmission justification statement** | New electricity transmission lines | 42 | — |
| 12 | **Appropriate Assessment Screening / Natura Impact Statement** | Any app that may affect a **Natura 2000** site (direct/indirect/in-combination) — screened on **all** | 51 | **SAC/SPA** |
| 13 | **Ecological Impact Assessment** | All proposals **within or near an SPA/SAC/NHA** | 51–52 | SAC/SPA/NHA |
| 14 | **Habitats Directive screening** | Quarries / projects likely to impact SAC/SPA | 19/51 | SAC/SPA |
| 15 | **EIA / EIAR** | Projects over EIA Directive thresholds (Sch.5 Pt 1/2) | 51 | — |
| 16 | **Architectural Heritage Assessment Report** (conservation architect) | Works to a **Protected Structure** | 60 | **RPS** |
| 17 | **Archaeological assessment** | Recorded monuments / archaeological sensitivity | 62 | RMP/SMR |
| 18 | **Energy-rating certification statement** | **ALL** applications (certify conformance to energy rating) | 63 | — |
| 19 | **Energy Efficiency & Climate Change Adaptation Design Statement** | Residential **>10 units**; retail **>1,000 m²** | 65/66 | — |
| 20 | **SuDS Assessment** (run-off rate/quality/habitat) | Development proposals | 68 | — |
| 21 | **Flood Risk Assessment + DM Justification Test** | Use **inappropriate to its Flood Zone** (A/B) | 69 | **flood zones** (Table 7) |
| 22 | **Construction & Demolition Waste Management Plan** | Significant C&D waste arising | 40 | — |
| 23 | **Construction-phase Waste Management Plan** | Significant waste in construction phase | 40 | — |
| 24 | **Noise Assessment** | Significant dev / industrial (≤55 dB(A) Leq at boundary) | 20/34 | — |
| 25 | **Site Suitability Assessment** (trial hole + percolation, EPA Code of Practice) | Where on-site wastewater (septic) proposed | 9 | unsewered areas |
| 26 | **Restoration plan + QS costing** | Extractive development / quarries | 19 | — |

## Two structural notes
- **Bat survey** is NOT named in Galway Ch.15 — it is subsumed under the **Ecological Impact Assessment** (#13)
  and the separate **NPWS derogation-licence** regime (see `PLANNING_PERMISSION_SCOPING.md` §10.2). Other councils
  may name it explicitly — confirm per-council.
- **"ALL applications" triggers** (#4 native planting, #12 AA screening, #18 energy cert) are universal gates —
  every application in Galway must carry them. These are the cheapest to model (no trigger logic needed).

## Why this is the ingestion model
Combine the **numeric standards** (FULL.md) + this **triggered-report checklist** + the **designation layers**
(SAC/SPA, flood zones, RPS, ACA, landscape sensitivity) joined to the 495k application points → you can, for any
application, reconstruct **which assessments it was obliged to carry** and **which numeric standards governed it**,
without scraping the per-application PDFs. That reconstructed obligation-set IS the decision logic.
