# Planning Rules — Coverage Manifest

**31 / 31 Irish local authorities captured** (28 ✅ complete · 3 🟡 partial). Each council dir holds
`dm_standards.md`, `required_assessments.md`, `_source.json`, and a gitignored `raw/` archive.

Status legend: ✅ captured · 🟡 partial (draft plan / standards dispersed / two-column extraction) ·
❌ not found.

_Last updated: 2026-06-13._

## Key cross-council finding

The "rule book" chapter is **not uniform**: chapter number and even the *form* vary widely — a numbered
"Development Management Standards" chapter (most), a **whole Volume** (Wexford Vol 2 Manual, Waterford
Vol 2, Mayo Vol 2), an **Appendix** (Wicklow App.1, Clare App.1, Tipperary Vol 3 App.6), or **distributed
across several chapters** (Cork County, South Dublin, Laois, Donegal). Cork City uses its own portal and
folds standards into "Placemaking and Managing Development". This confirms the scoping-doc thesis that the
rulebook is per-authority and non-standardised.

## City councils (3)

| Authority | Slug | Plan | DM-standards location | Status |
|---|---|---|---|---|
| Dublin City | `dublin_city_council` | Dublin City Development Plan 2022-2028 | Ch.15 Development Standards (density/parking in Appendices 3 & 5) | ✅ |
| Cork City | `cork_city_council` | Cork City Development Plan 2022-2028 | Ch.11 Placemaking & Managing Development (own portal) | ✅ |
| Galway City | `galway_city_council` | Galway City Development Plan 2023-2029 | Ch.11 Part B Development Standards | ✅ |

## City and County councils (2)

| Authority | Slug | Plan | DM-standards location | Status |
|---|---|---|---|---|
| Limerick City & County | `limerick_city_and_county_council` | Limerick Development Plan 2022-2028 | Ch.11 Development Management Standards | ✅ |
| Waterford City & County | `waterford_city_and_county_council` | Waterford City & County Development Plan 2022-2028 | Volume 2 Development Management Standards | ✅ |

## County councils (26)

| Authority | Slug | Plan | DM-standards location | Status |
|---|---|---|---|---|
| Carlow | `carlow_county_council` | Carlow County Development Plan 2022-2028 | Ch.16 Development Management Standards | ✅ |
| Cavan | `cavan_county_council` | Cavan County Development Plan 2022-2028 | Ch.13 Development Management (parking Ch.7; rural/sightlines Ch.12) | 🟡 |
| Clare | `clare_county_council` | Clare County Development Plan 2023-2029 | Appendix 1 Development Management Guidelines | ✅ |
| Cork County | `cork_county_council` | Cork County Development Plan 2022-2028 | Distributed: Ch.4 Housing / Ch.12 Transport (parking) / Ch.3 / Ch.5 Rural | ✅ |
| Donegal | `donegal_county_council` | County Donegal Development Plan **2024-2030 (DRAFT)** | Ch.16 Technical Standards + rural-housing ch. + Siting Guide | 🟡 |
| Dún Laoghaire-Rathdown | `dun_laoghaire_rathdown_county_council` | DLR County Development Plan 2022-2028 | Ch.12 Development Management | ✅ |
| Fingal | `fingal_county_council` | Fingal Development Plan 2023-2029 | Ch.14 Development Management Standards | ✅ |
| Galway | `galway_county_council` | Galway County Development Plan 2022-2028 | Ch.15 Development Management Standards | ✅ |
| Kerry | `kerry_county_council` | Kerry County Development Plan 2022-2028 | Vol.6 Ch.1 DM Standards & Guidelines | ✅ |
| Kildare | `kildare_county_council` | Kildare County Development Plan 2023-2029 | Ch.15 Development Management Standards | ✅ |
| Kilkenny | `kilkenny_county_council` | Kilkenny City & County Development Plan 2021-2027 | Ch.13 Requirements for Development | ✅ |
| Laois | `laois_county_council` | Laois County Development Plan 2021-2027 | Ch.13 (Design/Density) + Ch.10 parking; DM-coded | ✅ |
| Leitrim | `leitrim_county_council` | Leitrim County Development Plan 2023-2029 | Ch.13 Development Management Standards | ✅ |
| Longford | `longford_county_council` | Longford County Development Plan 2021-2027 | Ch.16 Development Management Standards | ✅ |
| Louth | `louth_county_council` | Louth County Development Plan 2021-2027 | Ch.13 Development Management Guidelines | ✅ |
| Mayo | `mayo_county_council` | Mayo County Development Plan 2022-2028 | Volume 2 Development Management Standards | ✅ |
| Meath | `meath_county_council` | Consolidated Meath CDP 2021-2027 (as varied) | Ch.11 DM Standards & Land Use Zoning Objectives | ✅ |
| Monaghan | `monaghan_county_council` | Monaghan County Development Plan 2025-2031 | Ch.15 Development Management Standards | ✅ |
| Offaly | `offaly_county_council` | Offaly County Development Plan 2021-2027 | Ch.13 Development Management Standards | ✅ |
| Roscommon | `roscommon_county_council` | Roscommon County Development Plan 2022-2028 | Ch.12 Development Management Standards | ✅ |
| Sligo | `sligo_county_council` | Sligo County Development Plan 2024-2030 | Vol.3 Ch.33 Development Management Standards | ✅ |
| South Dublin | `south_dublin_county_council` | South Dublin County Development Plan 2022-2028 | Ch.5 + Ch.12 + Appendix 10 (Height & Density) | ✅ |
| Tipperary | `tipperary_county_council` | Tipperary County Development Plan 2022-2028 | Vol.3 Appendix 6 Development Management Standards | ✅ |
| Westmeath | `westmeath_county_council` | Westmeath County Development Plan 2021-2027 | Ch.16 Development Management Standards (pp.457-495) | ✅ |
| Wexford | `wexford_county_council` | Wexford County Development Plan 2022-2028 | Volume 2 Development Management Manual | ✅ |
| Wicklow | `wicklow_county_council` | Wicklow County Development Plan 2021-2027 | Appendix 1 Development & Design Standards | 🟡 |

## Partials — what's outstanding

- **Donegal** 🟡 — downloaded source is the **draft** 2024-2030 plan; standards dispersed across Ch.16 + rural chapter + a separate siting guide. Re-pull on adoption; residential parking/separation are in `raw/` but not cleanly tabulated.
- **Wicklow** 🟡 — standards live in Appendix 1; source PDF is two-column, scrambling some per-use parking cells on extraction. Headline figures captured; full tables in `raw/`.
- **Cavan** 🟡 — Ch.13 is the DM chapter but parking standards sit in Ch.7 (Table 7.4) and rural siting/sightlines in Ch.12; cross-chapter consolidation pending.

> Re-pull cadence: re-scrape any council when it adopts a new plan or a variation (see README time-versioning note).
