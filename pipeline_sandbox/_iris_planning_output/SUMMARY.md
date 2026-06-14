# Iris Oifigiúil — Planning Notices Audit

Source: `data/silver/iris_oifigiuil/iris_notice_events_clean.csv` · notices: **50,042** · date range 2016-01 .. 2026-06.
Planning-related notices identified: **740** (1.5% of corpus).

> No-inference: Iris carries national planning **legislation** + State (Section 181) /
> marine (Foreshore) consents + An Bord Pleanála items + a thin band of local-authority
> planning & CPO/road-scheme notices. It does **not** carry per-application planning
> permission decisions or per-site Section 5 exempted-development declarations (those are
> on council registers — see doc/PLANNING_PERMISSION_SCOPING.md §10.1).

## Counts by sub-type

| Sub-type | Count | First | Last |
|---|---|---|---|
| planning_legislation_si | 550 | 2016-01-05 00:00:00 | 2026-06-05 00:00:00 |
| foreshore_consent | 114 | 2016-01-12 00:00:00 | 2024-10-18 00:00:00 |
| section_181_state_development | 21 | 2019-02-22 00:00:00 | 2025-06-20 00:00:00 |
| an_bord_pleanala_notice | 16 | 2017-01-06 00:00:00 | 2025-06-24 00:00:00 |
| cpo_or_road_scheme | 16 | 2016-07-22 00:00:00 | 2022-11-11 00:00:00 |
| exempted_development_regs | 14 | 2019-01-29 00:00:00 | 2023-07-21 00:00:00 |
| la_planning_notice | 9 | 2019-01-29 00:00:00 | 2025-07-11 00:00:00 |

## Sample titles

### planning_legislation_si
- EUROPEAN UNION (GOOD AGRICULTURAL PRACTICE FOR PROTECTION OF WATERS) REGULATIONS 2017
- EUROPEAN UNION (ENERGY PERFORMANCE OF BUILDINGS) (No. 2) REGULATIONS 2019
- PLANNING AND DEVELOPMENT ACT 2000 (PART IIB) (ESTABLISHMENT DAY) ORDER 2019
- RENT PRESSURE ZONE (LOCAL ELECTORAL AREA OF ARDEE ORDER 2019)
- AVlAN lNFLUENZA (PRECAUTlONARY CONFlNEMENT OF BIRDS) REGULATIONS 2016
- EUROPEAN UNION HABITATS (WEST OF ARDARA/MAAS ROAD SPECIAL AREA OF CONSERVATION 000197) REGULATIONS 2

### foreshore_consent
- FORESHORE ACT, 1933 NOTICE OF DECISION TO GRANT | FORESHORE LICENCES | The Minister for Agriculture,
- THE COMMISSION FOR COMMUNICATIONS REGULATION | THE COMMISSION FOR COMMUNICATIONS REGULATION: | DECIS
- Notice is hereby given that the Dungarvan/Lismore District of | Waterford in exercise of the powers 
- FORESHORE ACT, 1933 NOTICE OF DECISION TO | REFUSE TO GRANT FORESHORE LICENCES | The Minister for Ag
- NOTICE OF DECISION TO GRANT FORESHORE LICENCES | The Minister for Agriculture, Food and the Marine h
- PLANNING AND DEVELOPMENT AND FORESHORE (AMENDMENT) ACT 2022 (COMMENCEMENT) ORDER 2023

### section_181_state_development
- PLANNING AND DEVELOPMENT ACT 2000 (SECTION 181(2)(A))(NO.4) ORDER 2024 Pursuant to Section 181 (2)(a
- PLANNING AND DEVELOPMENT ACT 2000 (SECTION 181(2)(A)) ORDER 2024 Pursuant to Section 181 (2)(a) of t
- PLANNING AND DEVELOPMENT ACT 2000 SECTION 181(2) (A) ORDER No. 4 2019
- PLANNING AND DEVELOPMENT ACT 2000 (SECTION 181(2)(A)) (NO. 2) ORDER 2024 (REVOCATION) ORDER 2024
- PLANNING AND DEVELOPMENT ACT 2000 (SECTION 181(2)(A)) (No. 3) ORDER 2020
- EUROPEAN UNION (ENVIRONMENTAL IMPACT ASSESSMENT AND HABITATS) (SECTION 181 OF THE PLANNING AND DEVEL

### an_bord_pleanala_notice
- ACHT NA GAELTACHTA 2012 (AINMNIÚ LIMISTÉAR PLEANÁLA TEANGA) 2017. Rinne an tAire Stáit ag an Roinn C
- EUROPEAN UNION (RESTRICTIVE MEASURES CONCERNING THE DEMOCRATIC REPUBLIC OF THE CONGO) (NO. 2) REGULA
- WASTE WATER DISCHARGE (AUTHORISATION) (ENVIRONMENTAL IMPACT ASSESSMENT) REGULATIONS 2016
- ­S.I. No. 279 of 2019. | EUROPEAN UNION (ROADS ACT 1993) (ENVIRONMENTAL | IMPACT ASSESSMENT) (AMENDM
- EUROPEAN UNION (RAILWAY ORDERS) (ENVIRONMENTAL IMPACT ASSESSMENT) (AMENDMENT) REGULATIONS 2021
- APPOINTMENT TO AN BORD PLEANÁLA | In exercise of the powers conferred on the Minister for Housing, |

### cpo_or_road_scheme
- NOTICE OF INTENTION | ROADS ACT, 1993 | Kildare County Council gives notice, in accordance with Sect
- PLANNING AND DEVELOPMENT (AMENDMENT) (No. 2) REGULATIONS 2022
- NOTICE OF DECISION | ROADS ACT, 1993 | Kildare County Council gives notice, in accordance with Secti
- ROADS ACT 1993 (CLASSIFICATION OF REGIONAL ROADS) (AMENDMENT) ORDER 2018
- NOTICE OF DECISION | ROADS ACT 1993 | Kildare County Council, acting on behalf of Wicklow County Cou
- NOTICE OF INTENTION ROADS ACT, 1993 | Kildare County Council, on behalf of Transport Infrastructure 

### exempted_development_regs
- PLANNING AND DEVELOPMENT ACT 2000 (EXEMPTED DEVELOPMENT) REGULATIONS 2019
- PLANNING AND DEVELOPMENT ACT 2000 (EXEMPTED DEVELOPMENT) (No. 3) REGULATIONS 2020
- PLANNING AND DEVELOPMENT ACT (EXEMPTED DEVELOPMENT) REGULATIONS 2022
- PLANNING AND DEVELOPMENT ACT 2000 (EXEMPTED DEVELOPMENT) (No. 2) REGULATIONS 2019
- PLANNING AND DEVELOPMENT ACT 2000 (EXEMPTED DEVELOPMENT) (No. 4) REGULATIONS 2023
- PLANNING AND DEVELOPMENT ACT 2000 (EXEMPTED DEVELOPMENT) (No. 2) REGULATIONS 2020

### la_planning_notice
- Notice is hereby given, pursuant to Section 12(12)(a) of the Planning | and Development Act, 2000 (a
- LIMERICK CITY AND COUNTY COUNCIL | Limerick City and County Council Street Performance and | Busker 
- Courts and Civil Law (Miscellaneous Provisions) Act 2023 (Commencement) Order 2023
- Notice is hereby given that Mayo County Council, Claremorris | Municipal District, Adopted Bye-Laws 
- LOUTH COUNTY COUNCIL ARDEE MUNICIPAL DISTRICT | (CASUAL TRADING ACT 1995) BYE-LAWS 2024 | The Munici
- FISHERIES MANAGEMENT NOTICE NO. 11 OF 2019 | (March Mackerel Quota Management Notice) | I, MICHAEL C

## CPO / road-scheme slice (follow-up check)

**16** notices matched CPO / Roads-Act-1993-scheme wording. CPOs are **sparse** in
Iris — most compulsory-purchase notices are published in newspapers and on An Bord
Pleanála / council sites, not Iris Oifigiúil. The matches here are mainly Roads Act 1993
motorway/protected-road scheme notices plus a few explicit CPOs. See `iris_cpo_notices.csv`.