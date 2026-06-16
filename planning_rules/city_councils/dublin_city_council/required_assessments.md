# Required Assessments / Triggers — Dublin City Council

Source: Dublin City Development Plan 2022–2028, Chapter 15 — Development Standards, Section 15.3 (Environmental Assessment) and related sub-sections. Fetched 2026-06-13.
URL: https://www.dublincity.ie/dublin-city-development-plan-2022-2028/written-statement/chapter-15-development-standards/153-environmental-assessment-eia-aa-ecological-impact

## Environmental Impact Assessment (EIA / EIAR) — Section 15.3.1
- Mandatory thresholds for specified classes of development are set out in the Planning and Development Regulations 2001 (as amended).
- EIA may be required **below** mandatory thresholds where the development is likely to have significant environmental effects (sub-threshold EIA), having regard to nature, scale or location.
- **All planning applications undergo EIA Screening** by the Council; an EIA Screening Statement should be submitted with all applications (ref. OPR Practice Note PN02).
- Where likely significant effects exist, an Environmental Impact Statement / EIAR is required.

## Appropriate Assessment (AA) — Section 15.3.2
- Under Article 6 of the Habitats Directive; considers adverse effects on the integrity of a European Site (Natura 2000 network — SACs/candidate SACs and SPAs/proposed SPAs).
- **All applications are screened for AA** by the Council. Applicant must submit an AA Screening Statement and, if significant effects are likely, a Stage 2 Appropriate Assessment including a Natura Impact Statement.
- Guidance: NPWS/DEHLG "Appropriate Assessment of Plans and Projects in Ireland" (2009); OPR Practice Note PN01.

## Ecological Impact Assessment (EcIA) — Section 15.3.3
- Required for all developments **within or adjacent to** any sensitive habitat, ecological corridor, or specific landscape character area, or which have the potential to contain protected habitats/species.
- May be requested by the Planning Authority for any development considered ecologically sensitive (Policy GI14, Chapter 10).

## Traffic and Transport Assessment
- Addressed via Section 15.16 (Sustainable Movement and Transport) — Traffic and Transport Assessments, Mobility Management and Travel Planning are detailed in **Appendix 5**. Childcare design criteria (15.8.4.1) note a traffic and transport assessment may be required to set out vehicular movements where vehicular drop-off is needed.

## Architectural heritage / built heritage
- Built Heritage and Archaeology standards are set out in Section 15.15 (detail not extracted in this capture); protected structures/ACAs covered there and in Chapter 11.

## Flood risk
- Flood risk is addressed in Chapter 9 (Sustainable Environmental and Flood Risk) rather than enumerated as numeric triggers within Chapter 15.

## Other assessments referenced in Chapter 15
- Architectural Design Statement (50+ residential units).
- Daylight/Sunlight Assessment (Appendix 16).
- Community & Social Audit (50+ units); school-demand report (50+ dwellings).

Note: Dublin's Chapter 15 enumerates EIA, AA and EcIA triggers clearly; flood-risk and detailed traffic/transport triggers are located in other chapters/appendices of the plan and were not the focus of this Chapter 15 capture.

## Machine-readable concept checklist (parsed by rulebook.parse_checklist_concepts)

Maps each siting-catalogue node id to the assessment Dublin's Chapter 15 names for it, with the
plan's own trigger wording. Rows derived from **Table 15-1 (Reports & Thresholds)** of the Ch.15
written statement (`raw/chapter15_written_statement_DRAFT.pdf`). The node id is the key (Dublin numbers
by Section/Table, not "DM Standard N"). septic_groundwater / rural_need_zoning are intentionally
absent — Dublin City is fully sewered and has no rural one-off-housing policy.

| node | Required document | Trigger condition | Ref |
|------|-------------------|-------------------|-----|
| aa_screening | Appropriate Assessment Screening + Natura Impact Statement | An AA Screening is required for all developments; a Stage 2 AA / NIS where likely significant effects on a European Site cannot be excluded | Dublin City DP 2022–2028, Ch.15 §15.3.2 / Table 15-1 |
| european_site | Appropriate Assessment Screening / NIS + Ecological Impact Assessment | Development affecting, or within/adjacent to, a European Site (SAC/SPA) | Dublin City DP 2022–2028, Ch.15 §15.3.2–15.3.3 |
| bats | Ecological Impact Assessment | All developments within or adjacent to any sensitive habitat, ecological corridor or landscape character area, or with potential to contain protected habitats/species | Dublin City DP 2022–2028, Ch.15 §15.3.3 / Table 15-1 |
| eia | Environmental Impact Assessment | All developments within the thresholds of the Planning and Development Act 2000 (as amended) / Regulations; sub-threshold where significant effects likely | Dublin City DP 2022–2028, Ch.15 §15.3.1 / Table 15-1 |
| floodplain | Site-Specific Flood Risk Assessment | Any development within Flood Risk Zones A and B | Dublin City DP 2022–2028, Ch.15 §15.18.14 / Table 15-1 |
| surface_water | Surface Water Management Plan (Appendix 13) | 2 or more residential units / 100 sq.m or more | Dublin City DP 2022–2028, Ch.15 §15.6.2 / Table 15-1 |
| road_sightlines | Traffic and Transport Assessment + Road Safety Audit | TTA: 50+ residential units, or any development constructing new roads / materially affecting vulnerable road users; RSA: any new roads | Dublin City DP 2022–2028, Ch.15 / Table 15-1 |
| mobility_plan | Mobility Management Plan / Travel Plan | 20+ residential units, over 100 employees, or any development with zero/reduced car parking | Dublin City DP 2022–2028, Ch.15 / Table 15-1 |
| protected_structure | Conservation Report | Any development relating to a protected structure, within the curtilage of a protected structure, and/or affecting an Architectural Conservation Area | Dublin City DP 2022–2028, Ch.15 §15.15.2 / Table 15-1 |
| landscape_siting | Landscape and Visual Impact Assessment + Landscape Design Report | Site-specific circumstances; Landscape Design Report at 30+ residential units / 1,000 sq.m or more | Dublin City DP 2022–2028, Ch.15 §15.6.7 / Table 15-1 |
| waste_management | Construction & Demolition Waste Management Plan + Operational Waste Management Plan | 30+ residential units / 1,000 sq.m or more | Dublin City DP 2022–2028, Ch.15 / Table 15-1 |
| noise_assessment | Noise Assessment | Any noise-generating use and/or any development within designated noise zones on the development-plan zoning maps | Dublin City DP 2022–2028, Ch.15 / Table 15-1 |
| design_statement | Architectural Design Report | 30+ residential units (or site-specific commercial circumstances) | Dublin City DP 2022–2028, Ch.15 §15.5.8 / Table 15-1 |
| climate_statement | Climate Action and Energy Statement (incl. District Heating) | 30+ residential units / 1,000 sq.m or more | Dublin City DP 2022–2028, Ch.15 §15.7.3 / Table 15-1 |
