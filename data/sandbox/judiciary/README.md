# Judiciary exploration data — provenance & findings

Sandbox datasets from the judiciary-feature data exploration, **pulled & validated 2026-06-04**.
Not pipeline output. Reproduce via `extractors/persist_judiciary_data.py` (+ the
`probe_judiciary_*.py` probes). This file is now the canonical provenance record for the
judiciary sources (the original scoping plan has been retired).
Memory: `project_judiciary_feature_validation`. Each file is written as both `.parquet`
(zstd/level-3/statistics) and `.csv`.

## Provenance

| File | Rows | Source | URL / origin |
|---|--:|---|---|
| `judicial_appointments_spine` | 134 | Iris Oifigiúil (existing `public_appointments.parquet`, `appointment_type=='judicial'`) | internal gold |
| `judicial_appointments_exploded` | 153 | ↑ exploded to one row per named appointee, real-court only | derived |
| `judiciary_current_roster` | 198 | Courts Service "The Judges" (incl. ex-officio dup listings) | https://www.courts.ie/judges (Published 26/05/2026) |
| `judiciary_appointment_roster_join` | 153 | spine→roster name-join, `status` ∈ matched/elevated/unmatched | derived |
| `judicial_nominations_govie` | 16 | 4 gov.ie nomination press releases (nominee, prior career, vacancy cause) | gov.ie/department-of-the-taoiseach & /justice press-releases |
| `judiciary_hc_assignments` | 22 | High Court judge assignments, Hilary Term 2026 | https://www.courts.ie/news/assignments-of-high-court-judges---hilary-term-2026 |
| `judicial_conduct_stats` | 23 | Judicial Council Annual Report, statutory Section 87(4) table (2022–24) | https://judicialcouncil.ie/publications/ (fitz `find_tables`) |
| `courts_clearance` | 741 | Courts Service annual-report data, 2017–2024 (jurisdiction×category, incoming/resolved) | https://data.courts.ie (CC-BY 4.0) |
| `courts_waiting_times` | 45 | Courts Service Annual Report 2024 PDF, Waiting Times section pp.133–140 | courts.ie/docs/.../courts-service-annual-report-2024.pdf (fitz) |
| `courthouses` | 94 | Active courthouses (geocoded) | https://data.courts.ie/files/court-offices/court-offices.csv (CC-BY) |
| `judicial_salaries` | 8 | Judicial Remuneration Order (salary by rank) | https://www.irishstatutebook.ie/eli/2021/si/323/made/en/print |
| `judges_european_seats` | 8 | Irish judges at CJEU/ECtHR (standalone factual list, NOT bench-linked) | Wikidata SPARQL (CC0) |

## Linkages & interesting findings (2026-06-04 review)

1. **Cost of the bench ≈ €34.3M/yr** (ordinary judges only: roster counts × salary-by-rank), before
   president/CJ premiums. Fully transparent: public salary SI × public roster.
2. **The Court of Appeal is the system bottleneck — triangulated across three datasets:**
   lowest clearance (68% in 2024), the destination of the most elevations (17 of 28 HC→CoA), AND
   rising appeal waiting times (~22 weeks, up from 19.5–21). Three independent sources agree.
3. **Vacancy-chain lifecycle works.** gov.ie names the *predecessor* who created each vacancy, and
   several resolve straight into the spine: **O'Shea, Cormac Dunne, Elizabeth MacGrath (deceased),
   Marian O'Leary** are all in the appointment spine → full *appointed → departed → replaced* arcs.
4. **Elevation ladder:** 28 detected elevations — High Court is the launchpad (HC→Court of Appeal 17,
   HC→Supreme 6, District→Circuit 4, CoA→Supreme 1). gov.ie text cross-validates (Owens, Hyland,
   Costello elevations all reappear).
5. **Waiting-time anomaly: Limerick 50 weeks** vs ~4 weeks at every other venue (improving from 63).
6. **Coverage:** the 2016+ spine covers ~100 of ~160 current judges; **61 are pre-2016 veterans**
   absent from the spine (would need backfill); 15 spine appointees have since departed.

## Caveats / known data-quality issues

- **Roster (198 rows)** includes ex-officio cross-listings (court presidents appear under up to 3
  courts) — flagged `is_ex_officio_or_multi`, NOT deduped, to stay faithful to source. This also
  skews "current_court" in the join for presidents (e.g. Barniville/Costello).
- **No fuzzy name-matching to the bench.** Surname overlap between Wikidata lists and the roster
  yields false positives (common surnames colliding with *historical* people — e.g. the TD-judge
  "Costello" is Declan Costello, not sitting Caroline Costello). The revolving-door (TD/AG→judge)
  dataset was **REMOVED 2026-06-04** for this reason; do not reintroduce fuzzy name joins.
  `judges_european_seats` is kept only as a STANDALONE factual list, not linked to the sitting bench.
  Wikidata covers only ~4% of sitting judges — biographical depth is rich for senior/historic judges, thin otherwise.
- **`judiciary_appointment_roster_join`**: ~97% effective match; residue = pre-2016 veterans (true
  misses), ~3 court-record contaminants, a few diminutive/typo norm-fails, and one false elevation
  (HC→District, a name collision).
- **Conduct stats** are AGGREGATE-only (no named judges); 2020/21 pre-date the complaints regime
  (no table); 2022 is a partial first year (17). Privacy rule: never attribute to a named judge.
- **Legal Diary "cases up for judgement"** deliberately NOT persisted — privacy-saturated
  (wards of court, minors, childcare). **Judgments corpus** not persisted — redundant + copyright.
