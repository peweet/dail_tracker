# Claude Implementation Plan: Judiciary Feature Data Sources, Probes, Validation, Tests, and Provenance

## Purpose

Build a dedicated **Judiciary** feature for `dail_tracker` using the existing Iris Oifigiúil public-appointments data as the appointment spine.

Important correction:

- **Do not reimplement Iris Oifigiúil extraction.**
- Iris judicial appointments are already in the project.
- Treat Iris as the canonical internal appointment spine, then enrich those records from external, joinable sources.

The feature should focus on:

- judicial appointments;
- current judicial office;
- court/rank;
- nomination and vacancy context;
- assignments / specialist lists;
- court system metrics;
- published judgments / determinations only if a small probe proves the data is reliable;
- clear provenance and caveats.

Do **not** build judge rankings, bias scores, productivity scores, misconduct scores, or crude complaint metrics.

Safe product framing:

> Judicial appointment, current office, assignment, published-source, and courts-system context.

---

## Existing Project Spine

### Existing internal source

```text
public_appointments.parquet
```

Expected current use:

- Iris Oifigiúil public appointment notices;
- appointment type classification including judicial appointments;
- source notice/provenance fields.

Action:

1. Inspect current appointments parquet/schema.
2. Identify judicial rows.
3. Build stable `judicial_appointments` SQL view from existing appointment data.
4. Preserve Iris source URL, notice date, notice title, extracted person name, appointed role, and original text.
5. Do not duplicate Iris scraping.

Suggested view:

```text
sql_views/judiciary_appointments.sql
```

Suggested grain:

> one row per judicial appointment notice/person extracted from existing appointments data

---

## Source Catalogue

### Source 1 — Courts Service Data Portal

URL:

- https://data.courts.ie/en/datasets

Specific datasets checked:

- https://data.courts.ie/en/datasets/courts-service-annual-report-2024
- https://data.courts.ie/en/datasets/courts-service-courthouses

Direct structured resources observed:

- https://data.courts.ie/files/annual-report/2024/annual-report-2024.csv
- https://data.courts.ie/files/annual-report/2024/annual-report-2024.json
- https://data.courts.ie/files/court-offices/court-offices.csv
- https://data.courts.ie/files/court-offices/court-offices.json

Format:

- CSV
- JSON
- HTML dataset landing pages

Scrape burden:

- Low.
- Prefer direct CSV/JSON.
- Landing pages also expose metadata.

Potential datasets:

- Courts Service Annual Report 2017–2024
- Courts Service Courthouses / court offices

Useful for:

- court-level caseload/activity metrics;
- judicial capacity context;
- court offices/courthouses;
- district/circuit/court geography;
- system-level trends, not named-judge scoring.

Possible outputs:

```text
data/gold/parquet/courts_annual_metrics.parquet
data/gold/parquet/courts_courthouses.parquet
data/_meta/courts_data_portal_coverage.json
```

Follow-up actions:

1. Probe all dataset landing pages and resource URLs.
2. Confirm every annual report year has CSV/JSON.
3. Download JSON and CSV for at least 2024 and court offices.
4. Compare CSV row counts to JSON row counts.
5. Inspect field stability across years.
6. Create normalization map for metric names.
7. Store source URL, resource URL, retrieved timestamp, content hash, and licence.
8. Do not join these metrics directly to named judges except for high-level court/capacity context.

Validation checks:

- CSV and JSON row counts match.
- Required columns exist.
- Year value matches dataset year.
- Numeric metrics parse cleanly.
- No duplicate metric keys within same year/category unless expected.
- Court names normalize consistently.
- Unknown/changed metric labels are quarantined, not silently dropped.

Suggested tests:

```text
test/test_judiciary_courts_data_portal.py
```

Test cases:

- downloads/fixtures parse as CSV and JSON;
- CSV/JSON row counts match;
- annual report year extracted correctly;
- court office records have location/court/circuit fields where expected;
- duplicate metric keys are flagged;
- metadata includes source URL and retrieved timestamp.

Priority:

- High.
- This is the easiest structured source.

---

### Source 2 — Courts Service “The Judges” Current Roster

URL:

- https://www.courts.ie/visit-and-learn/the-judges

Format:

- HTML
- plain text sections by court
- includes publication date

Scrape burden:

- Low to medium.
- HTML is relatively readable.
- Needs careful parsing of headings, vacancy rows, ex officio sections, Circuit Court circuit brackets, and District Court district numbers.

Useful for:

- current serving judge roster;
- court;
- rank;
- president/chief justice roles;
- ex officio roles;
- vacancies;
- Circuit Court assignment/circuit;
- District Court Dublin/provincial/moveable categories;
- District Court district number.

Potential output:

```text
data/gold/parquet/judiciary_current_roster.parquet
data/_meta/judiciary_current_roster_coverage.json
```

Suggested grain:

> one row per judge-role listing on the Courts Service current roster

Suggested columns:

```text
judge_name_raw
judge_name_normalized
honorific_raw
court
court_rank
role_title
is_president
is_chief_justice
is_ex_officio
is_vacancy_row
vacancy_count
circuit_assignment
district_number
district_category
roster_section
source_url
source_published_at
source_retrieved_at
source_sha256
parse_confidence
requires_manual_review
```

Follow-up actions:

1. Fetch the page and cache raw HTML.
2. Parse headings into court sections.
3. Parse names and roles under each section.
4. Preserve vacancy rows separately.
5. Extract `Published at` date.
6. Normalize judge names for joining to Iris appointment records.
7. Add name-alias handling for:
   - “The Hon. Mr. Justice”
   - “The Hon. Ms. Justice”
   - “His Honour Judge”
   - “Her Honour Judge”
   - “Judge”
   - initials and apostrophes
   - Irish diacritics
8. Join to existing Iris judicial appointments by normalized name plus court where available.
9. Never overwrite Iris appointment facts with roster facts; keep them as separate source-backed facts.

Validation checks:

- Every parsed row has a court/section.
- Vacancy rows are not treated as judges.
- Ex officio rows are flagged.
- District Court numbered rows parse district numbers.
- Circuit Court bracketed assignments parse correctly.
- Parsed judge count by court is stable enough to detect sudden parser breakage.
- Source published date is captured.
- Duplicate judge names across ex officio/current roles are expected and should be retained with role flags.

Suggested tests:

```text
test/test_judiciary_current_roster.py
```

Test cases:

- Supreme Court section parses judges and vacancies.
- Court of Appeal ex officio rows flagged.
- Circuit Court `[Dublin]` style assignment parses.
- District Court numbered rows parse district number.
- “Vacancy (x2)” parses as vacancy count, not judge.
- Names normalize consistently.
- Existing Iris judicial appointee can match roster row.
- Unmatched roster row is retained with `match_status = unmatched`.

Priority:

- Very high.
- This is the best first external person-join source.

---

### Source 3 — Gov.ie Judicial Nomination / Appointment Announcements

Search strategy:

- `site:gov.ie judicial nominations appointments Court of Appeal District Court High Court judge nominations`
- `site:gov.ie "Government nominates" "District Court"`
- `site:gov.ie "Appointments to the Supreme Court"`
- `site:gov.ie "Nominations to the High Court"`

Example URLs found:

- https://www.gov.ie/en/department-of-the-taoiseach/press-releases/appointments-to-the-supreme-court/
- https://www.gov.ie/en/department-of-the-taoiseach/press-releases/government-nominates-8-new-members-of-the-district-court/
- https://www.gov.ie/en/department-of-justice-home-affairs-and-migration/press-releases/government-nominates-appointments-to-the-high-court/
- https://www.gov.ie/en/department-of-the-taoiseach/press-releases/nominations-to-the-court-of-appeal-high-court-and-district-court/
- https://www.gov.ie/en/department-of-the-taoiseach/press-releases/nominations-to-the-high-court-and-the-circuit-court-17-october-2023/
- https://www.gov.ie/en/department-of-the-taoiseach/press-releases/appointments-to-the-court-of-appeal-and-the-district-court/
- https://www.gov.ie/en/department-of-the-taoiseach/press-releases/appointment-to-the-supreme-court-and-circuit-court/
- https://www.gov.ie/en/department-of-the-taoiseach/news/appointments-to-the-supreme-court-and-the-high-court-2-november-2022/
- https://www.gov.ie/en/department-of-the-taoiseach/press-releases/appointments-to-the-supreme-court-court-of-appeal-and-to-the-high-court/

Format:

- HTML
- usually structured press-release pages
- good publication metadata
- may contain bullet lists and biographical details

Scrape burden:

- Medium.
- Use search/discovery plus targeted parser.
- Avoid broad uncontrolled crawling initially.

Useful for:

- nomination date;
- government meeting date;
- nominated person;
- target court;
- role type;
- previous judicial office;
- vacancy cause;
- predecessor / retirement / elevation / resignation;
- limited biography;
- appointment process notes;
- JAC or JAAB reference;
- promotion chain.

Potential output:

```text
data/gold/parquet/judicial_nominations_govie.parquet
data/_meta/judicial_nominations_govie_coverage.json
```

Suggested grain:

> one row per person nominated in a gov.ie judicial appointment/nominations announcement

Suggested columns:

```text
announcement_title
announcement_url
department
published_on
last_updated_on
government_meeting_date
nominee_name_raw
nominee_name_normalized
target_court
target_role
is_serving_judge
previous_court
previous_role
vacancy_reason_text
vacancy_predecessor_name
vacancy_predecessor_event
vacancy_effective_date
appointment_process_reference
biographical_text
source_text_excerpt
parse_method
parse_confidence
requires_manual_review
source_retrieved_at
source_sha256
```

Follow-up actions:

1. Build a small manual seed list of known gov.ie judicial appointment URLs.
2. Parse those pages first.
3. Extract metadata: title, department, published date, last updated date.
4. Extract nominee names from bullet lists or first paragraphs.
5. Extract target court and role.
6. Extract vacancy reason paragraphs.
7. Extract limited biography blocks, if present.
8. Join to Iris judicial appointments by normalized name + target court + close date window.
9. Preserve non-matches for manual review.
10. Add a discovery probe later using gov.ie search pages or sitemap if available.

Validation checks:

- Every output row has source URL.
- Every output row has a published date.
- Every nominee row has a target court or manual-review flag.
- Vacancy reason text is preserved, not over-normalized.
- Multi-person announcements produce multiple rows.
- “Nomination in principle” is flagged separately from direct nomination.
- JAC/JAAB references are preserved.
- Biography extraction does not overwrite canonical appointment facts.

Suggested tests:

```text
test/test_judiciary_govie_nominations.py
```

Test cases:

- multi-person District Court announcement expands to multiple rows;
- Supreme Court appointment extracts previous Court of Appeal roles;
- High Court announcement extracts vacancy due to elevation/retirement;
- vacancy predecessor name extraction is optional and confidence-scored;
- “nomination in principle” flag works;
- output rows preserve announcement URL and publication date;
- Iris join works only when date/name/court confidence is adequate.

Priority:

- High.
- Very useful enrichment and generally parseable.

---

### Source 4 — Judicial Appointments Commission Vacancies

URLs:

- https://www.judicialappointments.ie/
- https://www.judicialappointments.ie/vacancies
- https://www.judicialappointments.ie/application-process
- https://www.judicialappointments.ie/publications
- https://www.judicialappointments.ie/legislation
- https://www.judicialappointments.ie/documentation

Format:

- HTML
- linked external recruitment portal for active roles
- linked applicant documentation, likely PDFs or documents

Scrape burden:

- Medium.
- The vacancies page is easy.
- The external recruitment portal may require separate probing.
- Treat as snapshot/current-state source.

Useful for:

- vacancy pipeline;
- role being advertised;
- court level;
- applicant booklet links;
- application deadline where visible;
- eligibility/process context;
- post-2025 JAC process metadata.

Potential outputs:

```text
data/gold/parquet/judicial_vacancies_jac.parquet
data/gold/parquet/judicial_appointment_process_jac.parquet
data/_meta/judicial_vacancies_jac_coverage.json
```

Suggested grain:

> one row per vacancy listing snapshot

Suggested columns:

```text
vacancy_title
court
role
vacancy_url
external_application_url
applicant_booklet_url
application_deadline
eligibility_text
status
snapshot_date
source_url
source_retrieved_at
source_sha256
parse_confidence
requires_manual_review
```

Follow-up actions:

1. Snapshot JAC vacancies page.
2. Extract listed vacancies and external portal links.
3. Probe whether external portal pages are public and parseable.
4. Download applicant booklets if linked and public.
5. Extract deadline and eligibility only if stable.
6. Do not assume candidate names will be public.
7. Link JAC vacancy to later gov.ie nomination and Iris appointment by court/role/date window where possible.

Validation checks:

- Vacancy page snapshot stored even if no vacancies.
- Current vacancy rows include snapshot date.
- Closed/vanished vacancies are retained historically from snapshots.
- External links are stored but parser failure does not break pipeline.
- Deadline parsing has manual-review fallback.
- No applicant/candidate assumptions.

Suggested tests:

```text
test/test_judiciary_jac_vacancies.py
```

Test cases:

- active vacancy row parses;
- empty vacancies page produces valid zero-row dataset with metadata;
- external TAL portal link preserved;
- applicant booklet link preserved if present;
- snapshot date required;
- vanished vacancy remains in historical output after snapshot merge.

Priority:

- Medium-high.
- Useful from 2025 onward.

---

### Source 5 — Courts.ie High Court Assignment Notices

Example URL:

- https://www.courts.ie/news/assignments-of-high-court-judges---hilary-term-2026

Search strategy:

- `site:courts.ie "Assignments of High Court Judges"`
- `site:courts.ie "Judge in Charge of the Commercial List"`
- `site:courts.ie "Judge in Charge of the Planning and Environment List"`

Format:

- HTML news pages
- plain text list-style content

Scrape burden:

- Low to medium.
- Individual pages are easy.
- Discovery of historical pages needs a search/sitemap probe.

Useful for:

- judge-in-charge roles;
- specialist list assignments;
- term/year;
- court list;
- High Court administrative responsibilities.

Potential output:

```text
data/gold/parquet/judicial_assignments.parquet
data/_meta/judicial_assignments_coverage.json
```

Suggested grain:

> one row per judge assignment/list role per term/source notice

Suggested columns:

```text
judge_name_raw
judge_name_normalized
court
assignment_title
list_name
term_name
term_year
assignment_start_date
assignment_end_date
source_notice_title
source_url
source_published_at
source_retrieved_at
source_sha256
parse_confidence
requires_manual_review
```

Follow-up actions:

1. Build a small seed list of assignment notice URLs.
2. Parse Hilary Term 2026 page.
3. Extract judge name and assignment/list.
4. Identify term/year from page title and content.
5. Join to current roster by normalized name.
6. Join to Iris appointment history where possible.
7. Add historical discovery later.

Validation checks:

- Each assignment row has source URL and source published date.
- Multiple judges in one assignment row are split or flagged.
- “Judge in Charge” roles are distinguished from general assignments.
- List names normalize consistently.
- Ambiguous initials require manual review.
- Judge names must match roster or be flagged.

Suggested tests:

```text
test/test_judiciary_assignments.py
```

Test cases:

- “Mr. Justice Sanfey, Judge in Charge of the Commercial List” parses correctly.
- Multiple-judge row parses or flags review.
- Term/year extracted from title.
- Published date captured.
- Unknown judge does not silently match.

Priority:

- High after current roster and gov.ie nominations.
- Strong legal-professional value.

---

### Source 6 — Courts.ie Specialist Court / List Pages

Example URLs:

- https://courts.ie/organisation-information/about-the-planning-and-environment-court
- https://www.courts.ie/organisation-information/high-court-provincial-venues-terms-and-sittings
- https://www.courts.ie/organisation-information/high-court-law-terms
- https://www.courts.ie/organisation-information/district-court-sittings
- https://www.courts.ie/services/find-your-circuit-or-district-court-area

Format:

- HTML
- simple informational pages
- some pages link to ArcGIS or other external maps

Scrape burden:

- Low for text pages.
- Medium for ArcGIS/boundary data if used.

Useful for:

- court/list taxonomy;
- specialist court context;
- High Court terms/sittings;
- District/Circuit geography;
- legal calendar context;
- court venue/circuit/district mapping.

Potential outputs:

```text
data/gold/parquet/court_terms_sittings.parquet
data/gold/parquet/court_specialist_lists.parquet
data/gold/parquet/court_boundaries_context.parquet
```

Follow-up actions:

1. Parse High Court law terms page.
2. Parse District Court sittings page.
3. Parse specialist court/list pages where named judges appear.
4. Probe ArcGIS boundary map separately before deciding whether to ingest.
5. Join terms/sittings to assignment notices, not to individual judge metrics.

Validation checks:

- Calendar dates parse as dates.
- Year/term names normalized.
- Source URLs preserved.
- ArcGIS data is not ingested unless stable service endpoints are identified.
- No case-level or personal data is extracted from general info pages.

Suggested tests:

```text
test/test_judiciary_terms_sittings.py
test/test_judiciary_specialist_lists.py
```

Priority:

- Medium.
- Good context; not the first person-join target.

---

### Source 7 — Legal Diary Download

URLs:

- https://legaldiary.courts.ie/
- https://legaldiary.courts.ie/download

Observed formats:

- HTML landing page
- daily MS Word `.docx`
- daily PDF

Scrape burden:

- Medium.
- DOCX is preferable to PDF.
- Avoid OCR.
- It may only expose the current day from the download page, so historical coverage needs probing.

Useful for:

- daily court list snapshots;
- judge sitting/list appearances;
- list type;
- court;
- date;
- case title/context where public.

Potential output, if probe succeeds:

```text
data/gold/parquet/judicial_legal_diary_snapshots.parquet
data/_meta/judicial_legal_diary_coverage.json
```

Suggested grain:

> one row per public list item / judge appearance / court list entry, depending on parse quality

Important caution:

This is high-risk and later-phase.

Do not use Legal Diary data to create:

- judge productivity metrics;
- judge workload rankings;
- bias scores;
- outcome measures;
- named-judge controversy flags.

Safe use:

- “listed public sitting/activity context”
- “public legal diary appearances”
- “source-linked list entries”

Follow-up actions:

1. Download current DOCX.
2. Parse with `python-docx`.
3. Compare DOCX extraction to PDF text for one day.
4. Identify stable section headings and judge-name patterns.
5. Determine whether past dates are accessible by URL pattern or only current day.
6. Build a tiny fixture from one day only.
7. Decide whether this is worth pursuing.
8. Keep out of normal CI unless fixture-only.

Validation checks:

- DOCX parse succeeds without OCR.
- Every extracted row has diary date and source file hash.
- Judge names match current roster or remain unmatched.
- In camera/family/minor-sensitive contexts are flagged or excluded if necessary.
- Case titles are treated carefully.
- No outcome/performance inference.

Suggested tests:

```text
test/test_judiciary_legal_diary_probe.py
```

Test cases:

- fixture DOCX parses expected headings;
- source date extracted;
- judge names normalized;
- family/minor-sensitive list headings detected;
- parser emits provenance for every row;
- no row is produced without source section context.

Priority:

- Later.
- Do a small probe only.

---

### Source 8 — Courts.ie Judgments Search

URLs:

- https://www2.courts.ie/Judgments?page=0&sort=asc%3ADateUploaded
- https://www2.courts.ie/Judgments?page=0&sort=asc%3ADateOfDelivery
- https://www2.courts.ie/Judgments-by-Year

Format:

- HTML web app
- visible table/list of judgments
- filters for jurisdiction, neutral citation, judge
- links to judgment details/full-screen pages

Scrape burden:

- Medium to high.
- The initial HTML is parseable, but pagination/filter behavior needs probing.
- May require careful URL parameter handling.
- Do not assume all judgments are in static HTML until tested.

Useful for:

- published judgments by judge;
- court;
- judgment title;
- delivery date;
- upload date;
- neutral citation if available;
- source URL.

Potential output, if probe succeeds:

```text
data/gold/parquet/judicial_published_judgments.parquet
data/_meta/judicial_published_judgments_coverage.json
```

Suggested grain:

> one row per published judgment listing

Important caution:

Published judgments are not all work done by a judge.

Do not create:

- productivity scores;
- win/loss rates;
- bias metrics;
- ideological classifications;
- reversal rates without rigorous methodology and legal review.

Follow-up actions:

1. Probe the first 2–3 pages of judgment listings.
2. Inspect pagination.
3. Click a judgment detail page and inspect available metadata.
4. Test judge-name parsing against roster names.
5. Determine whether “Judgments by Year” provides easier coverage.
6. Build fixture with 10–20 judgment rows.
7. Decide whether source is stable enough for ongoing ingestion.
8. Keep output labelled “published judgments only.”

Validation checks:

- each row has title, court, judge, delivery date/upload date where present;
- source URL preserved;
- duplicate judgments detected by URL/title/citation/date;
- judge names join to roster with confidence score;
- null judge handled safely;
- private/anonymized case titles are not expanded or deanonymized;
- published-judgment count is not labelled workload.

Suggested tests:

```text
test/test_judiciary_judgments_probe.py
```

Test cases:

- listing page parses known rows;
- pagination discovery works or fails gracefully;
- detail page source URL preserved;
- judge-name normalization handles “Quinn J.” / “O'Donnell, Barry J.”;
- duplicate detection works;
- no productivity labels emitted.

Priority:

- Medium-later.
- Valuable, but only after roster/nominations/assignments.

---

### Source 9 — Supreme Court Decisions, Determinations, and Recordings

URLs:

- https://courts.ie/decisions/decisions-and-recordings
- https://courts.ie/decisions/determinations

Format:

- HTML pages
- links to Courts Service judgment/determination systems
- selected recordings

Scrape burden:

- Medium.
- Likely overlaps with Courts.ie judgments/determinations.

Useful for:

- Supreme Court decision context;
- determinations;
- selected recordings;
- source links.

Potential output:

```text
data/gold/parquet/supreme_court_determinations.parquet
data/gold/parquet/supreme_court_recordings.parquet
```

Follow-up actions:

1. Treat as a discovery/probe source only.
2. Determine whether determinations are better captured via the main Courts.ie judgments/determinations pages.
3. Preserve links but avoid duplicating rows.
4. If recordings are used, only store metadata and source links, not media.

Validation checks:

- no duplicate with judgments table;
- court is Supreme Court;
- source URL preserved;
- recording metadata remains source-linked.

Priority:

- Low-medium.
- Add after judgments probe.

---

### Source 10 — Judicial Council Publications

URL:

- https://judicialcouncil.ie/publications/

Observed linked materials include:

- Annual Report 2024
- Annual Report 2023
- Annual Report 2022
- Annual Report 2021
- Annual Report 2020
- Guidelines for Judicial Conduct and Ethics
- Personal Injuries Guidelines
- Sentencing Guidelines
- Protected Disclosures Annual Reports
- Prompt Payment Reports
- Contracts over €20k
- Financial Statements

Format:

- HTML index
- mostly PDFs/documents

Scrape burden:

- Low for index
- Medium for PDF extraction
- Use as aggregate/institutional context

Useful for:

- judicial conduct system aggregate metrics;
- training/institutional context;
- ethics guidelines;
- Judicial Council spending/procurement context;
- prompt payment/contracts over €20k;
- annual reporting;
- not named-judge scoring.

Potential outputs:

```text
data/gold/parquet/judicial_council_publications.parquet
data/gold/parquet/judicial_council_aggregate_metrics.parquet
data/gold/parquet/judicial_council_contracts_over_20k.parquet
data/_meta/judicial_council_coverage.json
```

Follow-up actions:

1. Parse publications index.
2. Download annual reports and key PDFs into bronze cache.
3. Extract document metadata only in first PR.
4. Later extract aggregate complaint/training/conduct metrics if tables are parseable.
5. Parse contracts over €20k if file is structured enough.
6. Do not link aggregate complaint counts to named judges.
7. Preserve source document URL and page number for extracted facts.

Validation checks:

- publication index parse captures title/year/document URL;
- PDFs downloaded with hash;
- PDF extraction has page-level provenance;
- aggregate counts match source table totals;
- named-judge complaint rows are not generated unless explicitly official/source-named and legally reviewed;
- contract/payment rows have value semantics and source caveats.

Suggested tests:

```text
test/test_judicial_council_publications.py
```

Test cases:

- publications index parses annual report links;
- document year extracted;
- PDF hash stored;
- aggregate metrics have page provenance;
- complaint aggregates are marked aggregate-only;
- contracts over €20k rows preserve supplier/source/value fields if parsed.

Priority:

- Medium.
- Useful institutional context; not first named-person enrichment.

---

### Source 11 — Judicial Conduct Committee / Complaint Process

URLs:

- https://judicialcouncil.ie/judicial-conduct-committee/
- https://judicialcouncil.ie/make-a-complaint/

Format:

- HTML
- linked PDFs/documents

Scrape burden:

- Low

Useful for:

- process explanation;
- institutional context;
- complaint procedure;
- admissibility caveats.

Potential output:

```text
data/gold/parquet/judicial_conduct_process_docs.parquet
```

Follow-up actions:

1. Capture process pages and document links.
2. Add static methodology/caveat text to Judiciary feature.
3. Do not create named judge complaint data.
4. If official annual reports include aggregate complaint outcomes, store only aggregate data.

Validation checks:

- process pages have source URL and retrieved date;
- no named-judge complaint scoring;
- aggregate-only caveat present.

Priority:

- Medium-low.

---

### Source 12 — OIC / FOI Context for Judicial Expenses

Example URL:

- https://oic.ie/en/ombudsman-decision/265f9-xx-and-the-courts-service/

Format:

- HTML decision page

Scrape burden:

- Low

Useful for:

- FOI precedent;
- privacy and public-interest caveats around judicial expenses;
- identifying possible FOI workflows.

Potential output:

```text
data/gold/parquet/judicial_foi_context.parquet
```

Follow-up actions:

1. Do not build named-judge expenses page initially.
2. Store OIC decisions as FOI/legal context only.
3. Build optional FOI lead templates:
   - aggregate expenses by court/year;
   - policy/process records;
   - not named expenses unless source and legal review support it.
4. Use this for caveats, not automated allegations.

Validation checks:

- OIC source URL preserved;
- decision date and public body captured;
- no named expense table created from FOI context alone;
- privacy caveat attached.

Priority:

- Low.
- Useful for future FOI workflows.

---

### Source 13 — High Court Search

URL:

- https://www.courts.ie/high-court-search

Format:

- HTML search interface

Scrape burden:

- High / not a first-phase target

Useful for:

- case-reference lookup only if user supplies a reference;
- not good for broad ingestion;
- sensitive.

Recommendation:

- Do not ingest for Judiciary feature phase 1.
- Treat as manual/user-driven source link only.
- Avoid case-level bulk scraping.

Priority:

- Low / defer.

---

### Source 14 — Irish Courts Strategic Plan

Example URLs:

- https://www.courts.ie/news/judiciary-publish-%27irish-courts-strategic-plan-2024--2027%27-on-supreme-court-website
- https://www.supremecourt.ie/supreme-court/news-and-resources/publications

Format:

- HTML pages
- linked PDF/document

Scrape burden:

- Low

Useful for:

- institutional context;
- court-level strategy;
- not person-level data.

Potential output:

```text
data/gold/parquet/judiciary_strategy_docs.parquet
```

Priority:

- Low.

---

## International Comparators (What Other Projects Build)

Surveyed 2026-06-04. These are **reference models, not ingestion targets**. They establish that judiciary-monitoring is a mature civic-tech field, identify which archetype fits Dáil Tracker, and flag which depth-plays could underpin a supporter/paid tier. Six archetypes:

### Archetype 1 — Appointment / biographical spine  (BEST FIT; matches the Iris-spine instinct)

- Federal Judicial Center — Biographical Directory of Article III Judges, 1789–present; searchable by appointing president; full export — https://www.fjc.gov/history/judges
- Free Law Project — Judge & Disclosure Database — https://free.law/projects/judge-db/

Government-built, 25 yrs live, appointment-history **not** scoring. Direct analog to `judiciary_appointments` + the appointing-government angle.

### Archetype 2 — Appointment as a political act  (ON-MISSION for an elected-accountability app)

- Alliance for Justice — Vacancy Tracker — https://afj.org/vacancy-tracker/
- Heritage Foundation — Judicial Appointments Tracker — https://datavisualizations.heritage.org/courts/judicial-appointments-tracker/
- Brennan Center — Federal Judicial Nominations — https://www.brennancenter.org/our-work/research-reports/federal-judicial-nominations
- ACS — On the Bench — https://www.acslaw.org/judicial-nominations/on-the-bench-tracking-president-bidens-judicial-nominations/
- Ballotpedia — Federal judicial appointments by president — https://ballotpedia.org/Federal_judicial_appointments_by_president

Same data, a spectrum of partisan framings → appointment-as-political-act is mainstream **and** contested.

### Archetype 3 — Financial disclosure / conflicts  (HIGHEST IMPACT; likely BLOCKED in Ireland)

- CourtListener — Judicial Financial Disclosures DB — https://www.courtlistener.com/financial-disclosures/
- Fix the Court — Financial Disclosures — https://fixthecourt.com/fix/financial-disclosures/

Only exists because the US Ethics in Government Act 1978 forces public filing; fed ProPublica's 2024 Pulitzer SCOTUS investigation. **Ireland has no equivalent public judicial financial-interest regime — CONFIRM.** If absent, this avenue is closed and the absence is itself a story.

### Archetype 4 — System performance metrics (aggregate, never named-judge)

- EU Justice Scoreboard — https://commission.europa.eu/strategy-and-policy/policies/justice-and-fundamental-rights/upholding-rule-law/eu-justice-scoreboard_en
- Council of Europe CEPEJ — https://www.coe.int/en/web/cepej/cepej-work/evaluation-of-judicial-systems
- World Justice Project — Rule of Law Index — https://worldjusticeproject.org/rule-of-law-index/

CEPEJ already publishes Irish **national** clearance-rate/disposition-time/pendency — don't re-derive; the gap is court-level granularity.

### Archetype 5 — Case-data / pendency analytics  (DEPTH-FOR-LAWYERS; = a second product)

- DAKSH (India) — High Court Data Portal — https://www.dakshindia.org/daksh-high-court-data-portal/
- Development Data Lab — Judicial Data Portal (India) — https://www.devdatalab.org/judicial-data

NGO-scale missions (data scientists + lawyers); scrape cause lists / national grids → open court-data portal. The model for "depth done well", and the scale warning.

### Archetype 6 — Ideology / performance scoring  (THE THIRD RAIL — banned by this plan, correctly)

- Martin–Quinn scores — https://en.wikipedia.org/wiki/Martin-Quinn_score
- Judicial Common Space — https://en.wikipedia.org/wiki/Judicial_Common_Space

Rigorous peer-reviewed academic ideology scoring from voting records; even its authors treat it as contested research. A civic tracker reproducing anything like it = defamation + mission failure.

### Adjacent / infrastructure

- Wikidata (judges as CC0 linked entities) — https://www.wikidata.org/wiki/Q16533
- UK Transparency Project (open-justice / family-court reporting reform) — https://transparencyproject.org.uk/
- Existing Irish judgment hosts (do **NOT** re-host): BAILII https://www.bailii.org ; courts.ie Judgments https://www2.courts.ie/Judgments ; vLex/Justis (commercial) https://ie.vlex.com/

### Support / monetisation signals

- **Free Law Project / CourtListener**: nonprofit; paid membership now unlocks full API + bulk access — proves a free-public-UI + paid-API/bulk tier model.
- **vLex / Justis**: commercial paywall; lawyer-grade headnotes/AI — proves willingness-to-pay exists in the Irish legal market (archetypes 1 + 5 are the saleable layers).
- **DAKSH**: grant / NGO funded.
- Implication: the saleable/fundable wedge is **structured + linked + queryable** (API / bulk) appointment + court data — the gap free Irish sources leave. Depth-for-lawyers (archetypes 1 + 5 with an API) is the credible paid tier; archetype 3 is highest-impact but legally gated; archetype 6 must never be built.

---

## VALIDATED AGAINST DATA — 2026-06-04

Data pulled and pressure-tested before committing to build. Probes:
`pipeline_sandbox/probe_judiciary_join.py` (spine→roster join) and
`pipeline_sandbox/probe_judiciary_pdf.py` (two-pass PDF). Source files cached in `C:\tmp`.
**Verdict: the privacy-safe, on-mission green core holds up; the dangerous/redundant tail stays deferred.**
Reframe: this is an *enrichment that completes the elected-accountability map* (the government appoints
the unelected) — NOT a second product. Stop at the green core.

### Green core — VALIDATED ✅

- **Appointment spine already exists** — `data/gold/parquet/public_appointments.parquet`,
  `appointment_type=='judicial'` → 134 notices, **114 clean** (real court), 2016–2026, 87% named.
  The 20 `body=='Courts'` rows are a junk bucket (companies/receivers/PO boxes) → drop.
- **Spine → live roster join** (courts.ie/judges, ~190 current judges): naive **87%** (134/154),
  **~97% effective** after removing departed-judge true-misses + ~3 contaminants. Real norm-failure ~3%
  (diminutive "Liz↔Elizabeth", typo "Gabett↔Gabbett") → fix with a ~10-line alias table.
- **Elevation/promotion feature works** — diffing court-appointed-to (Iris) vs court-now (roster)
  auto-detected **29 real promotion chains** (Costello HC→CoA→Supreme; Barniville/Donnelly/Hyland/O'Moore
  HC→Supreme; Burns/MacGrath/McDonald/Owens HC→CoA). *Caveat:* ex-officio cross-listing skews "current
  court" for court presidents — prefer the substantive seat. Iris has multiple notices/judge → event-type + dedup.
- **gov.ie nominations** — one PR tested gives Cabinet date, prior career, and **vacancy cause + predecessor**.
  Closes the loop: "resignation of Judge O'Shea" (gov.ie 2026) ↔ Brian O'Shea appointed DC 2017 (Iris) ↔
  8 replacements now on roster (courts.ie).
- **Conduct stats** — Judicial Council Annual Report, **fitz `find_tables()`** (NOT camelot — see below):
  statutory Section 87(4) table, 11 items, AGGREGATE-only, year-comparable (2024: 273 complaints, +26% YoY,
  1 inquiry, 0 reprimands). Born-digital → no OCR, no scraper.
- **Courts clearance** — `data.courts.ie` annual-report CSV (CC-BY): jurisdiction×area×category×incoming/resolved,
  2017–2024. 2024: District 493k/88%, **Court of Appeal 68% backlog**, Liquidated Debt 54%. System-level, no named-judge risk.
- **Courthouses** — `court-offices.csv` (direct CC-BY): 94 offices, **all with lat/long** → map-ready.
- **Wikidata** (CC0): judge DOB/education/positions → the **revolving-door** view (former TD/Minister/AG → judge),
  uniquely joinable to the app's existing elected-member data. No competitor can build this.

### Deferred — confirmed correct to defer

- **Judgments corpus** — redundant (BAILII/courts.ie/vLex own it) + copyright-risky. The PSB Data Catalogue
  "Judgements & Determinations" dataset is **NOT open** (API:No, Open Data:No, personal data) → only route is
  scraping the server-rendered `www2.courts.ie/Judgments` (its "Page 1 of 2" looks like a filtered slice — needs a
  coverage check). If ever built: **metadata + link only, never re-host.**
- **Legal Diary "cases up for judgement"** — EXISTS and parses from the daily DOCX (zipfile→XML, no python-docx):
  "JUDGMENTS FOR ELECTRONIC DELIVERY" / "ELECTRONIC JUDGMENTS" give named judge (joins roster) + court + time +
  case ref + parties. **BUT most privacy-sensitive source in the plan** — saturated with Wards of Court, minors
  (`[A MINOR]` special-ed JRs), childcare/Tusla, clinical negligence, repossessions naming private citizens. Only
  defensible slice = public-law JR-vs-State, count-level / hard-filtered; **never** family/minor/ward/in-camera detail.
  Current-day only (no history without daily capture).
- **Financial-conflicts** (highest-impact intl archetype; CourtListener→ProPublica Pulitzer) — **legally blocked**:
  Ireland has no public judicial financial-disclosure regime (confirm; the absence is itself a story).

### Tooling note (born-digital gov PDFs)

For these reports, **fitz `find_tables()` beats camelot stream** — camelot stream read two-column prose as ~11
false "tables", fitz cleanly isolated the one real ruled table and returned 0 on prose pages. camelot *lattice*
would compete but needs **ghostscript** (not installed). camelot is also **not a declared project dependency** (the
`*_camelot_extract_experimental.py` scripts can't run in the current venv); use an isolated venv for camelot probes.

---

## Recommended Build Order

### Phase 0 — Audit Existing Appointments Data

Goal:

Confirm the internal Iris appointment spine.

Actions:

1. Inspect `public_appointments.parquet`.
2. Identify all judicial appointment rows.
3. Confirm available fields:
   - appointee name;
   - appointment role;
   - appointment date;
   - source notice;
   - original text;
   - appointment type;
   - source URL/PDF.
4. Create `judicial_appointments` SQL view.
5. Add tests for judicial appointment classification.

Output:

```text
sql_views/judiciary_appointments.sql
test/test_judiciary_appointments_view.py
```

---

### Phase 1 — Current Roster

Goal:

Match Iris judicial appointees to current Courts Service roster.

Actions:

1. Build `pipeline_sandbox/judiciary_roster_probe.py`.
2. Fetch and cache Courts Service “The Judges” HTML.
3. Parse courts/sections/names/vacancies.
4. Normalize judge names.
5. Join to Iris judicial appointments.
6. Produce gold parquet and metadata.
7. Add SQL view and tests.

Output:

```text
data/gold/parquet/judiciary_current_roster.parquet
data/_meta/judiciary_current_roster_coverage.json
sql_views/judiciary_current_roster.sql
test/test_judiciary_current_roster.py
```

Acceptance:

- No vacancy rows treated as judges.
- Court/rank parsed.
- Source published date captured.
- Appointee-to-roster joins confidence-scored.
- Unmatched rows retained.

---

### Phase 2 — Gov.ie Nominations

Goal:

Add nomination/vacancy/promotion-chain context.

Actions:

1. Seed with known gov.ie appointment URLs.
2. Parse title, date, department, nominee list, target court, vacancy reason.
3. Extract limited biography where present.
4. Join to Iris and roster.
5. Produce parquet and metadata.
6. Add SQL view and tests.

Output:

```text
data/gold/parquet/judicial_nominations_govie.parquet
data/_meta/judicial_nominations_govie_coverage.json
sql_views/judicial_nominations_govie.sql
test/test_judiciary_govie_nominations.py
```

Acceptance:

- Multi-person announcements expand correctly.
- Vacancy reason preserved.
- Promotion/elevation facts are source-linked.
- “Nomination in principle” flagged.
- Non-matches retained for manual review.

---

### Phase 3 — Courts Data Portal

Goal:

Add structured system-level court metrics and courthouse/court-office context.

Actions:

1. Download annual report CSV/JSON for 2024.
2. Probe 2017–2024 datasets.
3. Download court offices/courthouses CSV/JSON.
4. Normalize metrics.
5. Produce gold parquet and metadata.
6. Add SQL views and tests.

Output:

```text
data/gold/parquet/courts_annual_metrics.parquet
data/gold/parquet/courts_courthouses.parquet
sql_views/courts_annual_metrics.sql
sql_views/courts_courthouses.sql
test/test_judiciary_courts_data_portal.py
```

Acceptance:

- CSV/JSON row counts match.
- Source licence preserved.
- Metrics are court/system-level.
- No named-judge scoring.

---

### Phase 4 — High Court Assignments

Goal:

Add specialist-list and judge-in-charge roles.

Actions:

1. Parse Hilary Term 2026 assignment notice.
2. Build discovery list for historical assignment notices.
3. Extract judge/list/term roles.
4. Join to current roster.
5. Produce parquet and tests.

Output:

```text
data/gold/parquet/judicial_assignments.parquet
sql_views/judicial_assignments.sql
test/test_judiciary_assignments.py
```

Acceptance:

- Assignment rows preserve source URL/date.
- Multiple-judge rows handled safely.
- Ambiguous names flagged.
- List names normalized.

---

### Phase 5 — JAC Vacancies

Goal:

Add vacancy pipeline context.

Actions:

1. Snapshot JAC vacancies page.
2. Extract active roles and application links.
3. Probe external recruitment portal.
4. Store applicant booklet links where public.
5. Link later to nominations/appointments by role/date.

Output:

```text
data/gold/parquet/judicial_vacancies_jac.parquet
sql_views/judicial_vacancies_jac.sql
test/test_judiciary_jac_vacancies.py
```

Acceptance:

- Snapshot date required.
- Empty page is valid.
- External links preserved.
- No candidate assumptions.

---

### Phase 6 — Optional Probe: Judgments

Goal:

Determine whether judgments can be ingested safely and reliably.

Actions:

1. Parse first page of judgments.
2. Probe pagination.
3. Probe judgment detail pages.
4. Normalize judge names.
5. Build fixture.
6. Decide go/no-go.

Output if successful:

```text
data/gold/parquet/judicial_published_judgments.parquet
sql_views/judicial_published_judgments.sql
test/test_judiciary_judgments_probe.py
```

Acceptance:

- Only labelled as published judgments.
- No productivity/bias/outcome metrics.
- Source URLs preserved.

---

### Phase 7 — Optional Probe: Legal Diary

Goal:

Determine whether DOCX legal diary is parseable enough for safe public-source context.

Actions:

1. Download current DOCX.
2. Parse with `python-docx`.
3. Compare to PDF text.
4. Identify headings/list structure.
5. Build fixture only.
6. Decide go/no-go.

Output if successful:

```text
data/gold/parquet/judicial_legal_diary_snapshots.parquet
test/test_judiciary_legal_diary_probe.py
```

Acceptance:

- DOCX parse only; no OCR.
- Source date and hash preserved.
- Sensitive contexts flagged.
- No performance metrics.

---

## Proposed SQL View Set

```text
sql_views/judiciary_appointments.sql
sql_views/judiciary_current_roster.sql
sql_views/judicial_nominations_govie.sql
sql_views/judicial_vacancies_jac.sql
sql_views/judicial_assignments.sql
sql_views/courts_annual_metrics.sql
sql_views/courts_courthouses.sql
sql_views/judiciary_profile_summary.sql
```

Optional later:

```text
sql_views/judicial_published_judgments.sql
sql_views/judicial_legal_diary_snapshots.sql
sql_views/judicial_council_aggregate_metrics.sql
```

Suggested UI-facing summary grain:

```text
one row per judge/person identity
```

Do not force all sources into one table. Keep fact tables separate and build summary views carefully.

---

## Proposed Data-Access Functions

Likely file:

```text
utility/data_access/appointments_data.py
```

or new file:

```text
utility/data_access/judiciary_data.py
```

Suggested functions:

```python
get_judiciary_profile_summary(...)
get_judicial_appointments(...)
get_current_roster(...)
get_judicial_nominations(...)
get_judicial_assignments(...)
get_judicial_vacancies(...)
get_courts_annual_metrics(...)
get_courthouse_context(...)
```

Rules:

- Do not silently return empty DataFrames on source failure.
- Expose unavailable/failure state to the UI.
- Preserve source/provenance fields.
- Keep named-judge facts separate from aggregate court-system metrics.

---

## Proposed UI Feature

Page name:

```text
Judiciary
```

Alternative if kept inside Appointments initially:

```text
Judicial Appointments & Courts Context
```

Suggested sections:

1. Judicial appointment spine
2. Current roster / court / rank
3. Gov.ie nomination and vacancy context
4. High Court / specialist-list assignments
5. Court system metrics
6. JAC vacancy pipeline
7. Optional published judgments panel
8. Source and caveats panel

Required caveat:

> This page is a source-linked public-record research aid. It does not score judges, evaluate performance, infer bias, or imply misconduct. Published judgments, Legal Diary appearances, assignments, and appointments are different record types and should not be merged into a single performance metric.

---

## Provenance Requirements

Every extracted row must preserve:

```text
source_name
source_url
source_page_title
source_published_at
source_retrieved_at
source_last_updated_at
source_content_type
source_sha256
source_row_or_section
source_document_page
parse_method
parse_confidence
requires_manual_review
```

For join outputs, preserve:

```text
left_source
right_source
join_key
join_method
join_confidence
join_notes
manual_review_required
```

Never display an enriched fact without a source link.

---

## Data Validation Requirements

Apply these to all sources.

### Raw-source validation

- HTTP status is 200 or explicitly logged.
- Content type is expected.
- Source hash is stored.
- Retrieved timestamp is stored.
- Raw file path is stored.
- Source URL is stored.
- Parser version is stored.

### Schema validation

- Required columns present.
- Column types stable.
- Dates parse correctly.
- Numeric fields parse correctly.
- Null rates checked.
- Duplicate keys checked.
- Unexpected columns logged.

### Join validation

- Exact name matches separated from fuzzy matches.
- Court/role/date used to disambiguate names.
- Multiple matches flagged.
- No match retained as unmatched, not dropped.
- Fuzzy-only joins require manual review.
- Join confidence preserved.
- Source facts are not overwritten by enrichment facts.

### Quality checks

- Count parsed people per court.
- Count vacancies per court.
- Count unmatched Iris judicial appointees.
- Count roster judges without appointment spine match.
- Count gov.ie nominees without Iris match.
- Count assignments without roster match.
- Count rows requiring manual review.
- Count duplicate normalized names.
- Count rows missing source URL.

---

## Unit Test Plan

Create a dedicated judiciary test group.

Suggested files:

```text
test/test_judiciary_appointments_view.py
test/test_judiciary_current_roster.py
test/test_judiciary_name_normalization.py
test/test_judiciary_govie_nominations.py
test/test_judiciary_courts_data_portal.py
test/test_judiciary_assignments.py
test/test_judiciary_jac_vacancies.py
test/test_judiciary_sql_views.py
test/test_judiciary_provenance.py
```

Optional probe tests:

```text
test/test_judiciary_judgments_probe.py
test/test_judiciary_legal_diary_probe.py
test/test_judicial_council_publications.py
```

Minimum test cases:

1. Iris judicial appointment rows are selected correctly.
2. Current roster parser handles Supreme Court, Court of Appeal, High Court, Circuit Court, District Court.
3. Vacancy rows are not treated as judges.
4. Ex officio rows are flagged.
5. Circuit Court bracketed assignments parse.
6. District Court district numbers parse.
7. Name normalization handles honorifics, initials, apostrophes, diacritics.
8. Gov.ie multi-person nomination pages expand to multiple nominee rows.
9. Gov.ie vacancy reasons are preserved.
10. “Nomination in principle” is flagged.
11. Courts Data Portal CSV/JSON row counts match.
12. Assignment notices parse judge/list/term.
13. JAC vacancy snapshot works when vacancies exist.
14. Empty JAC vacancy page is valid.
15. Every public-facing row has source URL.
16. Every join row has join method and confidence.
17. Fuzzy-only joins require manual review.
18. No metric labelled as judge performance.
19. Published judgments, if used, are labelled published judgments only.
20. Legal Diary, if used, does not emit rows without source date/hash.

---

## Integration / SQL Contract Tests

Add SQL tests for:

- views register successfully;
- expected columns exist;
- view grains are respected;
- source URLs are non-null where required;
- no duplicate primary keys in summary views;
- no vacancy rows in judge-person summary;
- no aggregate court metrics joined as named-judge metrics;
- confidence fields are present for joins;
- manual-review rows are not hidden.

---

## CI Guidance

Do not add network-dependent tests to normal CI.

Normal CI should use:

- small HTML fixtures;
- small CSV/JSON fixtures;
- one DOCX fixture only if Legal Diary probe is added;
- no live fetching.

Live source probes can be separate/manual:

```text
pytest -m sources
pytest -m integration
```

Heavy parsing or optional dependencies should stay out of normal CI until stable.

---

## Privacy / Legal Safety Rules

Do not build:

- judge rankings;
- judge productivity scores;
- judge bias scores;
- outcome-based judge scorecards;
- complaint scores;
- named expenses pages without legal/source review;
- inferred misconduct flags.

Do build:

- source-linked appointment history;
- current office/court/rank;
- official nomination/vacancy context;
- official assignment/list roles;
- aggregate court-system metrics;
- optional published-judgment listings with caveats;
- clear provenance.

Required caveat for the page:

> This page links public sources about judicial appointment, office, assignment, and courts-system context. It does not evaluate judicial performance, infer bias, or imply misconduct.

---

## Recommended First PR

Keep it small.

Scope:

1. Build `judiciary_appointments.sql` from existing Iris-derived appointments.
2. Add current roster probe/parser.
3. Produce `judiciary_current_roster.parquet`.
4. Add `judiciary_current_roster.sql`.
5. Add name normalization helper.
6. Add tests for roster parsing, vacancies, ex officio rows, and source provenance.
7. Add metadata coverage JSON.
8. No UI page yet unless data contract is stable.

---

## Recommended Second PR

Scope:

1. Add Gov.ie nomination parser using a manually seeded URL list.
2. Produce `judicial_nominations_govie.parquet`.
3. Add nomination SQL view.
4. Join to appointments/roster with confidence scoring.
5. Add tests for multi-person announcements and vacancy reasons.
6. Add source coverage metadata.

---

## Recommended Third PR

Scope:

1. Add Courts Data Portal CSV/JSON ingestion.
2. Add annual metrics/courthouses parquet outputs.
3. Add court-system context views.
4. Add tests for CSV/JSON consistency and provenance.
5. Add early Judiciary page or Appointments subpanel.

---

## Recommended Fourth PR

Scope:

1. Add High Court assignment notice parser.
2. Add assignment views and tests.
3. Add specialist-list context to Judiciary page.

---

## Deferred Probes

Only after the above are stable:

- Courts.ie Judgments Search
- Legal Diary DOCX/PDF
- Supreme Court determinations/recordings
- Judicial Council annual-report metric extraction
- FOI context around judicial expenses

---

## Final Acceptance Criteria

The Judiciary feature is acceptable for public alpha when:

- Iris remains the appointment spine.
- Current roster joins are confidence-scored.
- Gov.ie nomination facts are source-linked.
- Courts Data Portal metrics are treated as aggregate/system context.
- Assignment/list roles have source URLs.
- Every public fact has provenance.
- Unmatched and ambiguous records are visible or flagged.
- Fuzzy joins require manual review.
- No judge ranking/performance/bias/misconduct scoring exists.
- Tests cover parsing, joins, schemas, provenance, and dangerous misinterpretations.
- Network fetching is not required in normal CI.
