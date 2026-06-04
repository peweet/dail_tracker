# Claude Source Backlog: New Public-Money, Legal, Infrastructure, and Accountability Sources

## Scope

This backlog lists **new or not-clearly-captured source lanes** for `dail_tracker`.

It intentionally excludes sources already present or actively in progress in the branch, including:

- local-authority PO/payment ingestion;
- generic public-body PO/payment ingestion;
- OPW/TII/NTA/HEA payment pages already attempted;
- HSE/Tusla payment parser already in progress;
- eTenders;
- TED;
- local-authority AFS;
- CSO finance/budget probes;
- CRO/charity/CBI enrichment already represented elsewhere.

The purpose is to identify **additional public-record source candidates** that could strengthen the public-money / procurement / legal / infrastructure layer.

Do not wire these sources into the main pipeline immediately. Start with probes.

---

## General Implementation Rule

For each source, first create a probe-only artifact with:

```text
source_name
source_url
source_type
content_type
retrieved_at
source_hash
record_count
sample_rows
parse_confidence
provenance_fields_present
privacy_risk
recommended_value_kind
go_no_go
```

Do not promote a source unless it is:

- reproducible;
- source-linked;
- structured enough to validate;
- safely caveated;
- covered by fixtures/tests;
- clear about value semantics.

---

## Value-Type Discipline

Do not flatten all public-money signals into “spend”.

Use distinct value kinds:

```text
budget_allocated
voted_expenditure
afs_expenditure
award_value
framework_ceiling
purchase_order_value
payment_actual
grant_or_subvention
cpo_land_acquisition_signal
future_project_cost_range
board_approved_award
board_approved_procurement
contract_award_noted
audit_finding
foi_lead
unknown_value_type
```

---

## 1. Project Ireland 2040 / Capital Project Tracker

### Links

- https://www.gov.ie/en/department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation/collections/myprojectireland-interactive-map-and-tracker/

### Why useful

Future capital-project pipeline:

- project name;
- programme;
- responsible body;
- sector;
- location;
- stage/status;
- lifecycle stage;
- cost range;
- expected dates.

This is a forward-looking layer that can later connect to:

- procurement awards;
- public payments;
- CPO cases;
- board approvals;
- PAC/C&AG findings;
- local authority/capital programme records.

### Claude actions

1. Inspect the Gov.ie landing page.
2. Identify whether the tracker is powered by ArcGIS, StoryMap, JSON, CSV, or embedded data.
3. Probe for structured layer endpoints before scraping HTML.
4. Extract a tiny sample:
   - project name;
   - body;
   - county/location;
   - sector;
   - stage/status;
   - cost range;
   - source URL.
5. Store as `future_project_cost_range`, not spend.
6. Add source hash, retrieved timestamp, and parse confidence.

### Validation

- Every project row must have source URL.
- Cost ranges must not be summed as spend.
- Project stage vocabulary should be normalized but original value preserved.
- Missing coordinates/location should not drop row.
- Duplicate projects should be detected by project name/body/location.

### Suggested output

```text
data/silver/parquet/project_ireland_2040_tracker_probe.parquet
data/_meta/project_ireland_2040_probe_coverage.json
```

---

## 2. An Coimisiún Pleanála / Compulsory Purchase Order Cases

### Links

- https://www.pleanala.ie/en-ie/case-search
- https://www.pleanala.ie/en-ie/observations
- https://www.pleanala.ie/en-ie/weekly-lists

Search terms:

```text
site:pleanala.ie "Compulsory Purchase Order"
site:pleanala.ie "CPO" "case"
site:pleanala.ie "Local Authority Project" "Compulsory Purchase"
site:pleanala.ie "Road Development" "CPO"
```

### Why useful

CPOs are a strong **legal / infrastructure / land-acquisition signal**.

They can indicate:

- road schemes;
- housing/derelict-site schemes;
- transport projects;
- utility projects;
- local-authority land acquisition;
- objections and decisions;
- project stage before procurement/payment records appear.

### Claude actions

1. Probe the case-search interface.
2. Determine if search results can be queried by URL parameters.
3. Search specifically for CPO / compulsory purchase cases.
4. Extract sample cases:
   - case reference;
   - authority/applicant;
   - case type;
   - description;
   - lodged date;
   - decision date;
   - decision;
   - document links;
   - source URL.
5. Store at project/scheme level.
6. Avoid extracting or exposing personal landowner/property-level details by default.

### Privacy rule

Default public UI should be:

> scheme/project-level CPO context

not:

> named landowner or property-level exposure.

### Validation

- Case reference required.
- Authority/applicant required or manual-review flag.
- Decision date parsed if present.
- Document links preserved.
- Personal/property details excluded or quarantined.
- CPO status is not treated as procurement/spend.

### Suggested output

```text
data/silver/parquet/cpo_cases_probe.parquet
data/_meta/cpo_cases_probe_coverage.json
```

---

## 3. NTA Board Minutes / Approval Layer

### Links

- https://www.nationaltransport.ie/about-us/board/

Search terms:

```text
site:nationaltransport.ie "Board Meeting Minutes" "procurement"
site:nationaltransport.ie "Board Meeting Minutes" "CPO"
site:nationaltransport.ie "BusConnects" "CPO" "Board"
site:nationaltransport.ie "contract award" "Board Meeting Minutes"
```

### Why useful

NTA board minutes can reveal:

- CPO activation;
- contract approvals;
- procurement commencement;
- BusConnects decisions;
- capital programme governance;
- procurement/contract award approval context.

This source is useful as an **approval/decision layer**, not a transaction ledger.

### Claude actions

1. Scrape board page for minutes PDF links.
2. Download a small sample of recent PDFs.
3. Extract only sections containing:
   - procurement;
   - contract;
   - award;
   - CPO;
   - BusConnects;
   - MetroLink;
   - approval;
   - business case.
4. Store source page number if extracting from PDF.
5. Classify events:
   - `board_approved_procurement`
   - `contract_award_noted`
   - `cpo_activation`
   - `capital_project_update`
   - `business_case_approval`

### Validation

- Every extracted event needs source PDF URL and page number.
- Monetary values, if present, must not be treated as actual spend.
- Event classification must preserve original source text.
- Redacted sections must be marked as redacted, not parsed.
- No personal data extraction unless clearly public and necessary.

### Suggested output

```text
data/silver/parquet/nta_board_minutes_events_probe.parquet
data/_meta/nta_board_minutes_probe_coverage.json
```

---

## 4. TII Board Minutes / Approval Layer

### Links

- https://www.tii.ie/en/tii-library/reports-accounts/

Search terms:

```text
site:tii.ie "board minutes" "procurement"
site:tii.ie "board minutes" "contract award"
site:tii.ie "MetroLink" "procurement approval"
site:tii.ie "MMaRC" "contract award"
site:tii.ie "PPP" "board minutes" "TII"
```

### Why useful

TII board minutes and reports can reveal:

- road/rail/MetroLink procurement approvals;
- contract award notes;
- annual procurement lookahead;
- PPP decisions;
- capital-project governance.

### Claude actions

1. Inspect TII reports/accounts page.
2. Find board-minutes PDFs.
3. Build a small PDF fixture sample.
4. Extract only procurement/project approval events.
5. Store page-level provenance.
6. Classify event type separately from spend/payment.

### Validation

- Source URL/page number required.
- Project names normalized but original text preserved.
- Contract awards should not be treated as actual payments.
- Procurement-approval events must not be double-counted as award values unless linked to a proper award record.

### Suggested output

```text
data/silver/parquet/tii_board_minutes_events_probe.parquet
data/_meta/tii_board_minutes_probe_coverage.json
```

---

## 5. HSE Board Minutes / Governance Approvals

### Links

- https://about.hse.ie/board-and-executive/hse-board/
- https://about.hse.ie/publications/

Search terms:

```text
site:about.hse.ie "HSE Board Meeting Minutes" procurement
site:about.hse.ie "HSE Board Meeting Minutes" capital
site:about.hse.ie "HSE Board Meeting Minutes" contract
site:about.hse.ie "HSE Board Meeting Minutes" IFMS
```

### Why useful

HSE is a huge public-money source. Board minutes may add context around:

- capital approvals;
- procurement governance;
- major health infrastructure;
- IFMS/procurement systems;
- contract approvals;
- risk/audit committee notes.

### Claude actions

1. Locate current board-minutes index.
2. Prefer HTML pages where available before PDFs.
3. Probe recent minutes for procurement/capital keywords.
4. Extract only source-linked event rows.
5. Join cautiously to HSE payment/parser outputs by topic/body/date only.

### Validation

- Event source URL required.
- Page/section provenance required.
- No patient/clinical personal data extraction.
- Board-level event must not be treated as payment.
- Redactions preserved.

### Suggested output

```text
data/silver/parquet/hse_board_minutes_events_probe.parquet
data/_meta/hse_board_minutes_probe_coverage.json
```

---

## 6. C&AG Reports / Special Reports / Audit Findings

### Links

- https://www.audit.gov.ie/en/find-report/
- https://www.audit.gov.ie/en/publications/
- https://www.audit.gov.ie/en/about-us/our%20work/reports/

Search terms:

```text
site:audit.gov.ie procurement
site:audit.gov.ie "value for money"
site:audit.gov.ie "contract"
site:audit.gov.ie "overrun"
site:audit.gov.ie "Office of Public Works"
site:audit.gov.ie "Health Service Executive"
```

### Why useful

C&AG adds the accountability layer:

- audit findings;
- overspends;
- procurement weaknesses;
- project overruns;
- value-for-money findings;
- governance failures;
- public body accountability.

### Claude actions

1. Build report index first:
   - title;
   - year;
   - report type;
   - public body;
   - PDF URL;
   - publication date.
2. Do not parse all PDFs initially.
3. Create a keyword probe for:
   - procurement;
   - contract;
   - tender;
   - overrun;
   - value for money;
   - control weakness.
4. Extract small sample findings with page provenance.
5. Classify as `audit_finding`, not allegation.

### Validation

- Report metadata must be complete before fact extraction.
- Every extracted fact needs PDF URL and page number.
- Preserve original wording/excerpt.
- Do not infer wrongdoing beyond the source.
- Link to public body/year, not supplier, unless source explicitly identifies supplier.

### Suggested output

```text
data/silver/parquet/cag_reports_index.parquet
data/silver/parquet/cag_audit_findings_probe.parquet
data/_meta/cag_reports_coverage.json
```

---

## 7. Public Accounts Committee Reports / Hearings

### Links

- https://www.oireachtas.ie/en/committees/33/committee-of-public-accounts/pac-reports/
- https://www.oireachtas.ie/en/debates/find/

Search terms:

```text
site:oireachtas.ie "Committee of Public Accounts" "procurement"
site:oireachtas.ie "Committee of Public Accounts" "HSE"
site:oireachtas.ie "Committee of Public Accounts" "Office of Public Works"
site:oireachtas.ie "Committee of Public Accounts" "contract"
site:oireachtas.ie "Committee of Public Accounts" "Comptroller and Auditor General"
```

### Why useful

PAC is the parliamentary accountability layer for public expenditure.

Can link:

- C&AG reports;
- committee hearings;
- public bodies;
- procurement controversies;
- official evidence;
- recommendations;
- follow-up reports.

### Claude actions

1. Ingest PAC report metadata only first.
2. Extract:
   - report title;
   - date;
   - public body;
   - C&AG report link if present;
   - PDF/source URL.
3. Later link debates/hearings by topic/body.
4. Avoid summarizing allegations unless source-backed.

### Validation

- Report URL required.
- Publication date required.
- Public body extraction confidence tracked.
- C&AG/PAC links preserved.
- Avoid supplier-level matching unless explicitly named.

### Suggested output

```text
data/silver/parquet/pac_reports_index.parquet
data/_meta/pac_reports_coverage.json
```

---

## 8. Revised Estimates Volume / Voted Public Expenditure Databank

### Links

- https://www.gov.ie/en/department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation/collections/the-revised-estimates-volumes-for-the-public-service/
- https://www.gov.ie/en/department-of-public-expenditure-infrastructure-public-service-reform-and-digitalisation/collections/voted-expenditure-databank/

Search terms:

```text
site:gov.ie "Revised Estimates Volume" CSV
site:gov.ie "Voted Expenditure Databank"
site:gov.ie "REV" "Public Service" "Programme"
site:gov.ie "Gross Voted Expenditure"
```

### Why useful

Macro budget/appropriation context:

- department vote;
- programme;
- subhead;
- current/capital allocation;
- year;
- estimates vs outturn context where available.

This is useful to contextualize public money, but not supplier-level spend.

### Claude actions

1. Identify structured files first: XLSX/CSV if available.
2. Extract vote/programme/subhead/year.
3. Store as:
   - `budget_allocated`
   - `voted_expenditure`
4. Join only to departments/public bodies/programmes.
5. Do not mix into supplier totals.

### Validation

- Vote/year required.
- Programme/subhead original text preserved.
- Current/capital distinction preserved.
- Values not joined to supplier spend.
- Source file URL and sheet/table reference required.

### Suggested output

```text
data/silver/parquet/voted_expenditure_databank.parquet
data/_meta/voted_expenditure_databank_coverage.json
```

---

## 9. Local Authority Statutory Audit Reports

### Links

- https://www.gov.ie/en/department-of-housing-local-government-and-heritage/collections/audit-reports-since-2012/

Search terms:

```text
site:gov.ie "Audit Reports since 2012" "County Council"
site:gov.ie "Local Government Audit Service" "procurement"
site:gov.ie "Local Government Audit Service" "Chief Executive"
site:gov.ie "Local Authority" "audit report" "procurement"
```

### Why useful

This is the accountability layer on top of your existing local-authority payments and AFS work.

Can add:

- council/year audit findings;
- procurement weaknesses;
- governance issues;
- financial controls;
- auditor commentary;
- follow-up flags.

### Claude actions

1. Ingest index first:
   - council;
   - year;
   - PDF URL;
   - publication date.
2. Parse small sample PDFs.
3. Extract only structured/table-like facts or clearly labelled findings.
4. Join to LA payments/AFS by council/year only.

### Validation

- Council/year/PDF URL required.
- Page provenance required for extracted findings.
- Findings should be labelled `audit_finding`.
- Do not infer misconduct.
- Do not join to named suppliers unless source explicitly identifies them.

### Suggested output

```text
data/silver/parquet/local_authority_audit_reports_index.parquet
data/silver/parquet/local_authority_audit_findings_probe.parquet
data/_meta/local_authority_audit_reports_coverage.json
```

---

## 10. Grant / Subvention Datasets

### Links / discovery

- https://data.gov.ie/dataset
- https://www.gov.ie/en/
- https://www.sportscapitalprogramme.ie/
- https://www.gov.ie/en/department-of-tourism-culture-arts-gaeltacht-sport-and-media/collections/sports-capital-programme/

Search terms:

```text
site:data.gov.ie grant allocations CSV
site:data.gov.ie "grant" "allocations"
site:gov.ie "grant allocations" "CSV"
site:gov.ie "payments" "grant scheme"
site:sportscapitalprogramme.ie allocations payments
site:gov.ie "Sports Capital Programme" allocations
site:gov.ie "Housing Adaptation Grant" dataset
```

### Why useful

Public money outside procurement:

- grants;
- subventions;
- allocations;
- scheme payments;
- sports/community/housing/climate/enterprise funding.

This should not be mixed with procurement totals.

### Claude actions

1. Build a discovery list of structured grant datasets only.
2. Prefer CSV/XLSX/JSON.
3. Extract:
   - scheme;
   - awarding department/body;
   - recipient;
   - county/location;
   - amount;
   - allocation/payment distinction;
   - year;
   - source URL.
4. Store as `grant_or_subvention`.
5. Keep separate from procurement/public payments.

### Validation

- Scheme/source required.
- Amount semantics required:
   - allocated;
   - paid;
   - approved;
   - drawdown;
   - unknown.
- Recipient names may include clubs/individuals; privacy review required.
- Source URL required.
- Do not sum allocated and paid values together.

### Suggested output

```text
data/silver/parquet/grant_allocations_probe.parquet
data/_meta/grant_allocations_probe_coverage.json
```

---

## 11. Data.gov.ie Council Budget Tables

### Links / discovery

- https://data.gov.ie/dataset
- Example search URL: https://data.gov.ie/en_GB/dataset?theme=Government

Search terms:

```text
site:data.gov.ie "Annual Budget" "County Council"
site:data.gov.ie "Budget" "City Council" CSV
site:data.gov.ie "adopted budget" "council"
site:data.gov.ie "Fingal County Council" "Budget"
site:data.gov.ie "Local Authority Budget" "CSV"
```

### Why useful

This can complement AFS and payments:

- planned budget;
- adopted budget;
- service division;
- programme;
- income/expenditure;
- council/year.

This is not actual spend.

### Claude actions

1. Since LA AFS/budget probes already exist, treat data.gov.ie as a discovery catalogue.
2. Find councils with structured CSV/JSON budget tables.
3. Extract one sample council/year.
4. Normalize to `budget_allocated`.
5. Join to LA AFS/payments only at council/year/service-division level.

### Validation

- Council/year required.
- Budget/adopted estimate semantics preserved.
- Service division normalized.
- Values not mixed with AFS actuals or PO/payment rows.
- Source file URL and dataset page URL preserved.

### Suggested output

```text
data/silver/parquet/la_budget_tables_probe.parquet
data/_meta/la_budget_tables_probe_coverage.json
```

---

## 12. FOI / AIE Disclosure Logs

### Links / discovery

- https://data.gov.ie/dataset
- https://www.gov.ie/en/organisation-information/
- Search within public-body FOI pages.

Search terms:

```text
site:data.gov.ie "FOI disclosure log"
site:data.gov.ie "AIE disclosure log"
site:gov.ie "FOI disclosure log" procurement
site:gov.ie "FOI disclosure log" contract
site:gov.ie "AIE disclosure log" "CPO"
site:nationaltransport.ie "FOI Disclosure Log"
site:hse.ie "FOI disclosure log"
```

### Why useful

FOI/AIE logs can identify:

- records already requested;
- procurement/capital-project topics;
- repeated suppliers/public bodies;
- CPO/environmental requests;
- possible investigative leads.

This is a lead-generation layer, not proof of facts.

### Claude actions

1. Probe 2–3 bodies only.
2. Extract:
   - request date;
   - public body;
   - topic/summary;
   - decision;
   - request category;
   - source URL;
   - year/quarter.
3. Classify as `foi_lead`.
4. Do not infer underlying facts from request summaries.
5. Avoid personal requester details if present.

### Validation

- Source URL required.
- Requester names removed if present.
- Topic text preserved.
- Decision/status parsed if available.
- FOI logs should not be joined to suppliers unless the supplier is explicitly named in request summary and confidence is high.
- UI must label as “FOI request/disclosure log lead”.

### Suggested output

```text
data/silver/parquet/foi_aie_disclosure_logs_probe.parquet
data/_meta/foi_aie_disclosure_logs_probe_coverage.json
```

---

## Suggested Priority Order

1. **Project Ireland 2040 tracker**  
   Best future-project layer.

2. **An Coimisiún Pleanála CPO cases**  
   Strong legal/infrastructure signal.

3. **NTA + TII board minutes**  
   Strong approval/procurement decision layer.

4. **C&AG + PAC report metadata**  
   Accountability layer; start with metadata only.

5. **REV / Voted Expenditure Databank**  
   Macro budget context.

6. **Local authority statutory audit reports**  
   Accountability layer on top of LA payments/AFS.

7. **Grant/subvention datasets**  
   Public money outside procurement.

8. **FOI/AIE logs**  
   Later investigative-lead layer.

---

## Recommended First PR

Keep it small.

### Scope

Pick **one** source lane only.

Best first source:

```text
Project Ireland 2040 tracker
```

Alternative first source:

```text
An Coimisiún Pleanála CPO cases
```

### Deliverables

```text
pipeline_sandbox/<source>_probe.py
data/silver/parquet/<source>_probe.parquet
data/_meta/<source>_probe_coverage.json
test/test_<source>_probe.py
```

### Acceptance Criteria

- source can be fetched reproducibly;
- source hash stored;
- sample rows produced;
- source URL preserved;
- source value type clearly labelled;
- privacy risk assessed;
- parser has fixture tests;
- output is not wired into public app yet;
- go/no-go recommendation included in metadata.

---

## Public UI Rule

Do not add a new page for these immediately.

First use them as enrichment/context for future:

```text
Public Money Explorer
Supplier Dossier
Public Body Profile
Infrastructure Project Profile
FOI Lead Pack
```

Each public fact must link to source and preserve caveats.
