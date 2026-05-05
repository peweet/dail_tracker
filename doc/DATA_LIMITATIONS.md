# DATA_LIMITATIONS.md

Known data-quality issues, scope decisions, and silent-failure risks in the `dail_tracker` pipeline.

This project joins public Oireachtas data, Oireachtas-published PDFs, and lobbying.ie CSV exports. It is not an official record and should not be treated as a complete model of a politician's work, income, influence, conflicts of interest, or attendance.

The upstream source remains authoritative in every case.

---

## 1. Current scope

### 1.1 Sitting members only

The project is focused on currently sitting members of the current Dáil and Seanad where supported by the pipeline.

Former TDs and Senators are not the primary target of the live dataset.

**Reason:** the project is intended to support current democratic accountability. Historical data remains available from upstream sources, but joining every historical member would add complexity, privacy weight, and name-resolution risk.

### 1.2 Current Dáil focus

The Dáil member pipeline is currently centred on the 34th Dáil.

Some source pulls and derived outputs may contain broader or older records where the upstream API returns them, but the intended analytical surface is the current membership.

### 1.3 General 2020 cut-off for PDF-derived data

PDF-derived data should be treated as reliable only from around **2020 onward**, unless a specific parser has been written and validated for an older source layout.

This applies especially to:

- Register of Members' Interests PDFs
- Attendance PDFs
- Parliamentary Standard Allowance / payment PDFs

Pre-2020 PDFs often use substantially different layouts, headers, footers, table structures, line wrapping, and page-break behaviour. The current extractors are written against the more recent layouts. Backfilling older PDFs would require separate parser rules and separate validation.

**Important:** 2020 is a practical parsing boundary, not a legal or historical boundary.

---

## 2. Office holders, interests, and public disclosure gaps

### 2.1 Office holders

Some office holders are not represented in the same way as ordinary members in the published Register of Members' Interests.

This can affect, for example:

- Taoiseach
- Tánaiste
- Ministers
- Ministers of State
- Attorney General
- Ceann Comhairle
- Leas-Cheann Comhairle
- Some office holders or committee roles, depending on applicable rules

Where interests are missing for office holders, this is often a limitation of the public disclosure regime rather than a parser failure.

The dataset should therefore not be read as: "blank interests = no interests".

### 2.2 Family and household holdings

The dataset cannot reliably expose property, shares, business interests, or income held through spouses, partners, children, relatives, companies, trusts, or other indirect structures unless they are declared in the published source.

This is a source-data limitation. The pipeline cannot infer undisclosed interests.

### 2.3 Nil returns versus extraction failure

A member may have:

- No interests declared
- A blank source section
- A section that was not extracted correctly
- No published return in the relevant source

These cases can look similar in derived data. Where possible, downstream tables should preserve explicit flags for `nil`, `missing`, and `extraction_failed`, but this is not yet fully implemented.

---

## 3. PDF extraction limitations

The PDF pipeline relies on PyMuPDF table and text extraction. It is inherently brittle.

PDFs are presentation documents, not structured datasets. Small visual changes can break extraction without producing a hard error.

Known risks:

- Table columns shift between years
- Headers and footers are read as data
- Page breaks split a member's entry
- Names are embedded in the previous line of text
- Multi-line descriptions are merged incorrectly
- Amounts, dates, or category labels may be misread
- A parser may succeed technically while producing incomplete rows

The 2020+ cut-off reduces this risk but does not eliminate it.

---

## 4. Attendance limitations

Attendance data is based on official attendance PDFs.

It should not be interpreted as a full measure of parliamentary work.

The attendance data does **not** capture:

- Committee attendance
- Constituency work
- Ministerial duties outside the chamber
- Official travel
- Pairing arrangements
- Illness, maternity/paternity leave, or other justified absences
- Informal parliamentary work not recorded in the attendance PDF

A low plenary attendance figure does not automatically mean a member was inactive.

### 4.1 PDF layout risk

Attendance PDFs are parsed from tables. If a table layout changes, rows may be missed or columns may be misread.

The parser should be treated as best-effort, not as an official attendance engine.

---

## 5. Payments limitations

Payment data is extracted from official Parliamentary Standard Allowance / payment PDFs.

Known risks:

- Amounts may include symbols, commas, or OCR-like artefacts even in text PDFs
- Dates may be misread or omitted
- Table extraction can pull header/footer rows into the data
- Exceptional or unusual rows may be dropped by cleaning rules
- Dedupe logic can collapse rows if name, amount, and payment date appear identical

Payment totals are therefore derived figures. They should be checked against the original PDF before publication or reporting.

---

## 6. Member interests limitations

The Register of Members' Interests parser groups text into categories such as occupations, shares, land/property, directorships, gifts, travel facilities, remunerated positions, and contracts.

Known risks:

- Category headings may change between years
- A long declaration can be split across pages
- A member name can appear embedded in the previous member's final line
- `Nil`, `No interests declared`, and blank sections need careful handling
- Free-text descriptions are not fully normalised
- Property ownership and landlord flags are derived heuristics, not legal classifications

The parser is useful for search and exploration, but the official PDF should be checked before making claims about an individual member.

---

## 7. Lobbying.ie data limitations

### 7.1 Manual export, not live API

The lobbying.ie data is based on manually downloaded CSV exports.

There is no live API integration in the current pipeline. The dataset is therefore a snapshot of whatever CSV files were placed in the raw lobbying directory.

If a reporting period is missing from the downloaded files, the pipeline cannot know that automatically.

### 7.2 Practical cut-off for lobbying data

Lobbying data before roughly **2018/2019** is substantially messier to parse. The pipeline should be treated as reliable mainly for the more regular post-2018/post-2019 exports, with a practical analytical preference for **2020 onward**.

Older lobbying exports may contain inconsistent quoting, inconsistent separators, missing fields, and harder-to-split multi-value strings.

### 7.3 Split syntax used by lobbying.ie exports

Several lobbying.ie fields encode multiple values inside a single CSV cell.

The parser assumes two levels of splitting:

#### `::` separates repeated records

Example:

```text
Person A|Minister|Dáil::Person B|TD|Dáil
```

This means there are two lobbied public officials in one return.

#### `|` separates fields inside one record

Example:

```text
Person A|Minister|Dáil
```

This is split into:

- `full_name`
- `position`
- `chamber`

### 7.4 Fields using this pattern

`dpo_lobbied` is treated as:

```text
Name|Position|Chamber::Name|Position|Chamber
```

`lobbying_activities` is treated as:

```text
Action|Delivery method|Members targeted::Action|Delivery method|Members targeted
```

`clients` is treated as:

```text
Client name|Client address|Email|Telephone
```

`current_or_former_dpos` is treated as:

```text
Name|Position|Chamber
```

### 7.5 Exploding rows changes row counts

When a return names multiple politicians or multiple lobbying activities, the pipeline explodes that single source row into multiple analytical rows.

This is necessary for person-level and activity-level analysis, but it creates a counting risk.

For example, one lobbying return with:

- 3 named politicians
- 2 lobbying activities

can produce multiple rows after explosion.

Summary tables must therefore deduplicate by `primary_key` where the intended measure is "number of returns" rather than "number of exploded rows".

### 7.6 Delimiter risk

The split logic assumes that `::` and `|` are structural separators.

If either character appears inside a free-text field, or if lobbying.ie changes its export format, the parser may split a value incorrectly.

### 7.7 Collective targets are not individual people

Some returns target broad groups such as:

- `Dáil Éireann (all TDs)`
- `Seanad Éireann (all Senators)`
- `All Oireachtas members`
- `Members of Government`

These are not individual politicians. Person-level lobbying tables may drop or separate these records to avoid falsely attributing a return to every member.

This means person-level lobbying counts do not fully represent broad institutional lobbying.

---

## 8. Oireachtas API limitations

The Oireachtas API is the primary structured source for members, legislation, questions, debates, and votes.

Known risks:

- API schemas can change without the local flattening code failing clearly
- Some scripts use fixed `limit` values
- Not all endpoints implement full pagination in the local pipeline
- Date filters differ by endpoint
- A successful HTTP response can still contain partial or unexpected data

### 8.1 Pagination and truncation

Several API calls use large `limit` values, but a large limit is not the same as pagination.

Where the pipeline does not loop over `skip` / `limit`, records beyond the first returned page may be silently omitted.

This is especially relevant for high-volume endpoints such as questions, legislation, or votes.

### 8.2 Date windows are not identical

Different API pulls use different date ranges.

Examples of current practical behaviour:

- Members are scoped to the current chamber membership
- Questions are treated as a more recent analytical surface
- Legislation can reach further back than the current Dáil
- Votes include Dáil vote records but require careful interpretation by date and house number

This means different tables may not cover the exact same time period.

---

## 9. Legislation and votes limitations

Legislation, sponsors, stages, debates, and votes are flattened from nested API payloads.

Known risks:

- Multi-sponsor bills require careful handling to avoid over- or under-attribution
- Debate-to-bill matching is not always straightforward
- Vote records need house, date, and vote ID context
- A member's vote record can change if corrections are made upstream
- Vote URLs are generated from fields in the API response and should be checked against Oireachtas.ie before use

A vote row is a structured pointer into the official record, not a complete political interpretation.

---

## 10. External resource URL construction

Some outputs include links back to public source pages such as lobbying.ie returns, lobbying.ie organisation pages, Oireachtas bills, votes, and debate records.

These URLs are convenience links for verification and navigation. They are not the canonical data source used by the pipeline, and a broken link does not necessarily mean the underlying row is invalid.

### 10.1 Methodology

The pipeline uses the following order of preference when attaching external URLs:

1. Use an official URL or URI from the source payload where one exists.
2. Use a manually supplied lookup table where the source site does not expose a reliable route.
3. Build a best-effort URL from stable fields such as source ID, primary key, house, chamber, date, year, bill number, vote number, debate section, or organisation identifier.
4. Keep the source row even if a URL cannot be resolved.

The URL field should be treated as a pointer back to the public source, not as a primary key.

### 10.2 Lobbying.ie return URLs

Lobbying.ie CSV exports contain useful IDs, but the public website does not always provide a clean, stable, documented permalink for every return in the export.

Current approach:

- If `lobbyist/raw/lobby_urls.csv` exists, join it on `primary_key` and use the supplied `lobby_url`.
- If no lookup exists, fall back to a generated URL of the form:

```text
https://www.lobbying.ie/return/{primary_key}
```

This fallback may not work for every row. The `primary_key` in the CSV is not guaranteed to be the same as the public route expected by the website.

For organisation pages, the pipeline builds links from the organisation identifier and a slugified organisation name:

```text
https://www.lobbying.ie/organisation/{lobby_issue_uri}/{name-as-lowercase-hyphen-slug}
```

This can fail where:

- The organisation name has changed since the export was downloaded
- The website slug differs from the locally generated slug
- Special characters, punctuation, Irish names, company suffixes, or duplicate names are handled differently by lobbying.ie
- The organisation page exists but redirects to a different canonical URL

### 10.3 Oireachtas bills, votes, and debates

For Oireachtas public pages, the pipeline should prefer official URI fields from the API response where available. These are more reliable than reconstructing public web URLs from text.

Where a direct public URL is not available, a best-effort link may be constructed from fields such as:

- House or chamber (`dail`, `seanad`)
- Dáil or Seanad number
- Sitting date
- Bill year and bill number
- Vote or division identifier
- Debate date or debate section identifier
- Language suffix where relevant

These links can fail because Oireachtas public pages are not always one-to-one with API records.

Examples of failure modes:

- A bill has multiple related documents, stages, amendments, or debate records
- A vote is attached to a division, sitting, or debate section rather than a simple standalone page
- Debate pages may use anchors or section IDs that change over time
- The public website may redirect, rename, or reorganise pages while the API URI remains valid
- The relevant date may be a sitting date, publication date, stage date, or debate date, and these are not interchangeable
- Irish characters, apostrophes, punctuation, and whitespace can produce different slugs from the official site

A generated Oireachtas URL is therefore a verification aid, not proof that the row has been fully matched to the correct public page.

### 10.4 URL checking

Where URLs are checked automatically, the checker should record at least:

- Requested URL
- Final URL after redirects
- HTTP status
- Content type
- Content length where available
- Last modified header where available
- Check timestamp

A failed URL check should be interpreted carefully.

Common non-data reasons for failure:

- The site blocks `HEAD` requests even though `GET` works
- The page redirects through a search or language route
- The page is temporarily unavailable
- The site changes route structure
- Query-string filters are session-like or not durable
- A PDF or public page has moved but the source record is still valid

Broken or unresolved URLs should be flagged, not used as a reason to drop the underlying data row.

### 10.5 Public-use rule

Before citing a specific bill, vote, debate, lobbying return, or organisation page, manually open the generated URL and confirm that it points to the expected public record.

If the generated URL fails, use the stored source fields to search the upstream website manually.

---

## 11. Name matching and join keys

Several source datasets do not provide a common stable ID.

PDF sources often provide only a human-readable name. Names vary across sources because of:

- Irish-language characters
- Apostrophes
- Hyphens
- Initials
- Titles such as `Minister`, `Senator`, `Deputy`, `Dr`, `Mr`, `Ms`
- Reordered first name / last name formats
- Encoding differences

The pipeline uses normalised join keys as a pragmatic workaround.

Known risks:

- Spelling variants can fail to join
- Two different names can collide after normalisation
- Title stripping can remove meaningful text in edge cases
- A successful join may still be wrong if two names are too similar
- Some joins require manual inspection

The join key is an engineering compromise, not a legal identity key.

---

## 12. Output and caching limitations

The project uses a medallion-style directory structure under `data/`:

- `bronze` for raw API/PDF/CSV inputs
- `silver` for cleaned and transformed outputs
- `gold` for joined or analytical outputs

Known risks:

- Some scripts skip work if an output file already exists
- Old CSV or Parquet files may remain after upstream data changes
- Manually downloaded CSVs can be incomplete or duplicated
- Intermediate files can be regenerated in a different order if scripts are run manually
- CSV and Parquet outputs may not always be perfectly aligned if one write fails

For a clean rebuild, delete generated outputs and rerun the pipeline from the start.

### 12.1 Cron-staleness traps (audit 2026-05-05)

The current pipeline was designed for interactive runs. Several behaviours become silent staleness bugs once it is moved onto a recurring schedule:

- **Oireachtas API steps no-op after first run.** `services/oireachtas_api_main.py` short-circuits members, legislation, questions, and votes with an `output_exists` check (overwrite flags hard-coded `False`). On a daily cron the second and subsequent runs fetch zero new data, but the run itself reports success. New TDs, new bills, new questions, and new votes do not land until the flag is flipped manually. Tracked in `DAIL-160`.
- **PSA payments / attendance / member-interests URLs are hard-coded.** `pdf_endpoint_check.py` lists every monthly payment PDF and every annual register/attendance PDF as a Python literal. The discovery probe in `pipeline_sandbox/payment_pdf_url_probe.py` is unwired; `pdf_backfill_scraper.py` is a stub. New publications are only ingested after a human edits the URL list. Tracked in `DAIL-161`.
- **PDF re-issues at the same URL are invisible.** `pdf_downloader.py` skips on `destination.exists()`. If the publisher corrects a PDF in place (same URL, new bytes), the new version is never downloaded. The gold layer can drift permanently from the source. Tracked in `DAIL-162`.
- **`pipeline.py` halts on first failure.** A `break` after a single failed step skips every downstream source for the remainder of the run. One transient lobbying-CSV parse error nukes attendance, payments, votes, and enrichment. Tracked in `DAIL-163`.
- **Endpoint-check signal is not gated.** `pdf_endpoint_check.endpoint_checker` returns a list of broken URLs but `pipeline.py` never reads it. A run where every URL 4xx's still exits successfully. Tracked in `DAIL-164`.
- **Iris ETL re-extracts every PDF on every run.** ~1k PDFs × full PyMuPDF + regex pass on each invocation. Sandbox-staged shard caching exists in `pipeline_sandbox/iris_incremental_shards.py` but is not wired into the active script. Tracked in `DAIL-165`.
- **No per-source freshness manifest at gold.** `manifest.py` records run start/end only. There is no per-dataset "last upstream fetch" timestamp, so neither the UI nor a monitoring job can answer "is this data fresh?" without reading file mtimes. Tracked in `DAIL-166`.
- **Lobbying acquisition is fully manual.** The lobbying.ie ingestion path expects a human to log in, export the CSV, and drop it into `LOBBYING_RAW_DIR`. There is no scheduled component. Tracked in `DAIL-167` and the existing `DAIL-116`–`DAIL-119` lobbying-export track.

Until these are addressed, treat any "data as of <date>" claim derived from a cron run with caution. The run-finished timestamp does not imply that any of the upstream sources were re-pulled on that run.

### 12.2 Deltas not currently watched

Some kinds of upstream change have no automated detection at all:

- **TD turnover** (by-elections, resignations, party defection, vacancies) — `unique_member_code` joins go stale silently.
- **Committee membership rotations** — fetched from the API but only on overwrite (see 12.1).
- **Ministerial reshuffles** — same path; ministerial-office values cache until a forced refresh.
- **eISB-side SI amendment or revocation** — `eisb_url` is stamped at ingest and never re-resolved, so an Iris notice can be displayed for an SI that has since been revoked.
- **SIPO determinations and donations** — adjacent ethics dataset, not ingested.
- **Wikidata / Wikipedia biographical fields** — `test_wiki_data.py` exists outside the pipeline; not joined.

These are noted here for transparency, not because there is an active plan to ingest them in the next phase.

---

## 13. Testing and validation status

Testing exists, but it is work in progress.

Current validation should be treated as partial coverage only.

Known gaps:

- Not every source has fixture-based tests
- Not every parser has year-by-year regression tests
- Row-count assertions are incomplete
- Schema validation is incomplete
- Manual source checks are still needed for high-stakes claims

Tests are intended to catch obvious regressions. They do not prove that every extracted row is correct.

---

## 14. Interpretation warnings

### 14.1 Do not infer wrongdoing from presence in the data

A declared interest, lobbying contact, payment, or attendance figure is not evidence of wrongdoing.

These datasets show public records. They do not establish motive, conflict, misconduct, or causation.

### 14.2 More disclosure can mean more transparency

A member with many declared interests may simply be disclosing more completely than another member.

The absence of declared interests is not proof that no interests exist.

### 14.3 Lobbying contact is not proof of influence

A lobbying return shows that lobbying was reported. It does not prove that the lobbying changed a vote, question, policy position, or legislative outcome.

### 14.4 Attendance is not total work

Plenary attendance is only one visible slice of parliamentary work.

Committee work, constituency work, ministerial duties, and other official work are not fully represented in the attendance PDFs.

---

## 15. Minimum verification before public use

Before using the data in reporting, publication, or public claims:

1. Check the official upstream source.
2. Confirm the relevant date range.
3. Check whether the member was a TD, Senator, Minister, office holder, or former member at the time.
4. Check whether the row came from API data, PDF extraction, or manual CSV export.
5. For lobbying data, confirm whether the count is based on distinct returns or exploded activity rows.
6. For interests data, check whether a blank means `nil`, `not published`, `not extracted`, or `not applicable`.
7. For generated URLs, manually open the link and confirm it points to the expected bill, vote, debate, lobbying return, or organisation page.

