# DATA_LIMITATIONS.md

Known data-quality issues, scope decisions, and silent-failure risks in the `dail_tracker` pipeline.

This project joins public Oireachtas data, Oireachtas-published PDFs, and lobbying.ie CSV exports. It is not an official record and should not be treated as a complete model of a politician's work, income, influence, conflicts of interest, or attendance.

The upstream source remains authoritative in every case.

---

## 1. Current scope

### 1.1 Member-level focus on sitting members (with one historic exception)

The member-level pipeline is focused on currently sitting members of the current Dáil and Seanad where supported by the pipeline.

Former TDs and Senators are not the primary target of the live dataset, with one deliberate exception: the **Register of Members' Interests** ("What They Own" page) backfills historic declarations so the property/shares/company record spans former members too, recovering declarers that drop out of the year-scoped members API.

**Reason:** the project is intended to support current democratic accountability. Historical data remains available from upstream sources, but joining every historical member for every dataset would add complexity, privacy weight, and name-resolution risk.

### 1.1a State-level datasets are not member-scoped

The project has expanded well beyond individual-member data. Several major datasets describe the state rather than named TDs/Senators and have their own scope and time windows:

- Procurement awards and public-body payments (departments, agencies, HSE/Tusla, 31 local authorities) — §16, §17
- Corporate notices and regulated-entity cross-references (Iris Oifigiúil, CRO, Central Bank registers) — §15
- Courts and judiciary (bench roster, appointments, court performance, Legal Diary) — §18
- Ministerial diaries and their lobbying-register cross-reference — §19
- Political finance (SIPO donations and GE2024 election spending) — §20
- Local-authority accountability (annual financial statements, NOAC collection rates, derelict-sites levy, planning-appeal overturn rates) — §21
- CSO statistical data (housing supply on the Constituency page; the CPI deflator used to put procurement values in real terms) — §22

These carry the limitations documented in their own sections below and do not inherit the "sitting members only" scope.

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

### 12.1 Cron-staleness traps (audit 2026-05-05; status updated 2026-06-20)

The pipeline was originally designed for interactive runs, and a 2026-05-05 audit catalogued behaviours that become silent staleness bugs once it is moved onto a recurring schedule. Several have since been addressed; the remainder are still open. Status is noted per item.

**Resolved or substantially mitigated:**

- **`pipeline.py` halts on first failure → RESOLVED.** `pipeline.py` is now a thin dispatcher that wraps each domain chain in its own try/except and writes a per-chain manifest. One flaky source no longer poisons the rest of the run; the end-of-run summary lists which chains failed. (was `DAIL-163`.)
- **No per-source freshness manifest → RESOLVED.** The `freshness` chain writes `data/_meta/freshness.json` (data-age per domain) and the `source_health` chain writes `data/_meta/source_health.json` (per-source staleness / reachability). The UI and monitoring jobs can now answer "is this data fresh?" without reading file mtimes. (was `DAIL-166` / `DAIL-164`.)
- **Oireachtas API steps no-op after first run → FIXED.** The members / legislation / questions / votes overwrite path no longer short-circuits on a daily cron. (was `DAIL-160`.)
- **PDF re-issues at the same URL are invisible → FIXED.** The downloader no longer skips purely on `destination.exists()`. (was `DAIL-162`.)

**Still open:**

- **PSA payments / attendance / member-interests URLs are hard-coded.** `pdf_endpoint_check.py` lists every monthly payment PDF and every annual register/attendance PDF as a Python literal. New publications are only ingested after a human edits the URL list. Tracked in `DAIL-161`.
- **Iris ETL re-extracts every PDF on every run.** ~1k PDFs × full PyMuPDF + regex pass on each invocation. Sandbox-staged shard caching exists in `pipeline_sandbox/iris_incremental_shards.py` but is not yet wired into the active script. Tracked in `DAIL-165`.
- **Lobbying acquisition is fully manual.** The lobbying.ie ingestion path expects a human to log in, export the CSV, and drop it into `LOBBYING_RAW_DIR`. There is no scheduled component. Tracked in `DAIL-167` and the existing `DAIL-116`–`DAIL-119` lobbying-export track.

The read-only monitoring chains (`freshness`, `source_health`, `output_regressions`) and the scheduled GitHub Actions canaries now surface most staleness, but a fully automated cloud refresh of the data itself is still pending. Until that lands, treat any "data as of <date>" claim derived from a cron run with caution: the run-finished timestamp does not by itself guarantee every upstream source was re-pulled on that run.

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

## 15. Corporate Notices page — sandbox cross-references and heuristics

The Corporate Notices page surfaces five distinct overlays beyond the raw Iris feed: a receiver-appointer ranking with parent-fund typing, a receiver-firm concentration strip, a CBI register cross-reference, a corporate-rescue panel, and a methodology expander. Each carries its own caveats.

### 15.1 CBI register substrate is sandbox-grade

`data/sandbox/parquet/cbi_authorised_firms.parquet` (13.8k rows) is produced by `pipeline_sandbox/cbi_registers_extract.py`, which parses 56 of the 59 CBI downloads-page register PDFs via a heuristic multi-layout extractor. Known issues:

- PDF extraction picks up some header / boilerplate strings as firm names (e.g. `"LEI Code"`, `"Register of Credit Servicers Passporting In"`). These false positives sit in the substrate but are filtered out by the strict matching rules used in the UI cross-references.
- Two registers (CIT Providers, Designated Entities) fail the postback request entirely and are absent.
- Address, authorisation date, status, and classes of business are **not** captured — only firm name and CBI reference number.
- The substrate is a snapshot, not a continuously updated feed. Re-run the script to refresh.

### 15.2 Brand → parent fund alias map is hand-curated, citation-anchored

`data/_meta/loan_book_fund_aliases.csv` is the single source of truth for the receiver-appointer panel's vulture / Irish bank / credit servicer / state classification. ~30 alias entries cover ~32% of receivership notices. Known issues:

- Long-tail SPVs not in the alias map display under their original brand and contribute to the untagged ~68% rather than rolling up to a parent.
- Each row in the CSV must carry a `notes` column citation — adding aliases without sourcing them violates the no-inference editorial rule.
- Mapping is substring-based on UPPERCASE raw_text; a brand string that overlaps another (e.g. `OAKTREE` inside a hypothetical `OAKTREE LIMITED` not run by Oaktree Capital) could produce a false attribution. None observed at current scale.

### 15.3 Receiver-firm operator strip is regex notice-presence, not appointed-role

The "Who's doing the work" sub-strip counts notices whose `raw_text` mentions one of ~20 known professional firms (Big 6 accountancy + boutique insolvency firms). Each firm is counted at most once per notice. Known limitations:

- A notice mentioning a firm does not strictly mean that firm was the appointed receiver — the firm could appear in a different role (auditor, advisor, charge-holder reference). Most appearances in receivership notices ARE the appointed receiver, but the regex match is approximate.
- Case-sensitive matches are used for short abbreviations (`EY`, `KPMG`, `BDO`, `RBK`, `OCKT`) to avoid lowercase matches inside unrelated words.
- "Big 6 accountancy firms" is the Irish-industry shorthand for the seven-firm set used in the page (PwC, Deloitte, EY, KPMG, Grant Thornton, BDO, Mazars). The label is broadly accepted but slightly fuzzy at the edges.

### 15.4 SPV / DAC headline is a name-shape heuristic

The appointer-panel subhead reports notices whose `entity_name` contains `DAC`, `Designated Activity Company`, or `ICAV`. This is a name-shape match, not a regulatory classification. It cannot distinguish a Section 110 securitisation SPV from a regulated DAC that happens to share the legal form. The labelling on the page deliberately states *what was matched* (the substring), not *what it means* (a Section 110 vehicle).

### 15.5 Entity-name extraction quality

The `entity_name` column in `corporate_notices.parquet` is heuristic — for ~24% of rows it contains junk extractions (statutory text headers, notice-body prefixes, partial sentences). The Corporate Notices page filters these via `_JUNK_RE` in `corporate.py` when rendering cards, falling back to `display_title` or a graceful "Company name not extracted" label. The CBI cross-reference uses a normalised substring match which tolerates extraction prefixes ("presented to the High Court by Independent Trustee Company Limited" still matches "Independent Trustee Company").

### 15.6 CBI repeat-distress panel filters out solvent fund lifecycle

The "Regulated firms with repeat distress notices" panel (`v_corporate_cbi_repeat_distress`) deliberately excludes Members' Voluntary Liquidation (MVL) from its HAVING clause, because MVL is a solvent wind-up — fund-lifecycle, not distress. Without this filter, Vanguard / iShares / Invesco ETF sub-fund closures would dominate the panel and obscure the genuine distress signal (Independent Trustee Company, M&F Finance Ireland, etc.). This is a deliberate editorial choice documented in the SQL view header.

### 15.7 Sandbox status

All Corporate Notices CBI cross-references live in `data/sandbox/parquet/` and `sql_views/corporate_cbi_distress.sql`. They are **not** part of the gold layer. If the sandbox is wiped, the Corporate Notices page degrades gracefully — the receiver-appointer panel, rescue panel, and feed continue to render; the CBI panel and badges simply disappear.

### 15.8 Editorial rule for Corporate Notices UI copy

Per the project-wide no-inference rule (also documented as a memory feedback note), all UI copy on the Corporate Notices page is restricted to claims derivable from the parquet layer. Historical context (post-2008 loan-book sales, COVID-19 mortgage moratorium, vulture-fund era) is **not** asserted in panel subheads or sparkline annotations. Where such context is necessary for understanding, it lives only in the Sources & methodology expander or external cited footnotes.

---

## 16. Procurement awards (eTenders and TED)

Two award registers sit side by side and **must never be unioned or summed** — roughly 66% of TED winners also appear in eTenders by normalised name, so they are siblings cross-referenced per firm, not additive lanes.

**eTenders (national):**

- Covers **2013–2026**, from the OGP open-data CSV on data.gov.ie. One row per award × supplier.
- **"Awarded value" is not money paid.** It is the estimated contract value at the point of award. Framework and Dynamic Purchasing System (DPS) notices carry notional multi-year *ceilings*, and a multi-supplier framework repeats one ceiling across every supplier row. Only the `value_safe_to_sum` subset may be totalled — the naive sum (~€649bn) overstates the sum-safe figure (~€15.6bn) by roughly 40×.
- 651 rows (125 distinct suppliers) have a leading capital letter dropped in the OGP source (`name_truncated`); these cannot be reconstructed and are excluded from company matching.
- The CRO (Companies Registration Office) match reaches only ~61% of company-class suppliers. Ambiguous matches are excluded from the "clean" set, and LLPs (e.g. *Deloitte Ireland LLP*) fail the Ltd-only match.

**TED (EU):**

- Still a **silver** layer, regenerable, not gold. The Search-API lane covers **2024 onward only** — pre-2024 winner names are genuinely null in that API, so the date floor is a source limit, not a choice. A separate per-notice XML lane recovers 2016–2023 winners (~50% CRO-matched).
- The naive TED sum (~€624bn) is meaningless: **375 pan-EU outlier rows** (research frameworks such as GÉANT, where Ireland is one of dozens of participants, flagged `is_pan_eu_outlier`) account for ~€586bn. Trust **count and median**, never the sum.

Live / open tenders are a forward-looking planned-value *estimate* (`value_kind=estimate_advertised`, never sum-safe), include already-closed and DPS records back to 2023, and carry no CPV category.

---

## 17. Public-body payments (over €20,000)

The >€20k purchase-order / payments fact, extracted from published PDFs across departments, agencies, HSE/Tusla, 31 local authorities, and bespoke bodies (NPHDB, NTA, SEAI).

- **Not a single "€20,000 / Circular 07/2012" regime.** Disclosure basis and threshold vary by publisher: most publish over €20k under the FOI Act 2014 model scheme, but the HSE model threshold is €100k, CHI publishes over €25k incl-VAT, and utilities (ESB, EirGrid, Uisce Éireann) sit outside the scheme entirely. The basis is carried per row.
- **"Ordered" (a purchase-order commitment) and "paid" (an actual payment) are different lifecycle tiers and must never be summed together.** Only `value_safe_to_sum` rows total, and never across VAT bases — only ~32k of ~220k gold rows carry a known VAT basis; HSE/Tusla are VAT-inclusive while most others are exclusive.
- **Double-count trap.** The fact includes both central→council grant transfers (e.g. TII road grants, `supplier_class='public_body'`) and council→contractor payments. Exclude `supplier_class='public_body'` from spend totals.
- **Every figure is extraction-derived from PDFs, not an authoritative ledger.** Coverage is partial (~23 of 31 LAs; ~42 of 44 attempted publishers; HSE/Tusla layout-gated). Any aggregate is a **floor** — "at least €Y, from the documents we could read" — never the definitive amount. The reading-order department parser silently drops wrong-layout PDFs (notably pre-2019 Transport), leaving coverage holes.
- The CRO match on the consolidated payment fact is only ~46%, with some local authorities as low as ~5–9%.
- Companies published without a legal suffix can be misclassified as individuals and quarantined from public views (so DFAT, for example, loses ~65% of displayable spend) — captured but not displayed.
- The **HSE payments (16,972 rows, 2021–2025) are the only surviving public copy**: the HSE deleted the source in its 2026 rebuild and the Wayback Machine never archived it. The dead source URL is repointed to the HSE landing page.
- **NPHDB's ~€107.6m BAM row is ~49% of its corpus** and is a disputed adjudicator award, not a routine payment — never headline a raw NPHDB sum.
- Payments carry **no CPV**, so "what the money was for" is answerable only on the award side. Payments and awards meet at the supplier spine, never blended into one number.

---

## 18. Judiciary and courts

The bench / appointments / courts / Legal-Diary feature is **sandbox-grade, not pipeline output** — the validated datasets were pulled and pressure-tested once (2026-06-04) and are reproduced only by standalone probe scripts.

- The appointment spine begins in **2016** (Iris Oifigiúil), covering ~100 of ~160 current judges; pre-2016 appointees carry an honest "record begins 2016" note, not a full career arc.
- The current roster (~198 rows) is **not deduped**: ex-officio court presidents appear under up to three courts, which skews "current court".
- **No fuzzy name-matching to the bench.** A TD/AG→judge "revolving-door" dataset was deliberately removed (2026-06-04) because surname overlap produced false positives, and must not be reintroduced.
- Judicial-conduct figures are **aggregate-only and partial at the start** (no complaints regime before 2022; 2022 a partial first year) and must never be attributed to a named judge.
- Ireland has **no public judicial financial-disclosure regime**, so judges' conflicts/interests cannot be shown — the absence is itself the only available finding.
- The **Legal Diary** is self-published with a deliberate privacy contract: statutory in-camera categories (minors, family, wards, childcare, asylum) are dropped at extraction, every natural person is reduced to initials, and case references and solicitor names are stripped.
- Legal Diary court coverage spans two sources: the `.docx` feed (High Court) and OpenView (Circuit, Supreme, Appeal, Central Criminal). The **District Court is a genuine gap** — only a sittings schedule is published, no party-level lists.
- Legal Diary views are capped to a rolling ~7-day window; the full ~790k-row archive is not loaded because it made the page memory-prone.
- The diary→judge link is **surname-only and refuses ambiguous matches** (covering ~2/3 of diary judges). The plaintiff league ranks institutional applicants only, counting list appearances, not "cases brought".

---

## 19. Ministerial diaries × lobbying

- **Diaries are self-curated, non-exhaustive, and published quarterly in arrears.** Meeting counts rise as more departments are ingested — they are coverage-driven, never a trend.
- **A diary meeting is co-occurrence / access, never a lobbying return and never proof of influence or causation.** "Corroborated" means only that an organisation both met a minister and filed a lobbying return naming that same minister.
- **The "met but never lobbied" negative is deliberately hidden.** `total_lobbying_returns = 0` can mean "unknown" (a residual name-join miss), not "did not lobby"; surfacing it would defame heavy lobbyists. It stays hidden until a fuller org-identity alias map lands.
- **Org-name alias bridging is partial.** The acronym-tag cases (CIF, IFA, Ibec) are resolved via the curated map, but other forms — e.g. "Dublin Chamber" vs "Dublin Chamber of Commerce" — remain unbridged.
- The org-overlap view surfaces only the ~23% of meetings whose counterparty matched the gazetteer, so it under-shows the full diary; an unmatched-meetings view exists as the denominator.
- Corroboration joins on **minister surname only** (the diary minister is a filename guess), so a positive is indicative, not proof, and common surnames can collide.
- Match confidence is two-tier: **HIGH** (verbatim ≥2-token hit, ~96% precision) and **MEDIUM** (single-token + cue, precision unmeasured) — MEDIUM cannot be trusted blindly.
- ~237 diary files are image-only scans requiring off-box GPU OCR (PaddleOCR), which introduces OCR error risk (misread year headers, occasional mirrored-garbage scans).
- Company-influence matching is a coarse deterministic name-fold that deliberately **under-matches**: misses are false negatives, not false claims, and a fold collision (`n_suppliers_folded > 1`) means awarded/paid € sum more than one supplier.

---

## 20. Political finance — SIPO donations and GE2024 election spending

Scope is **locked to GE2024**; there are no 2025+ returns, and the separate annual per-TD/Senator/MEP donation register is not yet ingested.

- **Three incompatible grains must never be summed**: donations declared (money in), the national agent's per-candidate spend, and candidates' own expenses statements are different records at different grains. The rollup uses their sum only as a hidden sort key, never as a presented total.
- Agent-spend **under-counts** parties that book spend centrally, and agent-spend vs candidate-spend are overlapping views of the same campaign spend — non-additive.
- The party-expenses fact is a **PaddleOCR re-OCR of scanned returns**; every figure carries a "verify against the official SIPO PDF (page N)" caveat and a confidence/flag column.
- **Threshold effects.** Donations are only declared records above the €1,500 party threshold (sub-threshold gifts are invisible); expense figures are bounded by per-constituency statutory limits (€38,900 / €48,600 / €58,350), which are also used to flag OCR misreads.
- Coverage is **incremental/partial**: only candidates processed so far appear, several scanned national-agent statements are not yet parsed (e.g. Fine Gael), and the ~400–600-PDF per-candidate corpus is queued, not done.
- **Honest gaps, not zeros**: rows whose total was blank/unreadable or an OCR decimal-loss artefact carry no amount; a blank total is never asserted as €0.
- The free-text line-item "detail" mixes supplier names and item descriptions and must not be presented as a clean payee/vendor field.
- Per-party returns are harvested **manually** from the SIPO collection page; donor home addresses are captured but stripped at gold and never displayed.

---

## 21. Local-authority accountability (AFS, NOAC, and related indicators)

- **Annual Financial Statement (AFS) coverage is partial**: only ~22 of 31 councils are available, with nine unavailable for stated reasons (interactive viewer, scanned image, not located, unusual layout).
- AFS year-depth is **thin and uneven** — per-council history ranges from 1 year to 10 — and is not a clean panel; some statements fail parsing and yield zero rows.
- AFS extraction is **PDF-brittle** (locate the income-&-expenditure-by-division page, reconcile against the printed total; mostly PyMuPDF with occasional Camelot fallback). Six councils have no extractable capital page.
- **AFS grains must never be reconciled across each other**: per-LA revenue net-expenditure, per-LA capital expenditure, the national amalgamated layer, and the cash-PO `la_payments_fact` are distinct — sum only within a (council, year).
- The **chief-executive roster is hand-curated** (`data/_meta/la_chief_executives.csv`, 31 councils, each verified against an authoritative page); there is no API. CE salary is not published per council — only the national band is shown, as context.
- **NOAC is PDF-only** with no CSV or dashboard. Indicators that live as bar-chart figures are not extractable (local OCR is banned), leaving only Camelot-readable tables plus text-layer national averages and named best/worst.
- **Collection rates can legitimately exceed 100%** (prior-year arrears collected alongside the current year). The derelict-sites rate is null where nothing was levied (nine councils), is 2024-only, and refreshes only when the next return publishes.
- **Planning-appeal overturn** data is 2016 onward (~13k matched appeals). Cork County publishes no appeal reference number, so its appeals are recovered by a spatial+temporal fallback match, not a direct join.
- Indicators are shown beside the national benchmark with **no composite score and no editorial label** — a deliberate firewall-safe framing, not a completeness guarantee.

---

## 22. CSO statistical data — housing supply and the inflation deflator

**Housing-supply figures (Constituency page):**

- These are **council-area figures, not constituency figures.** New-home completions, residential vacancy, and median house price are published at local-authority / region granularity; the area is not the constituency, and the figures are never apportioned into a per-constituency number.
- The only natively constituency-keyed CSO table is the Census 2022 population count. RPPI is by RPPI region, vacancy by LA / electoral division, completions by local electoral area.
- **Residential vacancy is a metered-electricity proxy**, not a housing-stock census, and is labelled as such.

**CPI deflator (procurement real-terms):**

- Source is **CSO CPA07** (CPI by commodity group, annual average, All Items, 1975–2025, base year 2025).
- CSO splits the index across base-month rebasings (each null outside its window), so no single index level spans 2012–2025. One continuous index is reconstructed by **chain-linking the annual percentage-change series** — a documented method, cross-checked against published cumulative CPI (2012→2025 ≈ +24.7%).
- **Deflation re-expresses, it does not correct**: it scales a nominal € into base-year purchasing power and neither creates nor fixes magnitude errors in the input. A missing year returns null (treated as "leave nominal / exclude"), never 1.0 — a missing year can never masquerade as zero inflation.
- General CPI **understates the 2021–22 construction-cost surge**; a construction-materials WPI (WPM39, Works contracts, ~2021+ floor) is the secondary deflator for construction-heavy spend.
- ~41% of summable awards are multi-year but booked to a single year, so single-year deflation is **approximate** and labelled as such; null-year payments (~3.5%) stay nominal.
- **ESRI was ruled out** as a deflator source — its series footnote CSO/Eurostat or its own modelling and carry no unique data.

---

## 23. Minimum verification before public use

Before using the data in reporting, publication, or public claims:

1. Check the official upstream source.
2. Confirm the relevant date range.
3. Check whether the member was a TD, Senator, Minister, office holder, or former member at the time.
4. Check whether the row came from API data, PDF extraction, or manual CSV export.
5. For lobbying data, confirm whether the count is based on distinct returns or exploded activity rows.
6. For interests data, check whether a blank means `nil`, `not published`, `not extracted`, or `not applicable`.
7. For generated URLs, manually open the link and confirm it points to the expected bill, vote, debate, lobbying return, or organisation page.

