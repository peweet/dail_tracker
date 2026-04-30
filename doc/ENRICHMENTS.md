# Dáil Tracker — enrichment opportunities

_A forward-looking catalogue of public datasets that could expand Dáil Tracker's analytical reach. **This is not a roadmap.** The live roadmap is `dail_tracker_improvements_v4.md`. This doc deliberately stays at the "is this worth investigating?" level — enough information to decide, not enough to build._

---

## How to use this doc

Each source is a card. Cards are short on purpose. They should give you, in 30 seconds:

- whether the source exists and is open,
- how it would join to the existing pipeline,
- the rough engineering shape of ingestion,
- whether the cost-to-value is worth pursuing now, later, or never.

When a source moves from "interesting" to "next sprint", it graduates out of this doc and gets a proper section in v4 with cadence, manifest, page contract, and tests.

### Card schema

```text
What     — one line: what the source is
Why      — analytical leverage, especially when joined to existing data
URL(s)   — primary site + supporting docs
Format   — JSON / REST / CSV / HTML scrape / PDF / SPARQL etc.
Cadence  — how often the source updates
Auth     — none / registered / paid / contact-required
Licence  — open / restricted / unclear
Joins to — existing dim/fact tables it links to
Gotchas  — known issues, ambiguity, dirty fields
Rating   — Value · Cost · Risk (each H/M/L)
```

### Card legend

- **Value** = analytical leverage when joined into the existing surface, not standalone interest.
- **Cost** = engineering effort to ingest reliably and keep ingesting.
- **Risk** = likelihood of silent breakage, schema drift, or interpretation error.

A `Value: H · Cost: H` source is not automatically attractive — it has to clear the existing backlog of `H · L` sources first.

---

## Section A — Political accountability

The cluster closest to the project's existing surface. These pair directly with members, votes, lobbying, and interests.

### A.1 SIPO political donations register

```text
What     SIPO publishes annual donation statements from political parties,
         TDs, Senators, MEPs, and councillors. Names of donors above the
         disclosure threshold, amounts, and recipient.
Why      Closes the donor → lobbyist → vote loop that the project's
         existing lobbying view only half-shows. Single highest-leverage
         enrichment for the project's stated mission.
URL(s)   https://www.sipo.ie/reports-and-publications/
         https://www.sipo.ie/funding-of-political-parties/
Format   PDFs of annual reports + per-member returns.
Cadence  Annual (typically Q1 for prior year).
Auth     None.
Licence  Open public record.
Joins to dim_member, dim_party. New dim_donor.
Gotchas  PDF table layouts have changed across years.
         Disclosure thresholds have changed (currently €600).
         Some donations are aggregated by party rather than by recipient.
         Names below threshold are not published.
Rating   Value: H · Cost: M · Risk: M
```

### A.2 SIPO ethics returns (separate from Register of Members' Interests)

```text
What     The Ethics Acts (1995/2001) require designated office-holders to
         file additional returns to SIPO. These are distinct from the
         Register of Members' Interests held by the Oireachtas.
Why      Fills the disclosure gap §2.1 of DATA_LIMITATIONS calls out
         (Ministers and office-holders are partly outside RoMI). The two
         registers are complementary; together they're closer to a full
         picture for senior office-holders.
URL(s)   https://www.sipo.ie/acts-and-codes/ethics-acts/
Format   PDFs.
Cadence  Annual + ad-hoc on appointment.
Auth     None.
Licence  Open public record.
Joins to dim_member (via name resolution).
Gotchas  Different schema from RoMI.
         Categories don't exactly match.
         Need careful reconciliation to avoid double-counting interests.
Rating   Value: H · Cost: M · Risk: M
```

### A.3 Election results (Dáil, Seanad, local, EU, presidential)

```text
What     Per-constituency, per-candidate results, count-by-count where PR-STV.
Why      Lets the project show whether a TD's seat is safe or marginal,
         which contextualises voting behaviour and lobbying targeting.
         Historical results add a longitudinal axis.
URL(s)   https://www.electoralcommission.ie/  (since 2023)
         https://www.gov.ie/en/department-of-housing-local-government-and-heritage/
         https://electionsireland.org/  (third-party, comprehensive history)
Format   Mix: official PDFs/spreadsheets, third-party HTML/CSV.
Cadence  Per election (irregular).
Auth     None.
Licence  Crown of public record; third-party sites have own terms.
Joins to dim_member (TDs/Senators), dim_constituency, dim_party.
Gotchas  Constituency boundaries change between elections.
         Candidate name spelling varies between sources.
         electionsireland.org is single-maintainer; long-term continuity
         risk if used as primary source.
Rating   Value: M · Cost: L · Risk: L
```

### A.4 Election spending and donations returns (per candidate)

```text
What     Each Dáil candidate must file election expenditure and donation
         returns with SIPO after the election.
Why      Per-constituency campaign cost, donor patterns by candidate,
         comparable across cycles. Pairs with A.3 to show "what did
         winning this seat cost?".
URL(s)   https://www.sipo.ie/election-expenses/
Format   PDFs per candidate.
Cadence  Per election + by-elections.
Auth     None.
Licence  Open public record.
Joins to dim_member, dim_party, dim_constituency.
Gotchas  Hundreds of small PDFs per general election.
         OCR-clean but layout varies.
         "Notional" expenditure attribution rules are non-trivial.
Rating   Value: M · Cost: M · Risk: M
```

### A.5 Referendum campaign spending

```text
What     Campaign groups registered with SIPO for each referendum file
         expenditure and donation returns.
Why      Lets you connect issue-based campaign funding to TD voting on
         related legislation pre/post-referendum.
URL(s)   https://www.sipo.ie/referendums/
Format   PDFs.
Cadence  Per referendum.
Auth     None.
Licence  Open public record.
Joins to New dim_campaign_group. Loose joins to dim_member by stated
         endorsements (would need manual coding).
Gotchas  Smaller campaign groups often miss disclosure thresholds.
         "Connectedness" of campaign group ↔ TD is rarely explicit.
Rating   Value: L · Cost: M · Risk: M
```

---

## Section B — State spending and procurement

Where public money goes. The natural counterweight to lobbying-as-influence: contracts-as-outcome.

### B.1 eTenders (Irish public procurement)

```text
What     Ireland's national procurement portal. Contract notices, awards,
         contracting authority, supplier, value.
Why      Joining suppliers to lobbying.ie clients exposes the
         procurement-influence axis. One of the two highest-leverage
         enrichments alongside SIPO donations.
URL(s)   https://www.etenders.gov.ie/
         (most notices also flow through EU TED — see B.2)
Format   HTML portal; some bulk export. Many notices link to PDFs.
Cadence  Continuous (multiple per day).
Auth     None for public read.
Licence  Open public record.
Joins to New dim_supplier, dim_contracting_authority. Bridge to lobbying
         clients via company name resolution.
Gotchas  Supplier name normalisation is significant work.
         Award value sometimes redacted or "framework" placeholders.
         Same supplier can appear under multiple legal entities.
         Threshold-based publication: small contracts may not appear.
Rating   Value: H · Cost: H · Risk: M
```

### B.2 EU TED (Tenders Electronic Daily)

```text
What     EU-wide procurement publication. Irish above-threshold contracts
         flow through here too, with structured data.
Why      Better-structured than eTenders for above-threshold notices,
         plus full EU coverage for cross-border lobbying analysis.
URL(s)   https://ted.europa.eu/en/
         https://data.europa.eu/data/datasets?query=ted
Format   Bulk XML downloads + REST API.
Cadence  Daily.
Auth     None.
Licence  Open EU data.
Joins to dim_supplier (cross-jurisdictional via VAT or eForms IDs).
Gotchas  Schema migration ongoing (TED → eForms). Two formats coexist.
         Must filter to Irish contracting authorities.
Rating   Value: H · Cost: M · Risk: L
```

### B.3 Department grant registers and capital projects tracker

```text
What     gov.ie publishes department-by-department grant registers and
         a capital projects tracker for major investment.
Why      Many "soft" public spending decisions don't go through eTenders.
         Grants and discretionary funding are where political influence
         tends to land most visibly.
URL(s)   https://www.gov.ie/en/publication/9c6d5-capital-tracker/
         Department-specific publication pages on gov.ie.
Format   Mix: spreadsheets, PDFs, HTML tables.
Cadence  Quarterly to annual depending on department.
Auth     None.
Licence  Open public record.
Joins to dim_constituency (most grants are geographic), dim_member
         (when grants are announced "by" a TD).
Gotchas  Format varies wildly between departments.
         Some "grants" are statutory entitlements, not discretionary.
         Geographic attribution is sometimes scheme-level, not project-level.
Rating   Value: H · Cost: H · Risk: M
```

### B.4 Section 39 / 38 organisations (HSE-funded)

```text
What     Voluntary organisations funded by the HSE under sections 38/39
         of the Health Act 2004. Annual funding amounts published.
Why      Significant public spending channel; these organisations also
         appear in lobbying register. Joining shows funded-then-lobbying
         patterns.
URL(s)   https://www.hse.ie/eng/services/publications/corporate/
         (annual reports + service plans + sometimes per-org listings)
Format   PDFs, occasional spreadsheets.
Cadence  Annual.
Auth     None.
Licence  Open public record.
Joins to lobbying organisations, charities register, CRO companies.
Gotchas  Naming conventions vary between HSE publications.
         "Section 38" vs "Section 39" matters for relationship type.
         Large parent orgs vs sub-orgs sometimes conflated.
Rating   Value: M · Cost: H · Risk: M
```

### B.5 Sport Ireland, Arts Council, HEA funding

```text
What     Three large discretionary funding bodies that publish annual
         funding allocations.
Why      Constituency-level funding distribution; pairs with election
         and constituency demographics.
URL(s)   https://www.sportireland.ie/funding
         https://www.artscouncil.ie/funding-decisions/
         https://hea.ie/statistics/
Format   Spreadsheets / HTML / PDFs.
Cadence  Annual.
Auth     None.
Licence  Open public record.
Joins to dim_constituency, charities register.
Gotchas  Different schemes have different schemas and reporting cadences.
         Some allocations are multi-year and double-count if not careful.
Rating   Value: L · Cost: M · Risk: L
```

---

## Section C — Corporate, beneficial ownership, contracts

The "who actually owns what" layer. Resolves the gap where the Register of Members' Interests says "shares in X Ltd" but never tells you who else owns it.

### C.1 CRO companies register

```text
What     Companies Registration Office. All Irish-registered companies:
         directors, addresses, filing history, shareholdings (via filed
         returns), shareholder names (sometimes).
Why      Resolves "shares in X Ltd" interest declarations to actual legal
         entities and director networks. Connects TDs' declared
         directorships to other directors and to public contractors.
URL(s)   https://www.cro.ie/
         https://core.cro.ie/  (search portal)
Format   HTML search free; bulk data paid. Returns are PDF/structured.
Cadence  Continuous.
Auth     None for free search; paid bulk subscription for API.
Licence  Open data status varies; bulk under licence.
Joins to RoMI declared shareholdings, lobbying clients, eTenders suppliers.
Gotchas  Free search rate-limited and not bulk-friendly.
         Shareholder data quality varies (some only at AGM date).
         Shell companies and holding structures complicate ownership.
Rating   Value: H · Cost: H · Risk: M
```

### C.2 RBO — Register of Beneficial Owners

```text
What     Central Register of Beneficial Ownership of companies. Lists
         humans (not companies) who ultimately own ≥25% of a company.
Why      Gets past holding-company structures. Critical for understanding
         "who actually benefits" from a contract or donation.
URL(s)   https://rbo.gov.ie/
Format   HTML search; access is more restricted than CRO post-2022 CJEU
         ruling on public access.
Cadence  Continuous (companies must file within 5 months of formation).
Auth     Restricted post-2022. Designated persons (banks, lawyers) and
         "legitimate interest" applicants only.
Licence  Restricted.
Joins to CRO companies, lobbying clients, RoMI shareholdings.
Gotchas  Public access restriction is the main blocker post-CJEU
         judgment Nov 2022. Journalist/researcher access requires
         legitimate-interest applications.
         Compliance is patchy; many companies file late or not at all.
Rating   Value: H · Cost: H · Risk: H (access uncertainty)
```

### C.3 OpenCorporates (international)

```text
What     Aggregated global company data, including Ireland.
Why      Useful for cross-jurisdictional company linking when a TD
         declares directorships in non-Irish companies, or when a
         lobbying client is a UK/US/Lux entity.
URL(s)   https://opencorporates.com/
         https://api.opencorporates.com/documentation/API-Reference
Format   REST API.
Cadence  Continuous.
Auth     Free tier (rate-limited) or paid.
Licence  Open under attribution; commercial use restricted.
Joins to CRO Irish entries, RoMI declarations.
Gotchas  Quality varies massively by jurisdiction.
         Free tier inadequate for systematic ingestion.
Rating   Value: M · Cost: M · Risk: L
```

### C.4 Companies House UK

```text
What     UK companies register. Free, comprehensive, with REST API.
Why      Many Irish TDs declare directorships in UK companies; lobbying
         clients sometimes have UK parents.
URL(s)   https://www.gov.uk/government/organisations/companies-house
         https://developer.company-information.service.gov.uk/
Format   REST API + bulk download.
Cadence  Continuous.
Auth     Free API key.
Licence  Open Government Licence.
Joins to RoMI, lobbying clients (where UK entities appear).
Gotchas  Need careful matching — many Anglo-Irish business names overlap.
Rating   Value: M · Cost: L · Risk: L
```

### C.5 Charities Regulator

```text
What     Register of Irish charities, with annual returns including
         trustees, income, expenditure.
Why      Many lobbying organisations are charities. Some TDs serve as
         trustees (which they declare in RoMI). Cross-references the
         "civil society" half of the lobbying register.
URL(s)   https://www.charitiesregulator.ie/en/charity-search
Format   HTML search; some structured data downloadable.
Cadence  Annual returns + continuous.
Auth     None.
Licence  Open public record.
Joins to lobbying organisations, RoMI declared trusteeships.
Gotchas  Smaller charities have smaller/later returns.
         Trustee-name resolution is the bottleneck.
Rating   Value: M · Cost: M · Risk: L
```

### C.6 Pensions Authority

```text
What     Register of registered pension schemes; investigations and
         enforcement actions.
Why      Niche but relevant where TDs serve as trustees of large schemes,
         or where pension scheme failures intersect with regulatory action.
URL(s)   https://www.pensionsauthority.ie/
Format   HTML, some PDF.
Cadence  Continuous + annual report.
Auth     None.
Licence  Open public record.
Joins to RoMI trusteeships, regulatory enforcement actions.
Gotchas  Most pension data is aggregate, not entity-level.
Rating   Value: L · Cost: M · Risk: L
```

---

## Section D — Judicial and regulatory

Existing transparency surface for judicial and quasi-judicial bodies. Includes the judicial appointments deep dive.

### D.1 Judicial appointments — depth dive

This is the highest-leverage enrichment in this section and gets a fuller treatment because it closes a known transparency gap.

#### D.1.1 Why this is a (partial) black box

Six things were historically opaque, and most are still partly so even after JAC commencement:

1. Cabinet deliberations are constitutionally confidential under Article 28.4.3°. The reasoning behind any specific appointee being preferred over another is not disclosed.
2. Identity of unsuccessful applicants is confidential under both old (JAAB) and new (JAC) regimes.
3. JAAB recommended-candidate lists were not published. Cabinet was not bound to pick from them.
4. JAAB did not cover promotions within the courts. This was a major historical gap.
5. Reasons for choosing among the JAC's three-name shortlist are not published.
6. No public hearings, unlike US Senate confirmations.

Two illustrative cases from the pre-JAC era:

- **Máire Whelan, Court of Appeal (2017).** Then-AG; appointment made in the dying hours of the outgoing government without external advertising under the 1995 Act. The canonical case study for the old regime's weaknesses.
- **Seamus Woulfe, Supreme Court (2020).** Then-AG; vacancy filled without external advertising. Subsequent "Golfgate" controversy was about conduct, but the appointment process itself sharpened scrutiny of the AG-to-bench pipeline.

#### D.1.2 Pre-2025 process (JAAB, 1995–2024)

Established by the Courts and Court Officers Act 1995 in response to the 1994 "Whelehan affair":

1. Vacancy arises (retirement at mandatory 70, death, statutory expansion, promotion).
2. Justice Minister informs JAAB if filling is to be by external appointment.
3. JAAB invites applications from solicitors and barristers.
4. JAAB submits an unranked list of "suitable" names (typically 7+ per vacancy).
5. Cabinet decides — and may pick someone *not on the list*, especially for senior roles.
6. Taoiseach advises the President under Article 35.
7. President formally appoints; Iris Oifigiúil publishes.

**Promotions bypassed JAAB entirely.** Roughly half of senior judicial movements were never subject to even this minimal process.

#### D.1.3 Post-2025 process (JAC, 1 January 2025–)

Judicial Appointments Commission Act 2023 commenced fully on 1 January 2025 (S.I. No. 553/2024):

1. Vacancy arises.
2. JAC advertises publicly. Application form, references, eligibility checks.
3. JAC shortlists and interviews (panel of ≥3 Commissioners, ≥1 lay, ≥1 judge).
4. JAC produces a **ranked shortlist of three** for the vacancy.
5. Government picks from the three. Material deviation requires Oireachtas justification.
6. Taoiseach advises President. President appoints. Iris Oifigiúil publishes.

Key changes:

- **Lay-majority Commission.** 9 members: Chief Justice (chair), Court of Appeal President, 2 Judicial Council members, 4 lay members, AG (non-voting).
- **Promotions are now in scope.**
- **Cabinet is constrained to the three names** (with narrow exceptions).
- **Public advertising mandatory.**
- **Draft Judicial Selection Statement** required within 15 months of establishment — that window closes around April 2026.

Still confidential: applicant lists outside the three; relative ranking among the three; Cabinet reasoning; interview scoring.

#### D.1.4 Publicly knowable fields

| Field | Source | Confidence |
|---|---|---|
| Judge name | Courts.ie, Iris Oifigiúil | High |
| Court appointed to | Iris Oifigiúil, Courts.ie | High |
| Appointment date | Iris Oifigiúil | High |
| Government in office | Cross-ref Oireachtas API | High |
| Justice Minister at appointment | gov.ie, Oireachtas API | High |
| Whether appointment was a promotion | Inferred from prior judicial role | High |
| Prior role (barrister, solicitor, AG, adviser) | Bar Council, Law Society, gov.ie | Medium |
| Year called to Bar / SC year | Bar Council records | Medium |
| Was former TD/Senator/AG | Cross-ref Oireachtas API | High |
| Family/political connections | Manual curation, news archives | Low — flag as `manual` |
| Retirement date (mandatory at 70) | Birth year + statute | High once DOB known |

#### D.1.5 Proposed schema

```text
dim_judge
  judge_id, full_name, court, appointment_date, is_promotion,
  prior_court, appointment_type, retirement_date,
  iris_oifigiuil_url, appointed_under_govt, appointed_under_minister

bridge_judge_prior_career
  judge_id, role_type, role_start_year, role_end_year, role_detail, source

bridge_judge_political_link
  judge_id, link_type, related_member_id, related_party_id,
  evidence_url, confidence, notes

fact_judicial_vacancy_event
  vacancy_id, court, vacancy_date, reason, filled_date,
  filled_by_judge_id, process
```

#### D.1.6 Refresh strategy (when this graduates to v4)

- **Cadence:** weekly. Iris Oifigiúil publishes Tuesdays/Fridays; Monday cron picks both up.
- **Sources:** `iris_oifigiuil_scraper.py`, `courts_ie_directory_scraper.py`, `jac_announcements_scraper.py`.
- **Bootstrap:** hand-curate ~165–170 sitting judges from courts.ie one-off (~1–2 days). For each, record `prior_career` from public CVs / Bar Council / Law Society / news archives. Mark confidence honestly; "manual" is fine.
- **Caveats to surface:** prior-career is a structured public-record summary, not a full CV. Political links carry confidence flags and evidence URLs. Absence of a recorded link is not evidence one doesn't exist.

#### D.1.7 Three structural questions this enables

1. Court composition by appointing government.
2. The AG-to-bench pipeline (interval from leaving AG to judicial appointment).
3. The pre-JAC vs post-JAC contrast once a meaningful sample of post-2025 appointments accumulates.

#### D.1.8 Sources confirmed

```text
Judicial Appointments Commission Act 2023 — irishstatutebook.ie/eli/2023/act/33
S.I. No. 553/2024 commencement order — irishstatutebook.ie/eli/2024/si/553
gov.ie announcement — gov.ie/en/news/e9a93-judicial-appointments-commission-established/
JAC site — judicialappointments.ie
Iris Oifigiúil — irisoifigiuil.ie
Courts Service — courts.ie
Mason Hayes Curran briefing — mhc.ie/latest/insights/the-judicial-appointments-commission

Rating   Value: H · Cost: M · Risk: L
```

### D.2 Judicial Council annual reports

```text
What     The Judicial Council (statutory body since 2019) publishes
         annual reports including judicial conduct complaints handling.
Why      Adds the conduct/discipline dimension to the appointments view.
URL(s)   https://judicialcouncil.ie/
Format   PDFs.
Cadence  Annual.
Auth     None.
Licence  Open public record.
Joins to dim_judge.
Gotchas  Conduct complaints are aggregated; individual judges rarely named.
Rating   Value: M · Cost: L · Risk: L
```

### D.3 Comptroller and Auditor General

```text
What     C&AG audits public bodies and publishes annual reports plus
         special reports on specific issues.
Why      Where the C&AG flags concerns about a public body, that's
         strong corroborating signal alongside lobbying or grant data.
URL(s)   https://www.audit.gov.ie/
Format   PDFs (well-structured).
Cadence  Annual + ad-hoc special reports.
Auth     None.
Licence  Open public record.
Joins to public bodies, departments. Loose joins to grants.
Gotchas  PDFs are large and dense; topic extraction needs NLP or manual.
Rating   Value: M · Cost: M · Risk: L
```

### D.4 Public Accounts Committee reports

```text
What     PAC examines C&AG reports and publishes its own with witness
         testimony, recommendations, government responses.
Why      Adds the political response layer to the C&AG signal.
URL(s)   https://www.oireachtas.ie/en/committees/  (already partly covered)
Format   Available via Oireachtas API; transcripts are HTML/PDF.
Cadence  Per session.
Auth     None.
Licence  Open public record.
Joins to dim_member, debates, existing committees view.
Gotchas  Already partially covered by Oireachtas API; risk is duplicate
         ingestion. Need to dedupe by debate ID.
Rating   Value: M · Cost: L · Risk: L
```

### D.5 Tribunals and Commissions of Investigation

```text
What     Statutory inquiries (Mahon, Moriarty, Cregan, etc.) publish
         findings as reports. Some name TDs, parties, donors, contractors.
Why      Historical political-corruption signal where it has already been
         judicially examined. Strong linking value to A.1 donors and B.x
         contracts.
URL(s)   Various — typically on tribunal-specific gov.ie pages.
Format   Multi-volume PDFs, often thousands of pages.
Cadence  Per tribunal (irregular).
Auth     None.
Licence  Open public record.
Joins to dim_member, dim_party, lobbying orgs (where relevant).
Gotchas  Topic extraction is genuinely research-grade work.
         Findings are time-bounded and specific; not a structured dataset.
Rating   Value: M · Cost: H · Risk: M
```

---

## Section E — Regulatory enforcement

Where a state regulator has formally found wrongdoing. High signal, often dirty schema.

### E.1 Data Protection Commission enforcement

```text
What     DPC publishes annual reports + decision archive (incl. high-profile
         GDPR fines against tech firms).
Why      Where lobbying clients overlap with regulatory targets, useful
         signal. Less directly relevant to TD-centric analysis.
URL(s)   https://www.dataprotection.ie/en/dpc-guidance/decisions
Format   HTML + PDFs.
Cadence  Continuous.
Auth     None.
Licence  Open public record.
Joins to lobbying orgs, CRO companies.
Gotchas  Volume is dominated by very large tech-platform decisions.
Rating   Value: L · Cost: L · Risk: L
```

### E.2 Workplace Relations Commission decisions

```text
What     WRC decisions on employment disputes; named parties, awards.
Why      Where a public body or known organisation is a frequent
         respondent, useful context.
URL(s)   https://www.workplacerelations.ie/en/cases/
Format   HTML/PDF per decision.
Cadence  Continuous.
Auth     None.
Licence  Open public record.
Joins to public bodies, known orgs.
Gotchas  High volume, individual respondents (privacy considerations).
Rating   Value: L · Cost: M · Risk: M
```

### E.3 Labour Court decisions

```text
What     Labour Court appellate decisions on industrial relations.
Why      Companion to E.2. Pattern signal for repeat respondents.
URL(s)   https://www.labourcourt.ie/en/cases/
Format   HTML/PDF.
Cadence  Continuous.
Auth     None.
Licence  Open public record.
Joins to E.2 respondents, public bodies.
Gotchas  Same as E.2.
Rating   Value: L · Cost: L · Risk: L
```

### E.4 Corporate Enforcement Authority

```text
What     CEA (formerly ODCE) publishes annual reports and prosecutions
         under company law.
Why      Where directors of companies in lobbying or contracting data
         show up in CEA prosecutions, very strong signal.
URL(s)   https://www.cea.gov.ie/
Format   PDFs.
Cadence  Annual + occasional case publications.
Auth     None.
Licence  Open public record.
Joins to CRO companies, RoMI directorships.
Gotchas  Caseload is small relative to companies register.
Rating   Value: M · Cost: L · Risk: L
```

### E.5 Central Bank of Ireland enforcement

```text
What     CBI publishes administrative sanctions against regulated firms
         and individuals (PCFs).
Why      Financial-sector regulatory signal; complements donor and
         lobbying data when financial firms appear.
URL(s)   https://www.centralbank.ie/regulation/how-we-regulate/enforcement
Format   HTML + PDF press releases.
Cadence  Continuous.
Auth     None.
Licence  Open public record.
Joins to CRO, RoMI, lobbying clients (financial sector).
Gotchas  Some decisions name individuals; handle with the same care as
         personal data.
Rating   Value: M · Cost: L · Risk: M
```

### E.6 Coimisiún na Meán

```text
What     New media regulator (under Online Safety and Media Regulation
         Act 2022). Codes, investigations, sanctions.
Why      Adds emerging regulatory dimension for media and platforms.
URL(s)   https://www.cnam.ie/
Format   HTML + PDFs.
Cadence  Continuous.
Auth     None.
Licence  Open public record.
Joins to media organisations, lobbying clients (platform-related).
Gotchas  Body is new (operational since 2023); back-catalogue is small.
Rating   Value: L · Cost: L · Risk: L
```

### E.7 Revenue Commissioners — defaulters list

```text
What     Quarterly publication of tax defaulters: name, address, occupation,
         amount, settlement category.
Why      Where lobbying clients, donors, or directors show up here, very
         strong corroborating signal. But interpretation/legal sensitivity
         is high.
URL(s)   https://www.revenue.ie/en/corporate/press-office/tax-defaulters/
Format   PDFs (quarterly).
Cadence  Quarterly.
Auth     None.
Licence  Open public record (statutory publication).
Joins to CRO, lobbying clients, donors.
Gotchas  Significant reputational/legal sensitivity in publication.
         Names can be ambiguous; do not auto-link without manual review.
         Handle as "evidence pointer", not as a labelled join.
Rating   Value: H · Cost: M · Risk: H
```

---

## Section F — EU layer

Ireland-in-Europe surface. Most useful as cross-reference and for MEPs.

### F.1 HowTheyVote.eu (Irish MEPs)

```text
What     Existing third-party project mapping all European Parliament
         votes, including Irish MEPs.
Why      Adds Irish MEP coverage Dáil Tracker doesn't have. Small
         dedicated page. Inspirational comparator project too.
URL(s)   https://howtheyvote.eu/
         https://github.com/HowTheyVote/howtheyvote
Format   Public dataset + frontend.
Cadence  Continuous.
Auth     None.
Licence  Project-specific; check.
Joins to None directly (different chamber). Cross-ref via members where a
         former TD/Senator becomes an MEP.
Gotchas  Maintainer-dependency risk if used as primary source.
Rating   Value: M · Cost: L · Risk: L
```

### F.2 EU Transparency Register (lobbying)

```text
What     Voluntary register of lobbying activity at EU institutions.
Why      Many Irish-based lobbying organisations also register here,
         giving context to their domestic activity.
URL(s)   https://transparency-register.europa.eu/
Format   API + bulk download.
Cadence  Continuous.
Auth     Free API.
Licence  Open EU data.
Joins to lobbying.ie clients (organisation-name resolution).
Gotchas  Voluntary scope. Different categories than Irish register.
Rating   Value: M · Cost: M · Risk: L
```

### F.3 EU Council voting records

```text
What     How Ireland's representative voted at EU Council formations.
Why      Cabinet-level positions on EU files; complements domestic vote
         data with the EU dimension.
URL(s)   https://www.consilium.europa.eu/en/general-secretariat/
         corporate-policies/transparency/open-data/
Format   Open data portal.
Cadence  Per Council meeting.
Auth     None.
Licence  Open EU data.
Joins to dim_government, dim_member (where Minister attended).
Gotchas  Most Council decisions are by consensus; named votes are rarer.
Rating   Value: L · Cost: L · Risk: L
```

### F.4 EU funding (CAP, ERDF, etc.)

```text
What     EU funding to Irish recipients, published per fund.
Why      Major share of agricultural and regional spending in Ireland.
         Joining beneficiaries to constituencies, donors, or directors
         is high-leverage.
URL(s)   https://agriculture.ec.europa.eu/cap-my-country/cap-around-eu/cap-ireland_en
         https://cohesiondata.ec.europa.eu/
Format   Open data portal + per-country pages.
Cadence  Annual.
Auth     None.
Licence  Open EU data.
Joins to dim_constituency, CRO companies (beneficiaries are often companies).
Gotchas  CAP beneficiary publication has its own privacy framework.
         Beneficiary name resolution to CRO is non-trivial.
Rating   Value: M · Cost: M · Risk: M
```

### F.5 European Court of Justice case data

```text
What     CJEU rulings, including Irish-referred preliminary references.
Why      Where the Court rules on Irish matters that intersect with
         legislation, useful context.
URL(s)   https://curia.europa.eu/
Format   HTML + JSON via Eur-Lex.
Cadence  Continuous.
Auth     None.
Licence  Open EU data.
Joins to legislation (cases that touch Irish statutes).
Gotchas  Linking case → Irish statute is interpretation work.
Rating   Value: L · Cost: M · Risk: L
```

---

## Section G — Public discourse

Words said in public, by office or platform. High noise; high cost to do well.

### G.1 gov.ie press releases

```text
What     Government announcements across all departments.
Why      The "what the government claims it's doing" surface, complementary
         to the "what was actually voted on / spent" surface.
URL(s)   https://www.gov.ie/en/news/
Format   HTML; some structured metadata.
Cadence  Continuous (multiple per day).
Auth     None.
Licence  Open public record.
Joins to dim_member (Ministers named), dim_department.
Gotchas  Volume is high; topic classification is non-trivial.
         Press releases are intent, not outcome — easy to mis-cite.
Rating   Value: M · Cost: M · Risk: L
```

### G.2 Departmental press releases (legacy format)

```text
What     Some departments still publish ahead-of-schedule on their own
         legacy sites that haven't fully consolidated to gov.ie.
Why      Coverage gaps if you only ingest gov.ie.
URL(s)   merrionstreet.ie + per-department legacy sites.
Format   HTML.
Cadence  Continuous.
Auth     None.
Licence  Open public record.
Joins to G.1.
Gotchas  Will gradually become redundant as gov.ie consolidates.
Rating   Value: L · Cost: M · Risk: L
```

### G.3 Oireachtas debate transcripts

```text
What     Verbatim transcripts of Dáil and Seanad debates.
Why      Already partially covered by Oireachtas API for metadata.
         Full text is the bridge to topic-level analysis (which TD said
         what about which lobbied issue).
URL(s)   https://api.oireachtas.ie/
         https://www.oireachtas.ie/en/debates/
Format   API + HTML.
Cadence  Per sitting day.
Auth     None.
Licence  Open public record.
Joins to existing debates view, dim_member.
Gotchas  Already in pipeline at metadata level; full-text adds storage
         cost. Topic extraction needs NLP.
Rating   Value: M · Cost: M · Risk: L
```

### G.4 TD social media (Twitter/X, Bluesky, Mastodon)

```text
What     Public statements by TDs on social platforms.
Why      Real-time issue positioning; comparable to debate transcripts
         but with lower friction.
URL(s)   Per-platform.
Format   API or scrape (varies).
Cadence  Continuous.
Auth     Increasingly platform-restricted (X requires paid API).
Licence  ToS-restricted on most platforms.
Joins to dim_member.
Gotchas  Most expensive ongoing source for what it adds. ToS exposure.
         Easily becomes a sentiment-analysis project rather than civic data.
Rating   Value: M · Cost: H · Risk: H — defer indefinitely
```

### G.5 News archive cross-reference

```text
What     Where a TD, party, lobbying client, or donor has been the
         subject of news coverage.
Why      Ground-truth reality check on patterns surfaced by the data.
URL(s)   Newspaper archives (mostly paywalled), thejournal.ie, RTÉ.ie.
Format   Mixed. Most aren't open.
Cadence  Continuous.
Auth     Mostly paywalled.
Licence  Restricted.
Joins to anything.
Gotchas  Copyright, paywall, and ToS exposure. Not a viable bulk-ingest
         source. At most: maintain a manual-curation table of "important
         articles" that the UI can link to.
Rating   Value: H · Cost: H · Risk: H — keep manual, do not automate
```

---

## Section H — Geography and demographics

Context layer. Makes ratios meaningful.

### H.1 CSO statistics (PxStat API)

```text
What     Central Statistics Office. Census, labour, economic, regional
         statistics.
Why      Lets the project show "lobbied per capita", "TDs per population
         unit", and similar normalised figures.
URL(s)   https://www.cso.ie/
         https://data.cso.ie/  (PxStat API)
Format   PxStat REST API + bulk CSV.
Cadence  Varies by dataset.
Auth     None.
Licence  Open data (CC-BY).
Joins to dim_constituency, dim_county.
Gotchas  PxStat is well-documented but each table has its own dimensions;
         no single "join everything" path.
Rating   Value: M · Cost: L · Risk: L
```

### H.2 Constituency boundaries (geographic)

```text
What     Electoral Commission publishes shapefile boundaries for current
         and historical Dáil constituencies.
Why      Map-based UI for constituency analysis.
URL(s)   https://www.electoralcommission.ie/
         data.gov.ie has shapefiles.
Format   Shapefiles, GeoJSON.
Cadence  Per boundary review.
Auth     None.
Licence  Open data.
Joins to dim_constituency.
Gotchas  Map UI in Streamlit is feasible but heavy; pre-render tiles.
Rating   Value: L · Cost: L · Risk: L
```

### H.3 Property Price Register

```text
What     Public register of all residential property sales in Ireland
         since 2010, with price and date.
Why      Where TDs declare property holdings and a sale appears, useful
         corroborating signal.
URL(s)   https://www.propertypriceregister.ie/
Format   CSV download per year.
Cadence  Continuous.
Auth     None.
Licence  Open public record.
Joins to RoMI declared properties (loose, by address).
Gotchas  Address resolution is the bottleneck.
         Commercial property is not covered — only residential.
Rating   Value: L · Cost: M · Risk: M
```

### H.4 Land Registry / Tailte Éireann

```text
What     National land and property registry. Ownership records.
Why      Ground-truth on declared property holdings.
URL(s)   https://www.tailte.ie/
Format   Online portal; bulk access paid.
Cadence  Continuous.
Auth     Paid for searches.
Licence  Restricted.
Joins to RoMI declared properties.
Gotchas  Per-search cost makes systematic ingestion expensive.
Rating   Value: L · Cost: H · Risk: M
```

### H.5 An Bord Pleanála planning decisions

```text
What     Strategic planning decisions and appeals.
Why      Where development interests and political donations overlap,
         planning data is the third leg.
URL(s)   https://www.pleanala.ie/
Format   HTML + PDFs.
Cadence  Continuous.
Auth     None.
Licence  Open public record.
Joins to CRO companies (developers), constituencies.
Gotchas  Address/parcel resolution is a multi-month project on its own.
Rating   Value: M · Cost: H · Risk: M
```

---

## Section I — Cross-reference helpers

Not standalone analytical sources — they exist to disambiguate and link.

### I.1 Wikidata

```text
What     Structured cross-references for people, organisations, events.
Why      Cross-IDs (Oireachtas pId ↔ Wikidata Q-id), biographies, photos,
         birthdates (used for retirement-date inference). The project
         already has a test against this source.
URL(s)   https://www.wikidata.org/
         https://query.wikidata.org/
Format   SPARQL endpoint + REST API.
Cadence  Continuous (community-edited).
Auth     None.
Licence  CC0.
Joins to dim_member (via Q-id once curated), dim_judge, dim_party.
Gotchas  Community-edited — never use as an authoritative claim source.
         Use only for cross-IDs and uncontroversial demographic fields.
Rating   Value: M · Cost: L · Risk: M (don't trust as truth)
```

### I.2 OpenCorporates international

See C.3 above.

### I.3 Bar Council / Law Society directories

```text
What     Membership directories of practising barristers and solicitors.
Why      Critical for the judicial-appointments enrichment (D.1) — most
         judges come from these professions.
URL(s)   https://www.lawlibrary.ie/  (Bar of Ireland)
         https://www.lawsociety.ie/find-a-solicitor/
Format   HTML directory search.
Cadence  Continuous.
Auth     None.
Licence  Restricted (terms of use).
Joins to dim_judge prior career, RoMI declared practising roles.
Gotchas  Search-based UI; bulk ingestion needs careful rate-limiting and
         ToS check.
Rating   Value: M · Cost: M · Risk: M
```

---

## Section J — Deferred, niche, or out of scope

Sources that have been considered and parked, with the reason.

| Source | Why deferred |
|---|---|
| Court decisions / case law (Courts Service, BAILII) | Different problem from appointments. Linking decisions to legislation/MEPs is research-grade. |
| Local council records (county councils × 31) | Fragmentation across 31 councils; per-council schema. Would more than double pipeline maintenance cost. |
| Garda Síochána statistics | Mostly aggregate crime stats; weak join to political accountability surface. |
| Tweet/Bluesky/Mastodon archives at scale | See G.4. |
| Defence Forces deployments | Very limited public data; weak relevance to civic accountability axis. |
| National Archives FOI logs | Per-department; format varies; aggregation cost high vs analytical value. |
| NAMA loan books and recovered properties | Politically interesting but the windup is far advanced and back-coverage is hard. |

These are not "never" — they're "not until the priority backlog clears."

---

## Combinations worth building stories around

These are the cross-source views that would make the project genuinely novel in the Irish civic-data space.

1. **Donor → Lobbyist → Vote chain.** SIPO donations (A.1) × lobbying.ie returns (existing) × Dáil votes (existing). Show, for each TD, who donates to them, who lobbies them, and how they vote on related bills.
2. **Declared interest → State contract.** RoMI declared shareholdings (existing) × CRO companies (C.1) × eTenders/TED awards (B.1, B.2). Show where a TD's declared corporate interests received state contracts during their term.
3. **AG-to-bench pipeline.** dim_judge (D.1) × Oireachtas members (existing) × dim_government. Show the pattern of Attorneys General ascending to senior judicial office, broken out by appointing government and time-to-appointment.
4. **Constituency funding heat map.** Department grants (B.3) + Sport Ireland / Arts Council / HEA (B.5) × dim_constituency × CSO demographics (H.1). Per-capita grant flow by constituency, normalised for population.
5. **Section 39 funding × lobbying.** HSE Section 39 funding (B.4) × lobbying.ie organisations. "Funded then lobbying" patterns.

Any one of these, done well, becomes a publishable story. All of them present makes the project a tool serious journalists return to.

---

## What this list is not

- **Not exhaustive.** New public data sources appear regularly; this captures the strategic ones as of April 2026.
- **Not a roadmap.** Sources here are candidates. The roadmap (v4) decides which ones graduate, and when.
- **Not a quality guarantee.** Every source's `Risk` rating is a starting point. Real ingestion is where you find out how dirty the data is.
- **Not a justification.** Every additional source increases pipeline maintenance cost. The bar for adding one is "this answers a question we couldn't answer before", not "this exists and is open".
- **Not licence advice.** Each source's licensing must be checked at ingestion time. "Open public record" is a status, not a permission to redistribute.

---

## How a source graduates from here to v4

When a source is ready to be picked up:

1. Confirm licence and ToS. Document in `doc/source_licensing.md`.
2. Write a one-page contract under `dail_tracker_bold_ui_contract_pack_v5/utility/page_contracts/` if it's surfaced on its own page.
3. Add a sandbox script under `pipeline_sandbox/` first.
4. Add a refresh entry to v4 §3 with cadence and failure handling.
5. Add a manifest writer for the resulting mart.
6. Add a fixture-based regression test before promoting to `pipeline.py`.
7. Move the entry from this doc to v4 with a "graduated YYYY-MM-DD → see v4 §X.Y" pointer.

Steps 1–6 are non-negotiable. The pattern protects what's already working.
