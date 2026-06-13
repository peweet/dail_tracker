# Dáil Tracker — Central Bank of Ireland (CBI) Second-Pass Source Brief and Reconciliation

**Prepared:** 2026-06-05  
**Scope:** Central Bank of Ireland sources only, including PDFs and actual document families.  
**Purpose:** Reconcile what Dáil Tracker already appears to ingest against additional CBI sources worth researching for due-diligence, public-record intelligence, corporate-risk, regulated-entity, and enforcement use cases.

---

## 1. What Dáil Tracker already appears to ingest from CBI

Based on inspection of `pipeline.py` and `extractors/cbi_registers_extract.py`, the current CBI integration is focused on the **CBI Registers download page**.

### Confirmed current CBI chain

`pipeline.py` includes:

```text
("cbi", "extractors/cbi_registers_extract.py")
```

The pipeline comment says the CBI chain runs late because its corporate-notices cross-reference joins gold corporate notices produced by Iris Oifigiúil against the CBI register extract.

### Confirmed source

```text
https://registers.centralbank.ie/downloadspage.aspx
```

### Confirmed current outputs

The extractor docstring lists:

```text
data/sandbox/_cbi_raw/*.pdf
  cached source PDFs

data/sandbox/parquet/cbi_authorised_firms.parquet
  flattened firm rows, intermediate

data/sandbox/parquet/cbi_xref_member_interests.parquet
  experimental, unused

data/sandbox/parquet/cbi_xref_lobbying_entities.parquet
  experimental, unused

data/gold/parquet/cbi_xref_corporate_notices.parquet
  promoted, read by Corporate page

data/sandbox/_cbi_meta.json
  extraction stats
```

### Confirmed promoted use

The extractor states that only the corporate-notices cross-reference is promoted to gold, because it is load-bearing for the Corporate page via:

```text
v_corporate_cbi_notice_match
v_corporate_cbi_notice_repeat_distress
```

from:

```text
sql_views/corporate_cbi_distress.sql
```

### Current extractor limitations noted in repo

The extractor itself says:

```text
- Source PDFs are SSRS-rendered tables.
- Column inference is heuristic.
- The raw firm table carries false positives.
- Exact-match xref filters the corporate-notice match.
- Two registers, CIT Providers and Designated Entities, fail on direct postback today and are left out of scope.
- Member-interests and lobbying xrefs remain sandbox-grade.
- Match tiers are conservative.
```

### Practical interpretation

Dáil Tracker currently uses CBI primarily as:

```text
regulated-firm register extract
  + exact-name cross-reference against Iris corporate notices
  = regulatory/corporate-distress enrichment
```

It does **not** currently appear to ingest the broader CBI legal-notice, warning, enforcement, thematic-supervision, Dear CEO, protected-disclosure, consultation, or regulatory-publication universe.

---

## 2. CBI register coverage already partly captured

The registers download page is broad. Current ingestion likely attempts to download many PDFs from the register download page, subject to extractor limitations.

Source:

```text
https://registers.centralbank.ie/downloadspage.aspx
```

Examples of register categories exposed there include:

| Register family | Example records available | Already likely covered? | Notes |
|---|---|---:|---|
| Credit institutions | Register of Credit Institutions; Annual List of Licence Holders; Designated Credit Institutions | Yes, if PDF downloaded | Useful for banks and authorised credit institutions. |
| Credit unions | Register of Credit Unions | Yes | Important for local/regional financial institutions. |
| Life and non-life insurance undertakings | Life undertakings; freedom-of-services life undertakings; non-life undertakings; temporary run-off regime | Yes | Useful for insurer identity/regulatory footprint. |
| Reinsurance | Reinsurance undertakings and special purpose reinsurance vehicles | Yes | Niche but relevant for insurance markets. |
| MiFID investment firms | Authorised MiFID firms; data reporting service providers; third-country investment firm branches | Yes | Important for investment firm due diligence. |
| Investment intermediaries | Section 10 register; RAIPI register | Yes | Relevant to brokers/intermediaries. |
| Insurance distribution | Insurance Distribution Register; temporary run-off intermediaries | Yes | Large and potentially noisy. |
| Mortgage intermediaries | Revoked mortgage intermediaries; mortgage credit intermediaries; EEA mortgage credit intermediaries | Yes | Contains status/revocation-type signals. |
| Investment product intermediaries | Section 31 register | Yes | Retail/intermediary due diligence. |
| AIFs / UCITS / funds | ICAVs, designated investment companies, unit trusts, common contractual funds, investment limited partnerships, UCITS | Yes | High volume, high false-positive risk. |
| AIFMs / UCITS managers | Authorised/registered AIFMs and UCITS management companies, including cross-border/branch entries | Yes | Important for funds ecosystem. |
| High-cost credit providers | Active and revoked high-cost credit providers | Yes | Consumer-credit risk signal. |
| Retail credit and home reversion firms | Register of retail credit firms and home reversion firms | Yes | Mortgage/credit servicing relevance. |
| Moneybrokers, money transmission, bureaux de change | Authorised lists | Yes | AML/financial-services footprint. |
| Payment services and e-money | Payment institutions, AISPs, EMIs, small EMIs, related credit-union lists | Yes | Fintech/payments relevance. |
| Debt management, credit servicing | Debt management firms, credit servicing firms, credit servicers, passporting in | Yes | Credit/debt market intelligence. |
| ICAV registers | Registered ICAVs and ICAV charges | Yes | Fund/corporate charge signal, but extraction quality must be checked. |
| VASPs / CASPs | VASP register and MiCAR crypto-asset service provider register | Yes if present in download list | Crypto regulatory footprint. |
| Schedule 2 firms / TCSP subsidiaries | AML-relevant registers | Yes if downloaded | Useful for AML/TCSP due diligence. |
| ATM deployers / CIT providers / designated entities | Access-to-cash infrastructure registers | Partially / currently limited | Repo notes CIT Providers and Designated Entities fail on direct postback. |

### Reconciliation conclusion

The existing CBI ingestion already has **breadth across authorised/registered firms**, but it is:

```text
register-centric
PDF-centric
heuristic
mostly sandbox except corporate-notices xref
not yet a general regulated-entity product layer
```

The biggest missing CBI value is **not more register PDFs alone**. It is linking those registers to CBI enforcement, warning, revocation, prohibition, thematic-supervision, and correspondence documents.

---

## 3. CBI sources not currently covered or only indirectly covered

## 3.1 Warning Notices — unauthorised firms / clones

Source:

```text
https://www.centralbank.ie/news-media/warning-notices
```

### What is available

The Warning Notices page lists unauthorised-firm warnings with dates, category “Warning Notice,” Irish-language view links, and article pages.

Recent examples visible in search/page output include:

```text
HSBC Continental Europe (CLONE)
Insight Investment Solutions ICAV (CLONE)
AMOVA Asset Management Ireland Limited (Clone)
Apel Investments (CLONE)
Euro Bonds Finder / Irish Rates Finder
Research Vision Limited (CLONE)
Compare Bonds Ltd
Fire Financial Services Limited (CLONE)
Aviva Life & Pensions Ireland DAC (Clone)
Promontoria Scariff Designated Activity Company (Clone)
```

Example article:

```text
https://www.centralbank.ie/news/article/callanor--central-bank-of-ireland-issues-warning-on-unauthorised-firm
```

Another example:

```text
https://www.centralbank.ie/news/article/harbor-valtrix--central-bank-of-ireland-issues-warning-on-unauthorised-firm
```

### Why it matters

This is a high-value due-diligence dataset because it identifies:

```text
unauthorised firm names
clone firms
websites used
email addresses used
phone numbers used
claimed authorisation status
category of unauthorised activity
date of warning
```

### Reconciliation

This does **not** appear covered by the current CBI register extractor. The current extractor points only at the register download page, not news/legal warning pages.

### Suggested research questions

```text
- Are warning notices paginated in stable HTML?
- Can each article be parsed for unauthorised firm name, website, email, phone, category, and cloned authorised firm?
- Is there an RSS feed or site-search endpoint?
- Are Irish-language versions duplicative or useful?
- Can warning names be matched against CRO, company registers, domain names, and existing lobbying/procurement/company data?
```

---

## 3.2 Enforcement Actions — Administrative Sanctions Procedure

Source:

```text
https://www.centralbank.ie/news-media/legal-notices/enforcement-actions
```

Example article:

```text
https://www.centralbank.ie/news/article/enforcement-action--cantor-fitzgerald-ireland-limited-fined--452-790-by-the-central-bank-of-ireland-for-breach-of-the-market-abuse-regulation
```

Example PDF:

```text
https://www.centralbank.ie/docs/default-source/news-and-media/legal-notices/settlement-agreements/public-statement-relating-to-enforcement-action-against-danske-bank.pdf
```

Another PDF example:

```text
https://www.centralbank.ie/docs/default-source/news-and-media/legal-notices/settlement-agreements/public-statement-relating-to-enforcement-action-against-permanent-tsb-p-l-c.pdf
```

### What is available

The enforcement page explains that:

```text
- sanctions under admissions-based settlements since 19 April 2023 must be confirmed by the High Court;
- prohibition/disqualification notices are on the Prohibition Notices page;
- adverse assessment outcomes and post-assessment commentaries are separate pages.
```

Individual enforcement pages/PDFs contain:

```text
firm/person name
fine amount
reprimand/disqualification outcome
legal basis
breach description
period of misconduct
customer impact
mitigating/aggravating factors
cooperation
notes
related High Court confirmation context after April 2023
```

### Why it matters

This is one of the most important CBI due-diligence sources.

It can support:

```text
regulated-entity risk history
fine/enforcement timeline
breach category
market abuse / AML / consumer protection / governance failures
named individual accountability
relationship to authorised register
```

### Reconciliation

Not covered by current register-centric ingestion. Current CBI data can tell you if an entity is authorised/registered. It cannot tell you whether the entity has been fined or reprimanded unless that entity appears in Iris corporate notices and CBI register cross-ref.

### Suggested research questions

```text
- Can enforcement actions be parsed from both article HTML and PDF public statements?
- Can fine amounts and legal regimes be extracted reliably?
- Is there pagination or archive depth?
- Are PDF filenames stable?
- Can named individuals be separated from firms?
- Can post-2023 High Court confirmation status be captured?
```

---

## 3.3 Prohibition Notices, disqualifications, and Fitness & Probity actions

Source:

```text
https://www.centralbank.ie/news-media/legal-notices/prohibition-notices
```

Example PDF:

```text
https://www.centralbank.ie/docs/default-source/news-and-media/legal-notices/prohibition-notices/statement-re-anne-butterly-18-sept.pdf
```

Related guidance:

```text
https://www.centralbank.ie/regulation/how-we-regulate/fitness-probity/investigations-enforcement
```

Consultation / draft guidance:

```text
https://www.centralbank.ie/publication/consultation-papers/cp166-consultation-on-prohibition-notices-under-the-fitness-and-probity-regime
https://www.centralbank.ie/docs/default-source/regulation/how-we-regulate/fitness-probity/supplemental-guidance-on-prohibition-january-2026.pdf
```

### What is available

Prohibition notices relate to individuals who lack fitness and/or probity for controlled functions or pre-approval controlled functions.

PDFs and articles may include:

```text
person name
role
firm
duration of prohibition
function scope
fitness/probity findings
associated firm/regulatory context
legal basis
date
```

### Why it matters

This is the CBI source closest to individual-level regulatory due diligence.

### Reconciliation

The existing CBI register extractor does not appear to ingest prohibition notices. It may contain authorised firms but not fitness/probity sanctions against individuals.

### Sensitivity note

This is public legal/regulatory data, but it is individual-level and high sensitivity. Any downstream product should avoid unsupported inferences.

---

## 3.4 Adverse Assessments and Post Assessment Commentaries

Sources:

```text
https://www.centralbank.ie/news-media/legal-notices/adverse-assessments
https://www.centralbank.ie/news-media/legal-notices/enforcement-actions/post-assessment-commentaries
```

Example PDF:

```text
https://www.centralbank.ie/docs/default-source/news-and-media/legal-notices/settlement-agreements/post-assessment-commentaries/directors-commentary-dated-27-october-2022.pdf
```

### What is available

These relate to outcomes under assessor regimes and commentary following assessment outcomes.

### Why it matters

This fills a gap between general enforcement and individual/market-abuse assessment processes.

### Reconciliation

Not covered by current register extraction.

### Research questions

```text
- Are adverse assessment pages structured enough to parse?
- Do commentaries name individuals/firms, or mostly describe regime-level lessons?
- Can these be linked to enforcement/prohibition notices?
```

---

## 3.5 Revocation Notices

Source:

```text
https://www.centralbank.ie/news-media/legal-notices/revocation-notices
```

### What is available

Revocation notices are a legal-notice family on the Central Bank site. The sitemap confirms revocation notices are grouped alongside enforcement, prohibition notices, IFSAT tribunal decisions and adverse assessments.

### Why it matters

Revocation is a strong regulated-status signal:

```text
authorisation revoked
licence/register status changed
consumer/investor protection implications
historical regulatory risk
```

### Reconciliation

The registers download page includes some revoked registers, such as revoked mortgage intermediaries and revoked high-cost credit providers, but a revocation-notice archive is broader/different and likely not captured by the current CBI extractor.

---

## 3.6 IFSAT Tribunal Decisions

Source:

```text
https://www.centralbank.ie/news-media/legal-notices/irish-financial-services-appeals-tribunal-decisions
```

### What is available

Public statements by the Central Bank in relation to Irish Financial Services Appeals Tribunal decisions. One visible example concerns an appeal of a Central Bank decision under the Fitness & Probity regime.

### Why it matters

This is an appeal/outcome layer for CBI decisions.

### Reconciliation

Not covered by current register extraction.

### Research questions

```text
- Are decisions mostly HTML, PDF, or external tribunal documents?
- Do they name firms/individuals?
- Can they be linked to prohibition/adverse assessment records?
```

---

## 3.7 Inquiries

Source:

```text
https://www.centralbank.ie/news-media/legal-notices/inquiries
```

### What is available

Inquiry-related pages, such as:

```text
Insurance Intermediary Inquiry
Permanent TSB p.l.c. and Mr David Guinane Inquiry
Attend an Inquiry Hearing
```

### Why it matters

Inquiries are high-signal formal regulatory proceedings.

### Reconciliation

Not covered by current register extraction.

---

## 3.8 Dear CEO letters and supervisory correspondence

Source family:

```text
https://www.centralbank.ie/publication/correspondence
```

Examples:

```text
https://www.centralbank.ie/docs/default-source/publications/regulatory-and-supervisory-outlook-reports/dear-ceo-letter-key-regulatory-and-supervisory-priorities-2026.pdf

https://www.centralbank.ie/docs/default-source/regulation/industry-market-sectors/investment-firms/mifid-firms/regulatory-requirements-and-guidance/dear-ceo-letter-common-supervisory-action-on-mifid-ii-marketing-communications-requirements.pdf

https://www.centralbank.ie/docs/default-source/regulation/consumer-protection/compliance-monitoring/themed-inspections/dear-ceo-letter-thematic-review-on-early-mortgage-arrears.pdf

https://www.centralbank.ie/docs/default-source/publications/correspondence/dear-ceo-letters/thematic-review-on-ongoing-suitability-of-long-term-life-assurance-products.pdf

https://www.centralbank.ie/docs/default-source/regulation/industry-market-sectors/investment-firms/mifid-firms/regulatory-requirements-and-guidance/common-supervisory-action-on-mifid-ii-costs-and-charges-requirements.pdf
```

### What is available

Dear CEO / industry letters contain:

```text
sector targeted
thematic review findings
regulatory expectations
common failings
remediation expectations
legal/regulatory basis
industry-wide risk signal
sometimes sector-specific review methodology
```

### Why it matters

These are excellent **sector risk / policy intelligence** sources, even where no firm is individually named.

For business reports, they can support statements like:

```text
The Central Bank has identified sector-wide weaknesses in X.
The Central Bank expects firms in this sector to improve Y.
This entity operates in a sector currently under heightened supervisory focus.
```

### Reconciliation

Not covered by current register extraction.

### Research questions

```text
- Can letters be indexed by sector, date, regime, and risk topic?
- Can each letter be linked to relevant CBI register categories?
- Are some letters tied to named firms, or mostly sector-level?
- Can these power “regulatory climate” sections in reports?
```

---

## 3.9 AML/CFT correspondence, bulletins, and sector reports

Source:

```text
https://www.centralbank.ie/regulation/anti-money-laundering-and-countering-the-financing-of-terrorism/legislation
```

Examples on the page include:

```text
Dear CEO letter – to High Cost Credit Providers
AML Bulletin on Virtual Asset Service Providers
AML Bulletin on Funds and Fund Management Companies
Dear CEO Letter - Compliance by Entities Required to Register under Section 108A
AML Bulletin on Transaction Monitoring
Dear CEO Letter - Compliance by Trust or Company Service Providers
AML Bulletin on Customer Due Diligence
AML Bulletin on Suspicious Transaction Reporting
Report on AML/CFT and Financial Sanctions Compliance in Irish Life Insurance Sector
Report on AML/CFT and Financial Sanctions Compliance in Irish Funds Sector
Report on AML/CFT and Financial Sanctions Compliance in Irish Credit Union Sector
Report on AML/CFT and Financial Sanctions Compliance in Irish Bank Sector
```

### Why it matters

This is highly relevant for:

```text
VASP / crypto
funds
trust or company service providers
high-cost credit providers
payment firms
financial sanctions
AML/CFT compliance risk
```

### Reconciliation

The current CBI extractor may ingest Schedule 2 and VASP/CASP registers but not the sector-level AML/CFT guidance, bulletins, or thematic reports.

---

## 3.10 Regulatory & Supervisory Outlook Reports

Source:

```text
https://www.centralbank.ie/publication/regulatory---supervisory-outlook-report
```

Examples:

```text
https://www.centralbank.ie/docs/default-source/publications/regulatory-and-supervisory-outlook-reports/regulatory-supervisory-outlook-report-2025.pdf
https://www.centralbank.ie/docs/default-source/publications/regulatory-and-supervisory-outlook-reports/dear-ceo-letter-key-regulatory-and-supervisory-priorities-2026.pdf
```

### What is available

Annual supervisory priorities and risk outlook across the financial sector.

### Why it matters

This gives current regulatory priorities by sector/topic.

### Reconciliation

Not covered by current CBI register extract.

---

## 3.11 Annual Reports and Annual Performance Statements

Sources:

```text
https://www.centralbank.ie/publication/corporate-reports/central-bank-annual-report-and-annual-performance-statement-2024
https://www.centralbank.ie/publication/corporate-reports/annual-reports
https://www.centralbank.ie/publication/corporate-reports/annual-performance-statement
```

Example PDF:

```text
https://www.centralbank.ie/docs/default-source/publications/corporate-reports/annual-reports/annual-report-2024-and-annual-performance-statement-2024-2025.pdf
```

### What is available

Annual enforcement summaries, policy priorities, performance against mandate, regulatory activity, governance, and system-wide statistics.

The 2024 annual report search result referenced enforcement actions such as Goodbody, Waystone and other sanctions.

### Why it matters

Useful for:

```text
annual regulatory context
summary of enforcement activity
macro supervision themes
statistics and trends
public-body governance
```

### Reconciliation

Not covered by current register extract.

---

## 3.12 Consultation Papers

Source:

```text
https://www.centralbank.ie/publication/consultation-papers
```

Example:

```text
https://www.centralbank.ie/publication/consultation-papers/cp166-consultation-on-prohibition-notices-under-the-fitness-and-probity-regime
```

PDF examples:

```text
https://www.centralbank.ie/docs/default-source/publications/consultation-papers/cp166/cp166---prohibition-notices-under-the-fitness-and-probity-regime.pdf
https://www.centralbank.ie/docs/default-source/regulation/how-we-regulate/fitness-probity/supplemental-guidance-on-prohibition-january-2026.pdf
```

### Why it matters

Consultations show future regulatory change, upcoming obligations, and industry feedback windows.

### Reconciliation

Not covered by current register extraction.

---

## 3.13 Markets Updates, ESMA / IOSCO / Securities Markets materials

Source family visible in navigation:

```text
https://www.centralbank.ie/regulation/industry-market-sectors/securities-markets/markets-update
```

### Why it matters

Useful for funds, MiFID, securities markets, market abuse, listed entities, prospectus/markets changes.

### Reconciliation

Not covered by current register extraction.

---

## 3.14 Consumer Protection research, thematic inspections, compliance monitoring

Source families visible in CBI navigation and search results:

```text
https://www.centralbank.ie/regulation/consumer-protection/compliance-monitoring
https://www.centralbank.ie/publication/consumer-protection-research
```

Example PDFs already found:

```text
https://www.centralbank.ie/docs/default-source/regulation/consumer-protection/compliance-monitoring/themed-inspections/industry-letter---targeted-consumer-protection-risk-assessment.pdf

https://www.centralbank.ie/docs/default-source/regulation/consumer-protection/compliance-monitoring/themed-inspections/dear-ceo-letter-thematic-review-on-early-mortgage-arrears.pdf

https://www.centralbank.ie/docs/default-source/regulation/consumer-protection/compliance-monitoring/themed-inspections/retail-intermediaries/industry-letter---thematic-review-of-data-submitted-in-retail-intermediaries-annual-returns.pdf
```

### Why it matters

Strong for sector due-diligence and consumer-risk context.

### Reconciliation

Not covered by current register extraction.

---

## 3.15 Protected Disclosures / Whistleblowing reports

Source family visible in CBI navigation:

```text
https://www.centralbank.ie/regulation/how-we-regulate/protected-disclosures-whistleblowing
```

Related annual reports are linked in navigation as:

```text
2025 Report on Protected Disclosures
2024 Report on Protected Disclosures
```

### Why it matters

Useful for regulator-level governance and sector complaint/disclosure trend context.

### Reconciliation

Not covered by current register extraction.

---

## 3.16 Access to Cash / cash infrastructure monitoring

CBI navigation includes:

```text
Access to Cash Overview
Access to Cash Consultation Papers
Registration of ATM deployers and cash-in-transit providers
Quarterly Cash Infrastructure Monitoring Data
```

Registers page includes:

```text
Register of ATM Deployers
Register of CIT Providers
Register of Designated Entities
```

### Why it matters

This is a concrete overlap with your “local services / public infrastructure / due diligence” theme:

```text
cash access infrastructure
ATM deployers
cash-in-transit providers
designated entities
quarterly infrastructure monitoring
regional service coverage
```

### Reconciliation

The register page has the register PDFs, but the repo notes CIT Providers and Designated Entities fail on direct postback. The monitoring data is not covered by the register extractor.

---

## 3.17 Open Data Portal / Statistics / Frontier Statistics

CBI navigation exposes:

```text
Open Data Portal
Statistics
Frontier Statistics
Total Domestic Credit
Mortgage Interest Rate Distributions
Non-Bank New Lending to Irish Enterprises
Household Debt
Investment and Money Market Funds
Overdue Loan Balances
```

Source example:

```text
https://www.centralbank.ie/statistics
```

AnaCredit example:

```text
https://www.centralbank.ie/statistics/statistical-reporting-requirements/anacredit-in-ireland
```

AnaCredit page includes PDF manuals and XLS resources, including:

```text
AnaCredit Regulation Data Tables
Complete Central Bank of Ireland reports – Case data
AnaCredit Reporting Population
Central Bank AnaCredit and RS2 Mapping
Outlier Thresholds
```

### Why it matters

This is not entity due diligence in the same way, but it can support:

```text
sector context
credit market trends
mortgage / household debt context
non-bank lending context
funds market context
macro-financial background
```

### Reconciliation

Not covered by current register extraction.

---

## 3.18 DORA / Operational Resilience / Cyber reporting guidance

Example PDF:

```text
https://www.centralbank.ie/docs/default-source/regulation/dora-templates/guide-to-submitting-dora-registers-on-the-central-bank-of-ireland-portal.pdf
```

### Why it matters

Not public firm-level incident data, but useful for regulated-sector operational resilience context.

### Reconciliation

Not covered by current register extraction.

---

## 4. Highest-value CBI additions for Dáil Tracker

Ranked by likely value for due diligence:

| Rank | Source family | Why it is valuable | Relation to current CBI ingestion |
|---:|---|---|---|
| 1 | Enforcement actions | Direct firm/person sanctions, fines, breach descriptions | Not currently covered |
| 2 | Warning notices | Unauthorised/clone firm names, websites, emails, phone numbers | Not currently covered |
| 3 | Prohibition notices | Individual-level fitness/probity restrictions | Not currently covered |
| 4 | Revocation notices | Authorisation status loss / legal status change | Partially reflected in some registers, but not notice archive |
| 5 | Dear CEO / thematic supervision letters | Sector risk, supervisory expectations, common weaknesses | Not currently covered |
| 6 | AML/CFT correspondence and reports | Sector AML / sanctions / VASP / TCSP risks | Not currently covered |
| 7 | Regulatory & Supervisory Outlook | Annual regulatory priorities and risk themes | Not currently covered |
| 8 | Annual Report / APS | Annual enforcement and regulatory activity summary | Not currently covered |
| 9 | Adverse assessments / post-assessment commentaries | Assessor regime / market abuse commentary | Not currently covered |
| 10 | IFSAT decisions / inquiries | Appeals and formal proceedings | Not currently covered |
| 11 | Access-to-cash monitoring data | Infrastructure/service coverage layer | Registers partly attempted; monitoring not covered |
| 12 | Statistics/open data | Macro/sector context | Not currently covered |

---

## 5. CBI “regulated-entity intelligence” source model

Useful categories to research:

```text
AUTHORISED / REGISTERED
  CBI register PDFs already partly ingested

UNAUTHORISED / WARNING
  warning notices and clone warnings

SANCTIONED / ENFORCED
  enforcement actions, settlement agreements, public statements

REVOKED / PROHIBITED / DISQUALIFIED
  revocation notices, prohibition notices, disqualification notices

APPEALED / ASSESSED / INQUIRY
  IFSAT decisions, adverse assessments, post-assessment commentaries, inquiries

SECTOR-WIDE RISK
  Dear CEO letters, thematic reviews, AML/CFT correspondence, RSO reports

REGULATORY CHANGE
  consultation papers, guidance, codes, policy publications

MACRO / MARKET CONTEXT
  annual reports, statistical publications, open data, frontier statistics
```

---

## 6. Possible output fields to research for each source family

### Warning notice record

```text
notice_date
notice_title
unauthorised_firm_name
clone_flag
cloned_authorised_entity
activity_type
website_urls
email_addresses
phone_numbers
authorisation_statement
source_url
irish_language_url
retrieved_utc
```

### Enforcement action record

```text
action_date
entity_name
person_name
firm_c_code_if_available
legal_regime
breach_category
fine_amount_eur
reprimand_flag
disqualification_flag
settlement_flag
high_court_confirmation_required
high_court_confirmation_status
public_statement_pdf_url
article_url
source_text_excerpt
retrieved_utc
```

### Prohibition notice record

```text
notice_date
person_name
firm_name
role
function_scope
prohibition_duration
legal_basis
reason_summary
source_pdf_url
source_article_url
retrieved_utc
```

### Revocation notice record

```text
notice_date
entity_name
register_category
authorisation_type
revocation_reason
effective_date
source_url
retrieved_utc
```

### Dear CEO / thematic review record

```text
publication_date
title
sector
regulatory_topic
risk_theme
document_type
legal_basis
key_expectations
named_firms
source_pdf_url
retrieved_utc
```

### CBI annual/outlook record

```text
report_year
report_type
publication_date
topic
sector
key_risk_theme
enforcement_summary
source_pdf_url
retrieved_utc
```

---

## 7. Research checklist for Claude

Ask Claude to check:

```text
1. Which CBI legal-notice pages are paginated and how pagination works.
2. Whether warning notices have stable article structure.
3. Whether warning notices expose names/websites/emails/phones in structured HTML.
4. Whether enforcement action pages contain enough HTML, or whether PDF parsing is required.
5. Whether fine amounts and breach categories can be extracted reliably.
6. Whether High Court confirmation status appears on enforcement pages after April 2023.
7. Whether prohibition notices are all PDFs, HTML, or both.
8. Whether revocation notices contain named firms and effective dates.
9. Whether IFSAT decisions are Central Bank summaries only or link to tribunal documents.
10. Whether Dear CEO letters can be enumerated from the correspondence archive.
11. Whether thematic inspections are under stable sector paths.
12. Whether AML/CFT correspondence has a stable page with PDF links.
13. Whether Regulatory & Supervisory Outlook reports have stable annual archive links.
14. Whether Annual Reports and APS PDFs contain structured tables of enforcement actions.
15. Whether CBI Open Data Portal exposes machine-readable datasets or API endpoints.
16. Whether Access to Cash quarterly monitoring data is downloadable.
17. Whether CBI register PDFs have direct URLs that can replace ASP.NET postbacks.
18. Whether CIT Providers and Designated Entities can be obtained another way.
19. Whether all PDFs carry publication dates in metadata or page text.
20. Whether CBI source pages provide RSS feeds or sitemap discovery.
```

---

## 8. Reconciliation summary

Current Dáil Tracker CBI coverage:

```text
CBI register PDFs from registers.centralbank.ie/downloadspage.aspx
  -> heuristic firm extraction
  -> sandbox authorised-firm table
  -> sandbox member-interest and lobbying xrefs
  -> promoted corporate-notices xref
```

Major CBI gaps:

```text
warning notices
enforcement actions
public statement PDFs
prohibition/disqualification notices
revocation notices
adverse assessments
post-assessment commentaries
IFSAT decisions
inquiries
Dear CEO letters
thematic review letters
AML/CFT bulletins and reports
Regulatory & Supervisory Outlook reports
annual reports / APS
consultation papers
protected-disclosure reports
access-to-cash quarterly monitoring
CBI statistics/open-data publications
```

Best next CBI research angle:

```text
Turn CBI from “authorised-register enrichment” into “regulated-entity intelligence”:
  authorised / registered
  unauthorised / warning
  sanctioned / enforced
  revoked / prohibited
  subject of supervisory concern
  sector-wide regulatory risk
```

This would materially improve due-diligence reports because it connects an entity to both its regulatory status and its regulatory history.
