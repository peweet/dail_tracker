---
tier: PLAN
status: LIVE
domain: sources
updated: 2026-06-28
supersedes: []
read_when: scoping or prioritizing a new data source to ingest into the pipeline
key: PLAN|LIVE|sources
---

# NEW_SOURCE_INGESTION_PLAN.md — build-ready new-source roadmap

_Compiled 2026-06-28. This is the **implementation-level** companion to the two
existing catalogues:_

- **[`SOURCES.md`](SOURCES.md)** — what is *already implemented* (chains, outputs, API). Source of truth for "is it built?".
- **[`ROADMAP_SOURCES.md`](ROADMAP_SOURCES.md)** — the *candidate* list at card level (status · link · joins · value · difficulty).
- **[`archive/ENRICHMENTS.md`](archive/ENRICHMENTS.md)** — the long-form scoping cards + the parked URL audit (**archived 2026-06-28** — cited for provenance only; this plan is its live successor).

This document does the next thing those three don't: it **scores, prioritises,
and writes the actual build plans** (schemas, poll methods, join keys, GitHub
issues) for the sources worth adding next. It was produced by scanning
`pipeline.py`, `api/routers/catalog.py`, `data/_meta/freshness.json`,
`planning_rules/_corpus_registry/planning_corpus_seed.csv`, the `extractors/`,
`sql_views/`, and `utility/pages_code/` trees, and `DATA_LIMITATIONS.md` — then
verifying retrieval links against the live web on 2026-06-28.

> **Read the non-overclaiming rules (§12) before quoting anything here.** Every
> source below is `candidate` / `partial` / `sandbox` / `blocked` **unless a
> pipeline chain and a gold/silver output are cited.** Nothing in this file is a
> claim that a candidate source is implemented.

---

## 1. Executive summary

Dáil Tracker already ingests ~40 source families across parliament, lobbying,
procurement awards, public-body payments, Iris corporate/SI/appointments,
judiciary, political finance, local-authority accountability, CSO denominators,
and planning/geospatial layers (full list in `SOURCES.md`). The biggest *gaps*
are not in the parliamentary surface — they are in the **money-and-accountability
perimeter around it**: the independent audit layer, public money that flows as
grants rather than contracts, the electoral outcome that political finance
attaches to, and the regulatory/transparency-dispute record.

**Six sources are recommended as P0 (add next):**

| # | Source | Why now | Repo status today |
|---|---|---|---|
| 1 | **C&AG audit reports** (Special Reports + Report on the Accounts of the Public Services) | Independent audit layer; corroborates spending concerns; joins to every money fact already built | `candidate` — not found as ingested dataset |
| 2 | **OGP central arrangements / frameworks catalogue** | Pre-award route-to-market intelligence; the commercial product's missing "frameworks" leg | `candidate` — awards are built, the framework *catalogue* is not |
| 3 | **Department grant registers** | Captures public money *outside* procurement; joins to charities, companies, constituencies, lobbying | `candidate` |
| 4 | **Election results (Electoral Commission / count data)** | Seat-safety + spend-per-vote context for the already-built SIPO finance | `candidate` / `not-found` — SIPO finance is built; results are not |
| 5 | **OIC / FOI decisions** | Transparency-dispute surface; a live, structured, 3,434-record searchable DB | `candidate` |
| 6 | **data.gov.ie metadata discovery (general monitoring layer)** | Source discovery + link-rot detection; the CKAN action API is live (verified 2026-06-28: `success:true`, `metadata_modified` + resource `format` per dataset) | `candidate` — specific endpoints are used; a *general* crawler is not |

**Highest-value but access-constrained:** RBO beneficial ownership stays
`blocked` (post-2022 CJEU access restriction) and the Land Registry ownership
layer stays `blocked` (paid per-search). Do not start either without a documented
lawful access route.

**Cheapest high-leverage wins:** OIC/FOI decisions (clean HTML DB), Companies
House UK (free OGL REST API), the data.gov.ie monitor (one CKAN endpoint), and
operationalising the Oireachtas publications index that `pdf_endpoint_check.py`
already half-uses.

The single rule that governs all of this: **none of these new money sources may
be summed with each other or with the existing award / payment / allowance
grains** (`DATA_GRAINS.md`). C&AG audited outturn, grant disbursements, capital
cost estimates, procurement award ceilings, and public-body payments are five
different things.

---

## 2. Current implemented-vs-candidate correction table

Status taxonomy (matches `SOURCES.md`): `implemented` · `partial` · `sandbox` ·
`manual-input` · `candidate` · `blocked` · `not-recommended` · `deprecated/broken`.

> The point of this table is to stop the recurring confusion between a *built*
> component and an *adjacent candidate* that sounds the same.

| # | Candidate source | **Status after repo scan** | Evidence in repo | Do not confuse with… |
|---|---|---|---|---|
| 1 | C&AG audit reports | `candidate` | No extractor/chain; only cited in docs + as a UI citation on `accommodation_spend.py` | …LA AFS (`afs` chain) or NOAC (`noac_*` chains). Those are **council** financials, not the C&AG. |
| 2 | C&AG Appropriation Accounts | `candidate` | Not found; `cso` chain builds **national** gov-finance aggregates (GFA01/GFQ01/NA012 → `v_gov_finance_annual`), not Vote-level appropriation accounts | …CSO Government Finance Statistics (built, national) and …LA AFS (built, council). |
| 3 | PAC reports + transcripts | `partial` | `committee_evidence` chain ingests committee *meeting history* (incl. PAC) since 2024-09 → `v_committee_meetings`. PAC **report documents** + the C&AG-reference structure are **not** parsed | …the generic committees view; PAC-specific report parsing is the gap. |
| 4 | OGP central arrangements / frameworks | `candidate` | `procurement` chain builds eTenders/OGP **awards** (`procurement_awards`, 62,763). The framework **catalogue** (arrangement name, CPB, lots, expiry) is not | …eTenders/OGP **awards** (built). Different object entirely. |
| 5 | Department grant registers | `candidate` | No extractor | …public-body **payments** (`procurement_payments_fact`, built) and award ceilings. Grants are a third money channel. |
| 6 | Capital projects / investment tracker | `candidate` | No extractor | …procurement awards/payments. The tracker is *cost estimates + status*, not spend. |
| 7 | HSE Section 38/39 funding classification | `candidate` | `hse_tusla_payments` chain ingests HSE/Tusla **payments >€100k**; the §38/§39 funded-organisation **roster/classification** is not built | …the HSE payments fact (built). |
| 8 | HEA funding / statistics | `candidate` | No extractor | — |
| 9 | Sport Ireland / Arts Council grants | `candidate` | No extractor | — |
| 10 | EU Cohesion / Structural / CAP funds | `candidate` | `enrichment_promote_to_gold` built **EU-TAM state aid** + **ISIF portfolio** views; Cohesion/ERDF/CAP beneficiary data is not | …EU-TAM (built; use `aid_element_value`). |
| 11 | Electoral Commission **election results** | `candidate` / `not-found` | No results fact; SIPO political finance **is** built (`sipo_*`) | …**SIPO election finance** (built). Spend ≠ result. |
| 12 | EC boundaries / register / political advertising | `partial` | Constituency population/boundaries built (`ec_constituency_pop_2022`, 43 rows; `reference/ec_constituency_pop_extract.py`); register stats + political-advertising records are not | …the population denominator (built). |
| 13 | Referendum campaign finance | `candidate` | No extractor | …SIPO party/candidate election spend (built). |
| 14 | OIC / FOI decisions | `candidate` | No extractor; `procurement_public_body_extract.py` references the *FOI Act model scheme* (disclosure basis), not FOI **decisions** | — |
| 15 | DPC decisions | `candidate` | No extractor | — |
| 16 | Central Bank enforcement actions | **`implemented-not-api-exposed`** ⚠ *older docs say candidate — STALE* | **Built:** `pipeline_sandbox/cbi_enforcement_extract.py` → promoted to **gold** `cbi_enforcement_actions.parquet` by `extractors/enrichment_promote_to_gold.py`; `sql_views/corporate/corporate_cbi_enforcement.sql` (`v_corporate_cbi_enforcement`); wired in `utility/pages_code/corporate.py` + `corporate_data.py` + `dail_tracker_core/queries/corporate.py`; tested in `test/sql_views/test_enrichment_views.py`. **Firms only** (individuals dropped at promotion); `value_safe_to_sum=False`. **Not in API, not in `pipeline.py`** (manual enrichment-promote). | …CBI **authorisation registers** (separate `cbi` chain, sandbox substrate, 13.8k firms → `cbi_xref_corporate_notices`). Authorisations ≠ sanctions; **both** now exist. **Prohibition / F&P notices are deliberately NOT ingested** (name individuals). |
| 17 | Corporate Enforcement Authority (CEA/ex-ODCE) actions | `candidate` | No extractor | — |
| 18 | WRC / Labour Court decisions | `candidate` | No extractor | — |
| 19 | Coimisiún na Meán decisions / funding | `candidate` | No extractor | — |
| 20 | RBO beneficial ownership | `blocked` | No extractor; access restricted post-2022 CJEU | …CRO (built, Irish company register). RBO is the human-owner layer. |
| 21 | Companies House UK | `candidate` | No extractor; CRO (Irish) is built | …CRO (Irish, built). |
| 22 | Tailte Éireann / GeoHive boundaries & valuation | `partial` (boundaries) / `blocked` (ownership) | Tailte county/admin boundaries scoped in `planning_corpus_seed.csv` (PC27, CC-BY-4.0 ArcGIS); planning ArcGIS layers `used_live`/`partial`; **Land Registry ownership** is paid per-search | …planning layers (partial). Boundaries ≠ ownership records. |
| 23 | Property Price Register / vacant-derelict registers | `partial` | DHLGH **derelict-sites levy** built (`derelict_sites_levy_wide`, 31, 2024); **PPR transactions** + **vacant-site register** are not; CPO land-acquisition is sandbox (PC35) | …the derelict-sites **levy** (built). |
| 24 | Council minutes / agendas | `sandbox` | `pipeline_sandbox/council_minutes/` holds a multi-council OCR corpus (Cork City/County, Donegal, Kerry, Kilkenny, Monaghan, South Dublin, Tipperary…); not promoted to gold/API | — |
| 25 | Council named votes / material-contravention votes | `sandbox` / `partial` | `councillors_promote_to_gold` + `your_councillors.py` page (Carlow roll-call scoped); `planning_corpus_seed.csv` PC26 (material-contravention) `not_started` | — |
| 26 | data.gov.ie metadata discovery (general crawler) | `candidate` | Specific CKAN/ArcGIS endpoints are used (planning registry; OGP awards CSV); a **general** discovery/monitor layer is not built | …the specific endpoints already wired. |
| 27 | Oireachtas publications index | `partial` (operational) | Used as a fallback in `pdf_endpoint_check.py` for new-asset detection; no `dim_publication` mart | — |
| 28 | Gov.ie publication / press-release discovery | `candidate` | No extractor | — |

---

## 3. New-source catalogue

For each source: **status · owner · records/fields · format · cadence ·
join keys · joins to existing data · value · risk/caveats · difficulty · priority.**
Cards are grouped by the task's families. (Long-form scoping for many of these is
in `archive/ENRICHMENTS.md` under the section letter noted.)

### A. Audit & accountability

#### A.1 — C&AG audit reports — **P0** — `candidate`
- **Owner:** Office of the Comptroller and Auditor General (`audit.gov.ie`).
- **Records:** report title; report type (Appropriation Accounts / Report on the Accounts of the Public Services / Special Report); publication date; year; sector; topic; audited body; department; Vote/programme where applicable; key findings; recommendations; procurement/grants/governance/project flags; cost figures; source PDF URL; source HTML URL.
- **Format:** well-structured machine-generated PDFs, indexed by category + year on the site.
- **Cadence:** annual (Report on the Accounts of the Public Services) + ad-hoc Special Reports. No RSS/Last-Modified observed → poll the publications index for new entries.
- **Join keys:** `department`, `public_body_id`/`org_norm`, `year`, `vote`, `report_id`, `source_url`.
- **Joins to:** `procurement_payments_fact`, `procurement_awards`, departments/Votes, `v_gov_finance_annual`, accommodation spend, LA accountability, charities (where grant-funded bodies are audited).
- **Value:** H (public) / M–H (BI) — the only *independent* audit lens; turns "the data shows X" into "the auditor flagged X".
- **Risk/caveats:** dense PDFs; topic/finding extraction needs NLP or curation; a finding is a finding, **never** proof of wrongdoing elsewhere; do not blend audited outturn figures into any other money grain.
- **Difficulty:** M–H. **Build-readiness note:** the *Appropriation Accounts* portion (A.2) is the cheaper half — a born-digital, no-OCR extraction approach was already prototyped in scratch (memory `project_cag_appropriation_accounts_2026_06_21`: `get_text("words")` + x-coordinate banding, footing controls passing 95.6% for 2021–2024). That prototype lives only in transient scratch (`c:\tmp`), **not the repo** — but it means the parser is de-risked, not a cold start. Target the **2021–2024** window (non-competitive note wording drifts pre-2021); **never** difference vote-gross-outturn against procurement (big votes are ~99% grants/pay/transfers).

#### A.2 — C&AG Appropriation Accounts — **P0 (bundled with A.1)** — `candidate`
- **Owner:** C&AG (`audit.gov.ie`, "Appropriation Accounts" category).
- **Records:** Vote; department; programme; gross/net expenditure; variance vs allocation; notes; audit opinion; year; PDF/table links.
- **Format:** PDF tables (Camelot/fitz extractable, fidelity-gate as with NOAC).
- **Cadence:** annual.
- **Join keys:** `vote`, `department`, `year`.
- **Joins to:** `v_gov_finance_annual` (CSO national finance — *adjacent, not equal*), procurement/payments by department.
- **Value:** M–H — audited departmental baseline; a legitimate **denominator** for "department spent €Y on suppliers vs €Z voted".
- **Risk/caveats:** Vote-level audited outturn is **not** the CSO GFS aggregate and **not** procurement spend; label the grain explicitly.
- **Difficulty:** M.

#### A.3 — Public Accounts Committee reports + transcripts — **P1** — `partial`
- **Owner:** Houses of the Oireachtas (`oireachtas.ie/en/committees/`, partly via API).
- **Records:** committee meeting; report; public body; witnesses; C&AG report referenced; recommendations; government response; transcript link; publication date.
- **Format:** Oireachtas API (meetings) + HTML/PDF (reports). **Meetings since 2024-09 already ingested** by `committee_evidence`; the gap is the PAC *report* layer + the C&AG cross-reference.
- **Join keys:** `committee`, `member unique_member_code`, `public_body_id`, `report_id`, `debate_id` (dedupe against existing).
- **Joins to:** `v_committee_meetings`, members/debates, **A.1 C&AG reports** (the political-response layer).
- **Value:** M–H — closes "audit finding → political follow-up".
- **Risk/caveats:** dedupe by debate ID against existing committee ingest; do not double-count.
- **Difficulty:** L–M (reuses committee infra).

### B. Procurement & public money

#### B.1 — OGP central arrangements / frameworks catalogue — **P0** — `candidate`
- **Owner:** Office of Government Procurement + Education Procurement Service + LGOPC. **Verified 2026-06-28:** the catalogue is the OGP **Central Arrangements** page — `gov.ie/en/office-of-government-procurement/organisation-information/central-arrangements/` — plus the *Schedule of upcoming OGP Central Arrangements*; the Education Procurement Service lists its own at `educationprocurementservice.ie/resources/frameworks-arrangements/`. **The earlier-guessed `gov.ie/en/ogp-frameworks/` path is NOT the real URL.** (All `gov.ie` pages 403 automated fetches — human-valid; use a browser-like session.) Supplier member-lists + call-offs are gated behind **Buyer Zone** / **SupplyGov** (public-body accounts only) — ingest the public catalogue fields only.
- **Records:** arrangement name; central purchasing body; category; framework/DPS/panel type; lot; **supplier list only where public**; buyer eligibility; start date; end/expiry date; route-to-market instructions; Buyer-Zone reference; source page.
- **Format:** HTML listing (scrape; paginated, faceted).
- **Cadence:** continuous; poll listing `published_date`.
- **Join keys:** `cpv`/category, `supplier_norm` (where supplier lists are public), `cpb`, `arrangement_id`, `source_url`.
- **Joins to:** `procurement_awards` (which suppliers won places vs who actually got call-off awards), `v_procurement_supplier_summary`, suppliers, buyers.
- **Value:** H (BI especially) — the missing "frameworks intelligence" leg of the procurement product (renewal/expiry alerts, route-to-market for SMEs).
- **Risk/caveats:** **supplier member-lists are mostly Buyer-Zone-gated** — ingest only the public catalogue fields; do **not** scrape gated content. A framework place ≠ an award ≠ spend.
- **Difficulty:** M.

#### B.2 — Department grant registers — **P0** — `candidate`
- **Owner:** individual departments (per-dept `gov.ie` publication pages).
- **Records:** department; scheme; recipient; recipient type; amount; year; county/constituency if available; purpose; programme; source document.
- **Format:** wildly heterogeneous — XLSX / CSV / PDF / HTML tables per department.
- **Cadence:** quarterly–annual, per department.
- **Join keys:** `org_norm`/`charity_rcn`/`cro_number`, `department`, `county`/`constituency`, `year`, `source_url`.
- **Joins to:** charities (`charities_enriched`), CRO, constituencies, lobbying orgs, ministerial diaries.
- **Value:** H — the largest *uncaptured* public-money channel; "funded-then-lobbying" and constituency-funding-heatmap stories.
- **Risk/caveats:** per-department schema sprawl; some "grants" are statutory entitlements, not discretionary — flag `grant_basis`; geographic attribution is often scheme-level not project-level. **Do not** sum grant disbursements with procurement or payments.
- **Difficulty:** H (start with 3–5 highest-value departments, not all at once).

#### B.3 — Capital projects / investment tracker — **P1** — `candidate`
- **Owner:** Dept of Public Expenditure, Infrastructure, PSR & Digitalisation. `gov.ie/en/policy-information/96bb35-investment-projects-and-programmes-tracker/` (a.k.a. `gov.ie/2040`). Scope: projects/programmes **> €20m** (~340 projects + ~140 programmes).
- **Records:** project name; sponsoring department; delivery body; sector; county/location; cost estimate/band; status; stage; timeline; contractor where public; update date.
- **Format:** interactive map + downloadable dataset/PDF.
- **Cadence:** episodic (last full NDP update Q3 2025).
- **Join keys:** `department`, `delivery_body`/`org_norm`, `county`, `project_id`, `source_url`.
- **Joins to:** procurement awards/payments (which contractors are delivering), C&AG Special Reports (cost-overrun audits), constituencies.
- **Value:** H — infrastructure intelligence; pairs with C&AG project audits.
- **Risk/caveats:** cost **bands/estimates**, never spend; status is self-reported.
- **Difficulty:** M–H.

#### B.4 — HSE Section 38/39 funding classification — **P1** — `candidate`
- **Owner:** HSE. **Verified 2026-06-28:** the `non-statutory-sector/` *index* 404s, but the live data pages are `hse.ie/eng/services/publications/non-statutory-sector/section-38-documentation.html` + `.../section-39-documentation.html` + the 2024 §38/§39 pilot documentation + the per-service **funding schedules** (e.g. `section-38-non-acute-disability-schedules-2024…doc`, the actual per-org amounts). Context (verified): ~**€6.4bn to ~2,100 orgs in 2024** (~¼ of HSE budget); the C&AG / Irish Times (2024-10-24) flagged ~€1bn not under an appropriate contract — a direct hook to A.1/A.3.
- **Records:** organisation; §38/§39 classification; funding amount; service area; county; year; HSE area; source document.
- **Join keys:** `org_norm`, `charity_rcn`, `cro_number`, `county`, `year`.
- **Joins to:** charities, CRO, lobbying orgs, **the built HSE payments fact**, health policy.
- **Value:** M — major voluntary-sector funding channel; "funded then lobbying" pattern.
- **Risk/caveats:** §38 vs §39 matters (relationship type); naming varies between HSE publications; parent vs sub-org conflation.
- **Difficulty:** H (PDF-bound).

#### B.5 — HEA funding/statistics — **P2** — `candidate`
- **Owner:** Higher Education Authority (`hea.ie/statistics/`).
- **Records:** institution; funding stream; amount; student numbers; performance indicators; programme; year.
- **Join keys:** `institution`/`org_norm`, `year`.
- **Joins to:** constituencies (institution location), gov-finance education function.
- **Value:** M (education-sector context). **Difficulty:** M.

#### B.6 — Sport Ireland / Arts Council grants — **P2** — `candidate`
- **Owner:** Sport Ireland (`sportireland.ie`), Arts Council (`artscouncil.ie/funding-decisions/`).
- **Records:** recipient; grant scheme; amount; year; county; purpose; sector.
- **Join keys:** `org_norm`/`charity_rcn`, `county`, `year`.
- **Joins to:** charities, constituencies, ministerial announcements.
- **Value:** M (constituency funding distribution). **Caveat:** multi-year double-count risk. **Difficulty:** M.

#### B.7 — EU Cohesion / Structural / CAP funds — **P2** — `candidate`
- **Owner:** EU + Irish managing authorities (`cohesiondata.ec.europa.eu`, CAP beneficiary publications).
- **Records:** programme; fund; beneficiary; region; amount; project; year; managing authority.
- **Join keys:** `beneficiary`/`cro_number`, `region`/`county`, `year`.
- **Joins to:** CRO, constituencies, **adjacent to the built EU-TAM state-aid view** (do not merge).
- **Value:** M. **Caveat:** CAP beneficiary publication has its own privacy framework; beneficiary→CRO resolution non-trivial. **Difficulty:** M.

### C. Election & political context

#### C.1 — Election results (Electoral Commission / count data) — **P0** — `candidate`
- **Owner:** An Coimisiún Toghcháin / Electoral Commission (`electoralcommission.ie/general-elections/`). **Caveat:** the official site is thin on structured count-level downloads; count-by-count data lives with RTÉ, Irish Times, `electionsireland.org`, `irelandelection.com`, and the Oireachtas.
- **Records:** election event; constituency; candidate; party; first-preference votes; count-by-count transfers where available; elected/eliminated; turnout; quota; margin; seat count; boundary context.
- **Format:** mix — official PDFs/spreadsheets + third-party HTML/CSV.
- **Cadence:** per election (irregular).
- **Join keys:** `election_event`, `constituency`, `candidate_name`→`member unique_member_code` (normalised), `party`, `year`.
- **Joins to:** members, constituency, party, **SIPO election finance** (spend-per-vote, seat-safety).
- **Value:** H — completes the political-finance story; the most-requested missing political-context layer.
- **Risk/caveats:** boundaries change between elections (carry `boundary_review`); candidate name spelling varies; a third-party primary source is a **continuity risk** — prefer official + cross-check. SIPO finance ≠ results — keep them in separate facts.
- **Difficulty:** L–M.

#### C.2 — EC boundaries / register / political advertising — **P2** — `partial`
- **Owner:** Electoral Commission. Boundaries/population already partly built (`ec_constituency_pop_2022`).
- **Records:** boundary review; constituency changes; electoral-register statistics; political-advertising rules/records if public; referendum/election publications.
- **Value:** L–M (context). **Difficulty:** L (boundaries) / M (advertising). **Priority:** P2.

#### C.3 — Referendum campaign finance — **P2** — `candidate`
- **Owner:** SIPO (`sipo.ie/en/collection/9f7db-publications`).
- **Records:** campaign group; referendum; donations; expenses; donors where disclosed; source PDF.
- **Join keys:** `campaign_group`, `referendum_event`.
- **Joins to:** new `dim_campaign_group`; loose to members via stated endorsements (manual).
- **Value:** L–M. **Caveat:** small groups miss thresholds; group↔TD link rarely explicit. **Difficulty:** M.

### D. Transparency & legal/regulatory

#### D.1 — OIC / FOI decisions — **P0** — `candidate`
- **Owner:** Office of the Information Commissioner (`oic.ie/decisions/`). **Verified 2026-06-28:** searchable DB, **3,434 decisions**, fields = case title, summary, decision date, case number, public body, FOI Act + section; sortable by published date; latest decisions June 2026.
- **Records:** public body; decision date; case reference; FOI Act sections; topic; outcome; requester type if available; exemption grounds; public-interest findings; source URL.
- **Format:** clean HTML DB (paginated, faceted).
- **Cadence:** continuous; poll by published-date sort.
- **Join keys:** `public_body_id`/`org_norm`, `case_reference`, `decision_date`, `foi_section`.
- **Joins to:** public bodies, departments, procurement/grants/health/housing/policing contexts.
- **Value:** H — transparency-dispute surface; "where did a body refuse records?".
- **Risk/caveats:** a refused FOI request is **not** proof of wrongdoing; outcomes are legal findings about *access*, not about underlying conduct.
- **Difficulty:** M (the cleanest P0 to build).

#### D.2 — DPC decisions — **P2** — `candidate`
- **Owner:** Data Protection Commission (`dataprotection.ie/en/dpc-guidance/decisions`).
- **Records:** controller/processor; sector; decision date; GDPR/DPA articles; corrective measures; fine amount; topic; source decision.
- **Join keys:** `org_norm`/`cro_number`, `decision_date`, `sector`.
- **Joins to:** public bodies, CRO, lobbying orgs.
- **Value:** L–M (dominated by big-tech). **Difficulty:** L. **Priority:** P2 (P1 if bundled into a single "regulatory enforcement" mart).

#### D.3 — Central Bank enforcement actions — **DONE** — `implemented-not-api-exposed`
- **Status (verified 2026-06-28):** **already built** — `pipeline_sandbox/cbi_enforcement_extract.py` scrapes the `centralbank.ie` enforcement-actions hub (embedded `appData` JS array, 2007–2025) → promoted to **gold** `cbi_enforcement_actions.parquet` by `extractors/enrichment_promote_to_gold.py` → `v_corporate_cbi_enforcement` → Corporate Notices page. Tests in `test/sql_views/test_enrichment_views.py`. The older docs (`archive/ENRICHMENTS.md` E.5b / `ROADMAP_SOURCES.md`) calling this "candidate" are **stale**.
- **Scope built:** firm settlement/enforcement statements only; `fine_amount_eur` (NULL when older scans have no text layer — not zero), `value_safe_to_sum=False`. **Individuals dropped at promotion**; **prohibition / fitness-and-probity adverse-assessment notices are deliberately NOT ingested** (privacy — they name natural persons).
- **Remaining (optional) work — NOT a new build:** expose via `/v1/catalog` (currently UI-only); fold into `pipeline.py` (currently manual enrichment-promote); backfill older scanned statements (off-box OCR) for fine amounts. Owner: `enrichment_promote_to_gold` family.
- **Joins to:** the CBI authorisation substrate (which register a fined firm was on), Iris corporate notices (did it later enter distress?), lobbying.

#### D.4 — Corporate Enforcement Authority (ex-ODCE) actions — **P1** — `candidate`
- **Owner:** CEA (`cea.gov.ie/en-ie/`).
- **Records:** company; director/officer where public; prosecution/restriction/disqualification; investigation outcome; press statement; date.
- **Join keys:** `cro_number`/`org_norm`, `officer_name`, `date`.
- **Joins to:** CRO, corporate notices, RoMI directorships, suppliers.
- **Value:** M (small caseload, high signal). **Difficulty:** L.

#### D.5 — WRC / Labour Court decisions — **P2** — `candidate`
- **Owner:** WRC + Labour Court. **Verified 2026-06-28:** `workplacerelations.ie/en/cases/` is a **single search DB carrying BOTH WRC and Labour Court decisions** (plus historic tribunal data) — one extractor covers both; the separate `labourcourt.ie` search is redundant.
- **Records:** employer/respondent; complainant anonymisation; legislation; decision date; award amount; outcome; sector; public/private flag.
- **Join keys:** `respondent_org_norm`, `decision_date`, `sector`.
- **Joins to:** public bodies, suppliers.
- **Value:** L–M (repeat-respondent signal). **Caveat:** high volume; individual respondents → privacy. **Difficulty:** M.

#### D.6 — Coimisiún na Meán decisions / funding — **P2** — `candidate`
- **Owner:** Coimisiún na Meán (`cnam.ie`).
- **Records:** media service/platform; decision; complaint; code/standard breached; funding award; date.
- **Value:** L (new body, small back-catalogue). **Difficulty:** L.

> **Bundling note:** the remaining regulators (DPC / CEA / WRC / Labour Court /
> CnaM) are individually small. The efficient build is a single
> **`regulatory_enforcement_fact`** with a `regulator` discriminator, one
> extractor module per regulator, one shared schema — and the **already-built
> CBI enforcement gold (D.3) is the reference implementation** to generalise
> from (same firms-only privacy rule, same `value_safe_to_sum=False`). That makes
> the rest of the cluster a single P1 deliverable rather than a new pattern.

### E. Ownership, company, property & geospatial

#### E.1 — RBO beneficial ownership — **BLOCKED** — `blocked`
- **Owner:** Registrar of Beneficial Ownership (`rbo.gov.ie`).
- **Status:** access restricted since the Nov-2022 CJEU ruling; designated persons + legitimate-interest applicants only. **Do not ingest** without a documented lawful access + reuse route.
- **If lawful:** company; beneficial owner; ownership/control type; date; filing status; access basis.
- **Value:** H if accessible. **Priority:** blocked.

#### E.2 — Companies House UK — **P1** — `candidate`
- **Owner:** Companies House UK (`developer.company-information.service.gov.uk`). Free OGL REST API + bulk download (free API key). _Known-good public API; not re-fetched this pass — confirm key flow at build time._
- **Records:** UK company profile; officers; persons with significant control (PSC); filings; parent/subsidiary links; dissolution/insolvency status.
- **Join keys:** `company_name_norm`, `uk_company_number`; bridge to `cro_number` via name + officer overlap.
- **Joins to:** suppliers/lobbyists with UK parents, RoMI declared UK directorships, corporate notices.
- **Value:** M–H (cross-border resolution). **Caveat:** Anglo-Irish name overlap → match carefully (use officers + addresses, not name alone).
- **Difficulty:** L (free, well-documented API).

#### E.3 — Tailte Éireann / GeoHive boundaries & valuation — **P2** — `partial`/`blocked`
- **Boundaries (partial):** Tailte county/admin boundaries are scoped in `planning_corpus_seed.csv` PC27 (ArcGIS, CC-BY-4.0; last edit 2026-02-18). Cheap to promote for constituency/county spatial rollups.
- **Land Registry ownership (blocked):** paid per-search; do not bulk-ingest.
- **Join keys:** spatial (county/constituency), `eircode` (note Eircode→point is **licensed** ECAD/MapGenie — routing key only is free).
- **Value:** L–M. **Difficulty:** L (boundaries) / H (ownership). **Priority:** P2 (boundaries) / blocked (ownership).

#### E.4 — Property Price Register / vacant-derelict registers — **P2** — `partial`
- **Owner:** Property Services Regulatory Authority (`propertypriceregister.ie`); DHLGH derelict/vacant.
- **Status:** **derelict-sites *levy* is built** (`derelict_sites_levy_wide`); PPR transactions + the vacant-sites register are candidate.
- **Records:** property transaction; location; price; date; derelict/vacant status; local authority.
- **Join keys:** `local_authority`, `address`/`eircode` (loose), `date`.
- **Joins to:** RoMI declared properties (loose, by address), local government.
- **Value:** L–M. **Caveat:** address resolution is the bottleneck; PPR is residential-only. **Difficulty:** M.

### F. Local decision-making

#### F.1 — Council minutes / agendas — **P1 (promotion)** — `sandbox`
- **Owner:** 31 local authorities (per-council sites).
- **Status:** **sandbox corpus already exists** in `pipeline_sandbox/council_minutes/` (multiple councils, OCR'd). **Do not promote to gold/API without passing quality gates** (OCR confidence, council coverage floor, schema fixture tests).
- **Records:** council; meeting date; agenda item; motion; proposer/seconder; councillor attendance; named vote if available; decision; planning/material-contravention items; procurement/grant mentions; source PDF/HTML.
- **Join keys:** `local_authority`, `councillor_name`, `meeting_date`, `source_url`.
- **Joins to:** Your Councillors (Carlow roll-call), planning, LA accountability.
- **Value:** H (local accountability) but **maintenance-heavy** (31 schemas). **Caveat:** OCR/source quality varies heavily — keep sandbox until gates pass. **Difficulty:** H.

#### F.2 — Council named votes / material-contravention votes — **P2** — `sandbox`/`candidate`
- **Status:** `councillors_promote_to_gold` + Your Councillors page exist (Carlow only); `planning_corpus_seed.csv` PC26 (s.34(6)/s.140 material-contravention votes) is `not_started`.
- **Records:** councillor; motion; vote; meeting; topic; source.
- **Join keys:** `councillor_name`, `local_authority`, `meeting_date`.
- **Value:** M (local equivalent of vote tracking). **Difficulty:** H (depends on F.1). **Priority:** P2.

### G. Discovery & source-monitoring

#### G.1 — data.gov.ie metadata discovery (general monitor) — **P0** — `candidate`
- **Owner:** data.gov.ie (CKAN). **Verified 2026-06-28:** `https://data.gov.ie/api/3/action/package_search` returns `success:true`; each `result.results[]` carries `metadata_modified` and resources with a `format` field (total `result.count` not re-confirmed this pass — read it from the API at build time).
- **Records:** dataset title; publisher; resources; formats; licences; update frequency; `metadata_modified`; resource URLs; tags/themes; broken-resource status.
- **Format:** CKAN JSON API.
- **Cadence:** continuous; nightly `package_search` sweep + diff vs last snapshot.
- **Join keys:** `publisher`/`org_norm`, `dataset_id`, `resource_url`.
- **Joins to:** operational — feeds the candidate-source queue + link-rot detection across every existing source.
- **Value:** M (operational, not analytical) — source discovery + the missing automated link-rot watcher (`DATA_LIMITATIONS §12.1` notes hard-coded URL lists). High leverage for *maintenance*.
- **Risk/caveats:** this is a **monitoring layer**, not an analytical fact; it does not ingest the datasets, only their metadata. **Difficulty:** L.

#### G.2 — Oireachtas publications index — **P1 (operationalise)** — `partial`
- **Owner:** Oireachtas (`oireachtas.ie/en/publications/`, topic-faceted).
- **Status:** already used as a new-asset fallback in `pdf_endpoint_check.py`; no `dim_publication` mart.
- **Records:** publication title; date; committee/member/topic; document type; source PDF; publication category.
- **Caveat:** hard cap of 200 pages × 50 = 10,000 results — fine for new-asset detection, **not** for full historical backfill.
- **Value:** M (operational) + L–M (enrichment). **Difficulty:** L.

#### G.3 — Gov.ie publication / press-release discovery — **P1** — `candidate`
- **Owner:** gov.ie (`gov.ie/en/news/`, per-department publication pages).
- **Records:** department; publication title; category; date; document links; source body; topic tags.
- **Join keys:** `department`, `publication_id`, `date`.
- **Joins to:** the discovery route for grants (B.2), capital projects (B.3), annual reports, consultations; ministers/departments.
- **Value:** M — central discovery route. **Caveat:** press releases are *intent, not outcome* — easy to mis-cite (see `DATA_LIMITATIONS §14`). **Difficulty:** M.

---

## 4. Active-link table

Verified 2026-06-28 where marked **live (fetched)**. Rows marked **repo-audit**
inherit the status recorded in `archive/ENRICHMENTS.md`'s URL audit (re-check before
user-facing use). **Do not claim Wayback capture dates** — use the fallback
pattern only.

| Source | Active link | Link status | Last-updated signal | Wayback fallback | Notes |
|---|---|---|---|---|---|
| C&AG reports | `https://www.audit.gov.ie/en/find-report/publications/` | **live (fetched)** | Category + year index; 2026 reports present; no RSS | `https://web.archive.org/web/*/https://www.audit.gov.ie/en/find-report/publications/` | Poll index for new entries |
| C&AG Appropriation Accounts | same site, "Appropriation Accounts" category | **live (fetched)** | Year-filtered | as above | PDF tables |
| PAC | `https://www.oireachtas.ie/en/committees/` | repo-audit (working) | API + listing | `…/*/https://www.oireachtas.ie/en/committees/` | Meetings already ingested 2024-09+ |
| OGP central arrangements | `gov.ie/.../office-of-government-procurement/organisation-information/central-arrangements/` | **403 to bot** (human-valid 2026-06-28) | Schedule of upcoming arrangements | per-URL | **`ogp-frameworks` was the wrong path**; supplier lists gated in Buyer Zone/SupplyGov |
| Dept grant registers | per-dept `gov.ie` pages | repo-audit (varies) | per-page "last updated" | per-URL | No single endpoint |
| Capital tracker | `https://www.gov.ie/en/policy-information/96bb35-investment-projects-and-programmes-tracker/` | **live (search)** | NDP update Q3 2025 | `…/*/…96bb35-investment-projects-and-programmes-tracker/` | Replaces dead `9c6d5-capital-tracker` |
| Election results (official) | `https://www.electoralcommission.ie/general-elections/` | **live (search)** | Per election | `…/*/https://www.electoralcommission.ie/general-elections/` | Structured count data thin; see third parties |
| Election results (count data) | `https://electionsireland.org/` , `https://irelandelection.com/` | **403 to bot** (human-valid 2026-06-28) | Per election | per-URL | Single-maintainer continuity risk; official EC site has **no** count data |
| OIC/FOI decisions | `https://www.oic.ie/decisions/` | **live (fetched)** | 3,434 records; latest Jun 2026; sortable by published date | `…/*/https://www.oic.ie/decisions/` | Cleanest P0 |
| DPC decisions | `https://www.dataprotection.ie/en/dpc-guidance/decisions` | **live (fetched 2026-06-28)** | latest 2025-12-10 | per-URL | Structured + filterable (controller/articles/sector/fine) |
| CBI enforcement | `https://www.centralbank.ie/news-media/legal-notices/enforcement-actions` | repo-audit (working) | Continuous | per-URL | Hub JS-rendered; enumerate press releases |
| CEA | `https://cea.gov.ie/en-ie/` | **live (fetched 2026-06-28)** | News/Press Statements | per-URL | **non-`www`** only (TLS cert excludes `www.`) |
| WRC + Labour Court | `https://www.workplacerelations.ie/en/cases/` | **live (fetched 2026-06-28)** | Continuous | per-URL | **One DB for both** WRC + Labour Court |
| Labour Court | `https://www.labourcourt.ie/en/useful-information/guide-to-decisions-recommendations-search-facility/` | repo-audit (replacement) | Continuous | per-URL | Old `/en/cases/` 404 |
| Coimisiún na Meán | `https://www.cnam.ie/` | **live (fetched 2026-06-28)** | latest news 2026-06-25 | per-URL | Has Codes & legislation + funding schemes |
| RBO | `https://rbo.gov.ie/` | repo-audit (403/restricted) | — | per-URL | **Access-blocked** post-CJEU |
| Companies House UK | `https://developer.company-information.service.gov.uk/` | **live (fetched 2026-06-28)** | live REST API | n/a (API) | Free API + key; data under OGL |
| Tailte boundaries | ArcGIS `…/Counties___OSi_National_Statutory_Boundaries/FeatureServer/0` | from corpus registry | `lastEditDate` 2026-02-18 | n/a (API) | CC-BY-4.0 (PC27) |
| Property Price Register | `https://www.propertypriceregister.ie/` | repo-audit (TLS/403) | Continuous CSV | per-URL | Browser-verify |
| Council minutes | per-council sites | sandbox corpus | per-council | per-URL | Already in `pipeline_sandbox/council_minutes/` |
| data.gov.ie CKAN | `https://data.gov.ie/api/3/action/package_search` | **live (fetched)** | `success:true`; `metadata_modified` + resource `format` per dataset | n/a (API) | Discovery/monitor endpoint |
| Oireachtas publications | `https://www.oireachtas.ie/en/publications/` | repo-audit (working) | Continuous; 10k-result cap | per-URL | Operational, partial |
| Gov.ie news | `https://www.gov.ie/en/news/` | repo-audit (working) | Continuous | per-URL | Intent not outcome |
| HSE §38/39 | `hse.ie/.../non-statutory-sector/section-38-documentation.html` (+ `section-39-…`, schedules `.doc`) | **live (fetched 2026-06-28)** | Annual | per-URL | `non-statutory-sector/` index 404s — use the section pages |
| Sport Ireland / Arts Council / HEA | `sportireland.ie`, `artscouncil.ie/funding-decisions/`, `hea.ie/statistics/` | repo-audit (mixed) | Annual | per-URL | Per-scheme schemas |
| EU Cohesion / CAP | `https://cohesiondata.ec.europa.eu/` | repo-audit (working) | Annual | per-URL | CAP privacy framework |

**Caching mandate:** for every PDF/CSV/XLSX source, **cache the raw file to
bronze and store a SHA-256** (`source_document_hash`) at fetch time — the
project already does this for payments/NOAC/derelict-sites and it is the only
defence against silent upstream re-issue (`DATA_LIMITATIONS §12.1`).

---

## 5. Prioritisation framework

Scores are **1–5**. For the first five columns higher = better; for the last
four (difficulty, legal risk, privacy risk, maintenance) higher = **worse/more**.
Priority is the net judgment, not a raw sum.

| Source | Public value | BI value | Joinability | Stability | Update freq | Difficulty | Legal risk | Privacy risk | Maint. | **Priority** |
|---|---|---|---|---|---|---|---|---|---|---|
| C&AG audit reports | 5 | 4 | 5 | 5 | 2 | 4 | 1 | 1 | 3 | **P0** |
| C&AG Appropriation Accounts | 4 | 4 | 5 | 5 | 2 | 3 | 1 | 1 | 2 | **P0** |
| OGP central arrangements | 4 | 5 | 5 | 4 | 4 | 3 | 2 | 1 | 3 | **P0** |
| Department grant registers | 5 | 4 | 5 | 3 | 3 | 5 | 1 | 2 | 4 | **P0** |
| Election results | 5 | 3 | 5 | 4 | 1 | 2 | 1 | 1 | 2 | **P0** |
| OIC / FOI decisions | 4 | 3 | 4 | 4 | 5 | 3 | 2 | 2 | 2 | **P0** |
| data.gov.ie metadata monitor | 3 | 3 | 3 | 4 | 5 | 2 | 1 | 1 | 2 | **P0** |
| PAC reports/transcripts | 4 | 3 | 4 | 5 | 3 | 2 | 1 | 1 | 2 | **P1** |
| Capital projects tracker | 4 | 4 | 4 | 3 | 2 | 4 | 1 | 1 | 3 | **P1** |
| Regulatory enforcement (DPC/CEA/WRC) — *CBI already built* | 4 | 3 | 4 | 4 | 4 | 3 | 3 | 4 | 3 | **P1** |
| Companies House UK | 3 | 4 | 4 | 5 | 5 | 2 | 1 | 2 | 2 | **P1** |
| HSE Section 38/39 | 4 | 3 | 4 | 3 | 2 | 5 | 1 | 2 | 4 | **P1** |
| Council minutes (promotion) | 4 | 2 | 3 | 2 | 4 | 5 | 1 | 3 | 5 | **P1** |
| Oireachtas publications index | 3 | 2 | 3 | 5 | 5 | 2 | 1 | 1 | 2 | **P1** |
| Gov.ie publication discovery | 3 | 3 | 4 | 4 | 5 | 3 | 1 | 1 | 3 | **P1** |
| Sport Ireland / Arts Council / HEA | 3 | 3 | 4 | 4 | 2 | 3 | 1 | 1 | 3 | **P2** |
| EU Cohesion / CAP | 3 | 3 | 4 | 4 | 2 | 4 | 2 | 3 | 3 | **P2** |
| Tailte / property / valuation | 3 | 3 | 4 | 5 | 2 | 3 | 3 | 2 | 2 | **P2** |
| Property Price Register | 3 | 2 | 3 | 4 | 4 | 3 | 2 | 3 | 3 | **P2** |
| Coimisiún na Meán | 2 | 2 | 3 | 3 | 3 | 2 | 1 | 2 | 2 | **P2** |
| Referendum campaign finance | 3 | 2 | 3 | 4 | 1 | 3 | 1 | 2 | 2 | **P2** |
| EC register / political advertising | 3 | 2 | 4 | 4 | 2 | 3 | 1 | 1 | 2 | **P2** |
| Council named votes / material-contravention | 3 | 2 | 3 | 2 | 3 | 5 | 1 | 2 | 5 | **P2** |
| RBO beneficial ownership | 5 | 4 | 5 | 3 | 3 | 5 | 5 | 5 | 4 | **BLOCKED** |
| Land Registry ownership (paid) | 3 | 3 | 4 | 5 | 2 | 5 | 4 | 3 | 3 | **BLOCKED** |
| TD social media (G.4) | 2 | 1 | 2 | 1 | 5 | 4 | 4 | 3 | 5 | **NOT-RECOMMENDED** |
| News archive bulk ingest (G.5) | 3 | 2 | 3 | 2 | 5 | 5 | 5 | 3 | 5 | **NOT-RECOMMENDED** (keep manual) |

**Priority bands**

- **P0 (add next):** C&AG audit reports (+ Appropriation Accounts) · OGP central arrangements · department grant registers · election results · OIC/FOI decisions · data.gov.ie metadata monitor.
- **P1 (high value, planned):** PAC reports · capital projects tracker · regulatory-enforcement mart (DPC/CEA/WRC — *CBI enforcement already built*) · Companies House UK · HSE §38/39 · council-minutes promotion · Oireachtas publications index · gov.ie publication discovery.
- **P2 (useful, later):** Sport Ireland/Arts Council/HEA · EU Cohesion/CAP · Tailte/property/valuation · Property Price Register · Coimisiún na Meán · referendum finance · EC register/advertising · council named votes.
- **Blocked:** RBO · Land Registry ownership.
- **Not-recommended:** bulk social-media · automated news-archive ingest (keep the existing per-member Google-News RSS feed + a manual curation table).

---

## 6. P0 implementation plans

All P0 builds follow the project's existing pattern: **sandbox script first →
fixture test → promote to a `pipeline.py` chain** (`archive/ENRICHMENTS.md` graduation
steps 1–6 are non-negotiable). All money figures carry `value_kind` +
`value_safe_to_sum`. All writes use the atomic zstd+stats `save_parquet` helper
with a row-floor guard.

### P0-1 — C&AG audit reports (+ Appropriation Accounts)

- **Sandbox:** `pipeline_sandbox/cag/cag_reports_extract.py`.
- **Retrieve:** scrape `audit.gov.ie` publications index (categories: Special Reports, Report on the Accounts of the Public Services, Appropriation Accounts) → enumerate report pages → cache each PDF to `data/bronze/cag/` with SHA-256.
- **Parse:** title/type/date/year/Vote/department from the report page metadata (reliable); findings/recommendations from PDF text (curate or light-NLP, `confidence` flagged). Appropriation Accounts: Camelot/fitz table parse, fidelity-gated like NOAC.
- **Silver → gold:** `cag_reports` (one row per report) + `cag_appropriation_accounts` (Vote × year). Bridge table `cag_report_body` (report ↔ audited body).
- **Join keys:** `department`, `public_body_id`/`org_norm`, `vote`, `year`, `report_id`.
- **Cadence/poll:** weekly index poll; new `report_id` → fetch.
- **Tests:** fixture PDF per report type; row-floor; `safe_to_sum=false` on all narrative cost figures.
- **Difficulty:** M–H. **Gate to gold:** ≥1 fixture per report type passing.

### P0-2 — OGP central arrangements / frameworks catalogue

- **Sandbox:** `pipeline_sandbox/procurement_frameworks/ogp_frameworks_extract.py`.
- **Retrieve:** scrape the OGP **Central Arrangements** listing at `gov.ie/.../office-of-government-procurement/organisation-information/central-arrangements/` (browser-like session — gov.ie 403s bots); **public catalogue fields only** — do **not** touch Buyer-Zone/SupplyGov-gated supplier lists.
- **Silver → gold:** `procurement_frameworks` (arrangement_id, name, cpb, category, type [framework/DPS/panel], lot, start, expiry, eligibility, route-to-market, source_url). Where a supplier list *is* public, a child `procurement_framework_suppliers` with `supplier_norm`.
- **Join keys:** `cpv`/category, `supplier_norm`, `cpb`, `arrangement_id`.
- **Joins:** to `procurement_awards` (place-on-framework vs call-off award) and `v_procurement_supplier_summary`.
- **Value-add:** expiry/renewal alerting — `days_to_expiry` derived column.
- **Difficulty:** M. **Caveat baked in:** a framework place is not an award is not spend.

### P0-3 — Department grant registers

- **Sandbox:** `pipeline_sandbox/grants/grant_registers_extract.py` — **start with 3–5 departments** with the cleanest registers, not all.
- **Retrieve:** per-department `gov.ie` publication pages; cache each file (XLSX/CSV/PDF) to bronze + hash.
- **Silver → gold:** `grant_disbursements` (department, scheme, recipient, recipient_type, amount, currency, year, county/constituency, purpose, grant_basis [discretionary|statutory], programme, source_url, value_kind='grant_disbursed', value_safe_to_sum per-scheme only).
- **Join keys:** `org_norm`/`charity_rcn`/`cro_number`, `department`, `county`/`constituency`, `year`.
- **Joins:** charities, CRO, constituencies, lobbying, ministerial diaries.
- **Difficulty:** H (schema sprawl). **Gate:** per-department fixture + a `grant_basis` flag so statutory entitlements aren't read as discretionary largesse.

### P0-4 — Election results

- **Sandbox:** `pipeline_sandbox/elections/election_results_extract.py`.
- **Retrieve:** official Electoral Commission publications where structured; cross-check/supplement with a documented third-party (note continuity risk in provenance). Start with GE2024 + GE2020 to match existing SIPO finance coverage.
- **Silver → gold:** `election_results` (election_event, constituency, candidate_name, candidate_norm, party, count_number, first_pref, transfers, status [elected|eliminated], final_count, quota, turnout, seats, boundary_review, source_url).
- **Join keys:** `election_event`, `constituency`, `candidate_norm`→`unique_member_code`, `party`.
- **Joins:** SIPO finance (spend-per-vote — **separate facts, joined not summed**), members, constituency, party.
- **Difficulty:** L–M. **Gate:** name-resolution match-rate report vs `dim_member`; boundary metadata mandatory.

### P0-5 — OIC / FOI decisions

- **Sandbox:** `pipeline_sandbox/foi/oic_decisions_extract.py` (the cleanest P0).
- **Retrieve:** `oic.ie/decisions/` HTML DB, paginate by published-date sort; incremental since last `decision_date`.
- **Silver → gold:** `foi_decisions` (case_reference, public_body, org_norm, decision_date, foi_act, foi_sections[], topic, outcome, requester_type, exemption_grounds, public_interest_finding, source_url).
- **Join keys:** `public_body_id`/`org_norm`, `case_reference`, `decision_date`, `foi_section`.
- **Joins:** public bodies, departments, procurement/grants/health/housing contexts.
- **Difficulty:** M. **Gate:** outcome taxonomy fixed; copy carries "access dispute ≠ wrongdoing".

### P0-6 — data.gov.ie metadata monitor

- **Sandbox:** `tools/datagov_monitor.py` (a *monitoring* tool, not a data chain — sits alongside `source_health`).
- **Retrieve:** nightly `package_search` sweep (CKAN; page through `result.count`); snapshot `dataset_id`, `publisher`, `metadata_modified`, resource URLs + formats + licences.
- **Output:** `data/_meta/datagov_catalogue.json` + a diff report (new datasets, changed `metadata_modified`, dead resource URLs). Feeds the candidate-source queue and the link-rot watcher the `DATA_LIMITATIONS §12.1` hard-coded-URL problem needs.
- **Join keys:** `publisher`/`org_norm`, `dataset_id`, `resource_url`.
- **Difficulty:** L. **Note:** ingests **metadata only**, never the datasets — clearly an operational layer, not an analytical fact.

---

## 7. Data model / schema recommendations

**Medallion (existing convention):** `bronze` (raw cache) → `silver` (normalised)
→ `gold` (app/API-facing) → registered DuckDB `v_*` view → page/API. Polars for
ETL, pandas only in the UI layer.

**Provenance field set — add to every new silver/gold table:**

| Field | Purpose |
|---|---|
| `source_url` | the page/file the row came from |
| `source_document_hash` | SHA-256 of the cached bronze file (silent-reissue defence) |
| `fetched_at` | when we pulled it (UTC) |
| `source_published_date` | upstream publication date |
| `source_last_modified` | HTTP Last-Modified / CKAN `metadata_modified` / ArcGIS `lastEditDate` |
| `extraction_method` | `api` / `html_scrape` / `pdf_text` / `pdf_table_camelot` / `ocr` / `manual` |
| `confidence` | extraction confidence (esp. OCR/PDF/name-resolution) |
| `privacy_tier` | `public` / `restricted` / `quarantined` (drives the public_display gate) |
| `value_kind` | for money rows: `audited_outturn` / `grant_disbursed` / `framework_ceiling` / `cost_estimate` / `award_value` / `payment_actual` / `penalty` … |
| `value_safe_to_sum` | boolean — only `true` rows may be totalled, never across `value_kind` |

**Per-source gold tables (proposed names):**

- `cag_reports`, `cag_appropriation_accounts`, `cag_report_body`
- `procurement_frameworks`, `procurement_framework_suppliers`
- `grant_disbursements`
- `election_results` (+ derived `v_spend_per_vote` joining SIPO finance)
- `foi_decisions`
- `regulatory_enforcement_fact` (regulator discriminator: CBI/DPC/CEA/WRC/Labour Court/CnaM)
- `capital_projects`
- `companies_house_uk` (+ `company_xref_ie_uk` bridge)
- `hse_funded_orgs` (§38/§39)
- `oireachtas_publications` (`dim_publication`)
- `data/_meta/datagov_catalogue.json` (monitor, not a parquet fact)

**Money-grain rule (hard):** none of `audited_outturn`, `grant_disbursed`,
`framework_ceiling`, `cost_estimate`, `award_value`, `payment_actual`, `penalty`
may be unioned or summed with another. They meet only at the **supplier/org or
department spine**, joined never blended (`DATA_GRAINS.md`, `DATA_LIMITATIONS
§16–17`).

---

## 8. Join keys to use

Reuse the project's existing normalisers (NFKD accent-fold; do **not** invent
matching — `CLAUDE.md`):

`org_norm` (organisation normalised name) · `cro_number` · `charity_rcn` ·
`public_body_id` · `supplier_norm` · `department` · `local_authority` ·
`constituency` · `county` · `cpv` · `election_event` · `candidate_norm` ·
`unique_member_code` (the member join key) · `year` / `date` · `source_url` ·
`report_id` · `case_reference` · `uk_company_number` (Companies House) ·
`arrangement_id` (frameworks) · `vote` (Appropriation Accounts).

Bridge tables (not unions) for cross-jurisdiction (`company_xref_ie_uk`) and
audit→body (`cag_report_body`).

---

## 9. Legal / privacy / licensing caveats

- **RBO is blocked** — restricted access since the Nov-2022 CJEU ruling. Do not ingest without a documented lawful access + reuse route.
- **Land Registry / Tailte ownership** is paid per-search and licence-restricted; only the **CC-BY-4.0 boundary** layers are open. Eircode→coordinate is **licensed** (ECAD/MapGenie).
- **CAP beneficiary** data has its own EU privacy framework; **WRC/Labour Court** and **CBI F&P prohibition** notices name **individuals** — privacy-tier and apply the existing personal-data quarantine (`public_display=False`), as payments/SIPO already do.
- **Revenue tax-defaulters** (not in P0–P2): treat as an *evidence pointer*, never an auto-link — high reputational/legal sensitivity.
- **PPR addresses** are PII-adjacent; never expose raw address joins to RoMI without review.
- **Licence is per-source:** "open public record" is a status, not a redistribution permission. Confirm each licence at ingest and record it (`source_licence` column). data.gov.ie CKAN exposes `licence_id` per dataset — capture it.
- **Iris Oifigiúil** content is Crown copyright — facts only, as already handled.
- API output remains **CC-BY-4.0**; the `/v1/catalog` is deliberately curated — **do not auto-expose** any new PII-bearing table (FOI requester identities, grant recipient individuals, enforcement-named persons).

---

## 10. Suggested GitHub issues

Copy-paste stubs (one per P0/P1). Labels assume the repo's existing scheme.

**P0**

- `feat(sources): C&AG audit reports + Appropriation Accounts extractor` — labels: `source`, `procurement-adjacent`, `P0`. AC: `cag_reports` + `cag_appropriation_accounts` gold; fixture per report type; `safe_to_sum=false` on narrative costs; weekly index poll; no Wayback-date claims.
- `feat(sources): OGP central arrangements / frameworks catalogue` — labels: `source`, `procurement`, `P0`. AC: `procurement_frameworks` gold from the public OGP **Central Arrangements** catalogue (`gov.ie/.../organisation-information/central-arrangements/` — **not** the non-existent `ogp-frameworks` path); no Buyer-Zone scraping; `days_to_expiry`; joins to `procurement_awards`; "place ≠ award ≠ spend" caveat in view header.
- `feat(sources): department grant registers (3–5 pilot departments)` — labels: `source`, `money`, `P0`. AC: `grant_disbursements` gold; `grant_basis` flag; per-dept fixture; never summed with awards/payments.
- `feat(sources): election results fact (GE2024 + GE2020)` — labels: `source`, `political-finance`, `P0`. AC: `election_results` gold; `candidate_norm`→`unique_member_code` match-rate report; boundary metadata; `v_spend_per_vote` joins SIPO finance without summing.
- `feat(sources): OIC/FOI decisions extractor` — labels: `source`, `accountability`, `P0`. AC: `foi_decisions` gold from `oic.ie/decisions/`; incremental since last date; outcome taxonomy; "access dispute ≠ wrongdoing" copy.
- `feat(monitor): data.gov.ie CKAN metadata monitor + link-rot watcher` — labels: `infra`, `monitoring`, `P0`. AC: nightly `package_search` snapshot → `datagov_catalogue.json` + diff report; dead-resource detection feeds `source_health`; metadata-only.

**P1**

- `feat(sources): PAC reports + C&AG cross-reference (extend committee_evidence)` — labels: `source`, `committees`, `P1`. AC: dedupe by debate ID; link PAC reports to `cag_reports`.
- `feat(sources): capital projects / investment tracker` — labels: `source`, `infrastructure`, `P1`. AC: `capital_projects` gold; cost = estimate/band, never spend.
- `feat(sources): regulatory_enforcement_fact (DPC/CEA/WRC/Labour Court/CnaM)` — labels: `source`, `regulatory`, `P1`. AC: one schema, `regulator` discriminator, per-regulator module; **generalise from the existing CBI enforcement gold** (`cbi_enforcement_actions` / `v_corporate_cbi_enforcement`); firms-only privacy rule; `value_safe_to_sum=False`; "enforcement ≠ unrelated wrongdoing" caveat. *(CBI is already done — do not rebuild it.)*
- `feat(sources): Companies House UK (OGL REST API)` — labels: `source`, `corporate`, `P1`. AC: `companies_house_uk` + `company_xref_ie_uk` bridge; match on officers+address, not name alone.
- `feat(sources): HSE Section 38/39 funded-organisation classification` — labels: `source`, `health`, `P1`. AC: `hse_funded_orgs` with §38/§39 flag; joins charities/CRO.
- `chore(sources): promote council-minutes sandbox to gold (quality gates)` — labels: `source`, `local-gov`, `P1`. AC: OCR-confidence + coverage-floor + fixture gates **before** any gold/API; stays sandbox until all pass.
- `feat(sources): Oireachtas publications index → dim_publication` — labels: `source`, `discovery`, `P1`. AC: operationalise `pdf_endpoint_check.py` usage; document the 10k-result cap (no full backfill claim).
- `feat(sources): gov.ie publication/press-release discovery` — labels: `source`, `discovery`, `P1`. AC: feeds grants/capital discovery; "intent ≠ outcome" caveat.

---

## 11. Suggested ingestion order

1. **OIC/FOI decisions** (P0, cleanest build — proves the pattern end-to-end).
2. **data.gov.ie monitor** (P0, low-effort, immediately pays back as link-rot watcher).
3. **OGP central arrangements** (P0, directly feeds the commercial procurement product).
4. **Election results** (P0, completes the already-built SIPO finance story).
5. **C&AG audit reports + Appropriation Accounts** (P0, the audit spine; PDF work).
6. **Department grant registers** (P0, highest value but hardest — pilot departments first).
7. Then P1 in this order: PAC extension → Companies House UK → regulatory-enforcement mart → capital tracker → Oireachtas publications index → gov.ie discovery → HSE §38/39 → council-minutes promotion.
8. P2 as specific use-cases demand. **RBO / Land Registry ownership remain blocked.**

---

## 12. What not to claim until implemented

These warnings are mandatory wherever this roadmap is quoted (README, app copy,
methodology, marketing):

- **Do not claim candidate sources are implemented.** Everything in §3 is `candidate`/`partial`/`sandbox`/`blocked` unless a chain + output is cited in `SOURCES.md`.
- **Do not claim C&AG audit reports are implemented** — no extractor/output/API route exists yet.
- **Do not confuse LA AFS / NOAC with the C&AG.** AFS and NOAC are *council* financials (built); the C&AG is the *national* independent auditor (candidate).
- **Do not confuse SIPO election finance with election results.** SIPO finance is built; results are not. Spend ≠ outcome.
- **Do not confuse OGP central arrangements with eTenders/OGP awards.** Awards are built; the framework catalogue is not. A framework place ≠ an award ≠ spend.
- **Do not confuse specific CKAN/data.gov.ie/ArcGIS endpoints with a general data.gov.ie crawler.** Specific endpoints are wired (planning registry, OGP awards CSV); a general metadata monitor is not yet built.
- **Do not promote council minutes from sandbox without quality gates** (OCR confidence, coverage floor, fixture tests). The corpus exists in `pipeline_sandbox/council_minutes/`; it is not gold.
- **Do not combine incompatible money grains.** Audited outturn, grant disbursements, framework ceilings, cost estimates, award values, payments, and penalties are seven different things — never summed.
- **Do not imply lobbying caused a procurement outcome.** Co-occurrence only (`procurement_lobbying_overlap` is built on exactly this rule).
- **Do not imply a regulatory/enforcement/FOI record proves unrelated wrongdoing.** An FOI refusal, a fine, or a prohibition is a fact about that specific matter, not about anything else.
- **Do not expose personal/sensitive data without privacy review** — FOI requester identities, grant recipient individuals, enforcement-named persons, CAP/PPR PII all go through the existing quarantine before any public surface.

---

## 13. README / source-doc wording to add

Add to `README.MD` (and mirror the pointer in `SOURCES.md` / `ROADMAP_SOURCES.md`):

```markdown
### Source roadmap

Dáil Tracker's *implemented* sources are catalogued in
[`doc/SOURCES.md`](doc/SOURCES.md). Candidate sources we have **not** built are
scoped in [`doc/ROADMAP_SOURCES.md`](doc/ROADMAP_SOURCES.md), with build-ready
plans (schemas, join keys, priorities, GitHub issues) in
[`doc/NEW_SOURCE_INGESTION_PLAN.md`](doc/NEW_SOURCE_INGESTION_PLAN.md).

Nothing in the roadmap docs is a claim of implementation. In particular, C&AG
audit reports, OGP central frameworks, department grant registers, election
results, FOI/OIC decisions, regulatory-enforcement feeds, RBO, and Companies
House UK are **not yet ingested**. Where an adjacent component *is* built (SIPO
political finance, CBI authorisation registers **and CBI enforcement actions**,
public-body payments), that is flagged so the boundary is unambiguous. Do not combine different money grains
(audited outturn / grants / framework ceilings / award values / payments) — see
[`doc/DATA_GRAINS.md`](doc/DATA_GRAINS.md).
```

---

_Cross-references: `SOURCES.md` (implemented) · `ROADMAP_SOURCES.md` (candidate
cards) · `archive/ENRICHMENTS.md` (long-form scoping + URL audit) · `DATA_GRAINS.md`
(money-grain rules) · `DATA_LIMITATIONS.md` (§12 staleness, §14 interpretation,
§16–17 money grains) · `planning_rules/_corpus_registry/planning_corpus_seed.csv`
(the scoped geospatial endpoint registry — not a general crawler)._
