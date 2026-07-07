# SOURCES.md — implemented source catalogue

What Dáil Tracker actually ingests, links, and serves — derived from the
pipeline chains, the gold outputs, and the API catalogue, **not** from the
README or the enrichment wishlist.

**Source-of-truth hierarchy used to build this doc:**

1. [`pipeline.py`](../pipeline.py) chains — the strongest evidence of what the project tries to ingest or build.
2. [`api/routers/catalog.py`](../api/routers/catalog.py) — reviewed *public* API resources (curated, not an auto-dump).
3. [`data/_meta/output_baseline.json`](../data/_meta/output_baseline.json) — confirms actual parquet outputs and row counts.
4. Streamlit pages ([`utility/pages_code/`](../utility/pages_code/)) — app/product surfaces.
5. The README may be stale.
6. [`ENRICHMENTS.md`](ENRICHMENTS.md) is a *candidate* catalogue, not proof of implementation (see [`ROADMAP_SOURCES.md`](ROADMAP_SOURCES.md)).

---

## Source-status categories

| Status | Meaning |
|---|---|
| `implemented` | Pipeline chain and output exist. |
| `implemented-api-exposed` | Pipeline/output exists **and** a reviewed API resource exists. |
| `implemented-not-api-exposed` | Pipeline/output exists but is not directly exposed in the API (UI / views only). |
| `partial` | Some components implemented, but coverage or the chain is incomplete. |
| `sandbox` | Data exists in a sandbox/experimental area, not promoted to gold/API. |
| `manual-input` | Chain depends on a manual drop, OCR, or private/cached input (often no-op-safe). |
| `candidate` | Listed in enrichment docs / identified as useful, but no implementation found. |
| `not-found` | Searched, but no clear extractor/output/API route found. |
| `deprecated/broken` | An old link/source exists but should be replaced or re-verified. |

> **Note on the API.** The `/v1/catalog` resource is *deliberately curated, not an
> auto-dump of every view/parquet*: some underlying tables carry PII (SIPO donor
> addresses, personal insolvency) or are sandbox-only and must not be exposed.
> "Not API-exposed" therefore often means "intentionally withheld", not "missing".

---

## Implemented source catalogue

Columns: **Source / domain · Status · Chain / extractor · Output / view / API surface · Refresh mode · Main value · Main caveat.**

### Parliamentary / member data

| Source / domain | Status | Chain / extractor | Output / surface | Refresh | Main value | Main caveat |
|---|---|---|---|---|---|---|
| Oireachtas Open Data API — members, ministerial tenure, committees | `implemented-api-exposed` | `bootstrap`, `members` | `flattened_members`, `v_member_ministerial_tenure`, committees views; API `members` / `ministers` / `committees` | networked, overwrite | Canonical member dossiers + cabinet-on-a-date | Member-scoped to current Dáil/Seanad; turnover not auto-watched |
| Oireachtas API — legislation, questions, votes, debates | `implemented-api-exposed` | `legislation`, `bootstrap` | `current_dail_vote_history` (230k), `speeches_fact` (321k), `questions`; API `legislation` / `votes` | networked, overwrite | Bills, divisions, speeches, PQs | Some endpoints use fixed `limit`; pagination/truncation risk |
| data.oireachtas.ie PDFs — attendance | `implemented-not-api-exposed` | `attendance`, `seanad` | `attendance_by_td_year`, `seanad_attendance_by_year` | PDF poll | Plenary attendance | Plenary only; ≠ total work; PDF-brittle; hard-coded URL list |
| data.oireachtas.ie PDFs — allowances (PSA / T&A) | `implemented-api-exposed` | `payments`, `seanad` | `payments_full_psa` (12k); API `payments` | PDF poll | All-time T&A ranking | Derived totals; verify against PDF; hard-coded URL list |
| data.oireachtas.ie PDFs — Register of Members' Interests | `implemented-not-api-exposed` | `interests` | `member_interest_leaderboard`, member dossier | PDF poll | Property/shares/companies incl. former members | Office-holders partly outside RoMI; blank ≠ nil; heuristic landlord flags |
| Committee meeting history (witnesses, topics, transcripts) | `implemented-api-exposed` | `committee_evidence`, `committee_evidence_promote` | `v_committee_meetings`; API `committees` item | networked | Date · topics · witnesses · transcript link | Window starts 2024-09; re-fetches full window each run |
| Participation model (turnout, absence gaps, presence-vs-vote divergence, TAA) | `implemented-not-api-exposed` | `participation` | `v_attendance_participation_*`, `v_attendance_taa_compliance` | deterministic transform | Honest "showing up" model | Derives from attendance gold; inherits plenary-only caveat |
| Google News RSS — per-member news mentions | `implemented-not-api-exposed` | `news_mentions` | `news_mentions` silver, `v_member_news_mentions` | networked, **accumulates** | Discovery feed on member page | Name match ≠ article is about that person; coverage = Google indexing |
| Member contact details (oireachtas.ie profile scrape) | `implemented-not-api-exposed` | `member_contact` | `member_contact_details`, `v_member_contact_details` | networked | Office address / phone / email not in API | Honest NULLs on fetch failure |

### Lobbying / interests / entities

| Source / domain | Status | Chain / extractor | Output / surface | Refresh | Main value | Main caveat |
|---|---|---|---|---|---|---|
| Lobbying.ie returns, orgs, officials, policy areas | `implemented-api-exposed` / `manual-input` (acquisition) | `lobbying` | `top_lobbyist_organisations` (2.5k), `politician_policy_exposure` (52k), `bilateral_relationships` (68k); API `lobbying` | **manual CSV drop** | Who lobbied whom, on what | No live API; snapshot of dropped CSVs; `::`/`|` split risk; reliable ~2020+ |
| Revolving door (former office-holders now lobbying) | `implemented-api-exposed` | `lobbying` | `revolving_door_dpos` (1k); API `lobbying/revolving-door` | manual CSV | Former-DPO lobbying activity | Name-resolution dependent |
| CRO company register — supplier/notice matching | `implemented-not-api-exposed` | `cro`, within `lobbying`/`procurement` | `cro_xref_corporate_notices` (20k), `procurement_supplier_cro_match` (10k) | bulk export | Resolves "shares in X Ltd" / supplier → legal entity | Match ~46–61%; LLPs/truncated names miss |
| Charities Regulator — register + finances | `implemented-api-exposed` | `lobbying` (Tier-A) | `charities_enriched` (14k); API `charities` | XLSX | Charity finance + CRO/lobbying overlap | Figures as filed (filer errors); trustee names = sandbox |
| Procurement ↔ lobbying overlap | `implemented-api-exposed` | `procurement_lobbying` | `procurement_lobbying_overlap` (316); API `procurement/lobbying-overlap` | transform | Supplier-also-lobbyist leads | **Co-occurrence, never causation**; never sum awarded_value |

### Legislation and statutory instruments

| Source / domain | Status | Chain / extractor | Output / surface | Refresh | Main value | Main caveat |
|---|---|---|---|---|---|---|
| Bills + amendments + sponsors + sources | `implemented-api-exposed` | `legislation` | `v_legislation_index`, amendment-intensity views; API `legislation` | networked | Bill lifecycle dossiers | Multi-sponsor attribution care; debate→bill matching imperfect |
| Statutory instruments (Iris Oifigiúil) | `implemented-api-exposed` | `iris` | `statutory_instruments` (5,963); API `statutory-instruments` | PDF poll | SIs 2016+ with operation/domain/minister | 2016 floor; ~35.5% EU-derived |
| Bill → SI linkage | `implemented-not-api-exposed` | `iris`, `legislation` | `bill_statutory_instruments` (390); legislation dossier | transform | Act→SI chain | Act→SI only ~18%/5% (structural, not a matcher bug) |
| SI enrichment — current state, LRC/eISB classification | `partial` | `iris` | `si_current_state` (7,379), `si_lrc_enrichment_summary` (5,963) | transform | Operation / domain / current-state | `eisb_url` stamped at ingest, never re-resolved (revocation drift) |

### Iris / corporate / appointments

| Source / domain | Status | Chain / extractor | Output / surface | Refresh | Main value | Main caveat |
|---|---|---|---|---|---|---|
| Iris Oifigiúil — corporate distress/register notices | `implemented-api-exposed` | `iris` | `corporate_notices` (50,723); API `corporate/notices` | PDF poll | Receiverships, wind-ups, examinerships, SCARP | `entity_name` heuristic (~24% junk, filtered); corporate-only, no individuals |
| Receiver / appointer / operator-firm enrichment | `implemented-api-exposed` | `corporate_receiver` | `corporate_notices_enriched`, appointer/firm gold; API `corporate/receivers` | transform | Who appoints / runs receiverships | Brand→fund alias map hand-curated (~32% coverage); firm = notice-presence regex |
| CBI registers ↔ corporate notices | `partial` / `sandbox` substrate | `cbi` | `cbi_xref_corporate_notices` (228); API `corporate/repeat-distress` (experimental) | snapshot | Which wound-up firms were CBI-authorised | Substrate sandbox-grade; 2/59 registers fail; name+ref only |
| Public appointments (Iris notices) | `implemented-api-exposed` | `iris` | `public_appointments` (1,147); API `public-appointments` | PDF poll | State-board / public-appointment notices | One row per notice |
| State Boards register (DPER stateboards.ie) | `implemented-not-api-exposed` / `manual-input` (identities) | `stateboards` | `stateboards_roster` (2,070) | networked + curated CSV | Board membership universe (~250 boards) | Wikidata identities hand-curated; candidate generator un-wired |

### Procurement and public money

| Source / domain | Status | Chain / extractor | Output / surface | Refresh | Main value | Main caveat |
|---|---|---|---|---|---|---|
| eTenders / OGP awards | `implemented-api-exposed` | `procurement` | `procurement_awards` (62,763); API `procurement/suppliers` | networked CSV | Who wins public awards (2013–2026) | **Award value ≠ spend**; only `value_safe_to_sum` totals |
| Procurement supplier → CRO match | `implemented-not-api-exposed` | `procurement` | `procurement_supplier_cro_match` (9,979) | transform | Supplier → company status | ~61% of company-class suppliers |
| TED — Irish award notices | `partial` (silver) | `ted` | `procurement_ted_awards*`, `v_procurement_ted_*` | networked API/XML | EU-journal Irish awards | Silver (regenerable); winners 2024+ via API, 2016–23 via XML lane (~50% CRO) |
| TED — Irish competition/tender notices | `partial` (silver) | `ted_tenders` | `procurement_ted_tenders`; API `procurement` (open tenders) | networked API | Pre-award pipeline (what's out to tender) | A **third grain**, never summed; planned-value estimate only |
| Public-body PO/payment disclosures (>€20k) | `implemented-api-exposed` | `public_body_payments` → `procurement_consolidate` | `public_payments_fact` (sandbox) → `procurement_payments_fact` (gold, 423,989); API `public-body-payments` | networked, bronze-cached | Realised-spend grain across depts/agencies | Privacy-quarantined; coverage partial; extraction-derived floor |
| HSE / Tusla payments | `implemented-api-exposed` | `hse_tusla_payments` → consolidate | folded into `procurement_payments_fact` | cached FOI PDFs | HSE 2021–25 is **only surviving public copy** | Privacy_risk=high; HSE threshold €100k; VAT-inclusive |
| Disclosed national PO (BigQuery extract) | `manual-input` / `partial` | `disclosed_bq_po`, `disclosed_bq_po_newbodies` | `disclosed_bq_po_*_fact` silver → folded to gold | **manual drop**, no-op-safe | Recovers missing HSE history + new bodies | Phases 2/3 gated on fail-closed registry; exits 0 if drop absent |
| Local-authority PO/payments (>€20k, 31 LAs) | `partial` | `la_payments` → consolidate | `la_payments_fact` silver → gold | networked, bronze-cached | Council cash-PO disclosures | 20/31 councils parse clean (no OCR); listing-rot guarded |
| Procurement payment consolidation | `implemented-api-exposed` | `procurement_consolidate` | `procurement_payments_fact` (423,989) | transform | One app-facing payments gold | Carry-forward guard for vanished councils |
| Accommodation / IPAS spend | `implemented-not-api-exposed` | `procurement_dept_readingorder_parser` (promoted) | `dceidy_ipas_legacy_spend` (18k); Accommodation Spend page | transform | Asylum-accommodation spend lens | Dept-children PO = asylum (regressed out); read-order parser drops wrong-layout PDFs |
| EPA supplier compliance | `implemented-not-api-exposed` | `epa_promote_to_gold` (sandbox-promote) | `epa_supplier_compliance` (667); `procurement_epa_compliance.sql` | manual sandbox + promote | Environmental-licence/enforcement on suppliers | Capability register: SPENT ≠ AWARDED |
| ISIF portfolio / EU-TAM state aid | `implemented-not-api-exposed` | `enrichment_promote_to_gold` | `corporate_isif_portfolio.sql`, `procurement_eu_tam_state_aid.sql` | manual sandbox + promote | State investment / EU aid context | EU-TAM: use `aid_element_value` |

> **Procurement money rule (critical).** Do not treat eTenders/TED award values as
> actual spend. Do not sum awards, public-body payments, PO commitments, and
> audited expenditure together. See [`DATA_GRAINS.md`](DATA_GRAINS.md).

### Political finance / elections

> **Out-of-pipeline.** SIPO political finance is **not** a `pipeline.py` chain. It
> is produced by a dedicated extractor family run on a manual / OCR cadence
> (`sipo_candidate_ocr.py`, `sipo_*_paddle_etl.py`, `sipo_promote_to_gold.py`,
> `sipo_ge2020_promote.py`). The same is true of SSHA and EC constituency
> population — implemented, but ingested outside the main dispatcher.

| Source / domain | Status | Chain / extractor | Output / surface | Refresh | Main value | Main caveat |
|---|---|---|---|---|---|---|
| SIPO party donations | `implemented-api-exposed` | `sipo_donations_paddle_etl` → `sipo_promote_to_gold` | `sipo_donations` (74); API `political-finance/donations` | manual harvest + OCR | Declared party donations (money in) | Above €1,500 threshold only; donor addresses stripped at gold |
| GE2024 candidate expenses | `partial` / `implemented-api-exposed` | `sipo_candidate_expenses_crawl` → `sipo_candidate_ocr` → `_aggregate` | `sipo_candidate_expenses_fact` (473), `_items` (18k); API `political-finance/election-spend` | OCR | Per-candidate campaign spend | 473 of ~614 sourced; OCR caveats; coverage shown in-app |
| GE2024 party / national-agent spend | `implemented-not-api-exposed` | `sipo_expenses_paddle_etl`, `sipo_expense_items_paddle_etl` | `sipo_expenses_fact`, `sipo_campaign_spend_by_*`; Election Finance page | OCR | Party-level campaign spend | Under-counts central booking; non-additive with candidate spend |
| GE2020 party spending | `implemented-not-api-exposed` | `sipo_ge2020_promote` | `sipo_ge2020_expense_*`; Election Finance "Election 2020" tab | OCR | National-agent party expenses 2020 | **Separate fact, never unioned with GE2024**; some parties `reconciles=false` |
| **Actual election results** (counts, seats, turnout) | `not-found` / `candidate` | — | — | — | Seat safety / spend-per-vote context | **Not implemented.** Do not confuse with SIPO finance, which is. See [`ROADMAP_SOURCES.md`](ROADMAP_SOURCES.md) |

### Judiciary and courts

| Source / domain | Status | Chain / extractor | Output / surface | Refresh | Main value | Main caveat |
|---|---|---|---|---|---|---|
| Bench roster + appointments + elevations | `implemented-api-exposed` / `manual-input` (upstream) | `judiciary_bench` | `judiciary_bench` (194), `judiciary_appointments` (154); API `judiciary/appointments` | transform of **static** sandbox | Sitting bench + appointment ladder | Promotes a static 2026-06-04 sandbox; spine begins 2016; roster not deduped |
| Courts health (clearance, waiting) | `implemented-api-exposed` | `judiciary_bench` | `judiciary_courts_clearance` (741), `_waiting` (48); API `judiciary/courts-health` | transform | System throughput; names no judge | Aggregate-only; partial early years |
| Legal Diary (Courts Service) | `implemented-not-api-exposed` | `legal_diary_poller`, `legal_diary_extract`, `judiciary_diary_link` | `judicial_legal_diary_*` (incl. OpenView 786k) | **forward-accumulating, day-or-lost** | Daily court schedule, privacy-tiered | Run daily or lose history; District Court is a gap; 7-day window in UI |

> Judiciary is **sandbox-grade in origin** — validated once (2026-06-04) and
> promoted to gold by transform. Pulling *new* appointments still needs the
> sandbox refreshed from Iris. No fuzzy TD→judge matching (deliberately removed).

### Local government

| Source / domain | Status | Chain / extractor | Output / surface | Refresh | Main value | Main caveat |
|---|---|---|---|---|---|---|
| LA Annual Financial Statements (amalgamated) | `partial` | `afs` | spend-by-service-division silver; Council Spending page | networked PDF | Macro spend-by-service context | Amalgamated layer; per-LA AFS ~22/31 councils |
| NOAC revenue-collection rates (M2) | `implemented-not-api-exposed` | `noac_collection` | `noac_m2_collection_wide` (155) | git-tracked PDF | Commercial-rates / rent / housing-loan collection | PDF-only, fidelity-gated; can exceed 100% (arrears) |
| NOAC scorecard + history + full indicators | `implemented-not-api-exposed` | `noac_scorecard`, `_history`, `noac_indicators` | `noac_scorecard_wide`, `noac_scorecard_history`, `noac_indicators_long`; local_government page | git-tracked PDFs | 7 citizen-facing indicators + trends + ~125-series drill-down | Bar-chart-only figures not extractable (local OCR banned) |
| DHLGH derelict-sites levy | `implemented-not-api-exposed` | `derelict_sites` | `derelict_sites_levy_wide` (31) | cached XLSX | Per-LA levied/collected/outstanding | 2024-only; reconciled to file total |
| Planning-appeal overturn (council vs ACP) | `partial` | `planning_appeal_outcomes` | `planning_appeal_outcomes` silver | ArcGIS + committed silver | Authoritative ABP-overturn metric | 2016+; joins static planning silver (ingest not yet a chain) |
| Chief-executive roster | `manual-input` | (hand-curated CSV) | `data/_meta/la_chief_executives.csv` | manual | CE accountability | No API; CE salary national band only |
| Your Councillors / council named votes | `sandbox` | `council_minutes/` → `councillors_promote_to_gold` | sandbox; Your Councillors page (Carlow roll-call scoped) | manual scrape | Councillor roll-call | WIP; not a general feed |

### Planning / siting / ArcGIS / CKAN

| Source / domain | Status | Notes |
|---|---|---|
| National Planning Applications (ArcGIS REST) | `partial` | Committed `planning_applications_silver` is a **static input**; the national ingest is **not yet a chain**. |
| An Coimisiún Pleanála appeals (PC02) | `partial` | Fetched live by `planning_appeal_outcomes` (ArcGIS FeatureServer). |
| Siting Check (geospatial designation engine) | `implemented-not-api-exposed` (experimental) | Planning page; MCP `siting_check`. Deployment held on a `nearest()` lon-bug. |
| MyPlan zoning / NPWS designated areas + derogation / OPW flood zones / BCMS commencement (CKAN) / CSO planning permissions (PxStat) / Tailte boundaries | `partial` / `candidate` (scoped in registry) | Present in the planning corpus registry (`planning_corpus_seed.csv`). **This is a scoped registry of specific endpoints, not a general data.gov.ie crawler.** |

### CSO / denominators

| Source / domain | Status | Chain / extractor | Output / surface | Notes |
|---|---|---|---|---|
| CSO PxStat — housing / HAP | `implemented-not-api-exposed` | `cso` | `cso_hap*`, `cso_hsa07`, `cso_hpm*`, `cso_vac*` | Council-area grain (≠ constituency); vacancy is a metered-electricity proxy |
| CSO PxStat — government finance (GFA01/GFQ01/NA012) | `implemented-not-api-exposed` | `cso` | `cso_gfa01/gfq01/na012`, `v_gov_finance_annual` | National denominators; no dedicated page yet |
| CSO — population / constituency (PEA08, FY005) | `implemented-not-api-exposed` | `cso` | `cso_pea*`, `cso_fy005` | FY005 is the only natively constituency-keyed table (36/43 join clean) |
| Social-housing waiting list (Housing Agency SSHA) | `implemented-not-api-exposed` | (SSHA extract) | `ssha_a1_*` (Housing page) | Annual self-reported snapshot; net qualified need, not flow |
| Constituency population / boundaries (EC2023) | `implemented-not-api-exposed` | (EC extract) | `ec_constituency_pop_2022` (43) | 7 constituencies pending boundary-split spatial join |

### Monitoring / health

| Output | Status | Chain | Surface |
|---|---|---|---|
| Data-age / freshness signal | `implemented` | `freshness` | `data/_meta/freshness.json` → Streamlit badge + scheduled report |
| Source health (staleness / reachability) | `implemented` | `source_health` | `data/_meta/source_health.json` (links opt-in via `DAIL_CHECK_LINKS=1`) |
| Output regressions (row/column drop vs baseline) | `implemented` | `output_regressions` | `data/_meta/output_regressions.json` (CI `--strict` gates) |
| Atomic parquet writes + row-floor guards | `implemented` | (all writers) | zstd + statistics; refuses a thinned write |

---

## API resources

The `/v1/catalog` endpoint exposes a **curated public subset** (16 resources +
two meta endpoints). It is intentionally not every table/view: some carry PII or
need review before exposure.

| Resource | List endpoint | Item / extras | Count view |
|---|---|---|---|
| members | `/v1/members` | `/v1/members/{code}/dossier` | `v_member_registry` |
| legislation | `/v1/legislation` | `/v1/legislation/{bill_id}` | `v_legislation_index` |
| statutory-instruments | `/v1/statutory-instruments` | — | `v_statutory_instruments` |
| votes | `/v1/votes` | `/v1/votes/{vote_id}` | `v_vote_index` |
| payments | `/v1/payments` | — | `v_payments_alltime_ranking` |
| lobbying | `/v1/lobbying/organisations` | `/v1/lobbying/revolving-door` | `v_experimental_lobbying_org_index_enriched` |
| procurement | `/v1/procurement/suppliers` | `/suppliers/{norm}/dossier`, `/competition`, `/lobbying-overlap` | `v_procurement_supplier_summary` |
| committees | `/v1/committees` | `/v1/committees/{committee}` | `v_committee_member_detail` |
| ministers | `/v1/ministers` | `/v1/cabinet` | `v_member_ministerial_tenure` |
| ministerial-diaries | `/v1/ministerial/diary/organisations` | `/{name}`, `/meetings` | `v_ministerial_diary_meetings` |
| corporate | `/v1/corporate/notices` | `/repeat-distress`, `/receivers` | `v_corporate_notices` |
| political-finance | `/v1/political-finance/donations` | `/election-spend` | `v_sipo_donations` |
| judiciary | `/v1/judiciary/appointments` | `/courts-health` | `v_judiciary_appointments` |
| charities | `/v1/charities` | — | `v_charity_sector_totals_by_year` |
| public-body-payments | `/v1/public-body-payments` | — | `v_public_payments` |
| public-appointments | `/v1/public-appointments` | — | `v_public_appointments` |
| **meta** | `/v1/catalog` | `/v1/coverage` (scope, year ranges, money-grain rules) | — |
| **bulk** | exports router | parquet downloads (Phase 2, demand-gated) | — |

> The API is read-only, open (no key), versioned under `/v1`, served under
> **CC-BY-4.0**, with interactive docs at `/docs`. Routers are thin
> (parse → core → serialize); all retrieval lives in `dail_tracker_core`.

---

## Data products / app surfaces

The Streamlit app ships **27 pages** across **8 top-nav sections**:

| Section | Pages |
|---|---|
| **What They Own** | Register of Members' Interests (property / shares / companies, sitting + former) |
| **Your Area** | Constituencies, Your Council, Who Runs Your County, Your Councillors, Council Spending, Housing (SSHA) |
| **Members & Parliament** | Member Overview, Attendance, Votes, Committees |
| **The Money** | Payments, Election Finance (donations + GE2024/GE2020), Procurement, Follow the Money, Accommodation Spend, Public Payments, Companies |
| **Law & Records** | Legislation, Statutory Instruments, Corporate Notices, Courts & Judiciary |
| **Influence** | Lobbying, Who Ministers Meet (ministerial diaries), Appointments / State Boards |
| **Planning** | Siting Check (experimental geospatial engine) |
| **Glossary** | — |

Pages read **only** through registered DuckDB views (logic firewall:
`SELECT / WHERE / ORDER BY / LIMIT` only). All transformation lives in the
pipeline, SQL views, or gold layer. The same `dail_tracker_core` query layer
feeds both the dashboard and the API.

**Workflow-led directions** (the app is currently broad and dataset-led; it
should become more task-led): *search anything · supplier dossier · buyer
dossier · politician dossier · public-body dossier · company/charity dossier ·
money-claim verification · source coverage/freshness panel.* See
[`ROADMAP_SOURCES.md`](ROADMAP_SOURCES.md#ui--product-clarity).
